"""Lighter(Exchange) over core/lighter. What differs from Hyperliquid / Arcus and how it is handled here (the strategies
see none of it; the full list is in src/core/lighter/lighter.md):

- an order is a signed transaction: the reply only says "accepted" (like an Arcus ACK), the outcome comes on the
  account channels. A resting order is therefore reported ok at once and closed later by an order update ("rejected"
  when the engine cancelled it on arrival: post-only that would cross, margin...). An IOC is different: its real
  outcome is awaited (order update, at most `ioc_timeout`) so that `place` answers like Hyperliquid, with the filled
  size and the average price; if it does not come in time the result is ok with nothing filled, which the hedge
  already treats as "unknown, counted as filled until its fills come";
- a client id is a 48-bit integer, not a 128-bit hex string: the low 48 bits of the strategy's id are used (kept in a
  table to give the full id back on fills and updates);
- the order id given to the strategies is that client index (the exchange accepts it wherever it takes an order
  index, and it is known before the transaction is sent); an order placed outside the bot keeps its exchange index;
- a modify keeps the order (same id); on a partially filled order it is sent as cancel + new order in one batch
  (what `base_amount` means for a partly filled order is not documented);
- account state: positions and balances are pushed by the websocket; REST (heavy: weight 300) is only read at start,
  every `poll` seconds and when a caller asks, at most once per `account_interval`.
"""
import asyncio
import hashlib
import secrets
import time
from collections import OrderedDict
from decimal import Decimal
from adapters.exchange import (AccountListener, BBOListener, DepthListener, DisconnectListener, Exchange, FillListener,
                               OrderListener, PriceListener, PrintListener)
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Funding, FundingPayment, Instrument, Listing, MarginMode,
                             Modify, OpenOrder, Order, OrderUpdate, Portfolio, Position, Print, Result, Side, TIF, Volume)
from adapters.utils import SECONDS, aggregate, rounding, source
from core.lighter.client import LighterClient
from core.lighter.utils.constants import (CANCEL_ALL_ABORT, CANCEL_ALL_IMMEDIATE, CANCEL_ALL_SCHEDULED,
                                          EXPIRY_DAYS, MARGIN_CROSS, MARGIN_ISOLATED, MAX_CLIENT_INDEX, OPEN, REFUSED,
                                          TIF_GTT, TIF_IOC, TIF_POST_ONLY, TIMEFRAMES, TX_CREATE_ORDER, USDC_SCALE)
from core.lighter.utils.types import Market, number
from core.utils import call, cancel
from log.logger import Logger

TIFS = {TIF.GTC: TIF_GTT, TIF.ALO: TIF_POST_ONLY, TIF.IOC: TIF_IOC}
FROM_TIFS = {"good-till-time": TIF.GTC, "post-only": TIF.ALO, "immediate-or-cancel": TIF.IOC}
# fees of an account that stakes no LIT (docs: Account Types), used when the adapter is given none
TIERS = {"standard": (0.0, 0.0), "plus": (0.00005, 0.00005), "premium": (0.00004, 0.00028), "advanced": (0.00004, 0.00028)}
FREE = 10**9          # a cancel uses no quota


def leverage_of(fraction) -> float | None:
    """a position gives its initial margin fraction as "0.1" (fraction) or "10.00" (percent) depending on the endpoint"""
    v = number(fraction)
    if not v or v <= 0:
        return None
    return 1 / v if v <= 1 else 100 / v


def stamp(*values) -> float:
    """first timestamp given, in seconds whatever its unit (s, ms or µs)"""
    for v in values:
        x = number(v)
        if x:
            while x > 1e11:
                x /= 1000
            return x
    return time.time()


class Lighter(Exchange):
    name = "lighter"

    def __init__(self, client: LighterClient, maker_fee: float | None = None, taker_fee: float | None = None,
                 poll: float = 15.0, refresh: float = 60.0, transport: str = "rest", ioc_timeout: float = 4.0,
                 account_interval: float = 2.0):
        self.client = client
        self.messenger = client.messenger
        self.fee_options = (maker_fee, taker_fee)
        self.poll = poll
        self.refresh = refresh
        self.ioc_timeout = ioc_timeout
        self.account_interval = account_interval
        self._log = Logger("Lighter", client.messenger)
        self.account_index = client.account
        self.address = client.address
        client.exchange.transport = transport
        self.listeners: list[AccountListener] = []
        self.bbo_listeners: dict[str, list[BBOListener]] = {}
        self.bbos: dict[str, BBO] = {}
        self.fill_listeners: list[FillListener] = []
        self.order_listeners: list[OrderListener] = []
        self.oracle_listeners: dict[str, list[PriceListener]] = {}
        self.print_listeners: dict[str, list[PrintListener]] = {}
        self.depth_listeners: dict[str, list[tuple[DepthListener, int]]] = {}
        self.books: dict[str, dict] = {}
        self.tasks: list[asyncio.Task] = []
        self.running = False
        self.state = AccountState(self.name)
        self.default_leverages: dict[str, float] = {}   # market default of the symbols followed (see `defaults`)
        self.read_at = 0.0                              # last REST read of the account
        self.reading: asyncio.Task | None = None
        self.dirty = asyncio.Event()
        self.streams: dict[str, bool] = {}              # channel family -> subscribed (False: refused)
        self.cloids: OrderedDict[int, str] = OrderedDict()   # client order index -> the strategy's client id
        self.orders: dict[str, dict] = {}               # open orders by id, from the stream
        self.snapshots = 0                              # snapshots of the order channel received
        self.waiters: dict[int, asyncio.Future] = {}    # client order index of an IOC -> its final order row
        self.hashes: OrderedDict[str, tuple[int, str]] = OrderedDict()   # tx hash -> (client index, symbol) of a create
        self.confirmed: OrderedDict[int, bool] = OrderedDict()           # client indexes seen on the order channel
        self.replaced: set[str] = set()                 # orders we cancelled to re-place them (modify of a partial fill)
        self.seen: OrderedDict[str, float] = OrderedDict()
        self.latest = 0.0                               # time of the newest fill seen (s)
        self.stats: dict = {}                           # volumes of the account_all_trades snapshot
        self.volumes: tuple[float, Volume] | None = None

    # ==== lifecycle ====

    async def start(self):
        if self.running:
            return
        if not self.client.running:
            await self.client.start()
        self.running = True
        self.client.websocket.connect_listeners.append(self.reconnected)
        self.tasks.append(asyncio.create_task(self.watch_markets()))

    async def stop(self):
        self.running = False
        for task in self.tasks:
            await cancel(task)
        self.tasks.clear()
        for f in self.waiters.values():
            if not f.done():
                f.cancel()

    async def watch_markets(self):
        while self.running:
            await asyncio.sleep(self.refresh)
            try:
                await self.client.info.load_markets()
            except Exception as e:
                await self._log.warning(f"markets refresh failed: {e}", send_tg=False)

    async def reconnected(self):
        """the socket came back: every channel sends a fresh snapshot; what happened meanwhile is read by REST"""
        self.dirty.set()
        for book in self.books.values():
            book["nonce"] = None
        if self.fill_listeners and self.account_index is not None:
            self.tasks.append(asyncio.create_task(self.recover()))

    # ==== markets ====

    def market(self, symbol: str | int) -> Market:
        return self.client.info.market(symbol)

    @property
    def fees(self) -> tuple[float, float]:
        tier = next((v for k, v in TIERS.items() if k in (self.client.tier or "")), TIERS["premium"])
        maker, taker = self.fee_options
        return (tier[0] if maker is None else maker), (tier[1] if taker is None else taker)

    def instrument(self, symbol: str) -> Instrument:
        m = self.market(symbol)
        maker, taker = self.fees
        return Instrument(
            exchange=self.name, symbol=m.name, maker_fee=maker, taker_fee=taker,
            min_notional=float(m.min_quote), min_size=float(m.min_base),
            pricer=lambda px, side, passive: float(m.round_price(px, rounding(side, passive))),
            sizer=lambda sz: float(m.round_size(sz)),
            stepper=lambda px: float(m.tick),
            raw=m, max_leverage=m.max_leverage, maintenance=m.mmf, initial=m.min_imf, isolated=m.isolated)

    def tradable(self, symbol: str) -> bool:
        return self.market(symbol).online

    def listings(self) -> list[Listing]:
        return [Listing(m.name, m.name, "", "", m.online, m.volume) for m in self.client.info.by_id.values()]

    @property
    def connected(self) -> bool:
        return self.client.websocket.connected.is_set()

    def on_disconnect(self, listener: DisconnectListener) -> bool:
        self.client.websocket.disconnect_listeners.append(listener)
        return True

    @property
    def transport(self) -> str:
        return "ws" if self.client.exchange.streaming else "rest"

    def budget(self) -> dict[str, int] | None:
        """the volume quota: one unit per order placed or modified (and per cancel-all), a cancel is free. It is only
        enforced on Plus / Premium accounts (a Standard account is limited by its 60 requests per minute instead)"""
        quota = self.client.exchange.quota
        if quota is None or "standard" in (self.client.tier or ""):
            return None
        return {"order": quota, "cancel": FREE}

    # ==== market data ====

    async def subscribe_bbo(self, symbol: str, listener: BBOListener):
        m = self.market(symbol)
        if m.name in self.bbo_listeners:
            self.bbo_listeners[m.name].append(listener)
            if m.name in self.bbos:
                await call(listener, self.bbos[m.name])
            return
        self.bbo_listeners[m.name] = [listener]

        async def on_ticker(msg):
            t = msg.get("ticker") or {}
            bid, ask = t.get("b") or {}, t.get("a") or {}
            bbo = BBO(m.name, number(bid.get("price")), number(ask.get("price")), number(bid.get("size")),
                      number(ask.get("size")), stamp(t.get("last_updated_at"), msg.get("last_updated_at"), msg.get("timestamp")))
            self.bbos[m.name] = bbo
            for listener in self.bbo_listeners[m.name]:
                await call(listener, bbo)
        await self.client.websocket.subscribe(f"ticker/{m.id}", on_ticker)

    async def subscribe_trades(self, symbol: str, listener: PrintListener) -> bool:
        m = self.market(symbol)
        if m.name in self.print_listeners:
            self.print_listeners[m.name].append(listener)
            return True
        self.print_listeners[m.name] = [listener]

        async def on_trades(msg):
            if str(msg.get("type", "")).startswith("subscribed/"):
                return  # past trades
            now = time.time()
            for row in (msg.get("trades") or []) + (msg.get("liquidation_trades") or []):
                price, size = number(row.get("price")), number(row.get("size"))
                if not price or not size:
                    continue
                # the maker is the ask: the taker bought
                side = (Side.BUY if row["is_maker_ask"] else Side.SELL) if "is_maker_ask" in row else None
                maker_ask = bool(row.get("is_maker_ask"))
                ask, bid = row.get("ask_account_id"), row.get("bid_account_id")
                trade = Print(m.name, price, size, side, stamp(row.get("timestamp")), now,
                              str(ask if maker_ask else bid) if (ask if maker_ask else bid) is not None else None,
                              str(row.get("ask_id" if maker_ask else "bid_id") or "") or None,
                              str(bid if maker_ask else ask) if (bid if maker_ask else ask) is not None else None,
                              str(row.get("bid_id" if maker_ask else "ask_id") or "") or None,
                              str(row.get("trade_id")) if row.get("trade_id") is not None else None)
                for listener in self.print_listeners[m.name]:
                    await call(listener, trade)
        try:
            await self.client.websocket.subscribe(f"trade/{m.id}", on_trades)
            return True
        except ValueError as e:
            self.print_listeners.pop(m.name, None)
            await self._log.warning(f"trades websocket unavailable for {m.name}: {e}", send_tg=False)
            return False

    async def subscribe_depth(self, symbol: str, listener: DepthListener, levels: int = 10) -> bool:
        """order_book: a snapshot then the changed levels every 50 ms; each update must start at the nonce the previous
        one ended at, else the channel is subscribed again (a level at size 0 is removed)"""
        m = self.market(symbol)
        if m.name in self.depth_listeners:
            self.depth_listeners[m.name].append((listener, levels))
            return True
        self.depth_listeners[m.name] = [(listener, levels)]
        book = self.books[m.name] = {"bids": {}, "asks": {}, "nonce": None, "resync": False}
        channel = f"order_book/{m.id}"

        async def resubscribe():
            try:
                await self.client.websocket.unsubscribe(channel)
                await self.client.websocket.subscribe(channel, on_book)
            except Exception as e:
                await self._log.warning(f"{m.name}: order book resubscription failed ({e})", send_tg=False)
            finally:
                book["resync"] = False

        async def on_book(msg):
            data = msg.get("order_book") or {}
            snapshot = str(msg.get("type", "")).startswith("subscribed/") or book["nonce"] is None
            begin, nonce = data.get("begin_nonce"), data.get("nonce")
            if snapshot:
                book["bids"], book["asks"] = {}, {}
            elif begin is not None and begin != book["nonce"]:
                if not book["resync"]:   # an update was missed: start again from a snapshot
                    book["resync"] = True
                    self.tasks.append(asyncio.create_task(resubscribe()))
                return
            for side in ("bids", "asks"):
                for level in data.get(side) or []:
                    price, size = number(level.get("price")), number(level.get("size")) or 0.0
                    if price is None:
                        continue
                    if size > 0:
                        book[side][price] = size
                    else:
                        book[side].pop(price, None)
            book["nonce"] = nonce if nonce is not None else (book["nonce"] or 0)
            ts = stamp(data.get("last_updated_at"), msg.get("last_updated_at"), msg.get("timestamp"))
            bids = sorted(book["bids"].items(), reverse=True)
            asks = sorted(book["asks"].items())
            for listener, n in self.depth_listeners[m.name]:
                await call(listener, bids[:n], asks[:n], ts)
        try:
            await self.client.websocket.subscribe(channel, on_book)
            return True
        except ValueError as e:
            self.depth_listeners.pop(m.name, None)
            await self._log.warning(f"order book websocket unavailable for {m.name}: {e}", send_tg=False)
            return False

    async def watch_stats(self, m: Market) -> bool:
        """market_stats of a market: mark, index, funding (kept on the Market)"""
        key = f"stats:{m.id}"
        if key in self.streams:
            return self.streams[key]

        async def on_stats(msg):
            s = msg.get("market_stats") or {}
            if "market_id" not in s and isinstance(s.get(str(m.id)), dict):
                s = s[str(m.id)]
            m.stats.update({k: v for k, v in s.items() if v not in (None, "")})
            m.stats_ts = time.time()
            m.mark = number(m.stats.get("mark_price")) or m.mark
            m.index = number(m.stats.get("index_price")) or m.index
            if m.index and m.name in self.oracle_listeners:
                for listener in self.oracle_listeners[m.name]:
                    await call(listener, m.name, m.index)
        self.streams[key] = True
        try:
            await self.client.websocket.subscribe(f"market_stats/{m.id}", on_stats, timeout=5)
        except ValueError as e:
            self.streams[key] = False
            await self._log.warning(f"market stats websocket unavailable for {m.name}: {e}", send_tg=False)
        return self.streams[key]

    async def subscribe_oracle(self, symbol: str, listener: PriceListener) -> bool:
        """the index price of the market (what the mark and the funding are computed against)"""
        m = self.market(symbol)
        self.oracle_listeners.setdefault(m.name, []).append(listener)
        return await self.watch_stats(m)

    def mark(self, symbol: str) -> float | None:
        return self.market(symbol).mark

    async def fundings(self, symbols: list[str]) -> dict[str, Funding]:
        """market_stats gives the rates in PERCENT per hour (0.0008 = 0.0008 % an hour): `funding_rate` the last one
        paid, `current_funding_rate` the estimate of the next one. Paid at each hour mark, positive = longs pay"""
        out = {}
        for symbol in symbols:
            m = self.market(symbol)
            if not await self.watch_stats(m):
                continue
            for _ in range(20):          # first message of the channel
                if m.stats:
                    break
                await asyncio.sleep(0.05)
            last, forecast = number(m.stats.get("funding_rate")), number(m.stats.get("current_funding_rate"))
            if last is None and forecast is None:
                continue
            now = time.time()
            out[m.name] = Funding(m.name, last / 100 if last is not None else None,
                                  forecast / 100 if forecast is not None else None,
                                  (now // 3600 + 1) * 3600, False, m.stats_ts or now)
        return out

    async def funding_history(self, symbol: str, since: float) -> list[tuple[float, float]]:
        m = self.market(symbol)
        out: dict[float, float] = {}
        start = since
        for _ in range(10):
            rows = await self.client.info.fundings(m.id, start)
            for r in rows:
                rate = number(r.get("rate"))
                if rate is None:
                    continue
                # `direction` names the side that pays; the rate is in percent like the websocket one
                sign = -1 if str(r.get("direction") or "long").lower() == "short" else 1
                out[stamp(r.get("timestamp"))] = sign * abs(rate) / 100
            if len(rows) < 750:
                break
            start = max(out) + 1
        return sorted(out.items())

    async def funding_payments(self, since: float) -> list[FundingPayment]:
        if self.account_index is None:
            return []
        out, cursor = [], None
        for _ in range(50):
            rows, cursor = await self.client.info.position_funding(self.account_index, since, 100, cursor)
            for r in rows:
                usd = number(r.get("change"))
                if usd is None:
                    continue
                try:
                    name = self.market(int(r["market_id"])).name
                except (KeyError, ValueError):
                    continue
                rate = number(r.get("rate"))
                out.append(FundingPayment(name, stamp(r.get("timestamp")), usd, rate / 100 if rate is not None else None))
            if not cursor or len(rows) < 100:
                break
        return sorted(out, key=lambda p: p.ts)

    async def book(self, symbol: str) -> BBO | None:
        m = self.market(symbol)
        res = await self.client.info.book(m.id, 5)
        bids = [(number(o.get("price")), number(o.get("remaining_base_amount"))) for o in res.get("bids") or []]
        asks = [(number(o.get("price")), number(o.get("remaining_base_amount"))) for o in res.get("asks") or []]
        bid = max((x for x in bids if x[0]), default=(None, None))
        ask = min((x for x in asks if x[0]), default=(None, None))
        return BBO(m.name, bid[0], ask[0], bid[1], ask[1])

    async def candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        """1m, 5m, 15m, 30m, 1h, 4h, 12h, 1d are native (500 per call); another timeframe (3m, 2h, 8h...) is built from
        the largest native one that divides it"""
        seconds = SECONDS.get(timeframe)
        if not seconds:
            raise ValueError(f"Lighter timeframe {timeframe} unknown")
        native = timeframe if timeframe in TIMEFRAMES else source(TIMEFRAMES, seconds)
        if native is None:
            raise ValueError(f"Lighter has no candle shorter than 1m, got {timeframe}")
        factor = seconds // TIMEFRAMES[native]
        m = self.market(symbol)
        raw = await self.client.info.candles(m.id, native, min(max(count, 1) * factor + factor, 500))
        rows = [Candle(stamp(c.get("t", c.get("timestamp"))), float(c.get("o", c.get("open"))), float(c.get("h", c.get("high"))),
                       float(c.get("l", c.get("low"))), float(c.get("c", c.get("close"))), float(c.get("v") or c.get("volume0") or 0))
                for c in raw]
        rows.sort(key=lambda c: c.time)
        return (aggregate(rows, seconds) if factor > 1 else rows)[-count:]

    async def mid(self, symbol: str) -> float | None:
        m = self.market(symbol)
        bbo = self.bbos.get(m.name)
        if bbo and bbo.mid and time.time() - bbo.ts < 5:
            return bbo.mid
        try:
            bbo = await self.book(m.name)
            if bbo and bbo.mid:
                return bbo.mid
        except Exception:
            pass
        return m.mark or m.last

    # ==== account ====

    def parse_position(self, p: dict) -> tuple[str, Position, float | None] | None:
        try:
            symbol = p.get("symbol") if p.get("symbol") in self.client.info.by_name else self.market(int(p["market_id"])).name
        except (KeyError, ValueError, TypeError):
            return None
        lev = leverage_of(p.get("initial_margin_fraction"))
        size = (number(p.get("position")) or 0.0) * (-1 if int(number(p.get("sign")) or 1) < 0 else 1)
        value = abs(number(p.get("position_value")) or 0.0)
        mode = int(number(p.get("margin_mode")) or 0)
        liquidation = number(p.get("liquidation_price"))
        return symbol, Position(symbol=symbol, size=size, entry=number(p.get("avg_entry_price")) or None, value=value,
                                leverage=lev or 1.0, mode=MarginMode.ISOLATED if mode == MARGIN_ISOLATED else MarginMode.CROSS,
                                pnl=number(p.get("unrealized_pnl")) or 0.0,
                                liquidation=liquidation if liquidation and liquidation > 0 else None,
                                margin=number(p.get("allocated_margin")) or 0.0), lev

    def apply_positions(self, rows) -> None:
        for p in rows:
            parsed = self.parse_position(p) if isinstance(p, dict) else None
            if parsed is None:
                continue
            symbol, position, lev = parsed
            if lev:
                self.state.leverages[symbol] = lev
            if position.size:
                self.state.positions[symbol] = position
            else:
                self.state.positions.pop(symbol, None)

    def defaults(self, symbols: list[str]):
        """a market the account never set a leverage on trades at the market default: reported so that the strategies
        size on what the exchange will really apply (the trackers assume 1x when a leverage is unknown)"""
        for symbol in symbols:
            try:
                m = self.market(symbol)
            except ValueError:
                continue
            if m.imf:
                self.default_leverages[m.name] = 1 / m.imf

    def parse_account(self, raw: dict | None) -> AccountState:
        """REST /account: `total_asset_value` is the equity (collateral + unrealized PnL), `available_balance` what can
        still be used as margin"""
        state = AccountState(self.name, leverages={**self.default_leverages, **self.state.leverages})
        if not raw:
            return state
        previous, self.state = self.state, state
        self.apply_positions(raw.get("positions") or [])
        collateral = number(raw.get("collateral")) or 0.0
        equity = number(raw.get("total_asset_value"))
        state.equity = equity if equity is not None else collateral + sum(p.pnl for p in state.positions.values())
        free = number(raw.get("available_balance"))
        state.free = free if free is not None else previous.free
        state.margin = max(state.equity - state.free, 0.0)
        state.ts = time.time()
        return state

    async def read(self) -> AccountState:
        if self.account_index is None:
            raise ValueError("LIGHTER account index missing in the .env")
        raw = await self.client.info.account(self.account_index)
        self.read_at = time.time()
        return self.parse_account(raw)

    async def account(self, symbols: list[str]) -> AccountState:
        """the stream keeps `state` current; REST is read at most once per `account_interval` (weight 300 a call)"""
        self.defaults(symbols)
        if time.time() - self.read_at >= self.account_interval:
            if self.reading is None or self.reading.done():
                self.reading = asyncio.create_task(self.read())
            await asyncio.shield(self.reading)
        for symbol, lev in self.default_leverages.items():
            self.state.leverages.setdefault(symbol, lev)
        return self.state.copy()

    async def portfolio(self) -> Portfolio:
        return Portfolio.of(await self.account([]), f"account {self.account_index}")

    async def push(self):
        state = self.state.copy()
        for listener in self.listeners:
            await call(listener, state)

    async def on_positions(self, msg):
        positions = msg.get("positions")
        if isinstance(positions, dict):
            if str(msg.get("type", "")).startswith("subscribed/"):
                self.state.positions = {}
            self.apply_positions(positions.values())
            self.state.ts = time.time()
            self.dirty.set()

    async def on_stats(self, msg):
        s = msg.get("stats") or {}
        equity, free = number(s.get("portfolio_value")), number(s.get("available_balance"))
        if equity is not None:
            self.state.equity = equity
        if free is not None:
            self.state.free = free
        self.state.margin = max(self.state.equity - self.state.free, 0.0)
        self.state.ts = time.time()
        self.dirty.set()

    async def subscribe_account(self, symbols: list[str], listener: AccountListener):
        if not self.running:
            await self.start()
        self.listeners.append(listener)
        self.defaults(symbols)
        if len(self.listeners) > 1:
            await call(listener, await self.account(symbols))
            return
        ws, refused = self.client.websocket, []
        for channel, callback in ((f"account_all_positions/{self.account_index}", self.on_positions),
                                  (f"user_stats/{self.account_index}", self.on_stats)):
            try:
                await ws.subscribe(channel, callback, timeout=5)
            except ValueError as e:
                refused.append(str(e))
        if refused:
            self.poll = min(self.poll, 3.0)
            await self._log.warning(f"account websocket unavailable ({refused[0]}): REST polling every {self.poll}s")
        await call(listener, await self.account(symbols))
        self.tasks.append(asyncio.create_task(self.watch_account()))

    async def watch_account(self):
        """pushes the state after each websocket change (grouped over 50 ms); REST again every `poll` s"""
        while self.running and self.listeners:
            try:
                await asyncio.wait_for(self.dirty.wait(), timeout=self.poll)
                await asyncio.sleep(0.05)
                streamed = True
            except asyncio.TimeoutError:
                streamed = False
            self.dirty.clear()
            try:
                if not streamed or time.time() - self.read_at >= self.poll:
                    await self.read()
                await self.push()
            except Exception as e:
                await self._log.warning(f"account refresh failed: {e}", send_tg=False)

    # ==== client ids, orders stream ====

    def index_of(self, client: str | None, remember: bool = True) -> int:
        """48-bit client order index of a strategy's client id (its low 48 bits when it is hex, else a hash)"""
        if not client:
            return secrets.randbits(48) or 1
        try:
            n = int(client, 16) & MAX_CLIENT_INDEX
        except ValueError:
            n = int.from_bytes(hashlib.sha256(client.encode()).digest()[:6], "big")
        n = n or 1
        if remember:
            self.cloids[n] = client
            self.cloids.move_to_end(n)
            while len(self.cloids) > 200_000:
                self.cloids.popitem(last=False)
        return n

    @staticmethod
    def oid_of(client_index, order_index) -> str:
        return str(int(client_index)) if client_index not in (None, "", 0, "0") else str(order_index)

    def parse_update(self, row: dict) -> OrderUpdate | None:
        if row.get("order_index") is None and not row.get("client_order_index"):
            return None
        try:
            symbol = self.market(int(row["market_index"])).name
        except (KeyError, ValueError, TypeError):
            return None
        index = int(row.get("client_order_index") or 0)
        oid = self.oid_of(index, row.get("order_index"))
        raw = str(row.get("status") or "open")
        size = number(row.get("initial_base_amount")) or 0.0
        remaining = number(row.get("remaining_base_amount"))
        filled = number(row.get("filled_base_amount")) or 0.0
        if raw in OPEN:
            status = "open"
        elif raw == "filled":
            status, remaining = "filled", 0.0
        elif oid in self.replaced:
            status = "replaced"
        elif raw in REFUSED and not filled:
            status = "rejected"
        else:
            status = "canceled"
        return OrderUpdate(symbol, oid, Side.SELL if row.get("is_ask") else Side.BUY, number(row.get("price")) or 0.0, size,
                           remaining if remaining is not None else max(size - filled, 0.0), status,
                           self.cloids.get(index) if index else None,
                           stamp(row.get("transaction_time"), row.get("updated_at"), row.get("timestamp")),
                           {**row, "rejectionReason": raw} if status == "rejected" else
                           {**row, "cancelReason": raw} if status == "canceled" else row)

    async def emit(self, update: OrderUpdate, row: dict):
        if update.closed:
            self.orders.pop(update.oid, None)
            self.replaced.discard(update.oid)
        else:
            self.orders[update.oid] = row
        index = int(row.get("client_order_index") or 0)
        if index:
            self.confirmed[index] = True
            while len(self.confirmed) > 20_000:
                self.confirmed.popitem(last=False)
        waiter = self.waiters.get(index) if index else None
        if waiter is not None and update.closed and not waiter.done():
            waiter.set_result(row)
        for listener in self.order_listeners:
            await call(listener, update)

    async def on_orders(self, msg):
        orders = msg.get("orders")
        rows = [r for group in orders.values() for r in group or []] if isinstance(orders, dict) else (orders or [])
        if str(msg.get("type", "")).startswith("subscribed/"):
            self.snapshots += 1
            known, self.orders = self.orders, {}
            for row in rows:
                update = self.parse_update(row)
                if update and not update.closed:
                    self.orders[update.oid] = row
            if self.snapshots > 1 and known:
                # a snapshot after a reconnection: what is no longer there was filled or cancelled meanwhile
                self.tasks.append(asyncio.create_task(self.vanished({k: v for k, v in known.items() if k not in self.orders})))
            return
        for row in rows:
            update = self.parse_update(row)
            if update:
                await self.emit(update, row)

    async def vanished(self, gone: dict[str, dict]):
        """orders that left the book while the socket was down: their fills first (REST), then they are closed"""
        try:
            await self.recover()
        except Exception as e:
            await self._log.warning(f"fills recovery failed: {e}", send_tg=False)
        for oid, row in gone.items():
            update = self.parse_update({**row, "status": "canceled"})
            if update:
                update.remaining = 0.0
                await self.emit(update, row)

    async def on_txs(self, msg):
        """a transaction refused by the sequencer after the API accepted it: its order never existed"""
        for tx in msg.get("txs") or []:
            if tx.get("status") is None or int(number(tx.get("status")) or 0) != 0:
                continue
            known = self.hashes.pop(str(tx.get("hash") or ""), None)
            if known is not None:
                self.tasks.append(asyncio.create_task(self.refused(tx, *known)))

    async def refused(self, tx: dict, index: int, symbol: str):
        """status 0 is read as "failed"; it is only believed when the order itself never showed up on the order
        channel within a second (an order that exists is the stronger evidence)"""
        await asyncio.sleep(1.0)
        if index in self.confirmed or str(index) in self.orders:
            return
        lane = self.client.exchange.lanes.get(int(tx.get("api_key_index") or -1))
        if lane:
            lane.dirty = True
        reason = str(tx.get("event_info") or tx.get("info") or "")[:200]
        row = {"client_order_index": index, "market_index": self.market(symbol).id, "status": "canceled-invalid-balance",
               "initial_base_amount": "0", "remaining_base_amount": "0", "filled_base_amount": "0", "failed": reason}
        waiter = self.waiters.get(index)
        if waiter is not None and not waiter.done():
            waiter.set_result(row)
        update = self.parse_update(row)
        if update:
            update.status, update.raw = "rejected", {**row, "rejectionReason": f"transaction failed: {reason}"}
            await self.emit(update, row)

    async def stream(self) -> bool:
        """order updates and failed transactions of the account (private channels), subscribed once"""
        if "orders" in self.streams:
            return self.streams["orders"]
        self.streams["orders"] = True
        ws = self.client.websocket
        try:
            await ws.subscribe(f"account_all_orders/{self.account_index}", self.on_orders, private=True, timeout=5)
        except ValueError as e:
            self.streams["orders"] = False
            await self._log.warning(f"orders websocket unavailable: {e}")
        try:
            await ws.subscribe(f"account_tx/{self.account_index}", self.on_txs, private=True, timeout=5)
        except ValueError as e:
            await self._log.warning(f"transactions websocket unavailable ({e}): an order refused by the sequencer is "
                                    f"only seen by the periodic sync", send_tg=False)
        return self.streams["orders"]

    async def subscribe_orders(self, symbols: list[str], listener: OrderListener) -> bool:
        if not self.running:
            await self.start()
        self.order_listeners.append(listener)
        return await self.stream()

    # ==== fills ====

    def parse_fills(self, row: dict) -> list[Fill]:
        """our side(s) of a trade of the account. Fees are rates in millionths of the notional (28 = 0.0028 %)"""
        try:
            symbol = self.market(int(row["market_id"])).name
        except (KeyError, ValueError, TypeError):
            return []
        size, price = number(row.get("size")), number(row.get("price"))
        if not size or not price:
            return []
        usd = number(row.get("usd_amount")) or size * price
        maker_ask = bool(row.get("is_maker_ask"))
        ts = stamp(row.get("timestamp"), row.get("transaction_time"))
        out = []
        for ours, side, prefix in ((row.get("ask_account_id"), Side.SELL, "ask"), (row.get("bid_account_id"), Side.BUY, "bid")):
            if ours is None or int(ours) != self.account_index:
                continue
            maker = maker_ask if prefix == "ask" else not maker_ask
            index = int(row.get(f"{prefix}_client_id") or 0)
            rate = row.get("maker_fee" if maker else "taker_fee")
            out.append(Fill(symbol, side, size, price, self.oid_of(index, row.get(f"{prefix}_id")),
                            self.cloids.get(index) if index else None, f"{row.get('trade_id')}:{prefix}", maker, None, ts,
                            usd * float(rate) / 1e6 if rate not in (None, "") else None))
        return out

    async def dispatch(self, rows: list[dict]):
        now = time.time()
        for row in sorted(rows, key=lambda r: r.get("timestamp") or 0):
            for fill in self.parse_fills(row):
                if fill.id in self.seen:
                    continue
                self.seen[fill.id] = now
                self.latest = max(self.latest, fill.ts)
                self.dirty.set()
                for listener in self.fill_listeners:
                    await call(listener, fill)
        while len(self.seen) > 50_000:
            self.seen.popitem(last=False)

    async def on_fills(self, msg):
        trades = msg.get("trades")
        rows = [r for group in trades.values() for r in group or []] if isinstance(trades, dict) else (trades or [])
        if str(msg.get("type", "")).startswith("subscribed/"):
            self.stats = {k: msg[k] for k in ("daily_volume", "weekly_volume", "monthly_volume", "total_volume") if k in msg}
            for row in rows:        # the snapshot only marks what is already known
                for fill in self.parse_fills(row):
                    self.seen.setdefault(fill.id, time.time())
                    self.latest = max(self.latest, fill.ts)
            self.latest = max(self.latest, time.time() - 1) if not self.latest else self.latest
            return
        await self.dispatch(rows)

    async def recover(self):
        """fills since the newest one seen, read by REST (after a reconnection)"""
        if self.account_index is None or not self.latest:
            return
        rows, cursor, since = [], None, int((self.latest - 5) * 1000)
        for _ in range(20):
            page, cursor = await self.client.info.trades(self.account_index, since, 100, cursor)
            rows += page
            if not cursor or len(page) < 100:
                break
        if rows:
            await self.dispatch(rows)

    async def subscribe_fills(self, symbols: list[str], listener: FillListener) -> None:
        if not self.running:
            await self.start()
        self.fill_listeners.append(listener)
        if len(self.fill_listeners) > 1:
            return
        self.latest = time.time()
        await self.client.websocket.subscribe(f"account_all_trades/{self.account_index}", self.on_fills, timeout=10)

    async def volume(self) -> Volume | None:
        """the exchange gives the account volumes itself (day, week, month, total) with the trades channel; it does not
        split maker / taker and gives neither the number of fills nor the fees"""
        if self.volumes and time.time() - self.volumes[0] < 120:
            return self.volumes[1]
        if self.account_index is None:
            return None
        if not self.stats:
            holder: dict = {}

            async def once(msg):
                holder.update(msg)
            channel = f"account_all_trades/{self.account_index}"
            ws = self.client.websocket
            if channel.replace("/", ":") in ws.subscriptions:
                return Volume(self.name, error="volumes not received yet", label=f"account {self.account_index}")
            await ws.subscribe(channel, once, timeout=10)
            await ws.unsubscribe(channel)
            self.stats = {k: holder[k] for k in ("daily_volume", "weekly_volume", "monthly_volume", "total_volume") if k in holder}
        out = Volume(self.name, label=f"account {self.account_index}")
        for window, key in (("24h", "daily_volume"), ("7j", "weekly_volume"), ("30j", "monthly_volume")):
            out.volume[window] = number(self.stats.get(key)) or 0.0
            out.maker[window], out.fills[window], out.fees[window] = 0.0, 0, 0.0
        out.total = number(self.stats.get("total_volume"))
        self.volumes = (time.time(), out)
        return out

    # ==== orders ====

    async def open_orders(self, symbol: str | None = None) -> list[OpenOrder]:
        m = self.market(symbol) if symbol else None
        raw = await self.client.info.active_orders(self.account_index, m.id if m else None)
        out = []
        for o in raw:
            if str(o.get("status") or "open") not in OPEN:
                continue
            try:
                name = self.market(int(o["market_index"])).name
            except (KeyError, ValueError, TypeError):
                continue
            index = int(o.get("client_order_index") or 0)
            oid = self.oid_of(index, o.get("order_index"))
            self.orders[oid] = o
            out.append(OpenOrder(
                symbol=name, oid=oid, side=Side.SELL if o.get("is_ask") else Side.BUY, price=float(o["price"]),
                size=float(o.get("remaining_base_amount") or o.get("initial_base_amount") or 0),
                tif=FROM_TIFS.get(str(o.get("time_in_force")), TIF.GTC), reduce=bool(o.get("reduce_only")),
                client=self.cloids.get(index) if index else None, raw=o))
        return out

    def remember(self, results: list[dict], symbols: list[str]):
        for r, symbol in zip(results, symbols):
            tx = r.get("tx")
            if r.get("ok") and r.get("hash") and tx is not None and tx.type == TX_CREATE_ORDER and tx.client:
                self.hashes[str(r["hash"])] = (tx.client, symbol)
        while len(self.hashes) > 20_000:
            self.hashes.popitem(last=False)

    def convert(self, o: Order):
        """(market, client index, build) or an error text"""
        m = self.market(o.symbol)
        price = m.round_price(o.price, rounding(o.side, o.tif != TIF.IOC))
        size = m.round_size(o.size)
        if size <= 0:
            return f"{m.name}: size {o.size} rounds to 0 (step {m.step})"
        # the minimums are those of resting orders; a reduce-only order may be smaller
        if o.tif != TIF.IOC and not o.reduce and size < m.minimum(price):
            return f"{m.name}: size {size} < min {m.minimum(price)} ({m.min_base} or {m.min_quote} $)"
        if m.status != "active":
            return f"{m.name}: market {m.status}"
        if m.reduce_only and not o.reduce:
            return f"{m.name}: market in reduce-only mode"
        try:
            base, px = m.base_int(size), m.price_int(price)
        except ValueError as e:
            return str(e)
        index = self.index_of(o.client)
        expiry = 0 if o.tif == TIF.IOC else int(time.time() * 1000) + EXPIRY_DAYS * 86_400_000
        signer, tif, is_ask, reduce = self.client.exchange.signer, TIFS[o.tif], not o.side.is_buy, o.reduce

        def build(key: int, nonce: int):
            return signer.create_order(key, nonce, m.id, index, base, px, is_ask, tif, reduce, expiry)
        return m, index, build

    @staticmethod
    def result(r: dict, oid: str | None) -> Result:
        if r["ok"]:
            return Result(True, oid, "accepted", raw={"tx_hash": r.get("hash"), "eta_ms": r.get("eta")})
        return Result(False, oid, error=r.get("error") or "rejected", raw={"code": r.get("code")})

    async def finish(self, index: int, oid: str, r: dict) -> Result:
        """an accepted IOC: its outcome, from the order stream"""
        waiter = self.waiters.get(index)
        try:
            row = await asyncio.wait_for(asyncio.shield(waiter), timeout=self.ioc_timeout) if waiter else None
        except asyncio.TimeoutError:
            row = None
        finally:
            self.waiters.pop(index, None)
        if row is None:
            return Result(True, oid, "accepted", raw={"tx_hash": r.get("hash")})   # outcome unknown: the fills will tell
        filled = number(row.get("filled_base_amount")) or 0.0
        quote = number(row.get("filled_quote_amount")) or 0.0
        status = str(row.get("status") or "")
        if filled <= 0:
            reason = row.get("failed") or status or "canceled"
            return Result(False, oid, status, error=f"IOC could not match ({reason})", raw=row)
        return Result(True, oid, "filled" if status == "filled" else status, filled, quote / filled if quote else None, raw=row)

    async def place(self, orders: list[Order]) -> list[Result]:
        results: list[Result | None] = [None] * len(orders)
        lanes: dict[str, list[tuple[int, object, int, str]]] = {"maker": [], "taker": []}
        if any(o.tif == TIF.IOC for o in orders) and self.account_index is not None:
            await self.stream()
        for i, o in enumerate(orders):
            try:
                converted = self.convert(o)
            except ValueError as e:
                converted = str(e)
            if isinstance(converted, str):
                results[i] = Result(False, error=converted)
                continue
            m, index, build = converted
            if o.tif == TIF.IOC and self.streams.get("orders"):
                self.waiters[index] = asyncio.get_running_loop().create_future()
            lanes["taker" if o.tif == TIF.IOC else "maker"].append((i, build, index, m.name))

        async def run(lane: str, items):
            try:
                sent = await self.client.exchange.send([b for _, b, _, _ in items], lane)
            except BaseException:
                for _, _, index, _ in items:
                    self.waiters.pop(index, None)
                raise
            self.remember(sent, [name for _, _, _, name in items])
            waits = []
            for (i, _, index, _), r in zip(items, sent):
                oid = str(index)
                if r["ok"] and index in self.waiters:
                    waits.append((i, self.finish(index, oid, r)))
                else:
                    self.waiters.pop(index, None)
                    results[i] = self.result(r, oid)
            for (i, _), res in zip(waits, await asyncio.gather(*(w for _, w in waits))):
                results[i] = res
        jobs = [run(lane, items) for lane, items in lanes.items() if items]
        if len(jobs) == 1:
            await jobs[0]
        elif jobs:
            for (lane, items), outcome in zip([x for x in lanes.items() if x[1]], await asyncio.gather(*jobs, return_exceptions=True)):
                if isinstance(outcome, BaseException):
                    for i, _, _, _ in items:
                        results[i] = Result(False, error=f"{type(outcome).__name__}: {outcome}")
        return [r if r is not None else Result(False, error="missing response") for r in results]

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        results: list[Result | None] = [None] * len(modifies)
        builds, owners, symbols = [], [], []
        signer = self.client.exchange.signer
        for i, mo in enumerate(modifies):
            try:
                m = self.market(mo.order.symbol)
                price = m.round_price(mo.price, rounding(mo.order.side, True))
                size = m.round_size(mo.size)
                if size <= 0 or (not mo.order.reduce and size < m.minimum(price)):
                    raise ValueError(f"{m.name}: size {size} < min {m.minimum(price)}")
                if not mo.order.oid:
                    raise ValueError(f"{m.name}: order without id, NEVER PLACED")
                base, px, index = m.base_int(size), m.price_int(price), int(mo.order.oid)
            except ValueError as e:
                results[i] = Result(False, error=str(e))
                continue
            known = self.orders.get(mo.order.oid)
            if known is not None and (number(known.get("filled_base_amount")) or 0.0) > 0:
                # partly filled: cancel + new order with the wanted size, in the same batch (consecutive nonces)
                fresh = secrets.randbits(48) or 1
                if mo.order.client:
                    self.cloids[fresh] = mo.order.client
                expiry = int(time.time() * 1000) + EXPIRY_DAYS * 86_400_000
                tif = TIF_GTT if mo.order.tif == TIF.GTC else TIF_POST_ONLY
                self.replaced.add(mo.order.oid)
                builds.append(lambda key, nonce, m=m, index=index: signer.cancel_order(key, nonce, m.id, index))
                builds.append(lambda key, nonce, m=m, fresh=fresh, base=base, px=px, tif=tif, mo=mo, expiry=expiry:
                              signer.create_order(key, nonce, m.id, fresh, base, px, not mo.order.side.is_buy, tif,
                                                  mo.order.reduce, expiry))
                owners += [(i, None), (i, str(fresh))]
                symbols += [m.name, m.name]
            else:
                builds.append(lambda key, nonce, m=m, index=index, base=base, px=px: signer.modify_order(key, nonce, m.id, index, base, px))
                owners.append((i, mo.order.oid))
                symbols.append(m.name)
        if builds:
            sent = await self.client.exchange.send(builds, "maker")
            self.remember(sent, symbols)
            for (i, oid), r in zip(owners, sent):
                if not r["ok"]:
                    results[i] = self.result(r, None)
                    self.replaced.discard(modifies[i].order.oid)
                elif oid is not None and results[i] is None:
                    results[i] = self.result(r, oid)
        return [r if r is not None else Result(False, error="missing response") for r in results]

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        """a cancel uses no volume quota and has no rate limit. The API only says the transaction was accepted: an
        order already gone makes it a no-op, the order stream tells what really happened"""
        results: list[Result | None] = [None] * len(cancels)
        builds, indexes = [], []
        signer = self.client.exchange.signer
        for i, c in enumerate(cancels):
            try:
                m = self.market(c.symbol)
                index = int(c.oid) if c.oid else self.index_of(c.client, remember=False)
            except ValueError as e:
                results[i] = Result(False, error=str(e))
                continue
            builds.append(lambda key, nonce, m=m, index=index: signer.cancel_order(key, nonce, m.id, index))
            indexes.append((i, str(index)))
        if builds:
            sent = await self.client.exchange.send(builds, "maker")
            for (i, oid), r in zip(indexes, sent):
                results[i] = self.result(r, oid)
        return [r if r is not None else Result(False, error="missing response") for r in results]

    async def control(self, build) -> bool:
        sent = await self.client.exchange.send([build], "maker")
        if not sent[0]["ok"]:
            await self._log.warning(f"transaction refused: {sent[0]['error']}", send_tg=False)
        return bool(sent[0]["ok"])

    async def cancel_all(self, symbol: str | None = None) -> None:
        if symbol is None:
            return await self.purge()
        orders = await self.open_orders(symbol)
        if orders:
            await self.cancel([Cancel(o.symbol, o.oid, o.client) for o in orders])

    async def purge(self) -> None:
        """every order of the account in one transaction (it uses one unit of volume quota)"""
        signer = self.client.exchange.signer
        await self.control(lambda key, nonce: signer.cancel_all(key, nonce, CANCEL_ALL_IMMEDIATE, 0))

    async def schedule_cancel(self, seconds: float | None) -> bool:
        """dead man's switch: a cancel-all scheduled at now + seconds replaces the one scheduled before, None aborts it.
        Account-wide (every market), and EACH call uses one unit of volume quota"""
        signer = self.client.exchange.signer
        try:
            if seconds is None:
                return await self.control(lambda key, nonce: signer.cancel_all(key, nonce, CANCEL_ALL_ABORT, 0))
            at = int((time.time() + seconds) * 1000)
            return await self.control(lambda key, nonce: signer.cancel_all(key, nonce, CANCEL_ALL_SCHEDULED, at))
        except Exception as e:
            await self._log.warning(f"dead man's switch failed: {e}", send_tg=False)
            return False

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        m = self.market(symbol)
        leverage = int(max(1, min(leverage, m.max_leverage or leverage)))
        fraction = int(Decimal(10_000) / Decimal(leverage))
        mode = MARGIN_ISOLATED if (not cross or m.isolated) else MARGIN_CROSS
        signer = self.client.exchange.signer
        ok = await self.control(lambda key, nonce: signer.update_leverage(key, nonce, m.id, fraction, mode))
        if ok:
            self.state.leverages[m.name] = float(leverage)
            self.read_at = 0.0
        return ok

    async def add_margin(self, symbol: str, usd: float) -> bool:
        m = self.market(symbol)
        signer = self.client.exchange.signer
        amount = int(round(usd * USDC_SCALE))
        try:
            return await self.control(lambda key, nonce: signer.update_margin(key, nonce, m.id, amount, 1))
        except Exception as e:
            await self._log.warning(f"{m.name}: add margin failed ({e})", send_tg=False)
            return False
