"""Aster(Exchange) over core/aster (futures API v3, "Pro API"). What differs from Hyperliquid / Arcus and how it is
handled here (the strategies see none of it; the full list is in src/core/aster/aster.md):

- orders only go by HTTPS (no websocket order entry), 5 per request at most: a list is cut in batches sent
  concurrently, a single order goes alone with newOrderRespType=RESULT so that the reply carries what an IOC filled;
- post-only is the time in force GTX: a GTX order that would cross comes back EXPIRED, reported as a refusal;
- an order that left nothing (IOC not matched) is reported failed like on Hyperliquid, never as an open order;
- a modify keeps the order id but ALWAYS loses its time priority, takes the new TOTAL quantity (the adapter adds what
  was already filled to the remaining size the strategy asks for) and only exists for LIMIT orders;
- funding is paid every 4 or 8 hours depending on the market, not every hour: rates are given per hour (rate of the
  period / hours of the period) so that strategies compare venues on the same unit;
- the dead man's switch is per symbol (countdownCancelAll): `schedule_cancel` arms it on every symbol the adapter has
  been asked to follow or has traded;
- the account must be in One-way mode (checked at start); balances are read on the futures account in the margin asset
  the exchange reports (USDT / USD1...), counted as dollars.
"""
import asyncio
import re
import time
from collections import OrderedDict
from decimal import Decimal
from adapters.exchange import (AccountListener, BBOListener, DepthListener, DisconnectListener, Exchange, FillListener,
                               OrderListener, PriceListener, PrintListener)
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Funding, FundingPayment, Instrument, Listing, MarginMode,
                             Modify, OpenOrder, Order, OrderUpdate, Portfolio, Position, Print, Result, Side, TIF)
from adapters.utils import SECONDS, aggregate, rounding, source
from core.aster.api import AsterError
from core.aster.client import AsterClient
from core.aster.utils.constants import CLIENT_ID, GONE, STABLES, TIMEFRAMES
from core.aster.utils.types import D, Market, fmt, number
from core.utils import call, cancel
from log.logger import Logger

TIFS = {TIF.GTC: "GTC", TIF.ALO: "GTX", TIF.IOC: "IOC"}
FROM_TIFS = {"GTC": TIF.GTC, "GTX": TIF.ALO, "IOC": TIF.IOC, "FOK": TIF.IOC, "HIDDEN": TIF.GTC}
STATUSES = {"NEW": "open", "PARTIALLY_FILLED": "open", "FILLED": "filled", "CANCELED": "canceled", "EXPIRED": "canceled",
            "REJECTED": "rejected", "NEW_INSURANCE": "open", "NEW_ADL": "open"}
VALID_CLIENT = re.compile(CLIENT_ID)
# refusals that are answers, not incidents: reported in the results without a warning line each
QUIET = {-2010, -2011, -2013, -2018, -2019, -2021, -2022, -2025, -4016, -4024, -4164, -5022}


class Aster(Exchange):
    name = "aster"

    def __init__(self, client: AsterClient, maker_fee: float = 0.0001, taker_fee: float = 0.00035,
                 fees: dict[str, tuple[float, float]] | None = None, poll: float = 10.0, refresh: float = 30.0):
        self.client = client
        self.messenger = client.messenger
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.rates: dict[str, tuple[float, float]] = dict(fees or {})   # symbol -> (maker, taker), the account's real ones when read
        self.poll = poll
        self.refresh = refresh
        self._log = Logger("Aster", client.messenger)
        self.address = client.address
        client.api.quiet |= QUIET
        self.listeners: list[AccountListener] = []
        self.bbo_listeners: dict[str, list[BBOListener]] = {}
        self.bbos: dict[str, BBO] = {}
        self.fill_listeners: list[FillListener] = []
        self.order_listeners: list[OrderListener] = []
        self.oracle_listeners: dict[str, list[PriceListener]] = {}
        self.print_listeners: dict[str, list[PrintListener]] = {}
        self.symbols: set[str] = set()             # markets followed or traded: account reads, fills recovery, dead man's switch
        self.tasks: list[asyncio.Task] = []
        self.running = False
        self.streaming = False                     # the user data stream is started
        self.dirty = asyncio.Event()
        self.leverages: dict[str, float] = {}
        self.filled: OrderedDict[str, Decimal] = OrderedDict()   # order id -> quantity executed (for modify)
        self.seen: OrderedDict[str, float] = OrderedDict()
        self.latest = 0.0
        self.armed = False

    # ==== lifecycle ====

    async def start(self):
        if self.running:
            return
        if not self.client.running:
            await self.client.start()
        self.running = True
        self.tasks.append(asyncio.create_task(self.watch_markets()))

    async def stop(self):
        self.running = False
        for task in self.tasks:
            await cancel(task)
        self.tasks.clear()

    async def watch_markets(self):
        n = 0
        while self.running:
            try:
                if n % 10 == 0:
                    await self.client.info.load_volumes()
                if n:
                    await self.client.info.load_prices()
                if n and n % 10 == 0:
                    await self.client.info.load_markets()
            except Exception as e:
                await self._log.warning(f"markets refresh failed: {e}", send_tg=False)
            n += 1
            await asyncio.sleep(self.refresh)

    async def stream(self):
        """the user data stream (orders, fills, balances), started at the first subscription"""
        if self.streaming:
            return
        self.streaming = True
        user = self.client.user
        user.listeners.append(self.on_event)
        user.connect_listeners.append(self.reconnected)
        await user.start()

    async def reconnected(self):
        """events were missed while the stream was down: account read again, fills since the newest one read by REST"""
        self.dirty.set()
        if self.fill_listeners:
            self.tasks.append(asyncio.create_task(self.recover()))

    # ==== markets ====

    def market(self, symbol: str) -> Market:
        return self.client.info.market(symbol)

    def instrument(self, symbol: str) -> Instrument:
        m = self.market(symbol)
        maker, taker = self.rates.get(m.name, (self.maker_fee, self.taker_fee))
        return Instrument(
            exchange=self.name, symbol=m.name, maker_fee=maker, taker_fee=taker,
            min_notional=float(m.min_notional), min_size=float(m.min_size),
            pricer=lambda px, side, passive: float(m.round_price(px, rounding(side, passive))),
            sizer=lambda sz: float(m.round_size(sz)),
            stepper=lambda px: float(m.tick),
            raw=m, max_leverage=m.max_leverage, maintenance=m.mmf,
            initial=1 / m.max_leverage if m.max_leverage else None)

    def tradable(self, symbol: str) -> bool:
        return self.market(symbol).online

    def listings(self) -> list[Listing]:
        return [Listing(m.name, m.base, m.category, "", m.online, m.volume) for m in self.client.info.by_name.values()]

    @property
    def connected(self) -> bool:
        return self.client.websocket.connected.is_set() and (not self.streaming or self.client.user.connected.is_set())

    def on_disconnect(self, listener: DisconnectListener) -> bool:
        self.client.websocket.disconnect_listeners.append(listener)
        self.client.user.disconnect_listeners.append(listener)
        return True

    @property
    def transport(self) -> str:
        return "rest"

    async def follow(self, symbols: list[str]):
        """the account's own fee rates and leverage brackets of these markets (signed reads, once per market)"""
        for symbol in symbols:
            m = self.market(symbol)
            if m.name in self.symbols:
                continue
            self.symbols.add(m.name)
            if self.client.readonly:
                continue
            try:
                res = await self.client.info.commission(m.name)
                real = (float(res["makerCommissionRate"]), float(res["takerCommissionRate"]))
                configured = self.rates.get(m.name, (self.maker_fee, self.taker_fee))
                self.rates[m.name] = real
                if any(abs(a - b) > 1e-9 for a, b in zip(real, configured)):
                    await self._log.info(f"{m.name}: fees of the account {real[0] * 1e4:g} / {real[1] * 1e4:g} bp (maker / taker), "
                                         f"configured {configured[0] * 1e4:g} / {configured[1] * 1e4:g} bp: the account's are used")
            except Exception as e:
                await self._log.warning(f"{m.name}: commission rate not read ({e}), configured fees kept", send_tg=False)
            try:
                res = await self.client.info.brackets(m.name)
                rows = res if isinstance(res, list) else [res]
                row = next((r for r in rows if isinstance(r, dict) and r.get("symbol") == m.name), rows[0] if rows else {})
                m.brackets = sorted(row.get("brackets") or [], key=lambda b: number(b.get("notionalFloor")) or 0.0)
            except Exception as e:
                await self._log.debug(f"{m.name}: leverage brackets not read ({e})")

    # ==== market data ====

    async def subscribe_bbo(self, symbol: str, listener: BBOListener):
        m = self.market(symbol)
        if m.name in self.bbo_listeners:
            self.bbo_listeners[m.name].append(listener)
            if m.name in self.bbos:
                await call(listener, self.bbos[m.name])
            return
        self.bbo_listeners[m.name] = [listener]

        async def on_ticker(d):
            bbo = BBO(m.name, number(d.get("b")), number(d.get("a")), number(d.get("B")), number(d.get("A")),
                      float(d.get("T") or d.get("E") or time.time() * 1000) / 1000)
            self.bbos[m.name] = bbo
            for listener in self.bbo_listeners[m.name]:
                await call(listener, bbo)
        await self.client.websocket.subscribe(f"{m.name.lower()}@bookTicker", on_ticker)

    async def subscribe_trades(self, symbol: str, listener: PrintListener) -> bool:
        """aggTrade: the fills of one taker order at one price, every 100 ms; `m` true = the buyer was the maker"""
        m = self.market(symbol)
        if m.name in self.print_listeners:
            self.print_listeners[m.name].append(listener)
            return True
        self.print_listeners[m.name] = [listener]

        async def on_trade(d):
            price, size = number(d.get("p")), number(d.get("q"))
            if not price or not size:
                return
            trade = Print(m.name, price, size, Side.SELL if d.get("m") else Side.BUY, float(d.get("T") or 0) / 1000 or time.time(),
                          time.time(), id=str(d.get("a")) if d.get("a") is not None else None)
            for listener in self.print_listeners[m.name]:
                await call(listener, trade)
        try:
            await self.client.websocket.subscribe(f"{m.name.lower()}@aggTrade", on_trade)
            return True
        except ValueError as e:
            self.print_listeners.pop(m.name, None)
            await self._log.warning(f"trades websocket unavailable for {m.name}: {e}", send_tg=False)
            return False

    async def subscribe_depth(self, symbol: str, listener: DepthListener, levels: int = 10) -> bool:
        """partial book: the 5, 10 or 20 best levels, a full snapshot every 100 ms"""
        m = self.market(symbol)
        depth = next((n for n in (5, 10, 20) if n >= levels), 20)

        async def on_book(d):
            try:
                bids = [(float(p), float(q)) for p, q in d.get("b") or []]
                asks = [(float(p), float(q)) for p, q in d.get("a") or []]
            except (TypeError, ValueError):
                return
            bids.sort(reverse=True)
            asks.sort()
            await call(listener, bids[:levels], asks[:levels], float(d.get("T") or d.get("E") or time.time() * 1000) / 1000)
        try:
            await self.client.websocket.subscribe(f"{m.name.lower()}@depth{depth}@100ms", on_book)
            return True
        except ValueError as e:
            await self._log.warning(f"book websocket unavailable for {m.name}: {e}", send_tg=False)
            return False

    async def watch_mark(self, m: Market) -> bool:
        key = f"{m.name.lower()}@markPrice@1s"
        if key in self.client.websocket.streams:
            return True

        async def on_mark(d):
            self.client.info.price(m, d.get("p"), d.get("i"), d.get("r"), d.get("T"), time.time())
            if m.index and m.name in self.oracle_listeners:
                for listener in self.oracle_listeners[m.name]:
                    await call(listener, m.name, m.index)
        try:
            await self.client.websocket.subscribe(key, on_mark)
            return True
        except ValueError as e:
            await self._log.warning(f"mark price websocket unavailable for {m.name}: {e}", send_tg=False)
            return False

    async def subscribe_oracle(self, symbol: str, listener: PriceListener) -> bool:
        """the index price (the mark, the funding and the price limits are computed against it)"""
        m = self.market(symbol)
        self.oracle_listeners.setdefault(m.name, []).append(listener)
        return await self.watch_mark(m)

    def mark(self, symbol: str) -> float | None:
        return self.market(symbol).mark

    async def fundings(self, symbols: list[str]) -> dict[str, Funding]:
        """`lastFundingRate` is the rate of the period that is running, paid at `nextFundingTime` on the position held
        then; a period lasts `fundingIntervalHours` (8 or 4). Returned per hour"""
        out = {}
        for symbol in symbols:
            m = self.market(symbol)
            if m.funding is None or time.time() - m.priced > 120:
                try:
                    row = await self.client.info.premium(m.name)
                    self.client.info.price(m, row.get("markPrice"), row.get("indexPrice"), row.get("lastFundingRate"),
                                           row.get("nextFundingTime"), time.time())
                except Exception as e:
                    await self._log.debug(f"{m.name}: premium index not read ({e})")
            if m.funding is None:
                continue
            hourly = m.funding / (m.interval or 8.0)
            out[m.name] = Funding(m.name, hourly, hourly, m.next_funding, False, m.priced or time.time())
        return out

    async def funding_history(self, symbol: str, since: float) -> list[tuple[float, float]]:
        """one row per payment (every 4 or 8 h), as an hourly rate"""
        m = self.market(symbol)
        out: dict[float, float] = {}
        start = int(since * 1000)
        for _ in range(10):
            rows = await self.client.info.funding_rates(m.name, start)
            for r in rows or []:
                rate, ts = number(r.get("fundingRate")), number(r.get("fundingTime"))
                if rate is not None and ts:
                    out[ts / 1000] = rate / (m.interval or 8.0)
            if len(rows or []) < 1000:
                break
            start = int(max(out) * 1000) + 1
        return sorted(out.items())

    async def funding_payments(self, since: float) -> list[FundingPayment]:
        out, start, seen = [], int(since * 1000), set()
        for _ in range(20):
            rows = await self.client.info.income("FUNDING_FEE", start) or []
            for r in rows:
                key = (r.get("tranId"), r.get("symbol"), r.get("time"))
                usd, ts = number(r.get("income")), number(r.get("time"))
                if key in seen or usd is None or not ts:
                    continue
                seen.add(key)
                out.append(FundingPayment(str(r.get("symbol") or ""), ts / 1000, usd))
            if len(rows) < 1000:
                break
            start = int(max(number(r.get("time")) or 0 for r in rows)) + 1
        return sorted(out, key=lambda p: p.ts)

    async def book(self, symbol: str) -> BBO | None:
        m = self.market(symbol)
        d = await self.client.info.book_ticker(m.name)
        return BBO(m.name, number(d.get("bidPrice")), number(d.get("askPrice")), number(d.get("bidQty")), number(d.get("askQty")))

    async def candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        seconds = SECONDS.get(timeframe)
        if not seconds:
            raise ValueError(f"Aster timeframe {timeframe} unknown")
        native = timeframe if timeframe in TIMEFRAMES else source(TIMEFRAMES, seconds)
        if native is None:
            raise ValueError(f"Aster has no candle shorter than 1m, got {timeframe}")
        factor = seconds // TIMEFRAMES[native]
        raw = await self.client.info.klines(self.market(symbol).name, native, min(max(count, 1) * factor + factor, 1500))
        rows = [Candle(float(k[0]) / 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in raw or []]
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
        return m.mark

    # ==== account ====

    def parse_account(self, raw: dict | None, risks: list[dict] | None = None) -> AccountState:
        """GET /fapi/v3/account: totalMarginBalance = wallet + unrealized PnL (the equity), availableBalance = what can
        still open positions. positionRisk adds the liquidation and mark prices"""
        state = AccountState(self.name, leverages=dict(self.leverages))
        if not raw:
            return state
        state.equity = number(raw.get("totalMarginBalance")) or 0.0
        state.free = number(raw.get("availableBalance")) or 0.0
        state.margin = number(raw.get("totalInitialMargin"))
        if state.margin is None:
            state.margin = max(state.equity - state.free, 0.0)
        risk = {r.get("symbol"): r for r in risks or [] if (r.get("positionSide") or "BOTH") == "BOTH"}
        for symbol, r in risk.items():
            if symbol and number(r.get("leverage")):
                state.leverages[symbol] = self.leverages[symbol] = float(r["leverage"])
        for p in raw.get("positions") or []:
            symbol = p.get("symbol")
            if not symbol or (p.get("positionSide") or "BOTH") != "BOTH":
                continue
            lev = number(p.get("leverage"))
            if lev:
                state.leverages[symbol] = self.leverages[symbol] = lev
            size = number(p.get("positionAmt")) or 0.0
            if not size:
                continue
            r = risk.get(symbol) or {}
            mark = number(r.get("markPrice"))
            value = abs(number(p.get("notional")) or number(r.get("notional")) or size * (mark or number(p.get("entryPrice")) or 0.0))
            liquidation = number(r.get("liquidationPrice"))
            isolated = p.get("isolated") is True or str(r.get("marginType") or "").lower() == "isolated"
            state.positions[symbol] = Position(
                symbol=symbol, size=size, entry=number(p.get("entryPrice")) or None, value=value, leverage=lev or 1.0,
                mode=MarginMode.ISOLATED if isolated else MarginMode.CROSS,
                pnl=number(p.get("unrealizedProfit")) or number(r.get("unRealizedProfit")) or 0.0,
                liquidation=liquidation if liquidation and liquidation > 0 else None,
                margin=number(p.get("isolatedWallet") if isolated else p.get("initialMargin")) or 0.0)
        state.ts = time.time()
        return state

    async def account(self, symbols: list[str]) -> AccountState:
        raw, risks = await asyncio.gather(self.client.info.account(), self.client.info.position_risk(), return_exceptions=True)
        if isinstance(raw, BaseException):
            raise raw
        return self.parse_account(raw, risks if isinstance(risks, list) else None)

    async def portfolio(self) -> Portfolio:
        return Portfolio.of(await self.account([]), "futures")

    async def subscribe_account(self, symbols: list[str], listener: AccountListener):
        if not self.running:
            await self.start()
        await self.follow(symbols)
        self.listeners.append(listener)
        if len(self.listeners) > 1:
            await call(listener, await self.account(symbols))
            return
        try:
            await self.stream()
        except Exception as e:
            self.poll = min(self.poll, 2.0)
            await self._log.warning(f"user data stream unavailable ({e}): REST polling every {self.poll}s")
        await call(listener, await self.account(symbols))
        self.tasks.append(asyncio.create_task(self.watch_account()))

    async def watch_account(self):
        """the account is read by REST after each account / order event of the stream (grouped over 150 ms), and every
        `poll` s: an ACCOUNT_UPDATE only carries what changed, without the margins"""
        while self.running and self.listeners:
            try:
                await asyncio.wait_for(self.dirty.wait(), timeout=self.poll)
                await asyncio.sleep(0.15)
            except asyncio.TimeoutError:
                pass
            self.dirty.clear()
            try:
                state = await self.account([])
                for listener in self.listeners:
                    await call(listener, state)
            except AsterError as e:
                await asyncio.sleep((e.retry or 1000) / 1000)
            except Exception as e:
                await self._log.warning(f"account refresh failed: {e}", send_tg=False)

    # ==== user data stream ====

    @staticmethod
    def status(o: dict) -> str:
        raw = str(o.get("X") or o.get("status") or "NEW")
        status = STATUSES.get(raw, "open")
        executed = number(o.get("z", o.get("executedQty"))) or 0.0
        if raw == "EXPIRED" and str(o.get("f") or o.get("timeInForce")) == "GTX" and not executed:
            return "rejected"     # a post-only order that would have crossed
        return status

    def parse_update(self, o: dict, ts: float) -> OrderUpdate | None:
        if o.get("i") is None or not o.get("s"):
            return None
        size, executed = number(o.get("q")) or 0.0, number(o.get("z")) or 0.0
        status = self.status(o)
        raw = {**o, "rejectionReason": "GTX_WOULD_CROSS"} if status == "rejected" else o
        return OrderUpdate(str(o["s"]), str(o["i"]), Side.BUY if o.get("S") == "BUY" else Side.SELL, number(o.get("p")) or 0.0,
                           size, max(size - executed, 0.0), status, o.get("c") or None, ts, raw)

    def parse_fill(self, o: dict, ts: float) -> Fill | None:
        size, price = number(o.get("l")), number(o.get("L"))
        if not size or not price:
            return None
        total, executed = number(o.get("q")) or 0.0, number(o.get("z")) or 0.0
        fee = number(o.get("n"))
        if fee is not None and str(o.get("N") or "").upper() not in STABLES:
            fee = None            # paid in another asset: not a dollar amount
        return Fill(str(o["s"]), Side.BUY if o.get("S") == "BUY" else Side.SELL, size, price, str(o["i"]), o.get("c") or None,
                    f"{o.get('s')}:{o.get('t')}", bool(o.get("m")), max(total - executed, 0.0), ts, fee)

    async def publish(self, fill: Fill):
        if fill.id in self.seen:
            return
        self.seen[fill.id] = time.time()
        while len(self.seen) > 50_000:
            self.seen.popitem(last=False)
        self.latest = max(self.latest, fill.ts)
        for listener in self.fill_listeners:
            await call(listener, fill)

    async def on_event(self, msg: dict):
        kind = msg.get("e")
        if kind == "ORDER_TRADE_UPDATE":
            o = msg.get("o") or {}
            ts = float(o.get("T") or msg.get("T") or msg.get("E") or time.time() * 1000) / 1000
            self.dirty.set()
            if o.get("i") is not None:
                oid = str(o["i"])
                if str(o.get("X")) in ("NEW", "PARTIALLY_FILLED"):
                    self.filled[oid] = D(o.get("z") or 0)
                    while len(self.filled) > 20_000:
                        self.filled.popitem(last=False)
                else:
                    self.filled.pop(oid, None)
            if o.get("x") == "TRADE":
                fill = self.parse_fill(o, ts)
                if fill:
                    await self.publish(fill)
            update = self.parse_update(o, ts)
            if update:
                for listener in self.order_listeners:
                    await call(listener, update)
        elif kind == "ACCOUNT_UPDATE":
            self.dirty.set()
        elif kind == "ACCOUNT_CONFIG_UPDATE":
            ac = msg.get("ac") or {}
            if ac.get("s") and number(ac.get("l")):
                self.leverages[ac["s"]] = float(ac["l"])
            self.dirty.set()
        elif kind == "MARGIN_CALL":
            await self._log.warning(f"Aster margin call: {str(msg)[:300]}")

    async def subscribe_orders(self, symbols: list[str], listener: OrderListener) -> bool:
        if not self.running:
            await self.start()
        await self.follow(symbols)
        self.order_listeners.append(listener)
        try:
            await self.stream()
            return True
        except Exception as e:
            await self._log.warning(f"orders websocket unavailable: {e}")
            return False

    async def subscribe_fills(self, symbols: list[str], listener: FillListener) -> None:
        if not self.running:
            await self.start()
        await self.follow(symbols)
        self.fill_listeners.append(listener)
        if len(self.fill_listeners) > 1:
            return
        self.latest = time.time()
        await self.stream()

    async def recover(self):
        """fills of the followed markets since the newest one seen (REST, after a reconnection of the stream)"""
        if not self.latest:
            return
        since = int((self.latest - 5) * 1000)
        for symbol in sorted(self.symbols):
            try:
                rows = await self.client.info.trades(symbol, since)
            except Exception as e:
                await self._log.warning(f"{symbol}: fills recovery failed ({e})", send_tg=False)
                continue
            for r in sorted(rows or [], key=lambda r: r.get("time") or 0):
                size, price = number(r.get("qty")), number(r.get("price"))
                if not size or not price:
                    continue
                fee = number(r.get("commission"))
                if fee is not None and str(r.get("commissionAsset") or "").upper() not in STABLES:
                    fee = None
                await self.publish(Fill(symbol, Side.BUY if r.get("side") == "BUY" else Side.SELL, size, price,
                                        str(r.get("orderId")), None, f"{symbol}:{r.get('id')}", bool(r.get("maker")), None,
                                        float(r.get("time") or 0) / 1000 or time.time(), fee))

    # ==== orders ====

    async def open_orders(self, symbol: str | None = None) -> list[OpenOrder]:
        m = self.market(symbol) if symbol else None
        raw = await self.client.info.open_orders(m.name if m else None)
        out = []
        for o in raw or []:
            if o.get("status") not in (None, "NEW", "PARTIALLY_FILLED") or o.get("type", "LIMIT") != "LIMIT":
                continue
            executed = D(o.get("executedQty") or 0)
            self.filled[str(o["orderId"])] = executed
            out.append(OpenOrder(
                symbol=o["symbol"], oid=str(o["orderId"]), side=Side.BUY if o.get("side") == "BUY" else Side.SELL,
                price=float(o["price"]), size=float(D(o.get("origQty") or 0) - executed),
                tif=FROM_TIFS.get(o.get("timeInForce", "GTC"), TIF.GTC), reduce=bool(o.get("reduceOnly", False)),
                client=o.get("clientOrderId") or None, raw=o))
        return out

    def validate(self, m: Market, side: Side, size: Decimal, price: Decimal, reduce: bool) -> str | None:
        if size <= 0 or size < m.min_size:
            return f"{m.name}: size {size} < min {m.min_size}"
        if not reduce and size * price < m.min_notional:
            return f"{m.name}: notional {size * price} < min {m.min_notional}"
        if not m.online:
            return f"{m.name}: market {m.status}"
        if m.mark and time.time() - m.priced < 60:
            if side.is_buy and m.up and float(price) > m.mark * m.up:
                return f"{m.name}: buy at {price} above the price limit {m.mark * m.up:g} (mark x {m.up:g})"
            if not side.is_buy and m.down and float(price) < m.mark * m.down:
                return f"{m.name}: sell at {price} under the price limit {m.mark * m.down:g} (mark x {m.down:g})"
        return None

    def convert_order(self, o: Order) -> dict | str:
        m = self.market(o.symbol)
        price = m.round_price(o.price, rounding(o.side, o.tif != TIF.IOC))
        size = m.round_size(o.size)
        error = self.validate(m, o.side, size, price, o.reduce)
        if error:
            return error
        if o.client and not VALID_CLIENT.match(o.client):
            return f"{m.name}: client id {o.client!r} not accepted by Aster (1-36 characters among . A-Z : / a-z 0-9 _ -)"
        self.symbols.add(m.name)
        params = {"symbol": m.name, "side": o.side.value, "type": "LIMIT", "timeInForce": TIFS[o.tif],
                  "quantity": fmt(size), "price": fmt(price)}
        if o.reduce:
            params["reduceOnly"] = "true"
        if o.client:
            params["newClientOrderId"] = o.client
        params["newOrderRespType"] = "RESULT"
        return params

    @staticmethod
    def failure(row: dict) -> str:
        code, msg = row.get("code"), str(row.get("msg") or "rejected")
        return f"NOT_FOUND: {msg} ({code})" if code in GONE else f"{msg} ({code})"

    def parse_result(self, row: dict, tif: str | None = None) -> Result:
        if not isinstance(row, dict):
            return Result(False, error="missing response")
        if isinstance(row.get("code"), int) and row["code"] < 0 and "orderId" not in row:
            return Result(False, error=self.failure(row), raw=row)
        oid = str(row["orderId"]) if row.get("orderId") is not None else None
        status = str(row.get("status") or "")
        executed = number(row.get("executedQty")) or 0.0
        average = number(row.get("avgPrice")) or None
        tif = tif or row.get("timeInForce")
        if oid is not None and status in ("NEW", "PARTIALLY_FILLED"):
            self.filled[oid] = D(row.get("executedQty") or 0)
        if status in ("EXPIRED", "CANCELED", "REJECTED") and not executed:
            reason = "post only would cross" if tif == "GTX" else "IOC could not match" if tif == "IOC" else status.lower()
            return Result(False, oid, status, error=f"{reason} ({status})", raw=row)
        return Result(True, oid, status or "ACK", executed, average, raw=row)

    async def send(self, items: list, build, post, tifs: bool = False) -> list[Result]:
        results: list[Result | None] = [None] * len(items)
        requests, indexes = [], []
        for i, item in enumerate(items):
            try:
                r = build(item)
            except ValueError as e:
                r = str(e)
            if isinstance(r, str):
                results[i] = Result(False, error=r)
            else:
                requests.append(r)
                indexes.append(i)
        if requests:
            rows = await post(requests)
            for i, req, row in zip(indexes, requests, rows + [{}] * (len(requests) - len(rows))):
                results[i] = self.parse_result(row, req.get("timeInForce") if tifs else None)
        return [r if r is not None else Result(False, error="missing response") for r in results]

    async def place(self, orders: list[Order]) -> list[Result]:
        return await self.send(orders, self.convert_order, self.client.exchange.place, tifs=True)

    def convert_modify(self, mo: Modify) -> dict | str:
        """the strategy gives the size it wants LEFT on the book; Aster takes the new total quantity of the order"""
        m = self.market(mo.order.symbol)
        price = m.round_price(mo.price, rounding(mo.order.side, True))
        size = m.round_size(mo.size)
        error = self.validate(m, mo.order.side, size, price, mo.order.reduce)
        if error:
            return error
        if not mo.order.oid and not mo.order.client:
            return f"{m.name}: order without id, NEVER PLACED"
        raw = mo.order.raw if isinstance(mo.order.raw, dict) else {}
        executed = self.filled.get(mo.order.oid) if mo.order.oid else None
        if executed is None:
            executed = D(raw.get("executedQty") or raw.get("z") or 0)
        params = {"symbol": m.name, "side": mo.order.side.value, "quantity": fmt(size + executed), "price": fmt(price)}
        if mo.order.oid:
            params["orderId"] = int(mo.order.oid)
        else:
            params["origClientOrderId"] = mo.order.client
        return params

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        results = await self.send(modifies, self.convert_modify, self.client.exchange.modify)
        for mo, r in zip(modifies, results):
            if r.ok and r.oid is None:
                r.oid = mo.order.oid
        return results

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        results: list[Result | None] = [None] * len(cancels)
        groups: dict[str, tuple[list[tuple[int, int]], list[tuple[int, str]]]] = {}
        for i, c in enumerate(cancels):
            try:
                name = self.market(c.symbol).name
                oids, clients = groups.setdefault(name, ([], []))
                if c.oid:
                    oids.append((i, int(c.oid)))
                elif c.client:
                    clients.append((i, c.client))
                else:
                    results[i] = Result(False, error=f"{name}: cancel without id, NEVER PLACED")
            except ValueError as e:
                results[i] = Result(False, error=str(e))

        async def run(name: str, oids, clients):
            a, b = await self.client.exchange.cancel(name, [x for _, x in oids], [x for _, x in clients])
            for (i, _), row in list(zip(oids, a)) + list(zip(clients, b)):
                if isinstance(row, dict) and isinstance(row.get("code"), int) and row["code"] < 0 and "orderId" not in row:
                    results[i] = Result(False, error=self.failure(row), raw=row)
                else:
                    oid = str(row.get("orderId")) if isinstance(row, dict) and row.get("orderId") is not None else None
                    self.filled.pop(oid or "", None)
                    results[i] = Result(True, oid, str((row or {}).get("status") or "CANCELED"), raw=row)
        jobs = [run(name, oids, clients) for name, (oids, clients) in groups.items() if oids or clients]
        if len(jobs) == 1:
            await jobs[0]
        elif jobs:
            for (name, (oids, clients)), outcome in zip([g for g in groups.items() if g[1][0] or g[1][1]],
                                                        await asyncio.gather(*jobs, return_exceptions=True)):
                if isinstance(outcome, BaseException):
                    for i, _ in oids + clients:
                        if results[i] is None:
                            results[i] = Result(False, error=f"{type(outcome).__name__}: {outcome}")
        return [r if r is not None else Result(False, error="missing response") for r in results]

    async def cancel_all(self, symbol: str | None = None) -> None:
        """allOpenOrders is per symbol: without one, every symbol that has an open order"""
        if symbol is not None:
            await self.client.exchange.cancel_all(self.market(symbol).name)
            return
        names = {o.symbol for o in await self.open_orders(None)}
        await asyncio.gather(*(self.client.exchange.cancel_all(name) for name in sorted(names)))

    async def purge(self) -> None:
        await self.cancel_all(None)

    async def schedule_cancel(self, seconds: float | None) -> bool:
        """dead man's switch, PER SYMBOL on Aster: armed on every market followed or traded (weight 10 each call)"""
        if not self.symbols:
            return True
        ms = 0 if seconds is None else max(int(seconds * 1000), 1000)
        try:
            await asyncio.gather(*(self.client.exchange.countdown(name, ms) for name in sorted(self.symbols)))
            self.armed = seconds is not None
            return True
        except Exception as e:
            await self._log.warning(f"dead man's switch failed: {e}", send_tg=False)
            return False

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        m = self.market(symbol)
        leverage = int(max(1, min(leverage, m.max_leverage or leverage)))
        try:
            await self.client.exchange.set_margin_type(m.name, isolated=not cross)
        except AsterError as e:
            if e.code != -4046:     # "no need to change margin type"
                await self._log.warning(f"{m.name}: margin type not changed ({e.message})", send_tg=False)
        try:
            res = await self.client.exchange.set_leverage(m.name, leverage)
        except AsterError as e:
            await self._log.warning(f"{m.name}: leverage not changed ({e.message})", send_tg=False)
            return False
        self.leverages[m.name] = float(res.get("leverage") or leverage) if isinstance(res, dict) else float(leverage)
        return True

    async def add_margin(self, symbol: str, usd: float) -> bool:
        try:
            await self.client.exchange.add_margin(self.market(symbol).name, usd)
            return True
        except AsterError as e:
            await self._log.warning(f"{symbol}: add margin failed ({e.message})", send_tg=False)
            return False
