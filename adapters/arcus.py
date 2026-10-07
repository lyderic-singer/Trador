import asyncio
import time
from decimal import Decimal
from typing import Any
from adapters.exchange import AccountListener, BBOListener, Exchange, FillListener, OrderListener, PriceListener, PrintListener
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Instrument, Listing, MarginMode, Modify, OpenOrder, Order,
                             OrderUpdate, Portfolio, Position, Print, Result, Side, TIF, Volume)
from core.utils import call, cancel
from core.arcus.api import ArcusError
from core.arcus.client import ArcusClient
from core.arcus.exchange import ArcusExchange
from core.arcus.utils.constants import ACCEPTED, TIMEFRAMES
from core.arcus.utils.types import ArcusCancel, ArcusModify, ArcusOrder, Market
from log.logger import Logger

TIFS = {TIF.GTC: "GTT", TIF.ALO: "ALO", TIF.IOC: "IOC"}
FROM_TIFS = {"GTT": TIF.GTC, "GTC": TIF.GTC, "ALO": TIF.ALO, "IOC": TIF.IOC, "FOK": TIF.IOC}
VOLUMES = ("volume24h", "volume24H", "notionalVolume24h", "quoteVolume24h", "dayNtlVlm", "volume")
POOL_START = 20_000  # an order pool starts at 20,000 units and grows by 1 per UNIT of filled volume
UNIT = 0.10
STATUSES = {"FILLED": "filled", "CANCELED": "canceled", "MARGIN_CANCELED": "canceled", "REJECTED": "rejected",
            "LIQUIDATED": "canceled", "ADL": "canceled"}


def rows(contents) -> list[dict]:
    if isinstance(contents, list):
        return [r for r in contents if isinstance(r, dict)]
    if not isinstance(contents, dict):
        return []
    for k in ("fills", "orders"):
        if isinstance(contents.get(k), list):
            return contents[k]
    return [contents] if ("orderId" in contents or "tradeId" in contents) else []


def rounding(side: Side | None, passive: bool) -> str:
    if side is None:
        return "nearest"
    return ("down" if side.is_buy else "up") if passive else ("up" if side.is_buy else "down")


class Arcus(Exchange):
    name = "arcus"

    def __init__(self, client: ArcusClient, maker_fee: float = 0.0001, taker_fee: float = 0.0004,
                 poll: float = 5.0, refresh: float = 60.0, fills_poll: float = 1.5):
        self.client = client
        self.messenger = client.messenger
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.poll = poll
        self.refresh = refresh
        self._log = Logger("Arcus", client.messenger)
        self.address = client.address
        self.index = client.index
        self.listeners: list[AccountListener] = []
        self.bbo_listeners: dict[str, list[BBOListener]] = {}
        self.symbols: list[str] = []
        self.dirty = asyncio.Event()
        self.leverages: dict[str, float] = {}
        self.leverages_ts = 0.0
        self.tasks: list[asyncio.Task] = []
        self.running = False
        self.fills_poll = fills_poll
        self.fill_listeners: list[FillListener] = []
        self.order_listeners: list[OrderListener] = []
        self.oracle_listeners: dict[str, list[PriceListener]] = {}
        self.streamed: bool | None = None
        self.seen: dict[str, float] = {}
        self.listening = False
        self.latest = 0.0
        self.remaining: dict[str, int] = {}
        self.volumes: tuple[float, Volume] | None = None
        self.print_listeners: dict[str, list[PrintListener]] = {}
        self.print_logged = False

    async def start(self):
        if self.running:
            return
        if not self.client.running:
            await self.client.start()
        self.running = True
        self.tasks.append(asyncio.create_task(self.watch_markets()))
        self.tasks.append(asyncio.create_task(self.watch_budget()))

    async def stop(self):
        self.running = False
        for task in self.tasks:
            await cancel(task)
        self.tasks.clear()

    def market(self, symbol: str) -> Market:
        return self.client.info.market(symbol)

    def instrument(self, symbol: str) -> Instrument:
        m = self.market(symbol)
        return Instrument(
            exchange=self.name, symbol=m.name, maker_fee=self.maker_fee, taker_fee=self.taker_fee,
            min_notional=float(m.min_notional), min_size=float(m.min_size),
            pricer=lambda px, side, passive: float(m.round_price(px, rounding(side, passive))),
            sizer=lambda sz: float(m.round_size(sz)),
            stepper=lambda px: float(m.tick_at(Decimal(str(px)))),
            closed=m.closed, lower=m.lower, upper=m.upper,
            raw=m, max_leverage=m.max_leverage)

    def listings(self) -> list[Listing]:
        out = []
        for m in self.client.info.by_id.values():
            volume = next((m.raw[k] for k in VOLUMES if m.raw.get(k) not in (None, "")), 0)
            try:
                volume = float(volume)
            except (TypeError, ValueError):
                volume = 0.0
            out.append(Listing(m.name, m.base, m.category, "", m.online, volume))
        return out

    async def portfolio(self) -> Portfolio:
        await self.load_leverages()
        raw = await self.client.info.account(self.address, self.index)
        out = Portfolio.of(self.parse_account(raw, self.leverages, lambda i: self.client.info.market(i).name),
                           f"sub-account {self.index}")
        if raw is None:
            out.error = f"no account {self.index} for {self.address}"
        return out

    def tradable(self, symbol: str) -> bool:
        return self.market(symbol).online

    @property
    def connected(self) -> bool:
        return self.client.websocket.connected.is_set()

    def budget(self) -> dict[str, int] | None:
        return dict(self.remaining) if self.remaining else None

    async def watch_budget(self):
        while self.running:
            try:
                res = await self.client.info.rate_limit(self.address, self.index)
                for name in ("order", "cancel"):
                    pool = res.get(name) or {}
                    if "cap" in pool:
                        self.remaining[name] = int(pool["cap"]) - int(pool.get("used") or 0)
            except Exception as e:
                await self._log.debug(f"rate limit read failed: {e}")
            await asyncio.sleep(5 if self.remaining and min(self.remaining.values()) < 20_000 else 30)

    async def watch_markets(self):
        while self.running:
            await asyncio.sleep(self.refresh)
            try:
                await self.client.info.load_markets()
            except Exception as e:
                await self._log.warning(f"markets refresh failed: {e}")

    async def subscribe_bbo(self, symbol: str, listener: BBOListener):
        m = self.market(symbol)
        if m.name in self.bbo_listeners:
            self.bbo_listeners[m.name].append(listener)
            return
        self.bbo_listeners[m.name] = [listener]

        async def on_bbo(msg):
            c = msg.get("contents") or {}
            if "bestBid" not in c and "bestAsk" not in c:
                return
            bid, ask = c.get("bestBid"), c.get("bestAsk")
            bbo = BBO(m.name, float(bid["price"]) if bid else None, float(ask["price"]) if ask else None,
                      float(bid["size"]) if bid else None, float(ask["size"]) if ask else None,
                      (c.get("timestamp") or time.time() * 1e6) / 1e6)
            for listener in self.bbo_listeners[m.name]:
                await call(listener, bbo)
        await self.client.websocket.bbo(on_bbo, m.name)

    @staticmethod
    def parse_print(name: str, row: dict, now: float) -> Print | None:
        """public trade row; the field names are read tolerantly (price/px, size/quantity/qty/sz, side/takerSide)"""
        price = next((row[k] for k in ("price", "px", "tradePrice") if row.get(k) not in (None, "")), None)
        size = next((row[k] for k in ("size", "quantity", "qty", "sz", "baseSize") if row.get(k) not in (None, "")), None)
        if price is None or size is None:
            return None
        raw = str(row.get("takerSide") or row.get("side") or row.get("aggressorSide") or "").upper()
        side = Side.BUY if raw in ("BUY", "B", "BID", "LONG") else Side.SELL if raw in ("SELL", "S", "A", "ASK", "SHORT") else None
        ts = row.get("createdAt") or row.get("timestamp") or row.get("time")
        try:
            stamp = float(ts) if ts not in (None, "") else now
        except (TypeError, ValueError):
            stamp = now
        while stamp > 1e11:  # µs / ms -> s
            stamp /= 1000
        try:
            return Print(name, float(price), abs(float(size)), side, stamp, now)
        except (TypeError, ValueError):
            return None

    async def subscribe_trades(self, symbol: str, listener: PrintListener) -> bool:
        m = self.market(symbol)
        if m.name in self.print_listeners:
            self.print_listeners[m.name].append(listener)
            return True
        self.print_listeners[m.name] = [listener]

        async def on_trades(msg):
            if msg.get("type") == "subscribed":
                return  # snapshot of past trades
            contents = msg.get("contents")
            items = contents if isinstance(contents, list) else (contents.get("trades") if isinstance(contents, dict)
                                                                 and isinstance(contents.get("trades"), list) else [contents])
            now = time.time()
            for row in items:
                trade = self.parse_print(m.name, row, now) if isinstance(row, dict) else None
                if trade is None:
                    if not self.print_logged:
                        self.print_logged = True
                        await self._log.warning(f"unreadable public trade: {str(msg)[:300]}", send_tg=False)
                    continue
                for listener in self.print_listeners[m.name]:
                    await call(listener, trade)
        await self.client.websocket.trades(on_trades, m.name)
        return True

    async def book(self, symbol: str) -> BBO | None:
        m = self.market(symbol)
        res = await self.client.info.bbo(m.name)
        bid, ask = res.get("bestBid"), res.get("bestAsk")
        return BBO(m.name, float(bid["price"]) if bid else None, float(ask["price"]) if ask else None,
                   float(bid.get("size") or 0) if bid else None, float(ask.get("size") or 0) if ask else None)

    async def candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        if timeframe not in TIMEFRAMES:
            raise ValueError(f"Arcus timeframe must be one of {list(TIMEFRAMES)}, got {timeframe}")
        raw = await self.client.info.candles(self.market(symbol).name, timeframe, count=min(max(count, 1), 1500))
        return [Candle(c["openTime"] / 1e6, float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                       float(c.get("volume") or 0)) for c in raw]

    async def mid(self, symbol: str) -> float | None:
        m = self.market(symbol)
        try:
            value = await self.client.info.mid(m.name)
            if value:
                return value
        except Exception:
            pass
        bbo = await self.client.info.bbo(m.name)
        bid, ask = bbo.get("bestBid"), bbo.get("bestAsk")
        return (float(bid["price"]) + float(ask["price"])) / 2 if bid and ask else (m.mark or m.oracle)

    async def load_leverages(self, force: bool = False):
        if not force and time.time() - self.leverages_ts < 60:
            return
        try:
            rows = await self.client.info.leverages(self.address, self.index)
            self.leverages = {r["marketDisplayName"]: float(r["leverage"]) for r in rows}
            self.leverages_ts = time.time()
        except Exception as e:
            await self._log.warning(f"cannot load leverages: {e}")

    @staticmethod
    def parse_account(raw: dict | None, leverages: dict[str, float], name) -> AccountState:
        state = AccountState("arcus", leverages=dict(leverages))
        if not raw:
            return state
        state.equity = float(raw.get("equity") or 0)
        state.free = float(raw.get("freeCollateral") or 0)
        state.margin = max(state.equity - state.free, 0.0)
        for k, p in (raw.get("positions") or {}).items():
            symbol = p.get("marketDisplayName") or name(int(p.get("marketId", k)))
            value = abs(float(p.get("positionValueNotional") or 0))
            mark = float(p.get("markPx") or 0)
            if value == 0 or mark == 0:
                continue
            sign = -1 if (p.get("side") or "").upper() == "SHORT" or str(p.get("size", "")).startswith("-") else 1
            size = sign * value / mark
            pnl = float(p.get("unrealizedPnl") or 0)
            mode = (p.get("marginMode") or "").lower()
            state.positions[symbol] = Position(
                symbol=symbol, size=size, entry=mark - pnl / size, value=value,
                leverage=float(p.get("leverage") or leverages.get(symbol, 1)),
                mode=MarginMode(mode) if mode in ("cross", "isolated") else None,
                pnl=pnl, margin=float(p.get("marginUsed") or 0))
        state.ts = time.time()
        return state

    async def account(self, symbols: list[str]) -> AccountState:
        await self.load_leverages()
        raw = await self.client.info.account(self.address, self.index)
        return self.parse_account(raw, self.leverages, lambda i: self.client.info.market(i).name)

    async def subscribe_account(self, symbols: list[str], listener: AccountListener):
        if not self.running:
            await self.start()
        self.symbols = sorted(set(self.symbols) | {self.market(s).name for s in symbols})
        self.listeners.append(listener)
        if len(self.listeners) > 1:
            await call(listener, await self.account(self.symbols))
            return
        ws = self.client.websocket
        refused = []
        for channel in (ws.positions, ws.account):
            try:
                await channel(self.touch, self.address, self.index, timeout=5)
            except ValueError as e:
                refused.append(str(e))
        if not await self.stream():
            refused.append("orders channel refused")
        if refused:
            self.poll = min(self.poll, 2.0)
            await self._log.warning(f"account websocket unavailable ({refused[0]}): REST polling every {self.poll}s")
        await call(listener, await self.account(self.symbols))
        self.tasks.append(asyncio.create_task(self.watch_account()))

    def touch(self, msg):
        self.dirty.set()

    async def stream(self) -> bool:
        if self.streamed is None:
            try:
                await self.client.websocket.orders(self.on_orders, self.address, self.index, timeout=5)
                self.streamed = True
            except ValueError as e:
                await self._log.warning(f"orders websocket unavailable: {e}")
                self.streamed = False
        return self.streamed

    def name_of(self, row: dict) -> str:
        return row.get("marketDisplayName") or self.market(int(row["marketId"])).name

    def parse_update(self, row: dict) -> OrderUpdate | None:
        if not row.get("orderId") or not row.get("side"):
            return None
        status = str(row.get("status") or row.get("state") or "OPEN").upper()
        state = STATUSES.get(status, "open")
        if state == "canceled" and row.get("cancelReason") == "MODIFY_CANCELED":
            state = "replaced"
        size = float(row.get("originalSize") or row.get("size") or 0)
        remaining = float(row["remainingSize"]) if row.get("remainingSize") not in (None, "") else size
        return OrderUpdate(self.name_of(row), str(row["orderId"]), Side.BUY if row["side"] == "BUY" else Side.SELL,
                           float(row.get("price") or 0), size, remaining, state, row.get("clientId") or None,
                           float(row.get("updatedAt") or time.time() * 1e6) / 1e6, row)

    async def on_orders(self, msg):
        self.dirty.set()
        contents = msg.get("contents")
        if msg.get("type") == "subscribed" or (isinstance(contents, dict) and contents.get("isSnapshot")):
            return
        for row in rows(contents):
            update = self.parse_update(row)
            if update:
                for listener in self.order_listeners:
                    await call(listener, update)

    async def subscribe_orders(self, symbols: list[str], listener: OrderListener) -> bool:
        if not self.running:
            await self.start()
        self.order_listeners.append(listener)
        return await self.stream()

    def parse_fill(self, row: dict) -> Fill | None:
        if not row.get("side") or not row.get("size"):
            return None
        uid = f"{row.get('tradeId')}:{row.get('orderId')}"
        return Fill(self.name_of(row), Side.BUY if row["side"] == "BUY" else Side.SELL, float(row["size"]),
                    float(row["price"]), str(row.get("orderId")) if row.get("orderId") else None,
                    row.get("clientId") or None, uid, (row.get("role") or "").upper() == "MAKER" if row.get("role") else None,
                    float(row["remainingSize"]) if row.get("remainingSize") not in (None, "") else None,
                    float(row.get("createdAt") or time.time() * 1e6) / 1e6, self.fee_of(row))

    @staticmethod
    def fee_of(row: dict) -> float | None:
        for k in ("fee", "fees", "feeAmount", "tradingFee"):
            if row.get(k) not in (None, ""):
                try:
                    return float(row[k])
                except (TypeError, ValueError):
                    return None
        return None

    async def volume(self) -> Volume:
        """24h / 7j / 30j summed from /v1/fills; the total is derived from the order pool (1 unit per $0.10 filled)"""
        if self.volumes and time.time() - self.volumes[0] < 120:
            return self.volumes[1]
        now = time.time()
        cursor = int((now - 30 * 86_400) * 1e6)
        fills: dict[str, dict] = {}
        truncated = True
        for _ in range(40):
            try:
                items = await self.client.info.fills_since(self.address, self.index, cursor, 1000)
            except ArcusError as e:
                if e.status != 429:
                    raise
                await asyncio.sleep((e.retry or 2000) / 1000)
                continue
            for r in items or []:
                fills[f"{r.get('tradeId')}:{r.get('orderId')}"] = r
            last = max((int(r.get("createdAt") or 0) for r in items or []), default=0)
            if len(items or []) < 1000 or last < cursor:
                truncated = False
                break
            cursor = last + 1
        rows = []
        for r in fills.values():
            try:
                rows.append((float(r.get("createdAt") or 0) / 1e6, float(r["size"]) * float(r["price"]),
                             (r.get("role") or "").upper() == "MAKER", self.fee_of(r) or 0.0))
            except (KeyError, TypeError, ValueError):
                continue
        out = Volume.of(self.name, rows, now, f"sous-compte {self.index}")
        out.truncated = truncated
        try:
            pool = (await self.client.info.rate_limit(self.address, self.index)).get("order") or {}
            if pool.get("cap") is not None:
                out.total = max((int(pool["cap"]) - POOL_START) * UNIT, out.volume.get("30j", 0.0))
                out.estimated = True
        except Exception as e:
            out.error = f"rateLimit: {e}"
        self.volumes = (time.time(), out)
        return out

    async def dispatch(self, items: list[dict]):
        now = time.time()
        for row in sorted(items, key=lambda r: r.get("createdAt") or 0):
            fill = self.parse_fill(row)
            if fill is None or fill.id in self.seen:
                continue
            self.seen[fill.id] = now
            self.latest = max(self.latest, fill.ts)
            for listener in self.fill_listeners:
                await call(listener, fill)
        if len(self.seen) > 20000:
            self.seen = {k: t for k, t in self.seen.items() if now - t < 3600}

    async def on_fills(self, msg):
        self.dirty.set()
        contents = msg.get("contents")
        if msg.get("type") == "subscribed" or (isinstance(contents, dict) and contents.get("isSnapshot")):
            if self.listening:
                await self.dispatch([r for r in rows(contents) if float(r.get("createdAt") or 0) / 1e6 >= self.latest - 5])
                return
            self.listening = True
            for row in rows(contents):
                fill = self.parse_fill(row)
                if fill:
                    self.seen[fill.id] = time.time()
                    self.latest = max(self.latest, fill.ts)
            self.latest = max(self.latest, time.time())
            return
        await self.dispatch(rows(contents))

    async def subscribe_fills(self, symbols: list[str], listener: FillListener) -> None:
        if not self.running:
            await self.start()
        self.fill_listeners.append(listener)
        if len(self.fill_listeners) > 1:
            return
        try:
            await self.client.websocket.fills(self.on_fills, self.address, self.index, timeout=5)
        except ValueError as e:
            await self._log.warning(f"fills websocket unavailable ({e}): REST polling every {self.fills_poll}s")
            self.tasks.append(asyncio.create_task(self.watch_fills()))

    async def watch_fills(self):
        cursor = int(time.time() * 1_000_000)
        while self.running:
            await asyncio.sleep(self.fills_poll)
            try:
                items = await self.client.info.fills_since(self.address, self.index, cursor)
                if items:
                    cursor = max(cursor, max(int(r.get("createdAt") or 0) for r in items))
                    self.dirty.set()
                    await self.dispatch(items)
            except ArcusError as e:
                await asyncio.sleep((e.retry or 2000) / 1000 if e.status == 429 else 1)
            except Exception as e:
                await self._log.warning(f"fills poll failed: {e}")

    async def on_oracle(self, msg):
        contents = msg.get("contents") or {}
        for p in contents.get("prices", []) if isinstance(contents, dict) else []:
            name = p.get("marketDisplayName")
            if name in self.oracle_listeners and p.get("price"):
                for listener in self.oracle_listeners[name]:
                    await call(listener, name, float(p["price"]))

    async def subscribe_oracle(self, symbol: str, listener: PriceListener) -> bool:
        m = self.market(symbol)
        self.oracle_listeners.setdefault(m.name, []).append(listener)
        if len(self.oracle_listeners[m.name]) > 1:
            return True
        try:
            await self.client.websocket.oracle(self.on_oracle, m.name, timeout=5)
            return True
        except ValueError as e:
            await self._log.warning(f"oracle websocket unavailable for {m.name}: {e}")
            return False

    async def schedule_cancel(self, seconds: float | None) -> bool:
        try:
            res = await self.client.exchange.schedule_cancel(seconds)
            return res.get("status") in ("scheduled", "disarmed")
        except Exception as e:
            await self._log.warning(f"dead man's switch failed: {e}")
            return False

    async def watch_account(self):
        while self.running and self.listeners:
            try:
                await asyncio.wait_for(self.dirty.wait(), timeout=self.poll)
                await asyncio.sleep(0.25)
            except asyncio.TimeoutError:
                pass
            self.dirty.clear()
            try:
                state = await self.account(self.symbols)
                for listener in self.listeners:
                    await call(listener, state)
            except Exception as e:
                await self._log.warning(f"account refresh failed: {e}")

    async def open_orders(self, symbol: str | None = None) -> list[OpenOrder]:
        m = self.market(symbol) if symbol else None
        raw = await self.client.info.open_orders(self.address, self.index, m.name if m else None)
        out = []
        for o in raw:
            if o.get("status", "OPEN") != "OPEN":
                continue
            out.append(OpenOrder(
                symbol=o.get("marketDisplayName") or self.market(o["marketId"]).name, oid=str(o["orderId"]),
                side=Side.BUY if o["side"] == "BUY" else Side.SELL, price=float(o["price"]),
                size=float(o.get("remainingSize") or o.get("originalSize")),
                tif=FROM_TIFS.get(o.get("timeInForce", "GTT"), TIF.GTC),
                reduce=bool(o.get("reduceOnly", False)), client=o.get("clientId") or None, raw=o))
        return out

    @staticmethod
    def parse_responses(res: dict, size: int) -> list[Result]:
        rows = res.get("responses", []) if isinstance(res, dict) else []
        out = []
        for r in rows:
            status = r.get("status", "")
            ok = status in ACCEPTED and not r.get("error")
            out.append(Result(ok, r.get("orderId"), status, float(r.get("filledSize") or 0), None,
                              None if ok else (r.get("error") or r.get("rejectionReason") or status), r))
        out += [Result(False, error="missing response")] * (size - len(out))
        return out[:size]

    @staticmethod
    def validate(m: Market, size: Decimal, price: Decimal, reduce: bool) -> str | None:
        if size <= 0 or size < m.min_size:
            return f"{m.name}: size {size} < min {m.min_size}"
        if not reduce and size * price < m.min_notional:
            return f"{m.name}: notional {size * price} < min {m.min_notional}"
        if not m.online:
            return f"{m.name}: market {m.status}"
        return None

    async def send(self, items: list, build, post) -> list[Result]:
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
            res = await post(requests)
            limit = res.get("rateLimit") if isinstance(res, dict) else None
            if isinstance(limit, dict) and limit.get("pool") in ("order", "cancel") and limit.get("remaining") is not None:
                self.remaining[limit["pool"]] = int(limit["remaining"])
            for i, r in zip(indexes, self.parse_responses(res, len(requests))):
                results[i] = r
        return [r for r in results if r is not None]

    def convert_order(self, o: Order, expiry: int | None = None) -> ArcusOrder | str:
        m = self.market(o.symbol)
        price = m.round_price(o.price, rounding(o.side, o.tif != TIF.IOC))
        size = m.round_size(o.size)
        return self.validate(m, size, price, o.reduce) or ArcusOrder(m, o.side.is_buy, size, price, TIFS[o.tif], o.reduce,
                                                                     o.client, expiry=expiry)

    def convert_modify(self, mo: Modify) -> ArcusModify | str:
        raw: dict[str, Any] = mo.order.raw or {}
        m = self.market(mo.order.symbol)
        tif = raw.get("timeInForce") or TIFS[mo.order.tif]
        tif = "GTT" if tif == "GTC" else tif
        price = m.round_price(mo.price, rounding(mo.order.side, tif != "IOC"))
        size = m.round_size(mo.size)
        reduce = bool(raw.get("reduceOnly", mo.order.reduce))
        error = self.validate(m, size, price, reduce)
        if error:
            return error
        if not raw.get("goodTilTime"):
            return f"{m.name}: open order {mo.order.oid} has no goodTilTime to echo"
        return ArcusModify(m, mo.order.oid, mo.order.side.is_buy, size, price, tif, reduce, int(raw["goodTilTime"]),
                           raw.get("clientId") or None)

    async def place(self, orders: list[Order]) -> list[Result]:
        expiry = ArcusExchange.expiry()
        results = await self.send(orders, lambda o: self.convert_order(o, expiry), self.client.exchange.batch_place)
        for o, r in zip(orders, results):
            if r.ok:
                r.raw = {"goodTilTime": expiry, "timeInForce": TIFS[o.tif], "reduceOnly": o.reduce, "clientId": o.client,
                         **{k: v for k, v in (r.raw or {}).items() if v is not None}}
        return results

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        return await self.send(modifies, self.convert_modify, self.client.exchange.batch_modify)

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        return await self.send(cancels, lambda c: ArcusCancel(self.market(c.symbol), oid=c.oid) if c.oid
                               else ArcusCancel(self.market(c.symbol), client=c.client),
                               self.client.exchange.batch_cancel)

    async def cancel_all(self, symbol: str | None = None) -> None:
        orders = await self.open_orders(symbol)
        if orders:
            await self.cancel([Cancel(o.symbol, o.oid, o.client) for o in orders])

    async def purge(self) -> None:
        await self.client.exchange.cancel_all(None)

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        m = self.market(symbol)
        leverage = int(min(leverage, m.max_leverage or leverage))
        res = await self.client.exchange.set_leverage(m.id, leverage, isolated=not cross)
        await self.load_leverages(force=True)
        return res.get("status") in ("APPLIED", "ACK")
