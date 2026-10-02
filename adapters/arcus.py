import asyncio
import time
from decimal import Decimal
from typing import Any
from adapters.exchange import AccountListener, BBOListener, Exchange
from adapters.models import (AccountState, BBO, Cancel, Candle, Instrument, MarginMode, Modify, OpenOrder, Order,
                         Position, Result, Side, TIF)
from core.utils import call, cancel
from core.arcus.client import ArcusClient
from core.arcus.utils.constants import ACCEPTED, TIMEFRAMES
from core.arcus.utils.types import ArcusCancel, ArcusModify, ArcusOrder, Market
from log.logger import Logger

TIFS = {TIF.GTC: "GTT", TIF.ALO: "ALO", TIF.IOC: "IOC"}
FROM_TIFS = {"GTT": TIF.GTC, "GTC": TIF.GTC, "ALO": TIF.ALO, "IOC": TIF.IOC, "FOK": TIF.IOC}


def rounding(side: Side | None, passive: bool) -> str:
    if side is None:
        return "nearest"
    return ("down" if side.is_buy else "up") if passive else ("up" if side.is_buy else "down")


class Arcus(Exchange):
    name = "arcus"

    def __init__(self, client: ArcusClient, maker_fee: float = 0.0001, taker_fee: float = 0.0004,
                 poll: float = 5.0, refresh: float = 60.0):
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

    def market(self, symbol: str) -> Market:
        return self.client.info.market(symbol)

    def instrument(self, symbol: str) -> Instrument:
        m = self.market(symbol)
        return Instrument(
            exchange=self.name, symbol=m.name, maker_fee=self.maker_fee, taker_fee=self.taker_fee,
            min_notional=float(m.min_notional), min_size=float(m.min_size),
            pricer=lambda px, side, passive: float(m.round_price(px, rounding(side, passive))),
            sizer=lambda sz: float(m.round_size(sz)),
            raw=m)

    def tradable(self, symbol: str) -> bool:
        return self.market(symbol).online

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
        await ws.positions(self.touch, self.address, self.index)
        await ws.orders(self.touch, self.address, self.index)
        await ws.account(self.touch, self.address, self.index)
        await call(listener, await self.account(self.symbols))
        self.tasks.append(asyncio.create_task(self.watch_account()))

    def touch(self, msg):
        self.dirty.set()

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
            for i, r in zip(indexes, self.parse_responses(res, len(requests))):
                results[i] = r
        return [r for r in results if r is not None]

    def convert_order(self, o: Order) -> ArcusOrder | str:
        m = self.market(o.symbol)
        price = m.round_price(o.price, rounding(o.side, o.tif != TIF.IOC))
        size = m.round_size(o.size)
        return self.validate(m, size, price, o.reduce) or ArcusOrder(m, o.side.is_buy, size, price, TIFS[o.tif], o.reduce, o.client)

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
        return await self.send(orders, self.convert_order, self.client.exchange.batch_place)

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        return await self.send(modifies, self.convert_modify, self.client.exchange.batch_modify)

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        return await self.send(cancels, lambda c: ArcusCancel(self.market(c.symbol), oid=c.oid),
                               self.client.exchange.batch_cancel)

    async def cancel_all(self, symbol: str | None = None) -> None:
        await self.client.exchange.cancel_all(self.market(symbol).id if symbol else None)

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        res = await self.client.exchange.set_leverage(self.market(symbol).id, leverage, isolated=not cross)
        await self.load_leverages(force=True)
        return res.get("status") in ("APPLIED", "ACK")
