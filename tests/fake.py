import asyncio
import itertools
import math
import time
from adapters.exchange import Exchange
from adapters.models import AccountState, BBO, Cancel, Candle, Instrument, Modify, OpenOrder, Order, Position, Result, TIF
from core.utils import call
from log.messenger import Messenger


class FakeExchange(Exchange):
    def __init__(self, name: str, symbol: str, bid: float, ask: float, equity: float = 1000, leverage: float = 5,
                 tick: float = 0.01, step: float = 0.001, taker: float = 0.0004, maker: float = 0.0001, depth: float = 10.0):
        self.name = name
        self.messenger = Messenger(None)
        self.symbol = symbol
        self.bbo = BBO(symbol, bid, ask, depth, depth)
        self.equity = equity
        self.lev = leverage
        self.tick, self.step = tick, step
        self.taker, self.maker = taker, maker
        self.orders: dict[str, OpenOrder] = {}
        self.ids = itertools.count(1)
        self.position = Position(symbol, leverage=leverage)
        self.bbo_listeners, self.listeners = [], []
        self.rejects = 0
        self.sent: list = []

    async def start(self): pass
    async def stop(self): pass

    def pricer(self, px, side, passive):
        n = px / self.tick
        if side is None:
            n = round(n)
        elif passive:
            n = math.floor(n + 1e-9) if side.is_buy else math.ceil(n - 1e-9)
        else:
            n = math.ceil(n - 1e-9) if side.is_buy else math.floor(n + 1e-9)
        return round(n * self.tick, 10)

    def sizer(self, sz):
        return round(math.floor(sz / self.step + 1e-9) * self.step, 10)

    def instrument(self, symbol: str) -> Instrument:
        return Instrument(self.name, self.symbol, self.maker, self.taker, 10.0, self.step, self.pricer, self.sizer)

    async def subscribe_bbo(self, symbol, listener):
        self.bbo_listeners.append(listener)
        await call(listener, self.bbo)

    async def set_bbo(self, bid, ask, depth=10.0):
        self.bbo = BBO(self.symbol, bid, ask, depth, depth)
        for listener in self.bbo_listeners:
            await call(listener, self.bbo)

    async def candles(self, symbol, timeframe, count):
        now = time.time() // 60 * 60
        mid = self.bbo.mid
        return [Candle(now - 60 * (count - i), mid, mid * 1.001, mid * 0.999, mid) for i in range(count)]

    async def mid(self, symbol):
        return self.bbo.mid

    def state(self) -> AccountState:
        p = self.position
        mid = self.bbo.mid
        p.value = abs(p.size) * mid
        pnl = (mid - p.entry) * p.size if p.entry else 0
        return AccountState(self.name, self.equity + pnl, self.equity - p.value / self.lev, p.value / self.lev,
                            {self.symbol: p} if p.size else {}, {self.symbol: self.lev})

    async def subscribe_account(self, symbols, listener):
        self.listeners.append(listener)
        await call(listener, self.state())

    async def account(self, symbols):
        return self.state()

    async def open_orders(self, symbol=None):
        return list(self.orders.values())

    def fill(self, side, size, price):
        p = self.position
        new = p.size + side.sign * size
        if p.size == 0 or (p.size > 0) == (side.sign > 0):
            p.entry = ((p.entry or 0) * abs(p.size) + price * size) / (abs(p.size) + size)
        elif abs(new) > 1e-12 and (new > 0) != (p.size > 0):
            p.entry = price
        p.size = round(new, 10)
        if p.size == 0:
            p.entry = None

    async def place(self, orders: list[Order]):
        self.sent += orders
        out = []
        for o in orders:
            if self.rejects > 0:
                self.rejects -= 1
                out.append(Result(False, error="rejected (test)"))
                continue
            crosses = (o.side.is_buy and o.price >= self.bbo.ask) or (not o.side.is_buy and o.price <= self.bbo.bid)
            if o.tif == TIF.ALO and crosses:
                out.append(Result(False, error="post only would cross"))
            elif crosses:
                px = self.bbo.ask if o.side.is_buy else self.bbo.bid
                self.fill(o.side, o.size, px)
                out.append(Result(True, str(next(self.ids)), "filled", o.size, px))
            elif o.tif == TIF.IOC:
                out.append(Result(False, error="ioc could not match"))
            else:
                oid = str(next(self.ids))
                self.orders[oid] = OpenOrder(o.symbol, oid, o.side, o.price, o.size, o.tif, o.reduce)
                out.append(Result(True, oid, "resting"))
        for listener in self.listeners:
            await call(listener, self.state())
        return out

    async def modify(self, modifies: list[Modify]):
        out = []
        for m in modifies:
            if m.order.oid not in self.orders:
                out.append(Result(False, error="unknown order"))
                continue
            o = self.orders.pop(m.order.oid)
            oid = str(next(self.ids))
            self.orders[oid] = OpenOrder(o.symbol, oid, o.side, m.price, m.size, o.tif, o.reduce)
            out.append(Result(True, oid, "resting"))
        return out

    async def cancel(self, cancels: list[Cancel]):
        return [Result(self.orders.pop(c.oid, None) is not None) for c in cancels]


class MultiFake(Exchange):
    """several symbols, IOC fills at the touch, optional asynchronous fills (Arcus answers ACK before filling)"""

    def __init__(self, name: str, prices: dict[str, float], equity: float = 500.0, leverage: float = 5.0,
                 spread: float = 0.0002, taker: float = 0.0, min_notional: float = 10.0, step: float = 1e-4,
                 tick: float = 0.01, depth: float = 1000.0, listings=None, delay: float = 0.0):
        self.name = name
        self.messenger = Messenger(None)
        self.tick_size, self.step = tick, step
        self.taker, self.min_notional = taker, min_notional
        self.books = {}
        for s, mid in prices.items():
            self.books[s] = [round(mid * (1 - spread / 2), 6), round(mid * (1 + spread / 2), 6), depth]
        self.start_equity = equity
        self.lev = leverage
        self.qty: dict[str, float] = {s: 0.0 for s in prices}
        self.cash = 0.0
        self.fees = 0.0
        self.rows = listings or []
        self.delay = delay
        self.rejects = 0
        self.refuse: set[str] = set()
        self.sent: list[Order] = []
        self.leverages: dict[str, int] = {}
        self.closed: set[str] = set()
        self.bbo_listeners: dict[str, list] = {}
        self.listeners: list = []
        self.history: dict[str, list[float]] = {}
        self.ids = itertools.count(1)

    async def start(self): pass
    async def stop(self): pass

    def canonical(self, symbol: str) -> str:
        if symbol not in self.books:
            raise ValueError(f"unknown {symbol}")
        return symbol

    def listings(self):
        return list(self.rows)

    def instrument(self, symbol: str) -> Instrument:
        tick = self.tick_size

        def pricer(px, side, passive):
            n = px / tick
            if side is None:
                n = round(n)
            elif passive:
                n = math.floor(n + 1e-9) if side.is_buy else math.ceil(n - 1e-9)
            else:
                n = math.ceil(n - 1e-9) if side.is_buy else math.floor(n + 1e-9)
            return round(n * tick, 10)
        return Instrument(self.name, symbol, 0.0, self.taker, self.min_notional, self.step, pricer,
                          lambda sz: round(math.floor(sz / self.step + 1e-9) * self.step, 10), lambda px: tick,
                          symbol in self.closed, max_leverage=10)

    def bbo(self, symbol: str) -> BBO:
        bid, ask, depth = self.books[symbol]
        return BBO(symbol, bid, ask, depth, depth)

    async def subscribe_bbo(self, symbol, listener):
        self.bbo_listeners.setdefault(symbol, []).append(listener)
        await call(listener, self.bbo(symbol))

    async def set_book(self, symbol: str, bid: float, ask: float, depth: float | None = None, notify: bool = True):
        self.books[symbol][:2] = [bid, ask]
        if depth is not None:
            self.books[symbol][2] = depth
        for listener in (self.bbo_listeners.get(symbol, []) if notify else []):
            await call(listener, self.bbo(symbol))

    async def book(self, symbol: str):
        self.reads = getattr(self, "reads", 0) + 1
        return self.bbo(symbol)

    async def candles(self, symbol, timeframe, count):
        now = time.time() // 300 * 300
        bid, ask, _ = self.books[symbol]
        closes = self.history.get(symbol) or [(bid + ask) / 2] * count
        return [Candle(now - 300 * (len(closes) - i), c, c, c, c) for i, c in enumerate(closes)]

    async def mid(self, symbol):
        bid, ask, _ = self.books[symbol]
        return (bid + ask) / 2

    def state(self) -> AccountState:
        positions, pnl = {}, self.cash - self.fees
        for s, q in self.qty.items():
            mid = (self.books[s][0] + self.books[s][1]) / 2
            pnl += q * mid
            if abs(q) > 1e-12:
                positions[s] = Position(s, q, mid, abs(q) * mid, self.lev)
        gross = sum(p.value for p in positions.values())
        equity = self.start_equity + pnl
        return AccountState(self.name, equity, equity - gross / self.lev, gross / self.lev, positions,
                            {s: self.lev for s in self.books})

    async def push(self):
        for listener in self.listeners:
            await call(listener, self.state())

    async def subscribe_account(self, symbols, listener):
        self.listeners.append(listener)
        await call(listener, self.state())

    async def account(self, symbols):
        return self.state()

    async def open_orders(self, symbol=None):
        return []

    async def apply(self, o: Order, px: float, wait: float):
        if wait:
            await asyncio.sleep(wait)
        self.qty[o.symbol] += o.side.sign * o.size
        self.cash -= o.side.sign * o.size * px
        self.fees += o.size * px * self.taker
        await self.push()

    async def place(self, orders: list[Order]):
        self.sent += orders
        out = []
        for o in orders:
            bid, ask, depth = self.books[o.symbol]
            if self.rejects > 0 or o.symbol in self.refuse:
                self.rejects = max(self.rejects - 1, 0)
                out.append(Result(False, error="rejected (test)"))
                continue
            px = ask if o.side.is_buy else bid
            crosses = o.price >= ask if o.side.is_buy else o.price <= bid
            if o.reduce and abs(self.qty[o.symbol]) < 1e-12:
                out.append(Result(False, error="reduce only"))
            elif not crosses:
                out.append(Result(False, error="IOC could not match"))
            elif self.delay:
                asyncio.get_running_loop().create_task(self.apply(o, px, self.delay))
                out.append(Result(True, str(next(self.ids)), "ACK"))
            else:
                await self.apply(o, px, 0)
                out.append(Result(True, str(next(self.ids)), "filled", o.size, px))
        return out

    async def modify(self, modifies):
        return [Result(False, error="unsupported")] * len(modifies)

    async def cancel(self, cancels):
        return [Result(False, error="unsupported")] * len(cancels)

    async def set_leverage(self, symbol, leverage, cross=True):
        self.leverages[symbol] = leverage
        return True
