import itertools
import math
import random
import time
from collections import Counter, defaultdict
from adapters.exchange import Exchange
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Instrument, Modify, OpenOrder, Order, OrderUpdate,
                             Position, Result, Side, TIF)
from core.utils import call
from log.messenger import Messenger


class Book:
    def __init__(self, symbol: str, mid: float, tick: float, depth: float):
        self.symbol = symbol
        self.tick = tick
        self.bid = math.floor((mid - tick / 2) / tick + 1e-9) * tick
        self.ask = self.bid + tick
        self.bid_size = depth / mid
        self.ask_size = depth / mid
        self.oracle = mid
        self.closed = False
        self.lower: float | None = None
        self.upper: float | None = None
        self.online = True

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


class Simulator(Exchange):
    name = "sim"

    def __init__(self, prices: dict[str, float], tick: float = 0.01, step: float = 1e-7, depth: float = 20_000.0,
                 equity: float = 100_000.0, leverage: float = 10.0, streams: bool = True, deadman: bool = True,
                 taker: float = 0.0004):
        self.messenger = Messenger(None)
        self.books = {s: Book(s, p, tick, depth) for s, p in prices.items()}
        self.step = step
        self.start_equity = equity
        self.lev = leverage
        self.streams = streams
        self.dms = deadman
        self.taker = taker
        self.orders: dict[str, OpenOrder] = {}
        self.qty: dict[str, float] = defaultdict(float)
        self.cash: dict[str, float] = defaultdict(float)
        self.ids = itertools.count(1)
        self.trades = itertools.count(1)
        self.bbo_listeners = defaultdict(list)
        self.fill_listeners: list = []
        self.order_listeners: list = []
        self.oracle_listeners = defaultdict(list)
        self.account_listeners: list = []
        self.online = True
        self.pools: dict[str, int] | None = None
        self.deadmans: list = []
        self.actions: Counter = Counter()
        self.rejects: Counter = Counter()

    async def start(self): pass
    async def stop(self): pass

    def canonical(self, symbol: str) -> str:
        if symbol not in self.books:
            raise ValueError(f"unknown {symbol}")
        return symbol

    def tradable(self, symbol: str) -> bool:
        return self.books[symbol].online

    @property
    def connected(self) -> bool:
        return self.online

    def budget(self) -> dict[str, int] | None:
        return dict(self.pools) if self.pools is not None else None

    def instrument(self, symbol: str) -> Instrument:
        b = self.books[symbol]

        def pricer(px, side, passive):
            n = px / b.tick
            if side is None:
                n = round(n)
            elif passive:
                n = math.floor(n + 1e-9) if side.is_buy else math.ceil(n - 1e-9)
            else:
                n = math.ceil(n - 1e-9) if side.is_buy else math.floor(n + 1e-9)
            return round(n * b.tick, 10)

        def sizer(sz):
            return round(math.floor(sz / self.step + 1e-9) * self.step, 10)
        return Instrument("sim", symbol, 0.0, self.taker, 5.0, self.step, pricer, sizer, lambda px: b.tick,
                          b.closed, b.lower, b.upper)

    def ours(self, symbol: str, side: Side) -> list[OpenOrder]:
        return [o for o in self.orders.values() if o.symbol == symbol and o.side == side]

    def top(self, symbol: str) -> BBO:
        b = self.books[symbol]
        bids, asks = self.ours(symbol, Side.BUY), self.ours(symbol, Side.SELL)
        best_bid = max([b.bid] + [o.price for o in bids])
        best_ask = min([b.ask] + [o.price for o in asks])
        near = b.tick / 2
        bid_size = (b.bid_size if abs(b.bid - best_bid) < near else 0) + sum(o.size for o in bids if abs(o.price - best_bid) < near)
        ask_size = (b.ask_size if abs(b.ask - best_ask) < near else 0) + sum(o.size for o in asks if abs(o.price - best_ask) < near)
        return BBO(symbol, best_bid, best_ask, bid_size, ask_size)

    async def publish(self, symbol: str):
        bbo = self.top(symbol)
        for listener in self.bbo_listeners[symbol]:
            await call(listener, bbo)

    async def subscribe_bbo(self, symbol, listener):
        self.bbo_listeners[symbol].append(listener)
        await call(listener, self.top(symbol))

    async def candles(self, symbol, timeframe, count):
        rng = random.Random(symbol)
        mid = self.books[symbol].mid
        now = time.time() // 60 * 60
        out, px = [], mid
        for i in range(count):
            px *= math.exp(rng.gauss(0, 1.5e-4))
            out.append(Candle(now - 60 * (count - i), px, px, px, px))
        return out

    async def mid(self, symbol):
        return self.books[symbol].mid

    def state(self) -> AccountState:
        pnl = sum(self.cash[s] + self.qty[s] * self.books[s].mid for s in self.books)
        positions = {s: Position(s, self.qty[s], None, abs(self.qty[s]) * self.books[s].mid, self.lev)
                     for s in self.books if abs(self.qty[s]) > 1e-12}
        gross = sum(p.value for p in positions.values())
        equity = self.start_equity + pnl
        return AccountState("sim", equity, equity - gross / self.lev, gross / self.lev, positions, {s: self.lev for s in self.books})

    async def push(self):
        state = self.state()
        for listener in self.account_listeners:
            await call(listener, state)

    async def subscribe_account(self, symbols, listener):
        self.account_listeners.append(listener)
        await call(listener, self.state())

    async def account(self, symbols):
        return self.state()

    async def open_orders(self, symbol=None):
        return [o for o in self.orders.values() if symbol is None or o.symbol == symbol]

    async def subscribe_fills(self, symbols, listener):
        self.fill_listeners.append(listener)

    async def subscribe_orders(self, symbols, listener) -> bool:
        self.order_listeners.append(listener)
        return self.streams

    async def subscribe_oracle(self, symbol, listener) -> bool:
        self.oracle_listeners[symbol].append(listener)
        return True

    async def schedule_cancel(self, seconds) -> bool:
        self.deadmans.append(seconds)
        return self.dms

    async def emit(self, o: OpenOrder, status: str, size: float | None = None):
        if not self.streams:
            return
        update = OrderUpdate(o.symbol, o.oid, o.side, o.price, size if size is not None else o.size, o.size, status, o.client)
        for listener in self.order_listeners:
            await call(listener, update)

    def charge(self, kind: str, units: int = 1):
        self.actions[kind] += 1
        if self.pools is not None:
            pool = "cancel" if kind in ("cancel", "purge") else "order"
            self.pools[pool] = self.pools.get(pool, 0) - units

    def crosses(self, symbol: str, side: Side, price: float) -> bool:
        top = self.top(symbol)
        return price >= top.ask if side.is_buy else price <= top.bid

    async def execute(self, symbol: str, side: Side, size: float, price: float, maker: bool, o: OpenOrder | None = None):
        self.qty[symbol] += side.sign * size
        self.cash[symbol] -= side.sign * size * price + (0 if maker else size * price * self.taker)
        if self.pools is not None:
            for pool in self.pools:
                self.pools[pool] += int(size * price / 0.10)
        fill = Fill(symbol, side, size, price, o.oid if o else None, o.client if o else None, f"t{next(self.trades)}",
                    maker, o.size if o else 0.0)
        for listener in self.fill_listeners:
            await call(listener, fill)

    async def place(self, orders: list[Order]) -> list[Result]:
        out, touched = [], set()
        for o in orders:
            self.charge("place")
            b = self.books[o.symbol]
            if o.tif == TIF.ALO:
                if self.crosses(o.symbol, o.side, o.price) or not b.online:
                    self.rejects["post only"] += 1
                    out.append(Result(False, error="POST_ONLY_WOULD_CROSS"))
                    continue
                oid = str(next(self.ids))
                resting = OpenOrder(o.symbol, oid, o.side, o.price, o.size, TIF.ALO, o.reduce, o.client,
                                    {"goodTilTime": 1, "clientId": o.client})
                self.orders[oid] = resting
                out.append(Result(True, oid, "ACK", raw={"orderId": oid}))
                touched.add(o.symbol)
                await self.emit(resting, "open")
            else:
                ext = b.ask if o.side.is_buy else b.bid
                if (o.side.is_buy and o.price >= ext) or (not o.side.is_buy and o.price <= ext):
                    await self.execute(o.symbol, o.side, o.size, ext, False)
                    out.append(Result(True, str(next(self.ids)), "filled", o.size, ext))
                else:
                    out.append(Result(False, error="IOC_CANCELED"))
        for symbol in touched:
            await self.publish(symbol)
        await self.push()
        return out

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        out, touched = [], set()
        for m in modifies:
            self.charge("modify")
            o = self.orders.get(m.order.oid)
            if o is None:
                out.append(Result(False, error="ORDER_NOT_FOUND_FOR_MODIFY"))
                continue
            del self.orders[o.oid]
            if self.crosses(o.symbol, o.side, m.price):
                self.orders[o.oid] = o
                self.rejects["post only"] += 1
                out.append(Result(False, error="POST_ONLY_WOULD_CROSS"))
                continue
            await self.emit(o, "replaced")
            oid = str(next(self.ids))
            new = OpenOrder(o.symbol, oid, o.side, m.price, m.size, TIF.ALO, o.reduce, o.client, dict(o.raw))
            self.orders[oid] = new
            out.append(Result(True, oid, "ACK", raw={"orderId": oid}))
            touched.add(o.symbol)
            await self.emit(new, "open")
        for symbol in touched:
            await self.publish(symbol)
        return out

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        out, touched = [], set()
        for c in cancels:
            self.charge("cancel")
            o = self.orders.pop(c.oid, None) if c.oid else None
            if o is None and c.client:
                o = next((x for x in self.orders.values() if x.client == c.client), None)
                if o:
                    self.orders.pop(o.oid)
            if o is None:
                out.append(Result(False, error="ORDER_NOT_FOUND"))
                continue
            out.append(Result(True, o.oid, "CANCEL_ACKNOWLEDGED"))
            touched.add(o.symbol)
            await self.emit(o, "canceled")
        for symbol in touched:
            await self.publish(symbol)
        return out

    async def purge(self):
        self.charge("purge", 1000)
        for o in list(self.orders.values()):
            self.orders.pop(o.oid)
            await self.emit(o, "canceled")
        for symbol in self.books:
            await self.publish(symbol)

    async def fill(self, o: OpenOrder, size: float):
        o.size = round(o.size - size, 12)
        if o.size <= 1e-12:
            self.orders.pop(o.oid, None)
            o.size = 0.0
        await self.execute(o.symbol, o.side, size, o.price, True, o)
        await self.emit(o, "filled" if o.size == 0 else "open")

    async def take(self, symbol: str, side: Side, size: float):
        b = self.books[symbol]
        resting = sorted(self.ours(symbol, side.opposite), key=lambda o: (o.price if side.is_buy else -o.price, int(o.oid)))
        level = b.ask if side.is_buy else b.bid
        external = b.ask_size if side.is_buy else b.bid_size
        left = size
        for o in resting:
            better = o.price < level - b.tick / 2 if side.is_buy else o.price > level + b.tick / 2
            same = abs(o.price - level) < b.tick / 2
            if not (better or same):
                break
            if same and external > 0:
                used = min(left, external)
                left -= used
                external -= used
            if left <= 1e-12:
                break
            amount = min(left, o.size)
            await self.fill(o, amount)
            left -= amount
        await self.publish(symbol)
        await self.push()
        return size - left

    async def move(self, symbol: str, mid: float, oracle: bool = True):
        b = self.books[symbol]
        b.bid = math.floor((mid - b.tick / 2) / b.tick + 1e-9) * b.tick
        b.ask = b.bid + b.tick
        for o in list(self.orders.values()):
            if o.symbol != symbol:
                continue
            if (o.side.is_buy and o.price >= b.ask) or (not o.side.is_buy and o.price <= b.bid):
                await self.fill(o, o.size)
        if oracle:
            await self.set_oracle(symbol, mid)
        await self.publish(symbol)
        await self.push()

    async def set_oracle(self, symbol: str, price: float):
        self.books[symbol].oracle = price
        for listener in self.oracle_listeners[symbol]:
            await call(listener, symbol, price)
