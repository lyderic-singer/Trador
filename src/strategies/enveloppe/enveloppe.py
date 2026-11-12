import asyncio
import traceback
from typing import Iterable
from strategies.account import AccountTracker
from adapters.exchange import Exchange
from strategies.executor import Executor
from strategies.market import MarketData
from adapters.models import Modify, OpenOrder, Order, Side, TIF
from core.utils import cancel, remaining
from log.logger import Logger
from strategies.enveloppe.parameters import Parameters


class Grid:
    def __init__(self, levels: int, spread: float, offset: float, alpha: float):
        self.levels = levels
        self.spread = spread
        self.offset = offset
        self.alpha = alpha
        self.bids: list[float] = []
        self.asks: list[float] = []
        if self.levels <= 1:
            self.geo = 1.0
        elif self.alpha == 1:
            self.geo = self.levels - 1
        else:
            self.geo = (self.alpha ** (self.levels - 1) - 1) / (self.alpha - 1)

    def update(self, ema: float):
        raw = self.spread * ema / self.geo
        offset = self.offset * raw
        bids = []
        px = ema - offset
        for i in range(self.levels):
            bids.append(px)
            px -= raw * (self.alpha ** i)
        self.bids = list(reversed(bids))
        asks = []
        px = ema + offset
        for i in range(self.levels):
            asks.append(px)
            px += raw * (self.alpha ** i)
        self.asks = asks


class Enveloppe:
    def __init__(self, exchange: Exchange, symbol: str, parameters: Parameters | None = None):
        self.p = p = parameters or Parameters()
        self.exchange = exchange
        self.symbol = exchange.canonical(symbol)
        self.instrument = exchange.instrument(self.symbol)
        self.MAKER_FEES = p.MAKER_FEES if p.MAKER_FEES is not None else self.instrument.maker_fee
        self.MIN_VALUE = max(p.MIN_VALUE, self.instrument.min_notional)
        self.cycles = 0
        self.placed = 0
        self.modified = 0
        self.passed = 0
        self.placed_error = 0
        self.modified_error = 0
        self.task = None
        self.running = False
        self.last_ema = None
        self.to_modify = False
        self.beta = p.SKEW ** (1 / (p.LEVELS - 1)) if p.LEVELS > 1 else 1.0
        self.geo = (1 - self.beta ** p.LEVELS) / (1 - self.beta) if self.beta != 1.0 else float(p.LEVELS)
        self.grid = Grid(p.LEVELS, p.SPREAD, p.OFFSET, p.ALPHA)
        self._log = Logger(f"Enveloppe {exchange.name}:{self.symbol}", exchange.messenger)
        self.executor = Executor(exchange, p.COOLDOWN)
        self.market = MarketData(exchange, self.symbol, p.WINDOW, p.TIMEFRAME)
        self.account = AccountTracker(exchange, [self.symbol])

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self.exchange.start()
        await self.market.start()
        await self.account.start()
        await self.executor.start()
        self.running = True
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        if self.p.CLEANUP:
            try:
                await self.exchange.cancel_all(self.symbol)
            except Exception as e:
                await self._log.error(f"cleanup failed: {e}")
        await self.market.stop()
        await self.account.stop()
        await self.executor.stop()
        await self._log.info(
            f"{self.cycles} cycles: {self.passed} pass; "
            f"{self.placed}/{self.placed + self.placed_error} place and "
            f"{self.modified}/{self.modified + self.modified_error} modify")
        await self._log.success("Stopped")

    def order(self, is_buy: bool, size: float, price: float) -> Order:
        side = Side.of(is_buy)
        return Order(self.symbol, side, self.instrument.round_size(size), self.instrument.round_price(price, side), TIF.ALO)

    def adjust(self, order: OpenOrder, size: float, price: float) -> Modify:
        return Modify(order, self.instrument.round_size(size), self.instrument.round_price(price, order.side), TIF.ALO)

    async def state(self):
        orders = sorted(await self.exchange.open_orders(self.symbol), key=lambda o: o.price)
        bids, asks = [o for o in orders if o.is_buy], [o for o in orders if not o.is_buy]
        position = self.account.position(self.symbol)
        power = self.account.power(self.symbol)
        mid = self.market.mid
        ema = self.market.ema_close()
        best_bid = self.market.best_bid
        best_ask = self.market.best_ask
        if not (mid and ema and best_bid and best_ask):
            raise ValueError("market not inited")
        return (bids, asks), power, (position.size, position.signed_value), (mid, ema, best_bid, best_ask)

    def available(self, bids_value: float, asks_value: float, power: float, position_value: float, mid: float):
        longable = max(power - position_value - bids_value, 0)
        shortable = max(power + position_value - asks_value, 0)
        factor = self.p.CAP / mid
        return factor * longable, factor * shortable

    def valid(self, side: Side, price: float, size: float, best_bid, best_ask) -> bool:
        value = price * size
        if side.is_buy and best_ask:
            return value > self.MIN_VALUE and price < best_ask
        if not side.is_buy and best_bid:
            return value > self.MIN_VALUE and price > best_bid
        return False

    def sizes(self, available: float) -> list[float]:
        n = self.p.LEVELS
        if n == 0:
            return []
        if self.beta == 1.0 or n == 1:
            return [available / n] * n
        first = available / self.geo
        return [first * (self.beta ** i) for i in range(n)]

    def compute_place(self, bids: list[OpenOrder], asks: list[OpenOrder], longable: float, shortable: float,
                      longcap: float, shortcap: float, mid: float, ema: float, best_bid: float, best_ask: float) -> list[Order]:
        self.grid.update(ema)
        orders: list[Order] = []

        def process(existing: list[OpenOrder], grid: list[float], capacity: float, budget: float, is_buy: bool):
            if capacity <= 0 or budget <= 0:
                return
            sizes = self.sizes(capacity)
            nominal = list(reversed(sizes)) if is_buy else sizes
            missing_px = remaining([o.price for o in existing], grid)
            missing, ref, m = [], 0.0, 0
            for k, px in enumerate(grid):
                if m < len(missing_px) and px == missing_px[m]:
                    missing.append(k)
                    m += 1
                else:
                    ref += nominal[k]
            if not missing:
                return
            used = sum(o.size for o in existing)
            want = sum(nominal[k] for k in missing)
            scale = min(used / ref, 1.0) if ref > 0 else 1.0
            scale = min(scale, budget / want) if want > 0 else 0.0
            for k in missing:
                o = self.order(is_buy, scale * nominal[k], grid[k])
                if self.valid(o.side, o.price, o.size, best_bid, best_ask):
                    orders.append(o)

        if self.p.INV:
            if ema and ema < mid:
                process(bids, self.grid.bids, longcap, longable, True)
            else:
                process(asks, self.grid.asks, shortcap, shortable, False)
        else:
            process(bids, self.grid.bids, longcap, longable, True)
            process(asks, self.grid.asks, shortcap, shortable, False)
        return orders

    def compute_modify(self, bids: list[OpenOrder], asks: list[OpenOrder], longable: float, shortable: float,
                       mid: float, ema: float, best_bid: float, best_ask: float) -> list[Modify]:
        self.grid.update(ema)
        modifies: list[Modify] = []

        def process(orders: Iterable[OpenOrder], n: int, available: float, price):
            sizes = self.sizes(available)
            total = sum(sizes[-n:]) if n else 0
            scale = available / total if total > 0 else 0.0
            for i, o in enumerate(orders):
                mod = self.adjust(o, scale * sizes[-(i + 1)], price(i))
                if self.valid(mod.side, mod.price, mod.size, best_bid, best_ask):
                    modifies.append(mod)

        def bid_price(i: int) -> float:
            return min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid)

        def ask_price(i: int) -> float:
            return max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid)

        if self.p.INV:
            if ema and ema < mid:
                process(bids, len(bids), longable, bid_price)
            else:
                process(reversed(asks), len(asks), shortable, ask_price)
        else:
            process(bids, len(bids), longable, bid_price)
            process(reversed(asks), len(asks), shortable, ask_price)
        return modifies

    async def report(self, kind: str, items: list, executed: list[bool], bids, asks, longable, shortable, mid):
        position = self.account.position(self.symbol)
        detail = ";\n".join(f"{'Bid' if it.side.is_buy else 'Ask'} {'success' if ok else 'fail'} size={it.size} value={it.value}"
                            for it, ok in zip(items, executed))
        await self._log.warning(
            f"{len(executed) - sum(executed)}/{len(executed)} {kind} not executed\n"
            f"longable: {longable * mid / self.p.CAP}, shortable: {shortable * mid / self.p.CAP}\n"
            f"bids: value={sum(b.value for b in bids)} count={len(bids)}, asks value={sum(a.value for a in asks)} count={len(asks)}\n"
            f"position: size={position.size} value={position.value} pnl={position.pnl}\n"
            f"detail: {detail}")

    async def place(self, bids, asks, power, position_value, mid, ema, best_bid, best_ask):
        bids_value, asks_value = sum(b.value for b in bids), sum(a.value for a in asks)
        longcap, shortcap = self.available(0.0, 0.0, power, position_value, mid)
        longable, shortable = self.available(bids_value, asks_value, power, position_value, mid)
        orders = self.compute_place(bids, asks, longable, shortable, longcap, shortcap, mid, ema, best_bid, best_ask)
        if not orders:
            self.passed += 1
            return
        executed = [r.ok for r in await self.executor.place(orders)]
        self.placed += sum(executed)
        self.placed_error += len(executed) - sum(executed)
        if not all(executed):
            await self.report("orders", orders, executed, bids, asks, longable, shortable, mid)
        if executed:
            self.to_modify = True

    async def modify(self, bids, asks, power, position_value, mid, ema, best_bid, best_ask):
        longable, shortable = self.available(0.0, 0.0, power, position_value, mid)
        modifies = self.compute_modify(bids, asks, longable, shortable, mid, ema, best_bid, best_ask)
        if not modifies:
            self.passed += 1
            return
        executed = [r.ok for r in await self.executor.modify(modifies)]
        self.modified += sum(executed)
        self.modified_error += len(executed) - sum(executed)
        if not all(executed):
            await self.report("modifies", modifies, executed, bids, asks, longable, shortable, mid)
        self.last_ema = self.market.ema_close()
        if executed:
            self.to_modify = False

    async def run(self):
        levels = self.p.LEVELS
        try:
            while self.running:
                try:
                    (bids, asks), power, (_, position_value), (mid, ema, best_bid, best_ask) = await self.state()
                    args = (bids, asks, power, position_value, mid, ema, best_bid, best_ask)
                    if self.to_modify:
                        await self.modify(*args)
                    elif mid and ((mid > ema and len(bids) < levels) or (mid < ema and len(asks) < levels)):
                        await self.place(*args)
                    elif self.last_ema and ema and (abs(self.last_ema - ema) / ema) < self.p.THRESHOLD:
                        self.passed += 1
                    else:
                        await self.modify(*args)
                    self.cycles += 1
                except Exception:
                    await self._log.error(f"{traceback.format_exc()}")
                finally:
                    await asyncio.sleep(self.p.POLL_DELAY)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
