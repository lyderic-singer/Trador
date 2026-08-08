import asyncio
import traceback
from typing import Iterable
from core.HL.utils.functions import cancel, get_order, get_modify, remaining
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from core.HL.utils.types import Modify, Order
from log.logger import Logger

class Grid:
    def __init__(self, levels: int, spread: float, offset: float, alpha: float):
        self.running = False
        self.levels = levels
        self.spread = spread
        self.offset = offset
        self.alpha = alpha
        self.bids = []
        self.asks = []
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
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.SPREAD = 0.02        # 4.5% | minimum distance between the first and the last placed orders
        self.OFFSET = 0.0001      # 0.01% spread from ema for first
        self.ALPHA = 3.6          # exponential price factor between levels (>1 -> more and more spaced)
        self.SKEW = 10            # size ratio between last and first order (SKEW=10 -> last order is 10x bigger than first)
        self.LEVELS = 3           # orders count of each side
        self.CAP = 1              # 85% | max capacity of portfolio used
        self.MIN_VALUE = 10.5     # minimum order notional value in $ to avoid overfragmentation / fees inefficiency
        self.POLL_DELAY = 10.1    # delay in seconds between each grid evaluation cycle
        self.THRESHOLD = 0.0001   # 0.01% | relative EMA change threshold to trigger rebalancing
        self.TIMEFRAME = "5m"     # market data candle timeframe used for indicators (SMA/EMA)
        self.WINDOW = 40          # rolling window size for indicator (SMA/EMA period)
        self.INV = True           # stop place / modify in the part against mean if activated
        # ==== STATS ====
        self.cycles = 0           # nb of cycles for stats
        self.placed = 0           # nb of place for stats
        self.modified = 0         # nb of modify for stats
        self.passed = 0           # nb of pass for stats
        self.placed_error = 0     # nb of errors while place for stats
        self.modified_error = 0   # nb of errors while modify for stats
        # ==== UTILS ====
        self.task = None
        self.running = False
        self.last_ema = None
        self.beta = self.SKEW ** (1 / (self.LEVELS - 1)) if self.LEVELS > 1 else 1.0
        self.geo = (1 - self.beta ** self.LEVELS) / (1 - self.beta) if self.beta != 1.0 else float(self.LEVELS)
        self.grid = Grid(self.LEVELS, self.SPREAD, self.OFFSET, self.ALPHA)
        # ==== CORE ====
        self.hl = hl
        self.address = hl.address
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.dex = self.instrument.dex
        self.executor = Executor(hl)
        self.market = MarketHL(self.instrument, hl, self.WINDOW, self.TIMEFRAME)
        self.account = AccountPerpDexHL(self.dex, [self.instrument], hl)

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self.market.start()
        self.grid.running = True
        await self.account.start()
        await self.executor.start()
        self.running = True
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self.market.stop()
        await self.account.stop()
        await self._log.info(
            f"{self.cycles} cycles: {self.passed} pass; "
            f"{self.placed}/{self.placed + self.placed_error} place and "
            f"{self.modified}/{self.modified + self.modified_error} modify")
        await self._log.success("Stopped")
    
    async def state(self):
        orders = await self.hl.info.open_orders(self.address, dex=self.dex)
        orders.sort(key=lambda x: float(x["limitPx"]))
        bids, asks = [], []
        for o in orders:
            if o["side"] == 'B':
                bids.append(o)
            else:
                asks.append(o)

        position = self.account.positions.get(self.instrument.coin)
        if position is None: raise ValueError("position not found")
        position_value = position.value if position.size >= 0 else -position.value
        power = self.account.account.value * position.leverage

        mid = self.market.mid
        ema = self.market.ema_close()
        best_bid = self.market.best_bid
        best_ask = self.market.best_ask
        if not (mid and ema and best_bid and best_ask): raise ValueError("market nor inited")

        return (bids, asks), power, (position.size, position_value), (mid, ema, best_bid, best_ask)

    def available(self, bids_value: float, asks_value: float, power: float, position_size: float, position_value: float, mid: float):
        position_value = position_value if position_size >= 0 else -position_value
        longable = max(power - position_value - bids_value, 0) # TODO adapt for multiple coins
        shortable = max(power + position_value - asks_value, 0) # TODO adapt for multiple coins
        factor = self.CAP / mid # normalize $ to get [COIN]
        return factor * longable, factor * shortable # max_sz_bid, max_sz_ask

    def valid(self, order: Order, best_bid, best_ask):
        is_buy = order.get("b")
        px = float(order.get("p", 0))
        sz = float(order.get("s", 0))
        value = px * sz
        if is_buy and best_ask:
            return (value > self.MIN_VALUE) and (px < best_ask)
        elif not is_buy and best_bid:
            return (value > self.MIN_VALUE) and (px > best_bid)
        else:
            return False

    def sizes(self, available: float) -> list[float]:
        if self.LEVELS == 0: return []
        if self.beta == 1.0 or self.LEVELS == 1:
            unit = available / self.LEVELS
            return [unit] * self.LEVELS
        s0 = available / self.geo
        return [s0 * (self.beta ** i) for i in range(self.LEVELS)]

    def compute_place(self, bids: list, asks: list, longable: float, shortable: float, mid: float, ema: float, best_bid: float, best_ask: float):
        mid = (best_bid + best_ask) / 2
        bids_px, asks_px = [b["limitPx"] for b in bids], [a["limitPx"] for a in asks]
        self.grid.update(ema)
        orders: list[Order] = []
        def process(existing, existing_px, grid_px, available, is_buy):
            sizes = self.sizes(available)
            nominal = list(reversed(sizes)) if is_buy else sizes
            missing_px = remaining(existing_px, grid_px)
            missing, ref, m = [], 0.0, 0
            for k, px in enumerate(grid_px):
                if m < len(missing_px) and px == missing_px[m]:
                    missing.append(k)
                    m += 1
                else:
                    ref += nominal[k]
            used = sum(o[0] for o in existing)
            scale = min(used / ref, 1.0) if ref > 0 else 1.0
            for k in missing:
                sz = scale * nominal[k]
                order = get_order(self.instrument, is_buy, sz, grid_px[k], {"limit": {"tif": "Alo"}})
                if self.valid(order, best_bid, best_ask):
                    orders.append(order)
        if self.INV:
            if ema and ema < mid: process(bids, bids_px, self.grid.bids, longable, True)
            else: process(asks, asks_px, self.grid.asks, shortable, False)
        else:
            process(bids, bids_px, self.grid.bids, longable, True)
            process(asks, asks_px, self.grid.asks, shortable, False)
        return orders

    def compute_modify(self, bids: list, asks: list, longable: float, shortable: float, mid: float, ema: float, best_bid: float, best_ask: float): # both bids and asks in ascending order
        self.grid.update(ema)
        modifies: list[Modify] = []
        def process(orders: Iterable, n: int, available: float, price_fn):
            sizes = self.sizes(available)
            scale = max(available / sum(sizes[-n:]), 1)
            for i, o in enumerate(orders):
                sz = scale * sizes[-(i+1)]
                modify = get_modify(o, self.instrument, sz, price_fn(i), "Alo")
                if self.valid(modify["order"], best_bid, best_ask):
                    modifies.append(modify)
        if self.INV:
            if ema and ema < mid: process(bids, len(bids), longable, lambda i: min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid))
            else: process(reversed(asks), len(asks), shortable, lambda i: max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid))
        else:
            process(bids, len(bids), longable, lambda i: min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid))
            process(reversed(asks), len(asks), shortable, lambda i: max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid))
        return modifies

    async def place(self, bids, asks, power, position_size, position_value, mid, ema, best_bid, best_ask):
        longable, shortable = self.available(sum([sz * px for sz, px in bids]), sum([sz * px for sz, px in asks]), power, position_size, position_value, mid)
        orders = self.compute_place(bids, asks, longable, shortable, mid, ema, best_bid, best_ask)
        if orders:
            executed, error = await self.executor.batch_order(orders)
            if error:
                errors = len(executed) - sum(executed)
                self.placed_error += errors
                self.placed += len(executed) - errors
                position = self.account.positions[self.instrument.name]
                result = [f"{'Bid' if o['b'] else 'Ask'} {'success' if executed[i] else 'fail'} size={o['s']} value={float(o['s']) * float(o['p'])}" for i, o in enumerate(orders)]
                detail = ";\n".join(result)
                await self._log.warning(
                    f"{errors}/{len(executed)} orders not executed\n"
                    f"longable: {longable * mid / self.CAP} vs {position.longable}, shortable: {shortable * mid / self.CAP} vs {position.shortable}\n"
                    f"bids: value={bids_value} count={len(bids_px)}, asks value={asks_value} count={len(asks_px)}\n"
                    f"position: size={position.size} value={position.value} pnl={position.pnl}\n"
                    f"detail: {detail}")
            self.placed += len(executed)
            # TODO perhaps should update last_ema
        else:
            self.passed += 1
        
    async def modify(self, bids, asks, power, position_size, position_value, mid, ema, best_bid, best_ask):
        longable, shortable = self.available(0.0, 0.0, power, position_size, position_value, mid) # ignoring open orders
        modifies = self.compute_modify(bids, asks, longable, shortable, mid, ema, best_bid, best_ask)
        if modifies:
            executed, error = await self.executor.batch_modify(modifies)
            if error:
                errors = len(executed) - sum(executed)
                self.modified_error += errors
                self.modified += len(executed) - errors
                position = self.account.positions[self.instrument.name]
                result = [f"{'Bid' if o['order']['b'] else 'Ask'} {'success' if executed[i] else 'fail'} size={o['order']['s']} value={float(o['order']['s']) * float(o['order']['p'])}" for i, o in enumerate(modifies)]
                detail = ";\n".join(result)
                await self._log.warning(
                    f"{errors}/{len(executed)} modifies not executed\n"
                    f"longable: {longable * mid / self.CAP} vs {position.longable + bids_value}, shortable: {shortable * mid / self.CAP} vs {position.shortable + asks_value}\n"
                    f"bids: value={bids_value} count={len(bids)}, asks value={asks_value} count={len(asks)}\n"
                    f"position: size={position.size} value={position.value} pnl={position.pnl}\n"
                    f"detail: {detail}")
            self.last_ema = self.market.ema_close() # TODO if no modified that's not great but...
            self.modified += len(executed)
        else:
            self.passed += 1

    async def run(self):
        try:
            while self.running:
                try:
                    bids, bids_px, bids_value, asks, asks_px, asks_value = await self.state()
                    mid = self.market.mid
                    ema = self.market.ema_close()
                    if mid and ((mid > ema and len(bids) < self.LEVELS) or (mid < ema and len(asks) < self.LEVELS)):
                        await self.place(bids_px, asks_px, bids_value, asks_value)
                    elif self.last_ema and ema and (abs(self.last_ema - ema) / ema) < self.THRESHOLD:
                        self.passed += 1
                    else:
                        await self.modify(bids, asks, bids_value, asks_value)
                    self.cycles += 1
                except Exception as e:
                    await self._log.error(f"{traceback.format_exc()}")
                finally:
                    await asyncio.sleep(self.POLL_DELAY)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
