import asyncio
import traceback
from typing import Iterable, Iterator
from core.HL.utils.functions import cancel, get_order, get_modify, closests
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from core.HL.utils.types import Modify, Order
from log.logger import Logger

class Grid:
    def __init__(self, market: MarketHL, levels: int, width_min: float, width_max: float, min_span: float):
        self.running = False
        self.market = market
        self.levels = levels
        self.width_min = width_min
        self.width_max = width_max
        self.min_span = min_span
        self.width_range = width_max - width_min
        self.bids = []
        self.asks = []

    def update(self):
        if not self.running:
            raise ValueError("Must start before request deltas")
        ema = self.market.ema_close()
        close = self.market.ema_close()
        high = self.market.ema_high()
        low = self.market.ema_low()
        raw_bid = high + close - 2 * low # TODO try another formula to avoid losing during dumps
        raw_ask =  2 * high - close - low # TODO try another formula to avoid losing during dumps
        offset_bid = self.width_min * raw_bid
        offset_ask = self.width_min * raw_ask
        if self.width_max * raw_bid / ema > self.min_span:
            delta_bid = self.width_range * raw_bid / self.levels
        else:
            delta_bid = (self.min_span * ema - offset_bid) / self.levels
        if self.width_max * raw_ask / ema > self.min_span:
            delta_ask = self.width_range * raw_ask / self.levels
        else:
            delta_ask = (self.min_span * ema - offset_ask) / self.levels
        self.bids = [ema - offset_bid - (delta_bid * i) for i in range(self.levels -1, -1, -1)] # ascending
        self.asks = [ema + offset_ask + (delta_ask * i) for i in range(self.levels)] # ascending

class EnveloppeInv: # place orders grid each separed with delta
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.WIDTH_MIN = 0.05     # minimum multiplier for grid spread: controls tightness of orders around mid
        self.WIDTH_MAX = 8        # maximum multiplier for grid spread: controls maximum dispersion in volatile regimes
        self.MIN_SPAN = 0.012     # 0.8% | minimum distance between the first and the last placed orders
        self.LEVELS = 24          # orders count of each side
        self.CAP = 0.85           # 85% | max capacity of portfolio used
        self.MIN_VALUE = 10.5     # minimum order notional value in $ to avoid overfragmentation / fees inefficiency
        self.POLL_DELAY = 10.1    # delay in seconds between each grid evaluation cycle
        self.THRESHOLD = 0.0001   # 0.01% | relative SMA change threshold to trigger rebalancing
        self.TIMEFRAME = "5m"     # market data candle timeframe used for indicators (SMA/EMA)
        self.WINDOW = 40          # rolling window size for indicator (SMA/EMA period)
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
        self.hl = hl
        self.address = hl.address
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.dex = self.instrument.dex
        self.executor = Executor(hl)
        self.market = MarketHL(self.instrument, hl, self.WINDOW, self.TIMEFRAME)
        self.account = AccountPerpDexHL(self.dex, [self.instrument], hl)
        self.grid = Grid(self.market, self.LEVELS, self.WIDTH_MIN, self.WIDTH_MAX, self.MIN_SPAN)

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

    def sizes(self, mid: float, bids_value: float = 0.0, asks_value: float = 0.0):
        account = self.account
        position = account.positions.get(self.instrument.coin)
        if position is None: raise ValueError("position not found")
        position_value = position.value if position.size >= 0 else -position.value
        leveraged_account_value = account.account.value * position.leverage
        longable = max(leveraged_account_value - position_value - bids_value, 0) # TODO adapt for multiple coins
        shortable = max(leveraged_account_value + position_value - asks_value, 0) # TODO adapt for multiple coins
        factor = self.CAP / mid # normalize $ to get [COIN]
        return factor * longable, factor * shortable # max_sz_bid, max_sz_ask

    async def state(self):
        orders = await self.hl.info.open_orders(self.address, dex=self.dex)
        orders.sort(key=lambda x: float(x["limitPx"]))
        bids, bids_px, bids_value, asks, asks_px, asks_value = [], [], 0, [], [], 0
        for o in orders:
            px = float(o["limitPx"])
            sz = float(o["sz"])
            if o["side"] == 'B':
                bids.append(o)
                bids_px.append(px)
                bids_value += px * sz
            else:
                asks.append(o)
                asks_px.append(float(o["limitPx"]))
                asks_value += px * sz
        return bids, bids_px, bids_value, asks, asks_px, asks_value

    def valid(self, order: Order):
        is_buy = order.get("b")
        px = float(order.get("p", 0))
        sz = float(order.get("s", 0))
        value = px * sz
        if is_buy and self.market.best_ask:
            return (value > self.MIN_VALUE) and (px < self.market.best_ask)
        elif not is_buy and self.market.best_bid:
            return (value > self.MIN_VALUE) and (px > self.market.best_bid)
        else:
            return False

    async def modify(self, bids: list, asks: list, bids_value: float, asks_value: float): # both bids and asks in ascending order
        self.grid.update()
        mid = self.market.mid
        ema = self.market.ema_close()
        if mid is None: raise ValueError("market nor inited")
        longable, shortable = self.sizes(mid, 0, 0) # disregard bids and asks value because they gonna be modified
        modifies: list[Modify] = []
        min_sz = self.MIN_VALUE / mid
        def price_bid(i: int): return min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid)
        def price_ask(i: int): return max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid)
        def process(orders: Iterable, n: int, available: float, price_fn):
            sz = available / n if (n > 0) and (available > n * min_sz) else 0
            for i, o in enumerate(orders):
                modify = get_modify(o, self.instrument, sz, price_fn(i), "Alo")
                if self.valid(modify["order"]): modifies.append(modify)
        if ema and ema > mid: process(bids, len(bids), longable, price_bid)
        else: process(reversed(asks), len(asks), shortable, price_ask)
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

    async def place(self, bids_px: list[float], asks_px: list[float], bids_value: float, asks_value: float): # both bids and asks in ascending order
        self.grid.update()
        mid = self.market.mid
        ema = self.market.ema_close()
        if mid is None: raise ValueError("market nor inited")
        longable, shortable = self.sizes(mid, bids_value, asks_value)
        orders: list[Order] = []
        new_bids_px = closests(bids_px, self.grid.bids)
        new_asks_px = closests(asks_px, self.grid.asks)
        def process(prices: list[float], available: float, is_buy: bool):
            sz = available / len(prices) if prices else 0
            for px in prices:
                order = get_order(self.instrument, is_buy, sz, px, {"limit": {"tif": "Alo"}})
                if self.valid(order): orders.append(order)
        if ema and ema > mid: process(new_bids_px, longable, True)
        else: process(new_asks_px, shortable, False)
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

    async def run(self):
        try:
            while self.running:
                try:
                    bids, bids_px, bids_value, asks, asks_px, asks_value = await self.state()
                    mid = self.market.mid
                    ema = self.market.ema_close()
                    print(mid, ema)
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
