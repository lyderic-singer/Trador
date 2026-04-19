import asyncio
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
        sma = self.market.sma_close()
        close = self.market.ema_close()
        high = self.market.ema_high()
        low = self.market.ema_low()
        raw_bid = 2 * high - low - close # TODO try another formula to avoid losing during dumps
        raw_ask =  high + close - 2 * low # TODO try another formula to avoid losing during dumps
        offset_bid = self.width_min * raw_bid
        offset_ask = self.width_min * raw_ask
        if self.width_max * raw_bid / sma > self.min_span:
            delta_bid = self.width_range * raw_bid / self.levels
        else:
            delta_bid = (self.min_span * sma - offset_bid) / self.levels
        if self.width_max * raw_ask / sma > self.min_span:
            delta_ask = self.width_range * raw_ask / self.levels
        else:
            delta_ask = (self.min_span * sma - offset_ask) / self.levels
        self.bids = [sma - offset_bid - (delta_bid * i) for i in range(self.levels -1, -1, -1)] # ascending
        self.asks = [sma + offset_ask + (delta_ask * i) for i in range(self.levels)] # ascending

class EnveloppeHIP3: # place orders grid each separed with delta
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.WIDTH_MIN = 0.05     # minimum multiplier for grid spread: controls tightness of orders around mid
        self.WIDTH_MAX = 8        # maximum multiplier for grid spread: controls maximum dispersion in volatile regimes
        self.MIN_SPAN = 0.002     # 0.2% | minimum distance between the first and the last placed orders
        self.LEVELS = 8           # orders count of each side
        self.CAP = 0.90           # 90% | max capacity of portfolio used
        self.MIN_VALUE = 10.5     # minimum order notional value in $ to avoid overfragmentation / fees inefficiency
        self.POLL_DELAY = 10.1    # delay in seconds between each grid evaluation cycle
        self.THRESHOLD = 0.0001   # 0.01% | relative SMA change threshold to trigger rebalancing
        self.TIMEFRAME = "1m"     # market data candle timeframe used for indicators (SMA/EMA)
        self.WINDOW = 9           # rolling window size for indicator (SMA/EMA period)
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
        self.last_sma = None
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
        await self._log.info(f"{self.placed + self.modified + self.passed} actions: {self.placed - self.placed_error}/{self.placed} place, {self.modified - self.modified_error}/{self.modified} modify and {self.passed} pass")
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

    async def modify(self, bids: list, asks: list): # both bids and asks in ascending order
        mid = self.market.mid
        if mid is None: raise ValueError("market nor inited")
        longable, shortable = self.sizes(mid, 0, 0) # disregard bids and asks value because they gonna be modified
        modifies: list[Modify] = []
        min_sz = self.MIN_VALUE / mid
        if bids and longable > len(bids) * min_sz:
            sz = longable / len(bids)
            for i, b in enumerate(bids):
                px = min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid)
                modified = get_modify(b, self.instrument, sz, px or 0, "Alo")
                order = modified.get("order", {})
                p, s = float(order.get("p", 0)), float(order.get("s", 0))
                value = p * s
                if self.market.best_ask and (value > self.MIN_VALUE) and (p < self.market.best_ask):
                    modifies.append(modified)
        if asks and shortable > len(asks) * min_sz:
            sz = shortable / len(asks)
            for i, a in enumerate(reversed(asks)):
                max(self.grid.asks[self.LEVELS - i - 1], (1 + self.MAKER_FEES) * mid)
                px = max(self.grid.asks[self.LEVELS - i - 1], (1 + self.MAKER_FEES) * mid)
                modified = get_modify(a, self.instrument, sz, px or 0, "Alo")
                order = modified.get("order", {})
                p, s = float(order.get("p", 0)), float(order.get("s", 0))
                value = p * s
                if self.market.best_bid and (value > self.MIN_VALUE) and (p > self.market.best_bid):
                    modifies.append(modified)
        if modifies:
            executed, error = await self.executor.batch_modify(modifies)
            if error:
                self.modified_error += 1
                await self._log.warning(f"{sum(executed)}/{len(executed)} modifies executed, longable={longable}, shortable={shortable}: {executed}")
            self.last_sma = self.market.sma_close()
            self.modified += 1
        else:
            self.passed += 1

    async def place(self, bids_px: list[float], asks_px: list[float], bids_value: float, asks_value: float): # both bids and asks in ascending order
        mid = self.market.mid
        if mid is None: raise ValueError("market nor inited")
        longable, shortable = self.sizes(mid, bids_value, asks_value)
        new_bids_px = closests(bids_px, self.grid.bids)
        new_asks_px = closests(asks_px, self.grid.asks)
        sz_bid = longable / len(new_bids_px) if new_bids_px else 0
        sz_ask = shortable / len(new_asks_px) if new_asks_px else 0
        new_bids: list[Order] = []
        new_asks: list[Order] = []
        for px in new_bids_px:
            order = get_order(self.instrument, True, sz_bid, px, {"limit": {"tif": "Alo"}})
            p, s = float(order.get("p", 0)), float(order.get("s", 0))
            value = p * s
            if self.market.best_ask and (value > self.MIN_VALUE) and (p < self.market.best_ask):
                new_bids.append(order)
        for px in new_asks_px:
            order = get_order(self.instrument, False, sz_ask, px, {"limit": {"tif": "Alo"}})
            p, s = float(order.get("p", 0)), float(order.get("s", 0))
            value = p * s
            if self.market.best_bid and (value > self.MIN_VALUE) and (p > self.market.best_bid):
                new_asks.append(order)
        orders: list[Order] = new_bids + new_asks
        if orders:
            executed, error = await self.executor.batch_order(orders)
            if error:
                self.placed_error += 1
                await self._log.warning(f"{sum(executed)}/{len(executed)} orders executed, longable={longable}, shortable={shortable}: {executed}")
            self.placed += 1
        else:
            self.passed += 1

    async def run(self):
        try:
            while self.running:
                try:
                    bids, bids_px, bids_value, asks, asks_px, asks_value = await self.state()
                    mid = self.market.mid
                    sma = self.market.sma_close()
                    if mid and ((mid > sma and len(bids) < self.LEVELS) or (mid < sma and len(asks) < self.LEVELS)):
                        await self.place(bids_px, asks_px, bids_value, asks_value)
                    elif self.last_sma and (abs(self.last_sma - sma) / sma) < self.THRESHOLD:
                        self.passed += 1
                    else:
                        await self.modify(bids, asks)
                    await asyncio.sleep(self.POLL_DELAY)
                except Exception as e:
                    await self._log.error(f"{e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
