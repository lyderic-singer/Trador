import asyncio
from asyncio.windows_events import INFINITE
from core.HL.utils.functions import cancel, get_order, get_modify, closests
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from core.HL.utils.types import Modify, Order
from log.logger import Logger

class Grid:
    def __init__(self, market: MarketHL, levels: int, width_min: float, width_max: float, spread_max: float):
        self.running = False
        self.market = market
        self.levels = levels
        self.width_min = width_min
        self.width_max = width_max
        self.spread_max = spread_max
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
        raw_bid = 2 * high - low - close
        raw_ask =  high + close - 2 * low
        offset_bid = self.width_min * raw_bid
        offset_ask = self.width_min * raw_ask
        if self.width_max * raw_bid / sma > self.spread_max:
            delta_bid = self.width_range * raw_bid / self.levels
        else:
            delta_bid = (self.spread_max * sma - offset_bid) / self.levels
        if self.width_max * raw_ask / sma > self.spread_max:
            delta_ask = self.width_range * raw_ask / self.levels
        else:
            delta_ask = (self.spread_max * sma - offset_ask) / self.levels
        self.bids = [sma - offset_bid - (delta_bid * i) for i in range(self.levels -1, -1, -1)] # ascending
        self.asks = [sma + offset_ask + (delta_ask * i) for i in range(self.levels)] # ascending

class EnveloppeHIP3: # place orders grid each separed with delta
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.WIDTH_MIN = 0.05     # x0.05
        self.WIDTH_MAX = 13       # x13
        self.SPREAD_MAX = 0.002   # 0.2%
        self.LEVELS = 8
        self.CAP_VALUE = 0.90
        self.MIN_VALUE = 10.5
        self.LEVERAGE = 10
        self.POLL_DELAY = 10.1
        self.THRESHOLD = 0.0002   # 0.02%
        self.TIMEFRAME = "1m"
        self.WINDOW = 9
        # ==== UTILS ====
        self.task = None
        self.running = False
        self.last_sma = None
        self.placed = 0   # nb of place for stat
        self.modified = 0 # nb of modify for stat
        self.passed = 0   # nb of pass for stat
        self.hl = hl
        self.address = hl.address
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.dex = self.instrument.dex
        self.executor = Executor(hl)
        self.market = MarketHL(self.instrument, hl, self.WINDOW, self.TIMEFRAME)
        self.account = AccountPerpDexHL(self.dex, [self.instrument], hl)
        self.grid = Grid(self.market, self.LEVELS, self.WIDTH_MIN, self.WIDTH_MAX, self.SPREAD_MAX)

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
        await self._log.info(f"{self.placed + self.modified + self.passed} actions: {self.placed} place, {self.modified} modify and {self.passed} pass")
        await self._log.success("Stopped")

    def bid_px(self, px: float, mid: float | None = None) -> float | None:
        if mid is None:
            mid = self.market.mid
        if mid and px:
            return min(px, (1 - self.MAKER_FEES) * mid)

    def ask_px(self, px: float, mid: float | None = None) -> float | None:
        if mid is None:
            mid = self.market.mid
        if mid and px:
            return max(px, (1 + self.MAKER_FEES) * mid)

    async def modify(self, bids: list, bids_value: float, asks: list, asks_value: float): # both bids and asks in ascending order
        if not (bids or asks):
            return await self._log.warning("nothing to modify")
        position = self.account.positions.get(self.instrument.coin)
        if position is None:
            return await self._log.error("position not found in account")
        mid = self.market.mid
        if not mid:
            return await self._log.error("mid not inited")
        self.grid.update()
        longable = self.CAP_VALUE * (position.longable + bids_value) / mid
        shortable = self.CAP_VALUE * (position.shortable + asks_value) / mid
        if longable <= 0 and shortable <= 0:
            return await self._log.warning("no collateral")
        modifies: list[Modify] = []
        if bids and longable > 0:
            sz = longable / len(bids)
            for i, b in enumerate(bids):
                px = self.bid_px(self.grid.bids[i], mid)
                modified = get_modify(b, self.instrument, sz, px or 0, "Alo")
                order = modified.get("order", {})
                p, s = float(order.get("p", 0)), float(order.get("s", 0))
                value = p * s
                if self.market.best_ask and (value > self.MIN_VALUE) and (p < self.market.best_ask):
                    modifies.append(modified)
        if asks and shortable > 0:
            sz = shortable / len(asks)
            for i, a in enumerate(reversed(asks)):
                px = self.ask_px(self.grid.asks[self.LEVELS - i - 1], mid)
                modified = get_modify(a, self.instrument, sz, px or 0, "Alo")
                order = modified.get("order", {})
                p, s = float(order.get("p", 0)), float(order.get("s", 0))
                value = p * s
                if self.market.best_bid and (value > self.MIN_VALUE) and (p > self.market.best_bid):
                    modifies.append(modified)
        if modifies:
            await self.executor.batch_modify(modifies)
            self.last_sma = self.market.sma_close()
            self.modified += 1
        else:
            await self._log.debug("passing", styles="YELLOW")
            self.passed += 1

    async def place(self, bids_px: list[float], asks_px: list[float]): # both bids and asks in ascending order
        position = self.account.positions.get(self.instrument.coin)
        if position is None:
            return await self._log.error("position not found in account")
        mid = self.market.mid
        if not mid:
            return await self._log.error("mid not inited")
        self.grid.update()
        longable = self.CAP_VALUE * position.longable / mid
        shortable = self.CAP_VALUE * position.shortable / mid
        if longable <= 0 and shortable <= 0:
            return await self._log.warning("no collateral")
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
            await self.executor.batch_order(orders)
            self.placed += 1
        else:
            await self._log.debug("passing", styles="ORANGE")
            self.passed += 1

    async def run(self):
        try:
            while self.running:
                try:
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
                    mid = self.market.mid
                    sma = self.market.sma_close()
                    if mid and ((mid > sma and len(bids) < self.LEVELS) or (mid < sma and len(asks) < self.LEVELS)):
                        await self.place(bids_px, asks_px)
                    elif self.last_sma and (abs(self.last_sma - sma) / sma) < self.THRESHOLD:
                        await self._log.debug("passing")
                        self.passed += 1
                    else:
                        await self.modify(bids, bids_value, asks, asks_value)
                    await asyncio.sleep(self.POLL_DELAY)
                except Exception as e:
                    await self._log.error(f"{e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
