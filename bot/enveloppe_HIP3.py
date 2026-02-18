import asyncio
from core.HL.utils.functions import cancel, get_order, get_modify, closests
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from core.HL.utils.types import Modify, Order
from log.logger import Logger

class Enveloppe:
    def __init__(self, market: MarketHL, levels: int, spread_min: float, spread_max: float):
        self.running = False
        self.market = market
        self.levels = levels
        self.spread_min = spread_min
        self.spread_max = spread_max
        self.range = spread_max - spread_min
        self.bids = []
        self.asks = []

    def update(self):
        if not self.running:
            raise ValueError("Must start before request deltas")
        close = self.market.sma_close()
        high = self.market.sma_high()
        low = self.market.sma_low()
        raw_bid = 2 * high - low - close
        raw_ask =  high + close - 2 * low
        delta_bid = self.range * raw_bid / self.levels
        delta_ask = self.range * raw_ask / self.levels
        offset_bid = self.spread_min * raw_bid
        offset_ask = self.spread_min * raw_ask
        self.bids = [close - offset_bid - (delta_bid * i) for i in range(self.levels -1, -1, -1)] # ascending
        self.asks = [close + offset_ask + (delta_ask * i) for i in range(self.levels)] # ascending

class EnveloppeHIP3: # place orders grid each separed with delta
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.SPREAD_MIN = 0.1     # x0.1
        self.SPREAD_MAX = 5       # x5
        self.LEVELS = 5
        self.MIN_VALUE = 10.5
        self.LEVERAGE = 5
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
        self.enveloppe = Enveloppe(self.market, self.LEVELS, self.SPREAD_MIN, self.SPREAD_MAX)

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self.market.start()
        self.enveloppe.running = True
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

    async def modify(self, bids: list, asks: list): # both bids and asks in ascending order
        if not (bids or asks):
            return await self._log.warning("nothing to modify")
        position = self.account.positions.get(self.instrument.coin)
        if position is None:
            return await self._log.error("position not found in account")
        longable, shortable = position.longable, position.shortable
        if longable <= 0 and shortable <= 0:
            return await self._log.warning("no collateral")
        modifies: list[Modify] = []
        mid = self.market.mid
        self.enveloppe.update()
        if bids and longable > 0:
            sz = longable / len(bids)
            for i, b in enumerate(bids):
                px = self.bid_px(self.enveloppe.bids[i], mid)
                if px and not ((sz * (px or 0) < self.MIN_VALUE) or (abs(px - b["limitPx"]) / b["limitPx"] < self.THRESHOLD)):
                    modifies.append(get_modify(b, self.instrument, sz, px, "Alo"))
        if asks and shortable > 0:
            sz = shortable / len(asks)
            for i, a in enumerate(reversed(asks)):
                px = self.ask_px(self.enveloppe.asks[self.LEVELS - i - 1], mid)
                if px and not ((sz * (px or 0) < self.MIN_VALUE) or (abs(px - a["limitPx"]) / a["limitPx"] < self.THRESHOLD)):
                    modifies.append(get_modify(a, self.instrument, sz, px, "Alo"))
        if modifies:
            await self.executor.batch_modify(modifies)
            self.last_sma = self.market.sma_close()
            self.modified += 1
        else:
            await self._log.warning("no modifies to send")

    async def place(self, bids_px: list[float], asks_px: list[float]): # both bids and asks in ascending order
        position = self.account.positions.get(self.instrument.coin)
        if position is None:
            return await self._log.error("position not found in account")
        longable, shortable = position.longable, position.shortable
        if longable <= 0 and shortable <= 0:
            return await self._log.warning("no collateral")
        self.enveloppe.update()
        mid = self.market.mid
        new_bids_px = closests(bids_px, self.enveloppe.bids)
        new_asks_px = closests(asks_px, self.enveloppe.asks)
        sz_bid = longable / len(new_bids_px) if new_bids_px else 0
        sz_ask = shortable / len(new_asks_px) if new_asks_px else 0
        new_bids: list[Order] = []
        new_asks: list[Order] = []
        for px in new_bids_px:
            if px and mid and px < mid:
                new_bids.append(get_order(self.instrument, True, sz_bid, px, {"limit": {"tif": "Alo"}}))
        for px in new_asks_px:
            if px and mid and px > mid:
                new_asks.append(get_order(self.instrument, False, sz_ask, px, {"limit": {"tif": "Alo"}}))
        orders: list[Order] = new_bids + new_asks
        if orders:
            await self.executor.batch_order(orders)
            self.placed += 1
        else:
            await self._log.warning("no order to place")

    async def run(self):
        try:
            while self.running:
                orders = await self.hl.info.open_orders(self.address, dex=self.dex)
                orders.sort(key=lambda x: float(x["limitPx"]))
                bids, asks, bids_px, asks_px = [], [], [], []
                for o in orders:
                    if o["side"] == 'B':
                        bids.append(o)
                        bids_px.append(float(o["limitPx"]))
                    else:
                        asks.append(o)
                        asks_px.append(float(o["limitPx"]))
                mid = self.market.mid
                sma = self.market.sma_close()
                if mid and ((mid > sma and len(bids) < self.LEVELS) or (mid < sma and len(asks) < self.LEVELS)):
                    await self.place(bids_px, asks_px)
                elif self.last_sma and (abs(self.last_sma - sma) / sma) < self.THRESHOLD:
                    self.passed += 1
                else:
                    await self.modify(bids, asks)
                await asyncio.sleep(self.POLL_DELAY)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
