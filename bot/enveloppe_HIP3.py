import asyncio
from core.HL.utils.functions import cancel, get_modify
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from log.logger import Logger

class EnveloppeHIP3: # place orders grid each separed with delta
    def __init__(self, x: str | int, hl):
        self.task = None
        self.running = False
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
        self.RANGE = self.SPREAD_MAX - self.SPREAD_MIN
        self.TIMEFRAME = "1m"
        self.WINDOW = 9
        # ==== DATA ====
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
        await self.account.start()
        self.running = True
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self.market.stop()
        await self.account.stop()

    def get_deltas(self):
        if not self.running:
            raise ValueError("Must start before request deltas")
        close = self.market.sma_close()
        high = self.market.sma_high()
        low = self.market.sma_low()
        raw_bid = 2 * high - low - close
        raw_ask =  high + close - 2 * low
        delta_bid = self.RANGE * raw_bid / self.LEVELS
        delta_ask = self.RANGE * raw_ask / self.LEVELS
        offset_bid = self.SPREAD_MIN * raw_bid
        offset_ask = self.SPREAD_MIN * raw_ask
        return offset_bid, raw_bid, delta_bid, offset_ask, raw_ask, delta_ask

    async def modify(self, bids, asks):
        if not (bids or asks):
            return await self._log.warning("nothing to modify")
        offset_bid, raw_bid, delta_bid, offset_ask, raw_ask, delta_ask = self.get_deltas()
        position = self.account.positions.get(self.instrument.coin)
        if position is None:
            return await self._log.error("position not found in account")
        longable, shortable = position.longable, position.shortable
        if longable <= 0 and shortable <= 0:
            return await self._log.warning("no collateral")
        mid = self.market.sma_close()
        mods = []
        if bids and longable > 0:
            sz = longable / len(bids)
            for i, b in enumerate(bids):
                px = mid - offset_bid - (len(bids) - 1 - i) * delta_bid
                if sz * px < self.MIN_VALUE:
                    continue
                if abs(px - b["price"]) / b["price"] < self.THRESHOLD:
                    continue
                mods.append(get_modify(b, self.instrument, sz, px, "Alo"))
        if asks and shortable > 0:
            sz = shortable / len(asks)
            for i, a in enumerate(asks):
                px = mid + offset_ask + (len(asks) - 1 - i) * delta_ask
                if sz * px < self.MIN_VALUE:
                    continue
                if abs(px - a["price"]) / a["price"] < self.THRESHOLD:
                    continue
                mods.append(get_modify(a, self.instrument, sz, px, "Alo"))
        if mods:
            await self.executor.batch_modify(mods)

    async def run(self):
        try:
            while self.running:
                curr_orders = await self.hl.info.open_orders(self.address, dex=self.dex)
                curr_bids, curr_asks = [], []
                for o in curr_orders:
                    curr_bids.append(o) if o["side"] == 'B' else curr_asks.append(o)
                mid = self.market.mid
                sma_close = self.market.sma_close()
                sma_high = self.market.sma_high()
                sma_low = self.market.sma_low()
                await asyncio.sleep(self.POLL_DELAY)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False
