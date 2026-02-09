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
        self.LEVERAGE = 5
        self.POLL_DELAY = 10.1
        self.THRESHOLD = 0.0002
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
        modifies = []
        offset_bid, raw_bid, delta_bid, offset_ask, raw_ask, delta_ask = self.get_deltas()
        position = self.account.positions[self.instrument.coin]
        longable = position.longable
        shortable = position.shortable
        if not (longable and shortable):
            raise ValueError(f"cannot get value from position.longable / shortable")
        if max(len(asks), len(bids)) > self.LEVELS:
            raise ValueError(f"more orders than expected: {len(bids)} bids and {len(asks)} asks")
        size_bid = longable / len(bids)
        size_ask = shortable / len(asks)
        for i, b in enumerate(bids):
            px = self.market.sma_close() - offset_bid - (self.LEVELS - 1 - i) * delta_bid
            modifies.append(get_modify(b, self.instrument, size_bid, px, "Alo"))
        for i, a in enumerate(asks):
            px = self.market.sma_close() + offset_ask + (self.LEVELS - 1 - i) * delta_ask
            modifies.append(get_modify(a, self.instrument, size_ask, px, "Alo"))
        await self.executor.batch_modify(modifies)

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
