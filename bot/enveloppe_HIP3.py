import asyncio
from core.HL.utils.functions import cancel
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from core.HL.utils.types import Order, Modify
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
        self.MAX_LEVELS = 5
        self.MIN_LEVELS = 2
        self.LEVERAGE = 5
        self.POLL_DELAY = 10.1
        self.THRESHOLD = 0.0002
        self.TIMEFRAME = "1m"
        self.WINDOW = 9
        # ==== DATA ====
        self.hl = hl
        self.address = hl.address
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.dex = self.instrument.dex
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
        m = self.market
        raw_bid = m.sma_low() + m.sma_close() - 2 * m.sma_high()

    async def modify(self, bids, asks):
        for 

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
