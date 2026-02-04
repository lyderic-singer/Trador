import asyncio

from core.HL.utils.functions import cancel
from .market import MarketHL
from .account import AccountPerpHL
from log.logger import Logger

class EnveloppeHIP3:
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.SPREAD_MIN = 0.00001 # 0.001%
        self.SPREAD_MAX = 0.01    # 1%
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.SMA_PERIOD = 9
        self.SMA_CANDLE = "1m"
        self.LEVELS = 5
        self.SPREAD_ORDERS = (self.SPREAD_MAX - self.SPREAD_MIN) / self.LEVELS

        # ==== DATA ====
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.market = MarketHL(self.instrument, hl, self.SMA_PERIOD, self.SMA_CANDLE)
        self.account = AccountPerpHL([self.instrument], hl)

        # ==== STATE ====
        self.task = None
        self.running = False

    async def start(self):
        await self.market.start()
        await self.account.start()
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self.market.stop()
        await self.account.stop()

    async def run(self):
        self.running = True
        while self.running:
            await self._log.debug(f"{self.market}, {self.account}")
            await asyncio.sleep(10)
