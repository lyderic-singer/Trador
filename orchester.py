import asyncio
import time
from core.HL.utils.functions import call
from core.HL.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from core.session import Session
from log.logger import Messenger, Logger
from log.dispatcher import Dispatcher
from log.telegram import Telegram
from core.HL.hl import HL

class Orchester:
    def __init__(self, mainnet, strategy = None):
        self.url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.messenger = Messenger()
        self.session = Session(self.messenger)
        self._log = Logger(self, self.messenger)
        self.hl = HL(mainnet, self.messenger, self.session)
        self.address = self.hl.address
        self.telegram = Telegram(self)
        self.strategy = strategy
        self.dispatcher = Dispatcher(self.messenger, self.telegram, "log/logs.txt")
        self.dispatcher.start()
        self._running = False
        self._tg_task = None
        self._strategy_task = None

    async def start(self):
        t = time.time()
        await self._log.info("starting orchester")
        await self.session.start()
        await self.hl.start()
        if self.strategy:
            await call(self.strategy.start(self))
        await self.telegram.start()
        self._running = True
        await self._log.success(f"orchester started in {(time.time()-t):.2f} s", send_tg= True)

    async def stop(self):
        await self._log.info("stoping orchester", send_tg= True)
        if self.strategy:
            await self.strategy.stop()
        for task in [self._tg_task, self._strategy_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self.hl.stop()
        await self.telegram.stop()
        await self.session.stop()
        self._running = False
        await self._log.success("orchester stopped")
        await self.dispatcher.stop()
