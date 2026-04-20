import time
from core.HL.utils.functions import call
from core.session import Session
from log.logger import Logger
from log.messenger import Messenger
from log.telegram import Telegram
from core.HL.hl import HL

class Orchester:
    def __init__(self, mainnet):
        self.messenger = Messenger("log/logs.txt")
        self.strategy = None
        self.telegram = Telegram(self.messenger)
        self.session = Session(self.messenger)
        self._log = Logger(self, self.messenger)
        self.hl = HL(mainnet, self.messenger, self.session)
        self.address = self.hl.address
        self.running = False

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        try:
            t = time.time()
            await self.messenger.start(self.telegram)
            await self._log.info("Starting")
            await self.session.start()
            await self.hl.start()
            await self.telegram.start(self)
            self.running = True
            await self._log.success(f"Started in {(time.time()-t):.2f} s", send_tg= True)
        except Exception:
            await self._log.error("cannot start")

    async def stop(self):
        try:
            await self._log.info("Stopping", send_tg= True)
            self.running = False
            if self.strategy:
                await call(self.strategy.stop)
            await self.hl.stop()
            await self.session.stop()
            await self._log.success("Stopped", send_tg=True)
            await self.telegram.stop()
            await self.messenger.stop()
        except Exception:
            await self._log.error("Stop failed")

    async def set_strategy(self, strategy):
        if not self.running:
            raise ValueError("must init before set bot")
        try:
            self.strategy = strategy
            await strategy.start()
        except Exception as e:
            await self._log.error(f"{e}")

