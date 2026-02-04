import asyncio
import time
from core.HL.utils.functions import call
from core.session import Session
from log.logger import Logger
from log.messenger import Messenger
from log.telegram import Telegram
from core.HL.hl import HL

class Orchester:
    def __init__(self, mainnet, dexsHL: list[str] = []):
        self.messenger = Messenger("log/logs.txt")
        self.bot = None
        self.telegram = Telegram(self.messenger)
        self.session = Session(self.messenger)
        self._log = Logger(self, self.messenger)
        self.hl = HL(mainnet, self.messenger, self.session, dexsHL)
        self.address = self.hl.address
        self.running = False

    async def start(self, bot = None):
        try:
            t = time.time()
            self.bot = bot
            await self.messenger.start(self.telegram)
            await self._log.info("Starting")
            await self.session.start()
            await self.hl.start()
            if bot:
                await call(bot.start(self))
            await self.telegram.start(self)
            self.running = True
            await self._log.success(f"Started in {(time.time()-t):.2f} s", send_tg= True)
        except Exception:
            await self._log.error("cannot start")

    async def stop(self):
        try:
            await self._log.info("Stopping", send_tg= True)
            self.running = False
            if self.bot:
                await call(self.bot.stop())
            await self.hl.stop()
            await self.session.stop()
            await self._log.success("Stopped", send_tg=True)
            await self.telegram.stop()
            await self.messenger.stop()
        except Exception:
            await self._log.error("Stop failed")
