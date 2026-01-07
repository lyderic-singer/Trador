import asyncio
import time
from core.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from logs.logger import Messenger, Logger
from logs.dispatcher import Dispatcher
from logs.telegram import Telegram
from core.core import Core

class Orchester:
    def __init__(self, is_mainnet, strategy = None, skip_ws = False):
        self.base_url = MAINNET_API_URL if is_mainnet else TESTNET_API_URL
        self.messenger = Messenger()
        self._log = Logger(self, self.messenger)
        self.core = Core(self.base_url, self.messenger, skip_ws, is_mainnet)
        self.address = self.core.address
        self.telegram = Telegram(self)
        self.strategy = strategy
        self.dispatcher = Dispatcher(self.messenger, self.telegram, "logs/logs.txt")
        self.dispatcher.start()
        self._running = False
        self._tg_task = None
        self._strategy_task = None

    async def start(self):
        t = time.time()
        await self.core.start()
        if self.strategy:
            try:
                if asyncio.iscoroutinefunction(self.strategy.init):
                    await self.strategy.init(self.core)
                else:
                    self.strategy.init(self.core)
            except Exception as e:
                await self._log.error(f"Error in callback : {e}")
        await self.telegram.start()
        self._running = True
        await self._log.success(f"orchester demarré en {(time.time()-t):.2f} s", send_tg= True)

    async def stop(self):
        await self._log.info("orchester arrêt en cours...", send_tg= True)
        await asyncio.sleep(1)
        if self.strategy:
            await self.strategy.stop()
        for task in [self._tg_task, self._strategy_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self.core.stop()
        await self._log.success("orchester arrêté")
        await self.telegram.stop()
        await self.dispatcher.stop()
        await asyncio.sleep(0)
        self._running = False

