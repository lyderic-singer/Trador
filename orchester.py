import asyncio
import time
from core.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from logs.message_manager import MessageManager, Logger
from logs.log_manager import LogManager
from logs.telegram_manager import TelegramManager
from core.core_manager import CoreManager
from data.coin_manager import CoinManager

class Orchester:
    def __init__(self, is_mainnet, strategy = None, skip_ws = True):
        self.base_url = MAINNET_API_URL if is_mainnet else TESTNET_API_URL
        self.message_manager = MessageManager()
        self._logging = Logger(self, self.message_manager)
        self.core_manager = CoreManager(self.base_url, self.message_manager, skip_ws, is_mainnet)
        self.address = self.core_manager.address
        self.tg_manager = TelegramManager(self)
        self.strategy = strategy
        self.log_manager = LogManager(self.message_manager, self.tg_manager, "logs/logs.txt")
        self.log_manager.start()
        self._running = False
        self._tg_task = None
        self._strategy_task = None

    async def start(self):
        t = time.time()
        await self.core_manager.start()
        # await self.coin_manager.start()
        if self.strategy:
            asyncio.create_task(self.strategy.start(self.core_manager))
        await self.tg_manager.start()
        self._running = True
        await self._logging.success(f"orchester demarré en {(time.time()-t):.2f} s", send_tg= True)

    async def stop(self):
        await self._logging.info("orchester arrêt en cours...", send_tg= True)
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
        await self.core_manager.stop()
        await self._logging.success("orchester arrêté")
        await self.tg_manager.stop()
        await self.log_manager.stop()
        await asyncio.sleep(0)
        self._running = False

