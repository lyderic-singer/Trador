import asyncio
from logs.message_manager import MessageManager, Logger
from logs.log_manager import LogManager
from logs.telegram_manager import TelegramManager
from core.core_manager import CoreManager
from data.coin_manager import CoinManager

class Orchestrator:
    def __init__(self, base_url, key_path, skip_ws, strategy):
        self.base_url = base_url
        self.message_manager = MessageManager()
        self._logging = Logger(self, self.message_manager)
        self.core_manager = CoreManager(base_url, self.message_manager, key_path, skip_ws)
        self.tg_manager = TelegramManager(self)
        self.strategy = strategy
        self.log_manager = LogManager(self.message_manager, self.tg_manager, "logs/logs.txt")
        self.log_manager.start()
        self._running = False
        self._task = None

    async def start(self):
        await self.core_manager.start()
        await self.tg_manager.start()
        self.strategy.init(self.core_manager)
        self._running = True

    async def run(self):
        self._task = asyncio.create_task(self.strategy.run())

    async def stop(self):
        await self.strategy.stop()
        if self._task:
            await self._task
        await self.core_manager.stop()
        await self.tg_manager.stop()
        await self.log_manager.stop()
        self._running = False
