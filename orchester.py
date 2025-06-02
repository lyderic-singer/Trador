import asyncio
from logs.message_manager import MessageManager, Logger
from logs.log_manager import LogManager
from logs.telegram_manager import TelegramManager
from core.core_manager import CoreManager
from data.coin_manager import CoinManager
class Orchester:
    def __init__(self, base_url, key_path, skip_ws, strategy):
        self.base_url = base_url
        self.message_manager = MessageManager()
        self._logging = Logger(self, self.message_manager)
        self.core_manager = CoreManager(base_url, self.message_manager, key_path, skip_ws)
        self.address = self.core_manager.address
        self.tg_manager = TelegramManager(self)
        self.strategy = strategy
        self.log_manager = LogManager(self.message_manager, self.tg_manager, "logs/logs.txt")
        self.log_manager.start()
        self._running = False
        self._tg_task = None
        self._strategy_task = None

    async def start(self):
        await self.core_manager.start()
        self.strategy.init(self.core_manager)
        await self.tg_manager.start()
        self._running = True
        await self._logging.success("orchester demarré", send_tg= True)

    async def stop(self):
        await self._logging.info("orchester arrêt en cours...", send_tg= True)
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
        self._running = False

