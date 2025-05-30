from logs.message_manager import MessageManager, Logger
from logs.log_manager import LogManager
from logs.telegram_manager import TelegramManager
from core.core_manager import CoreManager
from data.coin_manager import CoinManager

class Orchestrator:
    def __init__(self, base_url, key_path, skip_ws, strategy, coin):
        self.base_url = base_url
        self.message_manager = MessageManager()
        self._logging = Logger(self, self.message_manager)
        self.core_manager = CoreManager(base_url, self.message_manager, key_path, skip_ws)
        self.tg_manager = TelegramManager(self)
        self.strategy = strategy.init(self.core_manager)
        self.log_manager = LogManager(self.message_manager, self.tg_manager, "logs/logs.txt")
        self.log_manager.start()
        self.coin_manager = CoinManager(self.core_manager.api, coin)
        self._running = False

    async def start(self):
        
