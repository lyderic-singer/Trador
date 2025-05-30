import os
import json

from eth_account import Account
from eth_account.signers.local import LocalAccount

from .session_manager import SessionManager
from .api import API
from .info import Info
from .exchange import Exchange
from logs.message_manager import MessageManager, Logger
from logs.telegram_manager import TelegramManager
from logs.log_manager import LogManager

def load_account(key_path: str) -> tuple[LocalAccount, str]:
    config_path = os.path.join(os.path.dirname(__file__), key_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    secret_key = config.get("secret_key")
    if not secret_key:
        raise ValueError("Le fichier de config doit contenir une clé 'secret_key' valide.")
    account: LocalAccount = Account.from_key(secret_key)
    address = config.get("account_address") or account.address
    return account, address

class CoreManager:
    def __init__(self, base_url, key_path, skip_ws):
        self.message_manager = MessageManager()
        self._logging = Logger(self, self.message_manager)
        account, address = load_account(key_path)
        self.account = account
        self.address = address
        self.base_url = base_url
        self.session_manager = SessionManager(self.message_manager)
        self.api = API(base_url, self.message_manager, self.session_manager)
        self.info = Info(self.api, self.message_manager, skip_ws)
        self.ws_manager = self.info.ws_manager
        self.exchange = Exchange(self.info, self.message_manager, account)
        self.tg_manager = TelegramManager(self, self.message_manager)
        self.log_manager = LogManager(self.message_manager, filepath= "logs/logs.txt")
        self._running = False

    async def start(self):
        if not self._running:
            try:
                await self.log_manager.start()
                await self._logging.info("tentative de demarrage du core")
                await self.session_manager.get_session()
                await self.info.start()
                await self.tg_manager.start()
                await self._logging.success("core démarré")
                self._running = True
            except Exception as e:
                await self._logging.error(f"erreur dans le demarrage du core : {e}")
        else:
            await self._logging.warning("le core est deja lancé")

    async def stop(self):
        if self._running:
            try:
                await self._logging.info("demande d'arret du core")
                await self.info.unsubscribe_all()
                await self.info.disconnect()
                await self.tg_manager.stop()
                await self.log_manager.stop()
                await self._logging.success("core arreté et deconnecté")
                self._running = False
            except Exception as e:
                await self._logging.error(f"le core n'a pas pu etre arreter correctement : {e}")
        else:
            await self._logging.warning("core deja arrété")
