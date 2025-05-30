import os
import json

from eth_account import Account
from eth_account.signers.local import LocalAccount

from .session_manager import SessionManager
from .api import API
from .info import Info
from .exchange import Exchange
from logs.message_manager import Logger

def load_account(config_path):
    if not os.path.isabs(config_path):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # <- tu remontes à HL_bot/
        config_path = os.path.join(base_dir, config_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    secret_key = config.get("secret_key")
    if not secret_key:
        raise ValueError("Le fichier de config doit contenir une clé 'secret_key' valide.")
    account: LocalAccount = Account.from_key(secret_key)
    address = config.get("account_address") or account.address
    return account, address

class CoreManager:
    def __init__(self, base_url, message_manager, key_path, skip_ws):
        self.message_manager = message_manager
        self._logging = Logger(self, message_manager)
        account, address = load_account(key_path)
        self.account = account
        self.address = address
        self.base_url = base_url
        self.session_manager = SessionManager(message_manager)
        self.api = API(base_url, message_manager, self.session_manager)
        self.info = Info(self.api, message_manager, skip_ws)
        self.ws_manager = self.info.ws_manager
        self.exchange = Exchange(self.info, message_manager, account)
        self._running = False

    async def start(self) -> None:
        if not self._running:
            try:
                await self._logging.info("tentative de demarrage du core")
                await self.session_manager.get_session()
                await self.info.start()
                await self._logging.success("core démarré")
                self._running = True
            except Exception as e:
                await self._logging.error(f"erreur dans le demarrage du core : {e}")
        else:
            await self._logging.warning("le core est deja lancé")

    async def stop(self) -> None:
        if self._running:
            try:
                await self._logging.info("demande d'arret du core")
                await self.info.unsubscribe_all()
                await self.info.disconnect()
                await self._logging.success("core arreté et deconnecté")
                self._running = False
            except Exception as e:
                await self._logging.error(f"le core n'a pas pu etre arreter correctement : {e}")
        else:
            await self._logging.warning("core deja arrété")
