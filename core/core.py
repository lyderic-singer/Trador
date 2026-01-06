import os
import json
from dotenv import load_dotenv

from eth_account import Account
from eth_account.signers.local import LocalAccount

from .session_manager import SessionManager
from .api import API
from .info import Info
from .exchange import Exchange
from HLB.logs.logger import Logger

load_dotenv()

def load_account_from_env(is_mainnet):
    private = "MAINNET_PRIVATE_API_KEY"
    public = "MAINNET_PUBLIC_KEY"
    if not is_mainnet:
        private = "TESTNET_PRIVATE_API_KEY"
        public = "TESTNET_PUBLIC_KEY"
    secret_key = os.getenv(private)
    if not secret_key:
        raise ValueError("Le fichier de config doit contenir une clé {private} valide.")
    account: LocalAccount = Account.from_key(secret_key)
    address = os.getenv(public)
    if not address:
        raise ValueError("Le fichier de config doit contenir une clé {public} valide.")
    return account, address

class CoreManager:
    def __init__(self, base_url, message_manager, skip_ws, is_mainnet):
        self.message_manager = message_manager
        self._logging = Logger(self, message_manager)
        account, address = load_account_from_env(is_mainnet)
        self.account = account
        self.address = address
        self.base_url = base_url
        self.session_manager = SessionManager(message_manager)
        self.api = API(base_url, message_manager, self.session_manager)
        self.info = Info(self.api, message_manager, skip_ws)
        self.ws_manager = None
        self.exchange = Exchange(self.info, message_manager, account)
        self._running = False

    async def start(self) -> None:
        if not self._running:
            try:
                await self._logging.info("tentative de demarrage du core")
                await self.session_manager.get_session()
                await self.info.start()
                self.ws_manager = self.info.ws_manager
                await self._logging.success("core démarré", )
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
