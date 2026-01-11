import os
import json
from dotenv import load_dotenv

from eth_account import Account
from eth_account.signers.local import LocalAccount

from ..session import Session
from .api import API
from .info import Info
from .exchange import Exchange
from log.logger import Logger

load_dotenv()

def load_account_from_env(mainnet):
    private = "MAINNET_PRIVATE_API_KEY"
    public = "MAINNET_PUBLIC_KEY"
    if not mainnet:
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

class HL:
    def __init__(self, base_url, messenger, skip_ws, mainnet):
        self.messenger = messenger
        self._log = Logger(self, messenger)
        account, address = load_account_from_env(mainnet)
        self.account = account
        self.address = address
        self.base_url = base_url
        self.session = Session(messenger)
        self.api = API(base_url, messenger, self.session)
        self.info = Info(self.api, messenger, skip_ws, perp_dexs=["xyz", "flx", "vntl", "hyna"])
        self.ws = None
        self.exchange = Exchange(self.info, messenger, account)
        self._running = False

    async def start(self) -> None:
        if not self._running:
            try:
                await self._log.info("tentative de demarrage du core")
                await self.session.get()
                await self.info.start()
                self.ws = self.info.ws
                await self._log.success("core démarré", )
                self._running = True
            except Exception as e:
                await self._log.error(f"erreur dans le demarrage du core : {e}")
        else:
            await self._log.warning("le core est deja lancé")

    async def stop(self) -> None:
        if self._running:
            try:
                await self._log.info("demande d'arret du core")
                await self.info.unsubscribe_all()
                await self.info.disconnect()
                await self._log.success("core arreté et deconnecté")
                self._running = False
            except Exception as e:
                await self._log.error(f"le core n'a pas pu etre arreter correctement : {e}")
        else:
            await self._log.warning("core deja arrété")
