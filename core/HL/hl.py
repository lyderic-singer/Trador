import os
from dotenv import load_dotenv
import asyncio
from .utils.constants import MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL, TESTNET_WS_URL
from eth_account import Account
from eth_account.signers.local import LocalAccount

from ..session import Session
from ..websocket import Websocket
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
    def __init__(self, mainnet, messenger, session):
        self.messenger = messenger
        self._log = Logger(self, messenger)
        account, address = load_account_from_env(mainnet)
        self.account = account
        self.address = address
        url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.session = session
        self.api = API(url, messenger, self.session)
        self.info = Info(url, messenger, self.api)
        self.ws = Websocket(MAINNET_WS_URL if mainnet else TESTNET_WS_URL, self.messenger)
        self.exchange = Exchange(url, messenger, account, self.info)
        asyncio.create_task(self.ws.start())
        self._running = False

    async def start(self) -> None:
        if not self._running:
            try:
                await self._log.info("tentative de demarrage du core")
                await self.session.get()
                await self.info.start()
                asyncio.create_task(self.ws.start())
                await self._log.success("core démarré")
                self._running = True
            except Exception as e:
                await self._log.error(f"erreur dans le demarrage du core : {e}")
        else:
            await self._log.warning("le core est deja lancé")

    async def stop(self) -> None:
        if self._running:
            try:
                await self._log.info("demande d'arret du core")
                await self.ws.stop()
                await self.api.stop()
                await self._log.success("core arreté et deconnecté")
                self._running = False
            except Exception as e:
                await self._log.error(f"le core n'a pas pu etre arreter correctement : {e}")
        else:
            await self._log.warning("core deja arrété")
