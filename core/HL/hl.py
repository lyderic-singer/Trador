import os
from dotenv import load_dotenv
from .utils.constants import MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL, TESTNET_WS_URL
from eth_account import Account
from eth_account.signers.local import LocalAccount
from .websocket import WebSocket
from .api import API
from .info import Info
from .exchange import Exchange
from log.logger import Logger
from log.messenger import Messenger

load_dotenv()

def load_account_from_env(mainnet):
    private = "MAINNET_PRIVATE_API_KEY"
    public = "MAINNET_PUBLIC_KEY"
    if not mainnet:
        private = "TESTNET_PRIVATE_API_KEY"
        public = "TESTNET_PUBLIC_KEY"
    secret_key = os.getenv(private)
    if not secret_key:
        raise ValueError(f"No valid {private}")
    account: LocalAccount = Account.from_key(secret_key)
    address = os.getenv(public)
    if not address:
        raise ValueError(f"No valid {public}")
    return account, address

class HL:
    def __init__(self, mainnet, messenger: Messenger, session):
        self.messenger = messenger
        self._log = Logger("HL Core", messenger)
        account, address = load_account_from_env(mainnet)
        self.account = account
        self.address = address
        url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.session = session
        self.api = API(url, messenger, self.session)
        self.info = Info(url, messenger, self.api)
        self.websocket = WebSocket(MAINNET_WS_URL if mainnet else TESTNET_WS_URL, self.messenger, self.info)
        self.exchange = Exchange(url, messenger, account, self.info)
        self.running = False

    async def start(self) -> None:
        if not self.running:
            try:
                await self._log.info("Starting")
                await self.info.start()
                await self.websocket.start()
                self.running = True
            except Exception as e:
                await self._log.error(f"{e}")
                raise
        await self._log.success("Started")

    async def stop(self) -> None:
        if self.running:
            try:
                await self._log.info("Stopping")
                await self.websocket.stop()
                self.running = False
            except Exception as e:
                await self._log.error(f"{e}")
                raise
        await self._log.success("Stopped")
