from eth_account import Account
from eth_account.signers.local import LocalAccount
from core.config import env
from core.session import Session
from core.HL.utils.constants import MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL, TESTNET_WS_URL
from core.HL.websocket import HLWebSocket
from core.HL.api import HLApi
from core.HL.info import HLInfo
from core.HL.exchange import HLExchange
from log.logger import Logger
from log.messenger import Messenger


def credentials(mainnet: bool) -> tuple[LocalAccount, str]:
    account: LocalAccount = Account.from_key(env("HL", mainnet, "PRIVATE_KEY"))
    return account, env("HL", mainnet, "ADDRESS")

class HLClient:
    def __init__(self, mainnet, messenger: Messenger, session: Session):
        self.messenger = messenger
        self._log = Logger("HL Client", messenger)
        account, address = credentials(mainnet)
        self.account = account
        self.address = address
        url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.session = session
        self.api = HLApi(url, messenger, self.session)
        self.info = HLInfo(url, messenger, self.api)
        self.websocket = HLWebSocket(MAINNET_WS_URL if mainnet else TESTNET_WS_URL, self.messenger, self.info)
        self.exchange = HLExchange(url, messenger, account, self.info)
        self.running = False

    async def start(self) -> None:
        if self.running:
            return await self._log.info("already started")
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
