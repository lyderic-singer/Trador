import time
from core.config import env
from core.session import Session
from core.arcus.api import ArcusApi
from core.arcus.exchange import ArcusExchange
from core.arcus.info import ArcusInfo
from core.arcus.utils.constants import MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL, TESTNET_WS_URL
from core.arcus.utils.signing import Signer
from core.arcus.websocket import ArcusWebSocket
from log.logger import Logger
from log.messenger import Messenger


def credentials(mainnet: bool) -> tuple[Signer, str, int]:
    signer = Signer(env("ARCUS", mainnet, "PRIVATE_KEY").strip())
    return signer, env("ARCUS", mainnet, "ADDRESS").strip().lower(), int(env("ARCUS", mainnet, "ACCOUNT", "0"))


class ArcusClient:
    def __init__(self, mainnet: bool, messenger: Messenger, session: Session):
        self.messenger = messenger
        self.mainnet = mainnet
        self._log = Logger("Arcus Client", messenger)
        signer, address, index = credentials(mainnet)
        self.signer = signer
        self.address = address
        self.index = index
        url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.api = ArcusApi(url, messenger, session)
        self.info = ArcusInfo(messenger, self.api)
        self.exchange = ArcusExchange(messenger, self.api, self.info, signer, address, index)
        self.websocket = ArcusWebSocket(MAINNET_WS_URL if mainnet else TESTNET_WS_URL, messenger)
        self.running = False

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self._log.info("Starting")
        await self.info.start()
        await self.check()
        await self.websocket.start()
        self.running = True
        await self._log.success(f"Started ({len(self.info.by_id)} markets, api key {self.signer.public[:8]})")

    async def check(self):
        try:
            keys = await self.info.api_keys(self.address)
        except Exception as e:
            return await self._log.warning(f"cannot list api keys of {self.address}: {e}")
        row = next((k for k in keys if str(k.get("apiKey", "")).lower() == self.signer.public), None)
        if row is None:
            known = ", ".join(str(k.get("apiKey", ""))[:8] for k in keys) or "none"
            raise ValueError(f"api key {self.signer.public[:8]} derived from ARCUS_{'MAINNET' if self.mainnet else 'TESTNET'}_PRIVATE_KEY "
                             f"is not registered for {self.address} (registered: {known})")
        status = row.get("status", "ACTIVE")
        if status != "ACTIVE":
            raise ValueError(f"api key {self.signer.public[:8]} is {status}")
        expiry = int(row.get("validUntil") or 0)
        if expiry and expiry < time.time() * 1000:
            raise ValueError(f"api key {self.signer.public[:8]} expired on {time.strftime('%Y-%m-%d', time.localtime(expiry / 1000))}")
        index = row.get("accountIndex")
        if index is not None and int(index) != self.index:
            await self._log.warning(f"api key {self.signer.public[:8]} is bound to account {index}, .env says {self.index}: using {index}")
            self.index = int(index)
            self.exchange.index = self.index
        await self._log.info(f"api key {self.signer.public[:8]} registered for {self.address}, account {self.index}")

    async def stop(self):
        if self.running:
            await self.websocket.stop()
            self.running = False
        await self._log.success("Stopped")
