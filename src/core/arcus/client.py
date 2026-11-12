import asyncio
import time
from core.config import env
from core.latency import LATENCY
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
        self.exchange.ws = self.websocket      # order entry over the socket (see ArcusExchange.transport)
        self.session = session
        self.url = url
        self.running = False
        self.clock_task = None

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self._log.info("Starting")
        await self.info.start()
        await self.check()
        await self.sync_clock()
        await self.websocket.start()
        # REST stays the fallback (socket down) and carries the dead man's switch: its TLS connections are kept warm
        self.session.warm(self.url + "/health", every=15.0, connections=3, venue="arcus")
        self.clock_task = asyncio.create_task(self.watch_clock())
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

    async def sync_clock(self, samples: int = 5) -> tuple[float, float] | None:
        """offset = exchange clock - local clock (ms), from the GET /v1/time of lowest round trip. Signed timestamps must
        be within +-30 s of the exchange: beyond 2 s of drift they are corrected (and a warning is sent)"""
        best = None
        for _ in range(samples):
            t0 = time.time_ns()
            try:
                server = await self.info.server_time()
            except Exception as e:
                await self._log.debug(f"clock sync failed: {e}")
                continue
            t1 = time.time_ns()
            if server is None:
                continue
            rtt = (t1 - t0) / 1e6
            offset = (server - (t0 + t1) / 2) / 1e6
            if best is None or rtt < best[1]:
                best = (offset, rtt)
        if best is None:
            return None
        LATENCY.clock["arcus"] = (best[0], best[1], time.time())
        if abs(best[0]) > 2000:
            self.exchange.clock = int(best[0] * 1e6)
            await self._log.warning(f"local clock is {best[0]:+.0f} ms off the Arcus clock: signed timestamps corrected "
                                    f"(install chrony / NTP on this server)")
        elif abs(best[0]) > 100:
            await self._log.warning(f"local clock {best[0]:+.0f} ms off the Arcus clock (rtt {best[1]:.0f} ms): the feed "
                                    f"delays measured are corrected, but sync this server with NTP", send_tg=False)
        else:
            await self._log.info(f"clock offset {best[0]:+.1f} ms (rtt {best[1]:.1f} ms)")
        return best

    async def watch_clock(self):
        while True:
            await asyncio.sleep(600)
            try:
                await self.sync_clock(3)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def stop(self):
        if self.clock_task:
            self.clock_task.cancel()
            try:
                await self.clock_task
            except BaseException:
                pass
            self.clock_task = None
        if self.running:
            await self.websocket.stop()
            self.running = False
        await self._log.success("Stopped")
