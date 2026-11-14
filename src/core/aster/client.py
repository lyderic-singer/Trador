import asyncio
import os
import time
from core.latency import LATENCY
from core.session import Session
from core.aster.api import AsterApi
from core.aster.exchange import AsterExchange
from core.aster.info import AsterInfo
from core.aster.utils.constants import CHAIN_ID, MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL, TESTNET_WS_URL
from core.aster.utils.signing import Signer
from core.aster.websocket import AsterMarketSocket, AsterUserSocket
from core.utils import cancel
from log.logger import Logger
from log.messenger import Messenger


def optional(mainnet: bool, field: str) -> str | None:
    return os.getenv(f"ASTER_{'MAINNET' if mainnet else 'TESTNET'}_{field}") or None


def credentials(mainnet: bool) -> Signer | None:
    """ASTER_<NET>_PRIVATE_KEY   private key of the API wallet ("agent") created on asterdex.com/en/api-wallet, Pro API
    ASTER_<NET>_ADDRESS       the main wallet the agent trades for (shown in /address, sent only if _SEND_USER=1)
    ASTER_<NET>_SIGNER        optional: the agent address, checked against the key
    Without key the client is read-only (market data)."""
    key = optional(mainnet, "PRIVATE_KEY")
    if not key:
        return None
    user = optional(mainnet, "ADDRESS")
    signer = Signer(key.strip(), CHAIN_ID[mainnet], user.strip() if user else None,
                    send_user=(optional(mainnet, "SEND_USER") or "0").strip().lower() in ("1", "true", "yes"))
    expected = optional(mainnet, "SIGNER")
    if expected and expected.strip().lower() != signer.address.lower():
        raise ValueError(f"ASTER_{'MAINNET' if mainnet else 'TESTNET'}_SIGNER is {expected} but the private key is the one "
                         f"of {signer.address}")
    return signer


class AsterClient:
    def __init__(self, mainnet: bool, messenger: Messenger, session: Session, signer: Signer | None = None):
        self.messenger = messenger
        self.mainnet = mainnet
        self._log = Logger("Aster Client", messenger)
        self.signer = signer or credentials(mainnet)
        self.readonly = self.signer is None
        self.address = (self.signer.user or self.signer.address) if self.signer else ""
        self.url = (optional(mainnet, "API_URL") or (MAINNET_API_URL if mainnet else TESTNET_API_URL)).rstrip("/")
        stream = optional(mainnet, "WS_URL") or (MAINNET_WS_URL if mainnet else TESTNET_WS_URL)
        self.api = AsterApi(self.url, messenger, session, self.signer)
        self.info = AsterInfo(messenger, self.api)
        self.exchange = AsterExchange(messenger, self.api)
        self.websocket = AsterMarketSocket(stream, messenger)
        self.user = AsterUserSocket(stream, messenger, self.exchange)
        self.session = session
        self.running = False
        self.clock_task: asyncio.Task | None = None
        self.hedged = False     # the account is in hedge mode (two positions per symbol): not supported

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self._log.info("Starting")
        await self.info.start()
        await self.sync_clock()
        if self.signer is not None:
            await self.check()
        await self.websocket.start()
        # every order is an HTTPS request: its TLS connections are kept warm
        self.session.warm(self.url + "/fapi/v3/ping", every=15.0, connections=4, venue="aster")
        self.clock_task = asyncio.create_task(self.watch_clock())
        self.running = True
        mode = "read-only" if self.readonly else f"agent {self.signer.address[:10]}"
        await self._log.success(f"Started ({len(self.info.by_name)} perps, {mode})")

    async def check(self):
        """one signed read: proves the agent key is accepted, and reads the position mode (one-way is required: the
        strategies hold ONE net position per market)"""
        try:
            res = await self.info.position_mode()
        except Exception as e:
            raise ValueError(f"Aster refused the API wallet {self.signer.address}: {e} (ASTER_"
                             f"{'MAINNET' if self.mainnet else 'TESTNET'}_PRIVATE_KEY must be the key of an agent created "
                             f"in 'Pro API' mode, and the main wallet must have deposited once)") from e
        self.hedged = str(res.get("dualSidePosition")).lower() == "true" if isinstance(res, dict) else False
        if self.hedged:
            raise ValueError("the Aster account is in Hedge Mode (dualSidePosition): switch it to One-way Mode, the "
                             "strategies hold one net position per market")

    async def sync_clock(self, samples: int = 4) -> tuple[float, float] | None:
        """offset = exchange clock - local clock (ms). A nonce more than 60 s away from the exchange clock is refused:
        beyond 2 s of drift the nonces are corrected (and a warning is sent)"""
        best = None
        for _ in range(samples):
            t0 = time.time()
            try:
                server = await self.info.server_time()
            except Exception as e:
                await self._log.debug(f"clock sync failed: {e}")
                continue
            t1 = time.time()
            if server is None:
                continue
            rtt, offset = (t1 - t0) * 1000, server - (t0 + t1) * 500
            if best is None or rtt < best[1]:
                best = (offset, rtt)
        if best is None:
            return None
        LATENCY.clock["aster"] = (best[0], best[1], time.time())
        if self.signer is not None:
            self.signer.clock = int(best[0] * 1000) if abs(best[0]) > 2000 else 0
        if abs(best[0]) > 2000:
            await self._log.warning(f"local clock is {best[0]:+.0f} ms off the Aster clock: nonces corrected "
                                    f"(install chrony / NTP on this server)")
        else:
            await self._log.info(f"clock offset {best[0]:+.0f} ms (rtt {best[1]:.0f} ms)", display=False)
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
        await cancel(self.clock_task)
        self.clock_task = None
        if self.user.running:
            await self.user.stop()
        if self.running:
            await self.websocket.stop()
            self.running = False
        await self._log.success("Stopped")
