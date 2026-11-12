import asyncio
import time
import aiohttp
from core.latency import LATENCY
from core.utils import cancel
from log.logger import Logger


class Session:
    """one aiohttp session shared by every REST client.

    Latency: a new HTTPS request on a cold connection pays DNS + TCP + TLS (3 to 4 round trips, 300-600 ms from Europe
    to Asia) before the request itself. The connector keeps connections alive and caches DNS, and `warm` pings a free
    endpoint every few seconds on several connections at once so that a REST request (fallback of the websocket order
    entry, dead man's switch, account reads) always finds an open TLS connection.
    """

    def __init__(self, messenger):
        self.session = None
        self._log = Logger(self, messenger)
        self.warmers: list[asyncio.Task] = []

    @staticmethod
    def make() -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit=100, limit_per_host=32,      # several requests in flight to the same host
            ttl_dns_cache=600,                 # no DNS lookup on the hot path
            keepalive_timeout=120,             # keep idle TLS connections (the warmer refreshes them)
            enable_cleanup_closed=True)
        return aiohttp.ClientSession(headers={"Content-Type": "application/json"}, connector=connector,
                                     timeout=aiohttp.ClientTimeout(total=30, connect=10))

    async def start(self):
        try:
            if self.session and not self.session.closed:
                return await self._log.info("already started")
            self.session = self.make()
            await self._log.success("Started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def get(self) -> aiohttp.ClientSession:
        if self.session is None:
            await self._log.debug("New session")
            self.session = self.make()
        if self.session.closed:
            await self._log.info("Session closed, creating a new one")
            self.session = self.make()
        return self.session

    def warm(self, url: str, every: float = 15.0, connections: int = 3, venue: str = "", method: str = "GET",
             body: dict | None = None):
        """keeps `connections` TLS connections to the host of `url` open (one request each, concurrently, every `every`
        s); the round trips are recorded as `<venue> health/rest` latency samples"""
        self.warmers.append(asyncio.create_task(self.warmer(url, every, connections, venue, method, body)))

    async def warmer(self, url: str, every: float, connections: int, venue: str, method: str, body: dict | None):
        async def ping():
            t = time.perf_counter()
            try:
                session = await self.get()
                async with session.request(method, url, json=body) as response:
                    await response.read()
                    ok = response.status < 400
                if venue:
                    LATENCY.record(venue, "health", "rest", (time.perf_counter() - t) * 1000, ok)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if venue:
                    LATENCY.record(venue, "health", "rest", (time.perf_counter() - t) * 1000, False, error=str(e))
        while True:
            try:
                await asyncio.gather(*(ping() for _ in range(max(connections, 1))))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(every)

    async def stop(self):
        for task in self.warmers:
            await cancel(task)
        self.warmers.clear()
        if self.session and not self.session.closed:
            await self.session.close()
            await self._log.success("Stopped")
        else:
            await self._log.info("Session already closed or does not exist")
