"""Reconnecting websocket of the Lighter engine (a base class: `handle`, `endpoint`, `opened`, `closed`).

Same rules as the Hyperliquid and Arcus sockets (which keep their own code): no compression, reconnection after 1 s then
exponential up to 10 s, a cut repaired at the first retry is not reported (one line in logs.txt), the error (and
Telegram) only comes when that retry fails too or when the new connections keep dying at once, and the end of a
reported incident is announced. Subclasses give `handle` (one frame) and, when needed, `endpoint` (the URL of the next
connection), `opened` (resubscribe) and `closed` (fail what was waiting for a reply).
"""
import asyncio
import time
from typing import Callable
import websockets
from core.utils import call, cancel
from log.logger import Logger
from log.messenger import Messenger


class Socket:
    def __init__(self, url: str, messenger: Messenger, name: str, ping: float | None = 20.0):
        self.url = url
        self.name = name
        self._log = Logger(name, messenger)
        self.ping = ping
        self.ws = None
        self.running = False
        self.connected = asyncio.Event()
        self.reconnect = 1
        self.task: asyncio.Task | None = None
        self.received = 0.0      # time.time() of the last frame
        self.connects = 0        # successful connections (1 + reconnections)
        self.since = 0.0         # time.time() of the current connection
        self.disconnect_listeners: list[Callable] = []
        self.connect_listeners: list[Callable] = []   # called after every RE-connection (not the first one)
        self.down_at = 0.0          # time of the first failure of the current incident (0 = up)
        self.down_reason = ""
        self.down_fails = 0
        self.down_logged = False
        self.flaps = 0              # connections in a row that died within flap_s of opening
        self.flap_s = 30.0

    async def endpoint(self) -> str:
        return self.url

    async def opened(self):
        """the socket has just (re)connected: send the subscriptions again"""

    def closed(self):
        """the socket has just dropped: fail the requests waiting for a reply"""

    async def handle(self, raw):
        raise NotImplementedError

    async def start(self, timeout: float = 20.0):
        if self.running:
            return await self._log.info("already started")
        self.running = True
        self.task = asyncio.create_task(self.run())
        try:
            await asyncio.wait_for(self.connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await self.stop()
            raise ConnectionError(f"{self.name}: no connection to {self.url} within {timeout:.0f}s")
        await self._log.success("Started")

    async def stop(self):
        if not self.running and self.task is None:
            return
        self.running = False
        await cancel(self.task)
        self.task = None
        ws, self.ws = self.ws, None
        if ws:
            try:
                await ws.close()
            except Exception:
                pass
        self.connected.clear()
        self.closed()
        await self._log.success("Stopped")

    async def send(self, text: str):
        ws = self.ws
        if ws is None or not self.connected.is_set():
            raise ConnectionError("websocket not connected")
        await ws.send(text)

    async def run(self):
        while self.running:
            try:
                url = await self.endpoint()
                # no permessage-deflate: compressing / inflating every frame costs CPU time on the hot path
                async with websockets.connect(url, ping_interval=self.ping, ping_timeout=self.ping, max_size=2**24,
                                              compression=None, open_timeout=10) as ws:
                    self.ws = ws
                    self.connects += 1
                    self.since = time.time()
                    self.reconnect = 1
                    self.connected.set()
                    await self.report_connected()
                    await self.opened()
                    if self.connects > 1:
                        for listener in list(self.connect_listeners):
                            try:
                                await call(listener)
                            except Exception as e:
                                await self._log.error(f"connect listener failed: {e}")
                    async for raw in ws:
                        self.received = time.time()
                        await self.handle(raw)
                    raise ConnectionError("closed by the server")     # reported like any other cut
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self.report_disconnected(e)
            finally:
                was = self.connected.is_set()
                self.connected.clear()
                self.ws = None
                self.closed()
                if was and self.running:
                    for listener in list(self.disconnect_listeners):
                        try:
                            await call(listener)
                        except Exception as e:
                            await self._log.error(f"disconnect listener failed: {e}")
            if self.running:
                await asyncio.sleep(self.reconnect)
                self.reconnect = min(self.reconnect * 2, 10)

    async def report_connected(self):
        down = time.time() - self.down_at if self.down_at else 0.0
        if self.connects == 1:
            await self._log.success("connected")
        elif self.down_logged:
            if self.flaps == 0:
                await self._log.success(f"reconnected (#{self.connects - 1}) after {down:.0f}s", send_tg=True)
        elif self.down_at:
            await self._log.info(f"cut ({self.down_reason}) and reconnected at the first retry in {down:.1f}s "
                                 f"(#{self.connects - 1})", display=False)
        self.down_at, self.down_reason, self.down_fails, self.down_logged = 0.0, "", 0, False

    async def report_disconnected(self, e: Exception):
        """called before the reconnection wait: says nothing when the first retry is going to work"""
        if not self.running:
            return
        retry = f", reconnect in {self.reconnect}s"
        if self.connected.is_set():                     # the connection was up: did it live?
            lived = time.time() - self.since
            self.flaps = self.flaps + 1 if lived < self.flap_s else 0
        if self.flaps >= 1 and self.connected.is_set():
            # reconnected, then died again within flap_s: the reconnection did not really work
            if self.flaps == 1 or self.flaps in (3, 10) or self.flaps % 50 == 0:
                await self._log.error(f"disconnected ({e}){retry}"
                                      + (f" [{self.flaps} short connections in a row]" if self.flaps > 1 else ""))
                self.down_logged = True
        elif self.down_at:
            # the reconnection attempt failed: now it is an incident
            self.down_fails += 1
            if not self.down_logged or self.down_fails in (10, 50) or self.down_fails % 100 == 0:
                await self._log.error(f"disconnected ({self.down_reason}) {time.time() - self.down_at:.0f}s ago, "
                                      f"reconnection failed ({e}){retry} [{self.down_fails} failed attempts]")
                self.down_logged = True
        if not self.down_at:
            self.down_at, self.down_reason, self.down_fails = time.time(), str(e) or type(e).__name__, 0
