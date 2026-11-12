import asyncio
import itertools
import json
import time
from typing import Callable
import websockets
from core.utils import call, cancel
from log.logger import Logger
from log.messenger import Messenger


def key(channel: str, id: str, index: int | None = None) -> str:
    return f"{channel}:{str(id).lower()}:{index or 0}"


class ArcusSubscription:
    def __init__(self, channel: str, id: str, callback: Callable, params: dict | None = None):
        self.channel = channel
        self.id = id
        self.params = params or {}
        self.callback = callback
        self.key = key(channel, id, self.params.get("accountIndex"))
        self.active = asyncio.Event()
        self.failed = asyncio.Event()
        self.reason = ""

    def message(self, kind: str = "subscribe") -> dict:
        return {"type": kind, "channel": self.channel, "id": self.id, **self.params}


class ArcusWebSocket:
    def __init__(self, url: str, messenger: Messenger):
        self.url = url
        self._log = Logger("Arcus WebSocket", messenger)
        self.ws = None
        self.subscriptions: dict[str, ArcusSubscription] = {}
        self.pending: dict[int, asyncio.Future] = {}
        # request ids must be strictly increasing per connection: one counter for the whole process (never reset) and
        # the id is taken and sent under a lock, so two concurrent requests can never reach the socket out of order
        self.ids = itertools.count(1)
        self.lock = asyncio.Lock()
        self.inflight = asyncio.Semaphore(45)   # the server allows 50 `post` in flight per connection
        self.running = False
        self.connected = asyncio.Event()
        self.reconnect = 1
        self.task = None
        self.received = 0.0      # time.time() of the last frame
        self.connects = 0        # successful connections (1 + reconnections)
        self.since = 0.0         # time.time() of the current connection
        self.disconnect_listeners: list[Callable] = []
        # a cut followed by a successful reconnection at the first retry is not reported (logs.txt only): the error is
        # written (and sent to Telegram) only when the retry fails too, or when the new connections keep dying at once
        self.down_at = 0.0          # time of the first failure of the current incident (0 = up)
        self.down_reason = ""
        self.down_fails = 0
        self.down_logged = False
        self.flaps = 0              # connections in a row that died within FLAP_S of opening
        self.flap_s = 30.0

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        self.running = True
        self.task = asyncio.create_task(self.run())
        await asyncio.wait_for(self.connected.wait(), timeout=20)
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        if self.ws:
            await self.ws.close()
        for f in self.pending.values():
            if not f.done():
                f.cancel()
        await self._log.success("Stopped")

    async def run(self):
        while self.running:
            try:
                # no permessage-deflate: compressing / inflating every frame costs CPU time on the hot path
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24,
                                              compression=None, open_timeout=10) as ws:
                    self.ws = ws
                    self.connects += 1
                    self.since = time.time()
                    self.reconnect = 1
                    self.connected.set()
                    await self.report_connected()
                    for s in self.subscriptions.values():
                        s.active.clear()
                        await ws.send(json.dumps(s.message()))
                    async for raw in ws:
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
                for f in self.pending.values():
                    if not f.done():
                        f.set_exception(ConnectionError("arcus websocket disconnected"))
                self.pending.clear()
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

    async def handle(self, raw):
        self.received = time.time()
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return await self._log.warning(f"non-json message: {raw[:200]}")
        kind = msg.get("type")
        rid = msg.get("id")
        if isinstance(rid, int) and rid in self.pending:
            future = self.pending.pop(rid)
            if not future.done():
                future.set_result(msg)
            return
        if kind in ("subscribed", "channel_data", "unsubscribed"):
            targets = self.route(msg)
            for s in targets:
                if kind == "subscribed":
                    s.active.set()
                elif kind == "unsubscribed":
                    s.active.clear()
            if kind == "unsubscribed":
                return
            callbacks = []
            for s in targets:
                if s.callback not in callbacks:
                    callbacks.append(s.callback)
            for callback in callbacks:
                try:
                    await call(callback, msg)
                except Exception as e:
                    await self._log.error(f"callback {msg.get('channel')} failed: {e}")
        elif kind == "error":
            waiting = [s for s in self.subscriptions.values() if not s.active.is_set() and not s.failed.is_set()]
            for s in waiting:
                s.reason = msg.get("message", str(msg))
                s.failed.set()
            if not waiting:
                await self._log.error(f"{msg}")

    def route(self, msg: dict) -> list[ArcusSubscription]:
        channel, id = msg.get("channel", ""), msg.get("id")
        exact = self.subscriptions.get(key(channel, id or "", msg.get("accountIndex")))
        if exact:
            return [exact]
        same = [s for s in self.subscriptions.values() if s.channel == channel]
        if id:
            same = [s for s in same if s.id.lower() == str(id).lower()]
        return same

    async def subscribe(self, channel: str, id: str, callback: Callable, timeout: float = 15, **params) -> ArcusSubscription:
        s = ArcusSubscription(channel, id, callback, {k: v for k, v in params.items() if v is not None})
        if s.key in self.subscriptions:
            await self._log.info(f"already subscribed {s.key}")
            return self.subscriptions[s.key]
        self.subscriptions[s.key] = s
        await self.connected.wait()
        assert self.ws is not None
        await self.ws.send(json.dumps(s.message()))
        active = asyncio.create_task(s.active.wait())
        failed = asyncio.create_task(s.failed.wait())
        done, pending = await asyncio.wait([active, failed], timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            await cancel(task)
        if not s.active.is_set():
            self.subscriptions.pop(s.key, None)
            raise ValueError(f"subscription {s.key} refused: {s.reason}" if s.failed.is_set() else f"subscription {s.key} not acknowledged")
        await self._log.success(f"Subscribed: {s.key}")
        return s

    async def unsubscribe(self, channel: str, id: str, index: int | None = None):
        s = self.subscriptions.pop(key(channel, id, index), None)
        if s and self.ws and self.connected.is_set():
            await self.ws.send(json.dumps(s.message("unsubscribe")))

    async def request(self, kind: str, request: dict, timeout: float = 10, wait: bool = True) -> dict:
        """RPC over the socket: {"type": kind, "id": n, "request": request} -> the reply carrying the same id.
        wait=False raises ConnectionError at once when the socket is down (an order must not wait for a reconnection)"""
        if not self.connected.is_set():
            if not wait:
                raise ConnectionError("arcus websocket not connected")
            await asyncio.wait_for(self.connected.wait(), timeout=timeout)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        gate = self.inflight if kind == "post" else None
        if gate:
            await gate.acquire()
        try:
            async with self.lock:
                ws = self.ws
                if ws is None or not self.connected.is_set():
                    raise ConnectionError("arcus websocket not connected")
                rid = next(self.ids)
                self.pending[rid] = future
                await ws.send(json.dumps({"type": kind, "id": rid, "request": request}, separators=(",", ":")))
            try:
                return await asyncio.wait_for(future, timeout=timeout)
            finally:
                self.pending.pop(rid, None)
        finally:
            if gate:
                gate.release()

    async def bbo(self, callback: Callable, market: str):
        return await self.subscribe("bbo", market, callback)

    async def oracle(self, callback: Callable, market: str, timeout: float = 15):
        return await self.subscribe("oraclePrices", market, callback, timeout)

    async def l2_book(self, callback: Callable, market: str, levels: int = 20):
        return await self.subscribe("l2Orderbook", market, callback, nLevels=levels)

    async def trades(self, callback: Callable, market: str):
        return await self.subscribe("trades", market, callback)

    async def candles(self, callback: Callable, market: str, timeframe: str):
        return await self.subscribe("candles", market, callback, timeframe=timeframe)

    async def account(self, callback: Callable, address: str, index: int = 0, timeout: float = 15):
        return await self.subscribe("account", address, callback, timeout, accountIndex=index)

    async def positions(self, callback: Callable, address: str, index: int = 0, market: str | None = None, timeout: float = 15):
        return await self.subscribe("positions", address, callback, timeout, accountIndex=index, market=market)

    async def orders(self, callback: Callable, address: str, index: int = 0, market: str | None = None, timeout: float = 15):
        return await self.subscribe("orders", address, callback, timeout, accountIndex=index, market=market)

    async def fills(self, callback: Callable, address: str, index: int = 0, market: str | None = None, timeout: float = 15):
        return await self.subscribe("userFills", address, callback, timeout, accountIndex=index, market=market)
