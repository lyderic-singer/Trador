import asyncio
import itertools
import json
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
        self.ids = itertools.count(1)
        self.running = False
        self.connected = asyncio.Event()
        self.reconnect = 1
        self.task = None

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
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24) as ws:
                    self.ws = ws
                    self.connected.set()
                    await self._log.success("connected")
                    for s in self.subscriptions.values():
                        s.active.clear()
                        await ws.send(json.dumps(s.message()))
                    async for raw in ws:
                        await self.handle(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._log.error(f"disconnected ({e}){f', reconnect in {self.reconnect}s' if self.running else ''}")
            finally:
                self.connected.clear()
                for f in self.pending.values():
                    if not f.done():
                        f.set_exception(ConnectionError("arcus websocket disconnected"))
                self.pending.clear()
            if self.running:
                await asyncio.sleep(self.reconnect)

    async def handle(self, raw):
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

    async def request(self, kind: str, request: dict, timeout: float = 10) -> dict:
        await self.connected.wait()
        assert self.ws is not None
        rid = next(self.ids)
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        await self.ws.send(json.dumps({"type": kind, "id": rid, "request": request}))
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending.pop(rid, None)

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
