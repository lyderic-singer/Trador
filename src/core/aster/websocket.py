import asyncio
import itertools
import json
import time
from typing import Callable
from core.aster.utils.constants import LISTEN_KEEPALIVE, MAX_STREAMS
from core.socket import Socket
from core.utils import call, cancel
from log.messenger import Messenger


class AsterMarketSocket(Socket):
    """public streams (bookTicker, aggTrade, partial depth, markPrice) on one connection, subscribed live with
    {"method": "SUBSCRIBE", "params": [...], "id": n}. Events are routed by their stream name when the server wraps
    them ({"stream", "data"}), else by event type and symbol. The server allows 10 incoming messages per second and
    200 streams per connection, pings every 5 minutes (the socket answers by itself; we also ping every 30 s to see a
    dead connection) and closes after 24 h."""

    EVENTS = {"bookTicker": "bookTicker", "aggTrade": "aggTrade", "depthUpdate": "depth", "markPriceUpdate": "markPrice"}

    def __init__(self, url: str, messenger: Messenger):
        super().__init__(url.rstrip("/") + "/ws", messenger, "Aster WebSocket", ping=30.0)
        self.streams: dict[str, Callable] = {}           # "btcusdt@bookTicker" -> callback
        self.pending: dict[int, asyncio.Future] = {}
        self.ids = itertools.count(1)
        self.lock = asyncio.Lock()
        self.sent = 0.0

    async def control(self, method: str, params: list[str], timeout: float = 10.0):
        """one control message every 250 ms at most (the server disconnects above 10 a second)"""
        async with self.lock:
            wait = 0.25 - (time.time() - self.sent)
            if wait > 0:
                await asyncio.sleep(wait)
            rid = next(self.ids)
            future = asyncio.get_running_loop().create_future()
            self.pending[rid] = future
            self.sent = time.time()
            await self.send(json.dumps({"method": method, "params": params, "id": rid}))
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self.pending.pop(rid, None)

    async def opened(self):
        names = list(self.streams)
        for i in range(0, len(names), 100):
            await self.ws.send(json.dumps({"method": "SUBSCRIBE", "params": names[i:i + 100], "id": next(self.ids)}))
            if i + 100 < len(names):
                await asyncio.sleep(0.25)

    def closed(self):
        for f in self.pending.values():
            if not f.done():
                f.set_exception(ConnectionError("aster websocket disconnected"))
        self.pending.clear()

    async def subscribe(self, stream: str, callback: Callable, timeout: float = 10.0):
        if stream in self.streams:
            return
        if len(self.streams) >= MAX_STREAMS:
            raise ValueError(f"more than {MAX_STREAMS} streams on one Aster connection")
        self.streams[stream] = callback
        await self.connected.wait()
        try:
            res = await self.control("SUBSCRIBE", [stream], timeout)
        except asyncio.TimeoutError:
            self.streams.pop(stream, None)
            raise ValueError(f"subscription {stream} not acknowledged")
        except ConnectionError:
            return   # subscribed again by `opened` at the reconnection
        if isinstance(res, dict) and res.get("error"):
            self.streams.pop(stream, None)
            raise ValueError(f"subscription {stream} refused: {res['error']}")
        await self._log.success(f"Subscribed: {stream}", display=False)

    async def unsubscribe(self, stream: str):
        if self.streams.pop(stream, None) and self.connected.is_set():
            try:
                await self.control("UNSUBSCRIBE", [stream], 5.0)
            except Exception:
                pass

    def find(self, data: dict) -> Callable | None:
        kind = self.EVENTS.get(str(data.get("e") or ""))
        symbol = str(data.get("s") or "").lower()
        if not kind or not symbol:
            return None
        prefix = f"{symbol}@{kind}"
        return next((cb for name, cb in self.streams.items() if name == prefix or name.startswith(prefix)), None)

    async def handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return await self._log.warning(f"non-json message: {raw[:200]}", send_tg=False)
        if not isinstance(msg, dict):
            return
        rid = msg.get("id")
        if rid is not None and ("result" in msg or "error" in msg):
            future = self.pending.pop(rid, None) if isinstance(rid, int) else None
            if future is not None and not future.done():
                future.set_result(msg)
            elif msg.get("error"):
                await self._log.warning(f"{msg}", send_tg=False)
            return
        callback, data = None, msg
        if "stream" in msg and isinstance(msg.get("data"), dict):
            data = msg["data"]
            callback = self.streams.get(msg["stream"])
        callback = callback or self.find(data)
        if callback is None:
            return
        try:
            await call(callback, data)
        except Exception as e:
            await self._log.error(f"callback {data.get('e')} failed: {type(e).__name__}: {e}")


class AsterUserSocket(Socket):
    """the user data stream: wss://.../ws/<listenKey>. The key comes from POST /fapi/v3/listenKey at every
    (re)connection, is kept alive every 30 minutes (it dies after 60) and a `listenKeyExpired` event forces a new
    connection. Events: ORDER_TRADE_UPDATE, ACCOUNT_UPDATE, ACCOUNT_CONFIG_UPDATE, MARGIN_CALL."""

    def __init__(self, url: str, messenger: Messenger, exchange):
        super().__init__(url.rstrip("/"), messenger, "Aster User WebSocket", ping=30.0)
        self.base = url.rstrip("/")
        self.exchange = exchange            # AsterExchange (listenKey requests)
        self.listeners: list[Callable] = []
        self.key: str | None = None
        self.keeper: asyncio.Task | None = None

    async def endpoint(self) -> str:
        self.key = await self.exchange.listen_key("POST")
        if not self.key:
            raise ConnectionError("no listenKey returned")
        return f"{self.base}/ws/{self.key}"

    async def start(self, timeout: float = 20.0):
        if self.running:
            return
        await super().start(timeout)
        self.keeper = asyncio.create_task(self.keep())

    async def stop(self):
        await cancel(self.keeper)
        self.keeper = None
        await super().stop()

    async def keep(self):
        while True:
            await asyncio.sleep(LISTEN_KEEPALIVE)
            try:
                await self.exchange.listen_key("PUT")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._log.warning(f"listenKey keepalive failed: {e}", send_tg=False)

    async def handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        if isinstance(msg.get("data"), dict) and "stream" in msg:
            msg = msg["data"]
        if msg.get("e") == "listenKeyExpired":
            await self._log.warning("listenKey expired: reconnecting the user stream", send_tg=False)
            ws = self.ws
            if ws:
                await ws.close()
            return
        for listener in self.listeners:
            try:
                await call(listener, msg)
            except Exception as e:
                await self._log.error(f"callback {msg.get('e')} failed: {type(e).__name__}: {e}")
