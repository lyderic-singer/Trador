import asyncio
import itertools
import json
from typing import Callable
from core.lighter.api import LighterError
from core.socket import Socket
from core.utils import call, cancel
from log.messenger import Messenger


def route(channel: str) -> str:
    """a subscription names its channel with slashes ("order_book/0"), the messages with colons ("order_book:0")"""
    return channel.replace("/", ":")


class LighterSubscription:
    def __init__(self, channel: str, callback: Callable, private: bool = False):
        self.channel = channel
        self.key = route(channel)
        self.callback = callback
        self.private = private
        self.active = asyncio.Event()
        self.failed = asyncio.Event()
        self.reason = ""


class LighterWebSocket(Socket):
    """one connection for the market data, the account channels and (transport "ws") the transactions.

    The server closes a connection that sent nothing for 2 minutes: the protocol pings of the socket (every 20 s) count
    as frames. Private channels carry the auth token in the subscribe message: `token` is a callable returning the
    current one; `refresh()` subscribes them again when the token is renewed."""

    def __init__(self, url: str, messenger: Messenger):
        super().__init__(url, messenger, "Lighter WebSocket")
        self.subscriptions: dict[str, LighterSubscription] = {}
        self.pending: dict[str, asyncio.Future] = {}     # request id -> reply (insertion order = send order)
        self.ids = itertools.count(1)
        self.lock = asyncio.Lock()
        self.token: Callable[[], str | None] | None = None

    def message(self, s: LighterSubscription, kind: str = "subscribe") -> str:
        msg = {"type": kind, "channel": s.channel}
        if s.private and kind == "subscribe":
            token = self.token() if self.token else None
            if not token:
                raise ValueError(f"{s.channel} needs an auth token (read-only client)")
            msg["auth"] = token
        return json.dumps(msg)

    async def opened(self):
        for s in self.subscriptions.values():
            s.active.clear()
            await self.ws.send(self.message(s))

    def closed(self):
        for f in self.pending.values():
            if not f.done():
                f.set_exception(ConnectionError("lighter websocket disconnected"))
        self.pending.clear()

    async def handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return await self._log.warning(f"non-json message: {raw[:200]}", send_tg=False)
        if not isinstance(msg, dict):
            return
        kind = str(msg.get("type") or "")
        if kind in ("connected", "pong"):
            return
        if kind == "ping":
            return await self.send('{"type":"pong"}')
        channel = msg.get("channel")
        if channel is not None and (kind.startswith("subscribed/") or kind.startswith("update/")):
            s = self.subscriptions.get(str(channel)) or self.subscriptions.get(str(channel).split(":")[0])
            if s is None and kind.split("/", 1)[-1] == "account_orders":
                # account_orders answers "account_orders:<market>" without the account
                s = next((x for x in self.subscriptions.values() if x.key.startswith(f"{channel}:")), None)
            if s is None:
                return
            s.active.set()
            try:
                await call(s.callback, msg)
            except Exception as e:
                await self._log.error(f"callback {channel} failed: {type(e).__name__}: {e}")
            return
        if kind == "unsubscribed":
            return
        if self.reply(msg, kind):
            return
        if "error" in msg:
            waiting = [s for s in self.subscriptions.values() if not s.active.is_set() and not s.failed.is_set()]
            for s in waiting:
                s.reason = str(msg["error"])
                s.failed.set()
            if not waiting:
                await self._log.error(f"{msg}", send_tg=False)

    def reply(self, msg: dict, kind: str) -> bool:
        """a reply to jsonapi/sendtx(batch): matched by the `id` we sent (echoed at the top or in `data`), else, for a
        jsonapi message or a bare error while requests are waiting, given to the oldest one (replies come in order)"""
        if not self.pending:
            return False
        data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
        rid = msg.get("id", data.get("id"))
        future = self.pending.pop(str(rid), None) if rid is not None else None
        if future is None and rid is None and (kind.startswith("jsonapi/") or ("error" in msg and not any(
                not s.active.is_set() and not s.failed.is_set() for s in self.subscriptions.values()))):
            future = self.pending.pop(next(iter(self.pending)))
        if future is None:
            return False
        if not future.done():
            future.set_result(msg)
        return True

    async def subscribe(self, channel: str, callback: Callable, private: bool = False, timeout: float = 15) -> LighterSubscription:
        s = LighterSubscription(channel, callback, private)
        if s.key in self.subscriptions:
            return self.subscriptions[s.key]
        text = self.message(s)
        self.subscriptions[s.key] = s
        await self.connected.wait()
        try:
            await self.send(text)
        except ConnectionError:
            pass  # sent again by `opened` at the reconnection
        active = asyncio.create_task(s.active.wait())
        failed = asyncio.create_task(s.failed.wait())
        await asyncio.wait([active, failed], timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for task in (active, failed):
            await cancel(task)
        if not s.active.is_set():
            self.subscriptions.pop(s.key, None)
            raise ValueError(f"subscription {channel} refused: {s.reason}" if s.failed.is_set()
                             else f"subscription {channel} not acknowledged")
        await self._log.success(f"Subscribed: {channel}", display=False)
        return s

    async def unsubscribe(self, channel: str):
        s = self.subscriptions.pop(route(channel), None)
        if s and self.connected.is_set():
            try:
                await self.send(self.message(s, "unsubscribe"))
            except ConnectionError:
                pass

    async def refresh(self):
        """the auth token was renewed: private channels are subscribed again with the new one (a fresh snapshot)"""
        if not self.connected.is_set():
            return
        for s in [x for x in self.subscriptions.values() if x.private]:
            try:
                await self.send(self.message(s, "unsubscribe"))
                await self.send(self.message(s))
            except ConnectionError:
                return

    async def post(self, kind: str, data: dict) -> asyncio.Future:
        """sends {"type": kind, "data": {"id", ...}} and returns the future of its reply: the caller holds its nonce
        lock only until the frame is written, the replies of several requests are then awaited concurrently"""
        if not self.connected.is_set():
            raise ConnectionError("lighter websocket not connected")
        future = asyncio.get_running_loop().create_future()
        async with self.lock:
            rid = str(next(self.ids))
            self.pending[rid] = future
            try:
                await self.send(json.dumps({"type": kind, "data": {"id": rid, **data}}, separators=(",", ":")))
            except Exception:
                self.pending.pop(rid, None)
                raise
        return future

    @staticmethod
    def parse(msg: dict, path: str) -> dict:
        """reply of jsonapi/sendtx(batch) -> the REST body; raises LighterError like the REST call"""
        data = msg.get("data") if isinstance(msg.get("data"), dict) else msg
        error = msg.get("error") or data.get("error")
        code = data.get("code")
        if isinstance(error, dict):
            raise LighterError(400, int(error["code"]) if error.get("code") is not None else None,
                               str(error.get("message") or error), path)
        if error:
            raise LighterError(400, int(code) if code is not None else None, str(error), path)
        if code is not None and int(code) != 200:
            raise LighterError(400, int(code), str(data.get("message") or ""), path)
        return data
