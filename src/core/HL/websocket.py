import asyncio
import itertools
import json
from time import time
import websockets
from log.logger import Logger
from log.messenger import Messenger
from core.HL.info import HLInfo
from typing import Callable, Dict, Optional
from core.HL.utils.types import Sub
from core.HL.utils.functions import key, call, extract_subscription_error, cancel

class Subscription:
    def __init__(self, sub: Sub, callback: Callable):
        self.type = sub["type"]
        self.user = sub.get("user")
        self.coin = sub.get("coin")
        self.interval = sub.get("interval")
        self.dex = sub.get("dex")
        self.active = asyncio.Event()
        self.inactive = asyncio.Event()
        self.error = asyncio.Event()
        self.callback = callback
        self.sub = sub
        self.key = key(sub)
        self.clear()

    def set(self):
        self.inactive.clear()
        self.active.set()

    def clear(self):
        self.active.clear()
        self.inactive.set()

    async def wait(self):
        error_task = asyncio.create_task(self.error.wait())
        active_task = asyncio.create_task(self.active.wait())
        _, pending = await asyncio.wait([active_task, error_task], return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            await cancel(p)
        if self.error.is_set():
            raise ValueError(f"wrong subscription: {self.sub}")

class HLWebSocket:
    def __init__(self, url: str, messenger: Messenger, info: HLInfo):
        self.url = url
        self._log = Logger("HL WebSocket", messenger)
        self.info = info
        self.ws = None
        self.subscriptions: Dict[str, Subscription] = {}
        self.running = False
        self.connected = asyncio.Event()
        self.reconnect_delay = 1
        self.ping_delay = 30
        self.task = None
        self.last_pong = 0
        # order entry over the socket ("post" requests): replies are matched by id
        self.ids = itertools.count(1)
        self.pending: Dict[int, asyncio.Future] = {}
        self.lock = asyncio.Lock()
        self.inflight = asyncio.Semaphore(90)   # HL allows 100 posts in flight
        self.received = 0.0
        self.connects = 0
        self.connected_at = 0.0     # time of the last successful connect (0 while down)
        self.flaps = 0              # connections in a row that died within FLAP_S of opening
        self.max_delay = 30.0
        self.flap_s = 30.0
        # a cut followed by a successful reconnection at the first retry is not reported (logs.txt only): the error is
        # written (and sent to Telegram) only when the retry fails too, or when the connections keep dying (flaps)
        self.down_at = 0.0          # time of the first failure of the current incident (0 = up)
        self.down_reason = ""
        self.down_fails = 0         # failed attempts since
        self.down_logged = False    # the incident was reported: its end is reported too

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        try:
            self.running = True
            self.task = asyncio.create_task(self.run())
            await self.connected.wait()
            await self._log.success("Started")
        except Exception as e:
            self.running = False
            await self._log.error(f"cannot start websocket: {e}")
            raise

    async def stop(self):
        try:
            subs = [subscription.sub for subscription in self.subscriptions.values()]
            await asyncio.gather(*(self.unsubscribe(sub) for sub in subs))
            self.running = False
            await cancel(self.task)
            if self.ws:
                await self.ws.close()
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            raise
    
    async def run(self):
        while self.running:
            try:
                # no permessage-deflate: inflating every frame costs CPU time on the hot path
                self.ws = await websockets.connect(self.url, compression=None, max_size=2**24, open_timeout=10)
                self.connects += 1
                self.connected_at = time()
                self.connected.set()
                await self.report_connected()
                for k in list(self.subscriptions):
                    subscription = self.subscriptions[k]
                    subscription.clear()    # active again only once the server confirms it on THIS connection
                    await self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription.sub}))
                self.last_pong = time()
                tasks = [asyncio.create_task(self.ping()), asyncio.create_task(self.handle())]
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                    raise ConnectionError("closed by the server")
                finally:
                    for task in tasks:
                        await cancel(task)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.drop()   # at once: an order sent now goes by HTTP instead of waiting for a dead socket
                lived = time() - self.connected_at if self.connected_at else 0.0
                self.connected_at = 0.0
                if lived and lived < self.flap_s:
                    self.flaps += 1
                    self.reconnect_delay = min(self.reconnect_delay * 2, self.max_delay)
                    if self.flaps >= 3:
                        await self.shed()
                else:
                    self.flaps, self.reconnect_delay = 0, 1
                await self.report_disconnected(e)
                await asyncio.sleep(self.reconnect_delay)
            finally:
                self.drop()
                if self.ws:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass

    async def report_connected(self):
        down = time() - self.down_at if self.down_at else 0.0
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
        """one message per incident, not one per reconnect (01/11: hundreds of Telegram alerts), and none at all when
        the first retry works"""
        retry = f", reconnect in {self.reconnect_delay}s" if self.running else ""
        if not self.running:
            return
        if self.flaps >= 1:
            # reconnected, then died again within flap_s: the reconnection did not really work
            if self.flaps == 1 or self.flaps in (3, 10) or self.flaps % 50 == 0:
                await self._log.error(f"disconnected ({e}){retry}"
                                      + (f" [{self.flaps} short connections in a row]" if self.flaps > 1 else ""))
                self.down_logged = True
        elif self.down_at:
            # the reconnection attempt failed: now it is an incident
            self.down_fails += 1
            if not self.down_logged or self.down_fails in (10, 50) or self.down_fails % 100 == 0:
                await self._log.error(f"disconnected ({self.down_reason}) {time() - self.down_at:.0f}s ago, reconnection "
                                      f"failed ({e}){retry} [{self.down_fails} failed attempts]")
                self.down_logged = True
        if not self.down_at:
            self.down_at, self.down_reason, self.down_fails = time(), str(e) or type(e).__name__, 0

    async def shed(self):
        """connections dying right after the resubscription, again and again (01/11 after the settlement of an
        outcome: its books were resubscribed at every reconnect): the outcome books ("#...") the server never
        confirmed on the last connection are dropped. User and perp subscriptions are never dropped here"""
        dead = [k for k, x in self.subscriptions.items()
                if not x.active.is_set() and str(x.coin or "").startswith("#")]
        for k in dead:
            self.subscriptions.pop(k, None)
        if dead:
            await self._log.warning(f"dropped {len(dead)} unconfirmed outcome subscription(s) after {self.flaps} short "
                                    f"connections: {', '.join(dead)}")

    def forget(self, sub: Sub):
        """stop following a subscription without talking to the server (it is not resent on reconnect)"""
        self.subscriptions.pop(key(sub), None)

    def drop(self):
        self.connected.clear()
        for f in self.pending.values():
            if not f.done():
                f.set_exception(ConnectionError("hyperliquid websocket disconnected"))
        self.pending.clear()

    async def ping(self):
        assert self.ws is not None
        while self.connected.is_set():
            await asyncio.sleep(self.ping_delay)
            await self.ws.send(json.dumps({"method": "ping"}))
            if time() - self.last_pong > self.ping_delay + 10:
                raise ValueError("pong not received")
    
    async def handle(self):
        assert self.ws is not None
        async for msg in self.ws:
            try:
                self.received = time()
                msg = json.loads(msg)
                channel = msg.get("channel")
                data = msg.get("data")
                if channel == "post":
                    future = self.pending.pop(data.get("id"), None) if isinstance(data, dict) else None
                    if future is not None and not future.done():
                        future.set_result(data.get("response") or {})
                    continue
                if channel == "pong":
                    self.last_pong = time()
                elif channel == "subscriptionResponse" and data:
                    method = data.get("method")
                    sub = data.get("subscription")
                    k = key(sub)
                    if k in self.subscriptions:
                        if method == "subscribe":
                            self.subscriptions[k].set()
                        elif method == "unsubscribe":
                            self.subscriptions[k].clear()
                        else:
                            await self._log.warning(f"unexpected method: {method}")
                elif channel == "error" and data:
                    data = extract_subscription_error(data)
                    sub: Sub = data.get("subscription", {})
                    k = key(sub)
                    if k in self.subscriptions:
                        self.subscriptions[k].error.set()
                        self.subscriptions[k].clear()
                elif channel and data:
                    k = key(msg)
                    if k in self.subscriptions:
                        subscription = self.subscriptions[k]
                        await call(subscription.callback, data)
            except Exception as e:
                await self._log.warning(f"{e}")
                continue
    
    async def post(self, request: dict, timeout: float = 10) -> dict:
        """{"method": "post", "id": n, "request": {"type": "action" | "info", "payload": ...}} -> the reply's
        `response` ({"type": "action" | "info" | "error", "payload": ...}); ConnectionError when the socket is down"""
        if not self.connected.is_set() or self.ws is None:
            raise ConnectionError("hyperliquid websocket not connected")
        future = asyncio.get_running_loop().create_future()
        await self.inflight.acquire()
        try:
            async with self.lock:
                ws = self.ws
                if ws is None or not self.connected.is_set():
                    raise ConnectionError("hyperliquid websocket not connected")
                rid = next(self.ids)
                self.pending[rid] = future
                await ws.send(json.dumps({"method": "post", "id": rid, "request": request}, separators=(",", ":")))
            try:
                return await asyncio.wait_for(future, timeout=timeout)
            finally:
                self.pending.pop(rid, None)
        finally:
            self.inflight.release()

    async def subscribe(self, sub: Sub, callback: Callable):
        assert self.ws is not None
        subscription = None
        try:
            subscription = Subscription(sub, callback)
            if subscription.key in self.subscriptions:
                await self._log.info("subscription already set")
                return sub
            self.subscriptions[subscription.key] = subscription
            await self.connected.wait()
            await self.ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
            await subscription.wait()
            await self._log.success(f"Subscribed: {subscription.key}")
            return sub
        except Exception as e:
            if subscription and subscription.key in self.subscriptions:
                k = subscription.key
                self.subscriptions.pop(k, None)
            await self._log.error(f"{e}")
            raise e
    
    async def unsubscribe(self, sub: Sub):
        assert self.ws is not None
        k = None
        try:
            k = key(sub)
            if not (k in self.subscriptions):
                await self._log.info("not subscribed")
                return
            subscription = self.subscriptions[k]
            await self.connected.wait()
            await self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription.sub}))
            try:
                await asyncio.wait_for(subscription.inactive.wait(), timeout=10)
                await self._log.success(f"Unsubscribed {subscription.key}")
            except asyncio.TimeoutError:
                raise ValueError(f"Timeout for unsubscribe: {subscription.key}")
        except Exception as e:
            await self._log.error(f"{e}")
        finally:
            if k:
                subscription = self.subscriptions.pop(k, None)
    
    async def all_mids(self, callback: Callable, dex: Optional[str] = None):
        subscription: Sub = {"type": "allMids"}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)
    
    async def notification(self, callback: Callable, user: str):
        subscription: Sub = {"type": "notification", "user": user}
        await self.subscribe(subscription, callback)
    
    async def web_data3(self, callback: Callable, user: str):
        subscription: Sub = {"type": "webData3", "user": user}
        return await self.subscribe(subscription, callback)
    
    async def twap_states(self, callback: Callable, user: str, dex: Optional[str] = ""):
        subscription: Sub = {"type": "twapStates", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def clearinghouse_state(self, callback: Callable, user: str, dex: Optional[str] = ""):
        subscription: Sub = {"type": "clearinghouseState", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def open_orders(self, callback: Callable, user: str, dex: Optional[str] = ""):
        subscription: Sub = {"type": "openOrders", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def candle(self, callback: Callable, x: str | int, interval: str):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "candle", "coin": instrument.coin, "interval": interval}
        await self.subscribe(subscription, callback)

    async def l2_book(self, callback: Callable, x: str | int, n_sig_figs: Optional[int] = None, mantissa: Optional[int] = None):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "l2Book", "coin": instrument.coin}
        if n_sig_figs: subscription["nSigFigs"] = n_sig_figs
        if mantissa: subscription["mantissa"] = mantissa
        await self.subscribe(subscription, callback)

    async def trades(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "trades", "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def order_updates(self, callback: Callable, user: str):
        subscription: Sub = {"type": "orderUpdates", "user": user}
        await self.subscribe(subscription, callback)

    async def user_events(self, callback: Callable, user: str):
        subscription: Sub = {"type": "userEvents", "user": user}
        await self.subscribe(subscription, callback)

    async def user_fills(self, callback: Callable, user: str, aggregate_by_time: Optional[bool] = None):
        subscription: Sub = {"type": "userFills", "user": user}
        if aggregate_by_time is not None: subscription["aggregateByTime"] = aggregate_by_time
        await self.subscribe(subscription, callback)

    async def user_fundings(self, callback: Callable, user: str):
        subscription: Sub = {"type": "userFundings", "user": user}
        await self.subscribe(subscription, callback)

    async def user_non_funding_ledger_updates(self, callback: Callable, user: str):
        subscription: Sub = {"type": "userNonFundingLedgerUpdates", "user": user}
        await self.subscribe(subscription, callback)

    async def active_asset_ctx(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "activeAssetCtx", "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def active_asset_data(self, callback: Callable, user: str, x: str | int):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "activeAssetData", "user": user, "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def user_twap_slice_fills(self, callback: Callable, user: str):
        subscription: Sub = {"type": "userTwapSliceFills", "user": user}
        await self.subscribe(subscription, callback)

    async def user_twap_history(self, callback: Callable, user: str):
        subscription: Sub = {"type": "userTwapHistory", "user": user}
        await self.subscribe(subscription, callback)

    async def bbo(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription: Sub = {"type": "bbo", "coin": instrument.coin}
        await self.subscribe(subscription, callback)
