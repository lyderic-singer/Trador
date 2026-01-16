import asyncio
import json
from time import time
import websockets
from log.logger import Logger, Messenger
from .info import Info
from typing import Callable, Dict, Any, Optional
from .utils.functions import key, call, extract_subscription_error

class Subscription:
    def __init__(self, sub: Dict[str, Any], callback: Callable):
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
            p.cancel()
        if self.error.is_set():
            raise ValueError(f"wrong subscription: {self.sub}")

class WebSocket:
    def __init__(self, url: str, messenger: Messenger, info: Info):
        self.url = url
        self._log = Logger(self, messenger)
        self.info = info
        self.ws = None
        self.subscriptions: Dict[str, Subscription] = {}
        self.running = False
        self.connected = asyncio.Event()
        self.reconnect_delay = 5
        self.ping_delay = 30
        self.task = None
        self.last_pong = 0

    async def start(self):
        try:
            self.running = True
            self.task = asyncio.create_task(self.connect())
            await self.connected.wait()
            await self._log.info("websocket started")
        except Exception as e:
            self.running = False
            await self._log.error(f"cannot start websocket: {e}")

    async def close(self):
        try:
            self.connected.clear()
            self.running = False
            if self.ws:
                await self.ws.close()
            await self._log.success("websocket closed")
        except Exception as e:
            self._log(f"cannot stop websocket: {e}")
    
    async def connect(self):
        while self.running:
            try:
                self.ws = await websockets.connect(self.url)
                self.connected.set()
                await self._log.success("connected")
                for k in self.subscriptions:
                    subscription = self.subscriptions[k]
                    await self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription.sub}))
                self.last_pong = time()
                await asyncio.gather(self.ping(), self.handle())
            except Exception as e:
                await self._log.error(f"disconnected ({e}){f" , reconnect in {self.reconnect_delay}s" if self.running else ""}")
                await asyncio.sleep(self.reconnect_delay)
            finally:
                if self.ws:
                    await self.ws.close()
                self.connected.clear()
    
    async def ping(self):
        while self.connected.is_set():
            await asyncio.sleep(self.ping_delay)
            await self.ws.send(json.dumps({"method": "ping"}))
            if time() - self.last_pong > self.ping_delay + 10:
                raise ValueError("pong not received")
    
    async def handle(self):
        async for msg in self.ws:
            try:
                msg = json.loads(msg)
                channel = msg.get("channel")
                data = msg.get("data")
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
                    sub = data.get("subscription")
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
    
    async def subscribe(self, sub: Dict[str, Any], callback: Callable):
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
            await self._log.success(f"Subscription added: {subscription.key}")
        except Exception as e:
            if subscription and subscription.key in self.subscriptions:
                k = subscription.key
                self.subscriptions.pop(k, None)
            await self._log.error(f"{e}")
        return sub
    
    async def unsubscribe(self, sub: Dict[str, Any]):
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
                await self._log.success(f"Successfully unsubscribed {subscription.key}")
            except asyncio.TimeoutError:
                await self._log.error(f"Timeout for unsubscribe: {subscription.key}")
        except Exception as e:
            await self._log.error(f"{e}")
        finally:
            if k:
                subscription = self.subscriptions.pop(k, None)
    
    async def all_mids(self, callback: Callable, dex: Optional[str] = None):
        subscription = {"type": "allMids"}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)
    
    async def notification(self, callback: Callable, user: str):
        subscription = {"type": "notification", "user": user}
        await self.subscribe(subscription, callback)
    
    async def web_data3(self, callback: Callable, user: str):
        subscription = {"type": "webData3", "user": user}
        return await self.subscribe(subscription, callback)
    
    async def twap_states(self, callback: Callable, user: str, dex: Optional[str] = None):
        subscription = {"type": "twapStates", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def clearinghouse_state(self, callback: Callable, user: str, dex: Optional[str] = None):
        subscription = {"type": "clearinghouseState", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def open_orders(self, callback: Callable, user: str, dex: Optional[str] = None):
        subscription = {"type": "openOrders", "user": user}
        if dex: subscription["dex"] = dex
        await self.subscribe(subscription, callback)

    async def candle(self, callback: Callable, x: str | int, interval: str):
        instrument = self.info.instrument(x)
        subscription = {"type": "candle", "coin": instrument.coin, "interval": interval}
        await self.subscribe(subscription, callback)

    async def l2_book(self, callback: Callable, x: str | int, n_sig_figs: Optional[int] = None, mantissa: Optional[int] = None):
        instrument = self.info.instrument(x)
        subscription = {"type": "l2Book", "coin": instrument.coin}
        if n_sig_figs: subscription["nSigFigs"] = n_sig_figs
        if mantissa: subscription["mantissa"] = mantissa
        await self.subscribe(subscription, callback)

    async def trades(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription = {"type": "trades", "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def order_updates(self, callback: Callable, user: str):
        subscription = {"type": "orderUpdates", "user": user}
        await self.subscribe(subscription, callback)

    async def user_events(self, callback: Callable, user: str):
        subscription = {"type": "userEvents", "user": user}
        await self.subscribe(subscription, callback)

    async def user_fills(self, callback: Callable, user: str, aggregate_by_time: Optional[bool] = None):
        subscription = {"type": "userFills", "user": user}
        if aggregate_by_time is not None: subscription["aggregateByTime"] = aggregate_by_time
        await self.subscribe(subscription, callback)

    async def user_fundings(self, callback: Callable, user: str):
        subscription = {"type": "userFundings", "user": user}
        await self.subscribe(subscription, callback)

    async def user_non_funding_ledger_updates(self, callback: Callable, user: str):
        subscription = {"type": "userNonFundingLedgerUpdates", "user": user}
        await self.subscribe(subscription, callback)

    async def active_asset_ctx(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription = {"type": "activeAssetCtx", "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def active_asset_data(self, callback: Callable, user: str, x: str | int):
        instrument = self.info.instrument(x)
        subscription = {"type": "activeAssetData", "user": user, "coin": instrument.coin}
        await self.subscribe(subscription, callback)

    async def user_twap_slice_fills(self, callback: Callable, user: str):
        subscription = {"type": "userTwapSliceFills", "user": user}
        await self.subscribe(subscription, callback)

    async def user_twap_history(self, callback: Callable, user: str):
        subscription = {"type": "userTwapHistory", "user": user}
        await self.subscribe(subscription, callback)

    async def bbo(self, callback: Callable, x: str | int):
        instrument = self.info.instrument(x)
        subscription = {"type": "bbo", "coin": instrument.coin}
        await self.subscribe(subscription, callback)
