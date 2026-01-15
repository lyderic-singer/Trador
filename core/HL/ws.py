import asyncio
import json
from time import time
import websockets
from log.logger import Logger
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from utils.functions import key, call, extract_subscription_error

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
        _, pending = await asyncio.wait([self.active.wait(), self.error.wait()], return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
        if self.error.is_set():
            raise ValueError("wrong subscription")

class WebSocket:
    def __init__(self, url: str, messenger, info):
        self.url = url
        self._log = Logger(messenger)
        self.info = info
        self.ws = None
        self.subscriptions: Dict[str, Subscription] = {}
        self.running = asyncio.Event()
        self.reconnect_delay = 5
        self.ping_delay = 30
        self.task = None
        self.last_pong = 0

    async def start(self):
        try:
            self.running.set()
            self.task = asyncio.create_task(self.connect())
            await self._log.info("websocket started")
        except Exception as e:
            await self._log.error(f"cannot start websocket: {e}")

    async def close(self):
        try:
            await self.unsubscribe_all()
            self.running.clear()
            if self.ws:
                await self.ws.close()
            await self._log.success("websocket closed")
        except Exception as e:
            self._log(f"cannot stop websocket: {e}")
    
    async def connect(self):
        while self.running.is_set():
            try:
                await self._log.info("connection attempt")
                self.ws = await websockets.connect(self.url)
                await self._log.success("connected")
                for k in self.subscriptions:
                    s = self.subscriptions[k]
                    await self.subscribe(s.sub, s.callback)
                await asyncio.gather(self.ping(), self.handle())
            except Exception as e:
                await self._log.error(f"disconnected ({e}){f" , reconnect in {self.reconnect_delay}s" if self.running.is_set() else ""}")
                await asyncio.sleep(self.reconnect_delay)
    
    async def ping(self):
        while self.running.is_set() and self.ws:
            await asyncio.sleep(self.ping_delay)
            await self.ws.send(json.dumps({"method", "ping"}))
            if time() - self.last_pong > self.ping_delay + 10:
                raise ValueError("pong not received")
    
    async def handle(self):
        async for msg in self.ws:
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
    
    async def subscribe(self, sub: Dict[str, Any], callback: Callable):
        try:
            subscription = Subscription(sub, callback)
            if subscription.key in self.subscriptions:
                await self._log.info("subscription already set")
                del subscription
                return
            self.subscriptions[subscription.key] = subscription
            await self.running.wait()
            await self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription.sub}))
            await subscription.wait()
            await self._log.success(f"Subscription added: {subscription.key}")
        except Exception as e:
            subscription = self.subscriptions.pop(subscription.key, None)
            del subscription
            await self._log.error(f"{e}")
    
    async def unsubscribe(self, sub: Dict[str, Any]):
        try:
            k = key(sub)
            if not (k in self.subscriptions):
                await self._log.info("not subscribed")
                return
            subscription = self.subscriptions[k]
            await self.running.wait()
            await self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription.sub}))
            try:
                await asyncio.wait_for(subscription.inactive.wait(), timeout=10)
            except asyncio.TimeoutError:
                await self._log.error(f"Timeout for unsubscribe: {subscription.key}")
            await self._log.success(f"Successfully unsubscribed {subscription.key}")
        except Exception as e:
            await self._log.error(f"{e}")
    
    # ========== FONCTIONS DÉDIÉES POUR CHAQUE TYPE ==========
    
    async def all_mids(self, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux prix mid de tous les actifs"""
        subscription = {"type": "allMids"}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def notification(self, user: str, handler: Callable):
        """Souscrire aux notifications d'un utilisateur"""
        subscription = {"type": "notification", "user": user}
        await self.subscribe(subscription, handler)
    
    async def web_data3(self, user: str, handler: Callable):
        """Souscrire aux données web d'un utilisateur"""
        subscription = {"type": "webData3", "user": user}
        await self.subscribe(subscription, handler)
    
    async def twap_states(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux états TWAP d'un utilisateur"""
        subscription = {"type": "twapStates", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def clearinghouse_state(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire à l'état de la clearinghouse d'un utilisateur"""
        subscription = {"type": "clearinghouseState", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def open_orders(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux ordres ouverts d'un utilisateur"""
        subscription = {"type": "openOrders", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def candle(self, coin: str, interval: str, handler: Callable):
        """Souscrire aux chandeliers d'un actif"""
        subscription = {"type": "candle", "coin": coin, "interval": interval}
        await self.subscribe(subscription, handler)
    
    async def l2_book(self, coin: str, handler: Callable, 
                                n_sig_figs: Optional[int] = None, 
                                mantissa: Optional[int] = None):
        """Souscrire au carnet d'ordres L2 d'un actif"""
        subscription = {"type": "l2Book", "coin": coin}
        if n_sig_figs:
            subscription["nSigFigs"] = n_sig_figs
        if mantissa:
            subscription["mantissa"] = mantissa
        await self.subscribe(subscription, handler)
    
    async def trades(self, coin: str, handler: Callable):
        """Souscrire aux trades d'un actif"""
        subscription = {"type": "trades", "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def order_updates(self, user: str, handler: Callable):
        """Souscrire aux mises à jour d'ordres d'un utilisateur"""
        subscription = {"type": "orderUpdates", "user": user}
        await self.subscribe(subscription, handler)
    
    async def user_events(self, user: str, handler: Callable):
        """Souscrire aux événements utilisateur"""
        subscription = {"type": "userEvents", "user": user}
        await self.subscribe(subscription, handler)
    
    async def user_fills(self, user: str, handler: Callable, 
                                   aggregate_by_time: Optional[bool] = None):
        """Souscrire aux fills d'un utilisateur"""
        subscription = {"type": "userFills", "user": user}
        if aggregate_by_time is not None:
            subscription["aggregateByTime"] = aggregate_by_time
        await self.subscribe(subscription, handler)
    
    async def user_fundings(self, user: str, handler: Callable):
        """Souscrire aux paiements de funding d'un utilisateur"""
        subscription = {"type": "userFundings", "user": user}
        await self.subscribe(subscription, handler)
    
    async def user_non_funding_ledger_updates(self, user: str, handler: Callable):
        """Souscrire aux mises à jour du ledger (hors funding)"""
        subscription = {"type": "userNonFundingLedgerUpdates", "user": user}
        await self.subscribe(subscription, handler)
    
    async def active_asset_ctx(self, coin: str, handler: Callable):
        """Souscrire au contexte d'un actif actif"""
        subscription = {"type": "activeAssetCtx", "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def active_asset_data(self, user: str, coin: str, handler: Callable):
        """Souscrire aux données d'actif actif (Perps uniquement)"""
        subscription = {"type": "activeAssetData", "user": user, "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def user_twap_slice_fills(self, user: str, handler: Callable):
        """Souscrire aux fills de tranches TWAP"""
        subscription = {"type": "userTwapSliceFills", "user": user}
        await self.subscribe(subscription, handler)
    
    async def user_twap_history(self, user: str, handler: Callable):
        """Souscrire à l'historique TWAP d'un utilisateur"""
        subscription = {"type": "userTwapHistory", "user": user}
        await self.subscribe(subscription, handler)
    
    async def bbo(self, coin: str, handler: Callable):
        """Souscrire au meilleur bid/offer d'un actif"""
        subscription = {"type": "bbo", "coin": coin}
        await self.subscribe(subscription, handler)
