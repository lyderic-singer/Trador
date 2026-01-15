
import asyncio
import json
from time import time
import websockets
from log.logger import Logger
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from utils.functions import key

class Subscription:
    def __init__(self, type: str, callback: Callable, user: str | None = None, coin: str | None = None, interval: str | None = None, dex: str | None = None):
        self.type = type
        self.user = user
        self.coin = coin
        self.interval = interval
        self.dex = dex
        self.subscrived = asyncio.Event()
        self.callback = callback
        self.sub = {}
        self.sub["type"] = type
        if user: self.sub["user"] = user
        if coin: self.sub["coin"] = coin
        if interval: self.sub["interval"] = interval
        if dex: self.sub["dex"] = dex
        self.key = key(self.sub)

class WebSocket:
    def __init__(self, url: str, messenger, info):
        self.url = url
        self._log = Logger(messenger)
        self.info = info
        self.ws = None
        self.subscriptions: Dict[str, Subscription] = {}
        self.running = False
        self.reconnect_delay = 5
        self.ping_delay = 30
        self.tasks = []
        self.last_pong = 0
    
    async def connect(self):
        while self.running:
            try:
                await self._log.info("connection attempt")
                self.ws = await websockets.connect(self.url)
                await self._log.success("connected")
                for k in self.subscriptions:
                    s = self.subscriptions[k]
                    await self.subscribe(s.sub, s.callback)
                await asyncio.gather(self.ping(), self.handle())
            except Exception as e:
                await self._log.error(f"disconnected ({e}){f" , reconnect in {self.reconnect_delay}s" if self.running else ""}")
                await asyncio.sleep(self.reconnect_delay)
    
    async def ping(self):
        while self.running and self.ws:
            await asyncio.sleep(self.ping_delay)
            await self.ws.send(json.dumps({"method", "ping"}))
            if time() - self.last_pong > self.ping_delay + 10:
                raise ValueError("pong not received")
    
    async def handle(self):
        async for msg in self.ws:
            msg = json.loads(msg)
            c = msg.get("channel")
            d = msg.get("data")
            if c == "pong":
                self.last_pong == time()
            elif c == "subscriptionResponse" and d:
                s = d.get("subscription")
                k = key(s)
                if k in self.subscriptions:
                    self.subscriptions[k].subscrived.set()
                else:
                    self.subscriptions[k] = s
            elif c and d:

    async def _send_message(self, method: str, subscription: Dict[str, Any]):
        """Envoie un message de souscription/désouscription"""
        if not self.ws or self.ws.closed:
            raise Exception("WebSocket non connecté")
        message = {
            "method": method,
            "subscription": subscription
        }
        await self.ws.send(json.dumps(message))
    
    def on(self, channel: str, handler: Callable):
        """Enregistre un handler pour un canal spécifique"""
        if channel not in self.handlers:
            self.handlers[channel] = set()
        self.handlers[channel].add(handler)
    
    def off(self, channel: str, handler: Optional[Callable] = None):
        """Désenregistre un handler"""
        if channel in self.handlers:
            if handler:
                self.handlers[channel].discard(handler)
            else:
                del self.handlers[channel]
    
    # ========== MÉTHODE GÉNÉRIQUE ==========
    
    async def subscribe(self, subscription: Dict[str, Any], handler: Optional[Callable] = None):
        """Méthode générique de souscription"""
        sub_key = key(subscription)
        self.subscriptions[sub_key] = subscription
        
        if handler:
            channel = subscription["type"]
            self.on(channel, handler)
        
        if self.ws and not self.ws.closed:
            await self._send_message("subscribe", subscription)
        
        print(f"[{datetime.now()}] Souscription ajoutée: {sub_key}")
    
    async def unsubscribe(self, subscription: Dict[str, Any]):
        """Désouscrire d'un flux spécifique"""
        sub_key = key(subscription)
        
        if sub_key in self.subscriptions:
            del self.subscriptions[sub_key]
            
            if self.ws and not self.ws.closed:
                await self._send_message("unsubscribe", subscription)
            
            print(f"[{datetime.now()}] Désouscription: {sub_key}")
    
    async def unsubscribe_all(self):
        """Désouscrire de tous les flux"""
        for subscription in list(self.subscriptions.values()):
            try:
                if self.ws and not self.ws.closed:
                    await self._send_message("unsubscribe", subscription)
            except Exception as e:
                print(f"[{datetime.now()}] Erreur lors de la désouscription: {e}")
        
        self.subscriptions.clear()
        self.handlers.clear()
        print(f"[{datetime.now()}] Toutes les souscriptions ont été supprimées")


    
    # ========== FONCTIONS DÉDIÉES POUR CHAQUE TYPE ==========
    
    async def subscribe_all_mids(self, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux prix mid de tous les actifs"""
        subscription = {"type": "allMids"}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def subscribe_notification(self, user: str, handler: Callable):
        """Souscrire aux notifications d'un utilisateur"""
        subscription = {"type": "notification", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_web_data3(self, user: str, handler: Callable):
        """Souscrire aux données web d'un utilisateur"""
        subscription = {"type": "webData3", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_twap_states(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux états TWAP d'un utilisateur"""
        subscription = {"type": "twapStates", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def subscribe_clearinghouse_state(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire à l'état de la clearinghouse d'un utilisateur"""
        subscription = {"type": "clearinghouseState", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def subscribe_open_orders(self, user: str, handler: Callable, dex: Optional[str] = None):
        """Souscrire aux ordres ouverts d'un utilisateur"""
        subscription = {"type": "openOrders", "user": user}
        if dex:
            subscription["dex"] = dex
        await self.subscribe(subscription, handler)
    
    async def subscribe_candle(self, coin: str, interval: str, handler: Callable):
        """Souscrire aux chandeliers d'un actif"""
        subscription = {"type": "candle", "coin": coin, "interval": interval}
        await self.subscribe(subscription, handler)
    
    async def subscribe_l2_book(self, coin: str, handler: Callable, 
                                n_sig_figs: Optional[int] = None, 
                                mantissa: Optional[int] = None):
        """Souscrire au carnet d'ordres L2 d'un actif"""
        subscription = {"type": "l2Book", "coin": coin}
        if n_sig_figs:
            subscription["nSigFigs"] = n_sig_figs
        if mantissa:
            subscription["mantissa"] = mantissa
        await self.subscribe(subscription, handler)
    
    async def subscribe_trades(self, coin: str, handler: Callable):
        """Souscrire aux trades d'un actif"""
        subscription = {"type": "trades", "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def subscribe_order_updates(self, user: str, handler: Callable):
        """Souscrire aux mises à jour d'ordres d'un utilisateur"""
        subscription = {"type": "orderUpdates", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_events(self, user: str, handler: Callable):
        """Souscrire aux événements utilisateur"""
        subscription = {"type": "userEvents", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_fills(self, user: str, handler: Callable, 
                                   aggregate_by_time: Optional[bool] = None):
        """Souscrire aux fills d'un utilisateur"""
        subscription = {"type": "userFills", "user": user}
        if aggregate_by_time is not None:
            subscription["aggregateByTime"] = aggregate_by_time
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_fundings(self, user: str, handler: Callable):
        """Souscrire aux paiements de funding d'un utilisateur"""
        subscription = {"type": "userFundings", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_non_funding_ledger_updates(self, user: str, handler: Callable):
        """Souscrire aux mises à jour du ledger (hors funding)"""
        subscription = {"type": "userNonFundingLedgerUpdates", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_active_asset_ctx(self, coin: str, handler: Callable):
        """Souscrire au contexte d'un actif actif"""
        subscription = {"type": "activeAssetCtx", "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def subscribe_active_asset_data(self, user: str, coin: str, handler: Callable):
        """Souscrire aux données d'actif actif (Perps uniquement)"""
        subscription = {"type": "activeAssetData", "user": user, "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_twap_slice_fills(self, user: str, handler: Callable):
        """Souscrire aux fills de tranches TWAP"""
        subscription = {"type": "userTwapSliceFills", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_user_twap_history(self, user: str, handler: Callable):
        """Souscrire à l'historique TWAP d'un utilisateur"""
        subscription = {"type": "userTwapHistory", "user": user}
        await self.subscribe(subscription, handler)
    
    async def subscribe_bbo(self, coin: str, handler: Callable):
        """Souscrire au meilleur bid/offer d'un actif"""
        subscription = {"type": "bbo", "coin": coin}
        await self.subscribe(subscription, handler)
    
    async def close(self):
        """Ferme proprement la connexion WebSocket"""
        self.running = False
        await self.unsubscribe_all()
        if self.ws:
            await self.ws.close()
        print(f"[{datetime.now()}] WebSocket fermé")
