import asyncio
import json
from time import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple
from Log.logger import Logger

import websockets

class Subscription(NamedTuple):
    subscription: Dict[str, Any]
    callback: Callable[[Any], Any]
    subscription_id: int

def subscription_to_identifier(subscription: Dict[str, Any]) -> str:
    t = subscription["type"]
    if t == "allMids": return "allMids"
    if t == "l2Book": return f'l2Book:{subscription["coin"].lower()}'
    if t == "activeAssetCtx": return f'activeAssetCtx:{subscription["coin"].lower()}'
    if t == "trades": return f'trades:{subscription["coin"].lower()}'
    if t == "userEvents": return "userEvents"
    if t == "userFills": return f'userFills:{subscription["user"].lower()}'
    if t == "candle": return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    if t == "orderUpdates": return "orderUpdates"
    if t == "userFundings": return f'userFundings:{subscription["user"].lower()}'
    if t == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    if t == "webData2": return f'webData2:{subscription["user"].lower()}'
    if t == "bbo": return f'bbo:{subscription["coin"].lower()}'
    raise ValueError(f"Unknown subscription type: {t}")

def ws_msg_to_identifier(ws_msg: Dict[str, Any]) -> Optional[str]:
    c = ws_msg["channel"]
    d = ws_msg.get("data")
    if c == "pong": return "pong"
    if c == "allMids": return "allMids"
    if c == "l2Book": return f'l2Book:{d["coin"].lower()}'
    if c == "activeAssetCtx": return f'activeAssetCtx:{d["coin"].lower()}'
    if c == "trades": return f'trades:{d[0]["coin"].lower()}' if d else None
    if c == "user": return "userEvents"
    if c == "userFills": return f'userFills:{d["user"].lower()}'
    if c == "candle": return f'candle:{d["s"].lower()},{d["i"]}'
    if c == "orderUpdates": return "orderUpdates"
    if c == "userFundings": return f'userFundings:{d["user"].lower()}'
    if c == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{d["user"].lower()}'
    if c == "webData2": return f'webData2:{d["user"].lower()}'
    if c == "bbo": return f'bbo:{d["coin"].lower()}'
    return None

class Websocket:
    def __init__(self, base_url: str, messenger):
        self.subscription_id_counter = 0
        self.queued_subscriptions: List[Tuple[Dict[str, Any], Subscription]] = []
        self.active_subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self.ws_ready = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.uri = base_url.replace("https", "wss").replace("http", "ws") + "/ws"
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.last_pong = asyncio.get_event_loop().time()
        self.callback_queue: asyncio.Queue = asyncio.Queue()
        self._log = Logger(self, messenger)
        self.last_useful_message = None

    async def _ping(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            if self.ws:
                try:
                    await self.ws.send(json.dumps({"method": "ping"}))
                    await self._log.debug("Ping sent", save = False)
                except Exception as e:
                    await self._log.warning(f"Failed to send ping: {e}")

    async def _pong(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(10)
            if asyncio.get_event_loop().time() - self.last_pong > 40:
                await self._log.error("Pong timeout reached, reconnecting...")
                await self._reconnect()

    async def _sub(self, identifier: str, subscription: Dict[str, Any], subscription_id: int):
        try:
            await self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
            await self._log.info(f"Subscription validée pour {identifier} (id={subscription_id})")
        except (ConnectionError, asyncio.TimeoutError) as e:
            await self._log.error(f"Erreur lors du subscribe de {identifier} (id={subscription_id}): {e}")
            raise

    async def _subqueue(self):
        for subscription, active_sub in self.queued_subscriptions:
            await self.subscribe(subscription, active_sub.callback, active_sub.subscription_id)
        self.queued_subscriptions.clear()

    async def _callback(self):
        while not self.stop_event.is_set():
            callback, msg = await self.callback_queue.get()
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(msg)
                else:
                    callback(msg)
            except Exception as e:
                await self._log.error(f"Error in callback : {e}")

    async def _reconnect(self):
        self.ws_ready.clear()
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                await self._log.error(f"Error closing websocket during reconnect: {e}", exc_info=True)
        self.ws = None

    async def _resubscribe(self):
        for identifier, subs_list in self.active_subscriptions.items():
            for sub in subs_list:
                try:
                    await self._sub(identifier, sub.subscription, sub.subscription_id)
                except Exception as e:
                    await self._log.error(f"Erreur dans le resubscribe de {identifier} : {e}")

    async def _receive(self):
        try:
            async for message in self.ws:
                try:
                    ws_msg = json.loads(message)
                except Exception as e:
                    await self._log.warning(f"Erreur lors du parsing JSON : {e} - Message: {message}")
                    continue
                identifier = ws_msg_to_identifier(ws_msg)
                if identifier == "pong":
                    self.last_pong = asyncio.get_event_loop().time()
                    continue
                if not identifier:
                    continue
                for active_sub in self.active_subscriptions.get(identifier, []):
                    self.last_useful_message = int(time())
                    await self.callback_queue.put((active_sub.callback, ws_msg))
        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
            await self._log.warning(f"WebSocket closed: {e}")
        except Exception as e:
            await self._log.error(f"Unexpected error in _receive() : {e}")

    async def subscribe(self, subscription: Dict[str, Any], callback: Callable[[Any], Any], subscription_id: Optional[int] = None) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        identifier = subscription_to_identifier(subscription)
        if identifier in {"userEvents", "orderUpdates"} and self.active_subscriptions[identifier]:
            await self._log.warning(f"Tentative de multiples abonnements interdits pour {identifier}")
            raise NotImplementedError(f"Multiple subscriptions to {identifier} not allowed")
        for sub in self.active_subscriptions.get(identifier, []):
            if sub.subscription == subscription:
                await self._log.info(f"Souscription déjà active pour {identifier}, aucun nouvel envoi (id existant={sub.subscription_id})")
                return sub.subscription_id
        active_sub = Subscription(subscription, callback, subscription_id)
        if not self.ws_ready.is_set():
            await self._log.info(f"WS pas prête, mise en file d'attente de l'abonnement {identifier} (id={subscription_id})")
            self.queued_subscriptions.append((subscription, active_sub))
        else:
            self.active_subscriptions[identifier].append(active_sub)
            try:
                await self._sub(identifier, subscription, subscription_id)
            except (ConnectionError, asyncio.TimeoutError):
                self.queued_subscriptions.append((subscription, active_sub))
                raise
        return subscription_id

    async def unsubscribe(self, subscription: Dict[str, Any], subscription_id: int) -> bool:
        try:
            if not self.ws_ready.is_set():
                raise RuntimeError("Cannot unsubscribe before websocket is ready")
            identifier = subscription_to_identifier(subscription)
            subs = self.active_subscriptions.get(identifier, [])
            new_subs = [s for s in subs if s.subscription_id != subscription_id]
            if len(new_subs) == len(subs):
                await self._log.warning(f"Abonnement id={subscription_id} non trouvé pour {identifier}")
                return False
            if not new_subs:
                try:
                    await self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
                    await self._log.info(f"Unsubscribe envoyé pour {identifier} (id={subscription_id})")
                except (ConnectionError, asyncio.TimeoutError) as e:
                    await self._log.error(f"Erreur lors de l'envoi du message unsubscribe pour {identifier} (id={subscription_id}): {e}")
                    raise
            self.active_subscriptions[identifier] = new_subs
            return True
        except Exception as e:
            await self._log.error(f"Erreur inattendue dans unsubscribe: {e}")
            raise

    async def unsubscribe_all(self) -> None:
        if not self.ws_ready.is_set():
            raise RuntimeError("Websocket is not ready; cannot unsubscribe all.")
        for identifier, subs_list in list(self.active_subscriptions.items()):
            for sub in subs_list:
                try:
                    await self.ws.send(json.dumps({
                        "method": "unsubscribe",
                        "subscription": sub.subscription
                    }))
                except Exception as e:
                    await self._log.error(f"Erreur pendant l'unsubscribe de {identifier} (id={sub.subscription_id}): {e}")
            self.active_subscriptions[identifier] = []
        await self._log.info("Toutes les subscriptions ont été supprimées")

    async def start(self):
        asyncio.create_task(self._callback())
        asyncio.create_task(self._pong())

        while not self.stop_event.is_set():
            try:
                await self._log.info("Tentative de connexion au WebSocket...")
                self.ws = await websockets.connect(self.uri)
                self.ws_ready.set()
                self.last_pong = asyncio.get_event_loop().time()
                await self._log.success("Connecté au WebSocket.")
                await self._subqueue()
                await self._resubscribe()
                recv_task = asyncio.create_task(self._receive())
                ping_task = asyncio.create_task(self._ping())
                done, pending = await asyncio.wait(
                    [recv_task, ping_task],
                    return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
            except Exception as e:
                await self._log.warning(f"Connexion échouée : {e}. Nouvelle tentative dans 5 secondes.")
            finally:
                await self._reconnect()
                if not self.stop_event.is_set():
                    await asyncio.sleep(5)

    async def stop(self):
        await self._log.info("Arrêt du WebSocket demandé.")
        self.stop_event.set()
        if self.ws:
            try:
                await self.ws.close()
                await self._log.success("Connexion WebSocket fermée.")
            except Exception as e:
                await self._log.error(f"Erreur pendant la fermeture de la connexion au websocket : {e}")
        self.ws = None
        self.ws_ready.clear()
        await self._log.success("WebSocket arrêté proprement.")
