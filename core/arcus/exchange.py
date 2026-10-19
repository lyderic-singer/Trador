import time
from typing import Any
from core.arcus.api import ArcusApi, ArcusError
from core.arcus.info import ArcusInfo
from core.arcus.utils.constants import EXPIRY_DAYS, MAX_BATCH, SIDE, TIF
from core.arcus.utils.signing import Signer, cancel_payload, modify_payload, place_payload
from core.arcus.utils.types import ArcusCancel, ArcusModify, ArcusOrder, fmt
from core.latency import LATENCY
from log.logger import Logger
from log.messenger import Messenger

# a websocket reply with one of these statuses means the method itself is not served over the socket: REST from then on
UNSUPPORTED = (404, 405, 501)


class ArcusExchange:
    def __init__(self, messenger: Messenger, api: ArcusApi, info: ArcusInfo, signer: Signer, address: str, index: int = 0):
        self._log = Logger("Arcus Exchange", messenger)
        self.api = api
        self.info = info
        self.signer = signer
        self.address = address
        self.index = index
        # order entry over the already open websocket (no HTTPS round trip, no TLS, no headers): "ws" or "rest";
        # REST is used anyway when the socket is down at send time (never as a retry: the order may have arrived)
        self.transport = "ws"
        self.ws = None              # ArcusWebSocket, wired by the client
        self.timeout = 5.0          # s, an order reply later than that is reported as failed (the stream reconciles)
        self.clock = 0              # ns added to time.time_ns() in signed timestamps (set when the local clock is off)
        self.shapes: set[str] = set()   # reply shapes already logged once (unknown formats)

    @staticmethod
    def expiry() -> int:
        return int(time.time() * 1_000_000) + EXPIRY_DAYS * 86_400 * 1_000_000

    def now_ns(self) -> int:
        return time.time_ns() + getattr(self, "clock", 0)

    @property
    def streaming(self) -> bool:
        ws = getattr(self, "ws", None)
        return getattr(self, "transport", "rest") == "ws" and ws is not None and ws.connected.is_set()

    def headers(self, ts: int, signature: str) -> dict:
        return {"X-API-Key": self.signer.public, "X-Timestamp": str(ts), "X-Signature": signature}

    def build_place(self, o: ArcusOrder, ts: int, expiry: int) -> dict:
        m = o.market
        expiry = o.expiry or expiry
        side = "BUY" if o.is_buy else "SELL"
        payload = place_payload(self.address, self.index, ts, expiry * 1000, m.id, m.to_ticks(o.price),
                                m.to_quantums(o.size), o.reduce, SIDE[side], TIF[o.tif], o.client)
        element = {
            "address": self.address, "accountIndex": self.index, "marketId": m.id,
            "orderSide": side, "orderType": o.kind, "quantity": fmt(o.size), "price": fmt(o.price),
            "timeInForce": o.tif, "goodTilTime": str(expiry), "timestamp": ts,
            "reduceOnly": o.reduce, "signature": self.signer.sign_payload(payload),
        }
        if o.client:
            element["clientId"] = o.client
        return element

    def build_modify(self, mo: ArcusModify, ts: int) -> dict:
        m = mo.market
        side = "BUY" if mo.is_buy else "SELL"
        payload = modify_payload(self.address, self.index, ts, mo.expiry * 1000, mo.oid, m.id, m.to_ticks(mo.price),
                                 m.to_quantums(mo.size), mo.reduce, SIDE[side], TIF[mo.tif])
        return {
            "address": self.address, "accountIndex": self.index, "marketId": m.id,
            "orderId": mo.oid, "side": side, "quantity": fmt(mo.size), "price": fmt(mo.price),
            "timeInForce": mo.tif, "reduceOnly": mo.reduce, "goodTilTime": str(mo.expiry),
            "clientTime": str(ts), "signature": self.signer.sign_payload(payload),
        }

    def build_cancel(self, c: ArcusCancel, ts: int) -> dict:
        payload = cancel_payload(self.address, self.index, ts, c.market.id, c.oid, c.client)
        element = {
            "address": self.address, "accountIndex": self.index, "marketId": c.market.id,
            "timestamp": ts, "signature": self.signer.sign_payload(payload),
        }
        if c.oid is not None:
            element.update(kind="orderId", orderId=c.oid)
        else:
            element.update(kind="clientId", clientId=c.client)
        return element

    @staticmethod
    def normalize(result: Any) -> dict:
        """websocket `result` of a batch -> the REST body shape {"responses": [...], "rateLimit": ...}"""
        if isinstance(result, dict):
            for k in ("responses", "results", "orders", "cancels", "modifies"):
                if isinstance(result.get(k), list):
                    return {"responses": result[k], "rateLimit": result.get("rateLimit")}
            if "orderId" in result or "status" in result:
                return {"responses": [result], "rateLimit": result.get("rateLimit")}
            return {"responses": [], "rateLimit": result.get("rateLimit")}
        if isinstance(result, list):
            return {"responses": result, "rateLimit": None}
        return {"responses": [], "rateLimit": None}

    async def rpc(self, method: str, body: dict, ts: int, signature: str, items: int) -> dict:
        """one signed `post` over the websocket; raises ArcusError like the REST call on a 4xx / 5xx reply"""
        assert self.ws is not None
        request = {"type": method, "payload": body, "apiKey": self.signer.public, "timestamp": str(ts),
                   "signature": signature}
        t = time.perf_counter()
        try:
            msg = await self.ws.request("post", request, timeout=self.timeout, wait=False)
        except Exception as e:
            LATENCY.record("arcus", method, "ws", (time.perf_counter() - t) * 1000, False, items, type(e).__name__)
            raise
        ms = (time.perf_counter() - t) * 1000
        status = msg.get("status")
        try:
            status = int(status) if status is not None else (400 if msg.get("error") else 200)
        except (TypeError, ValueError):
            status = 200 if str(status).upper() in ("OK", "ACK") else 400
        if status >= 400 or (msg.get("error") and msg.get("result") is None):
            error = msg.get("error") or msg
            LATENCY.record("arcus", method, "ws", ms, False, items, str(status))
            if status in UNSUPPORTED:
                self.transport = "rest"
                await self._log.warning(f"{method} refused over the websocket (HTTP {status}: {error}): orders go by REST now")
            raise ArcusError(status, error, method)
        LATENCY.record("arcus", method, "ws", ms, True, items)
        out = self.normalize(msg.get("result"))
        if len(out["responses"]) != items:
            # a reply we cannot map back to the orders: the strategies would lose track of live orders, so REST from
            # now on (the format seen is logged once to adapt the parser)
            shape = f"{method}:{type(msg.get('result')).__name__}:{len(out['responses'])}/{items}"
            self.transport = "rest"
            if method not in self.shapes:
                self.shapes.add(method)
                await self._log.warning(f"unexpected websocket reply for {method} ({shape}): orders go by REST now | "
                                        f"{str(msg)[:400]}")
        return out

    async def batch(self, path: str, key: str, items: list, build) -> dict:
        responses: list[dict] = []
        limits = None
        method = path.rsplit("/", 1)[-1]
        for i in range(0, len(items), MAX_BATCH):
            ts = self.now_ns()
            elements = [build(x, ts) for x in items[i:i + MAX_BATCH]]
            # every element carries its own signature; the envelope / header one is any element's (required, unused)
            if self.streaming:
                res = await self.rpc(method, {key: elements}, ts, elements[0]["signature"], len(elements))
            else:
                res = await self.api.post(path, {key: elements}, self.headers(ts, elements[0]["signature"]),
                                          {"address": self.address}, len(elements))
            responses += res.get("responses", []) or []
            limits = res.get("rateLimit", limits)
        return {"responses": responses, "rateLimit": limits}

    async def batch_place(self, orders: list[ArcusOrder]) -> dict:
        if not orders:
            return {"responses": []}
        expiry = self.expiry()
        await self._log.debug(f"placing {len(orders)} orders")
        return await self.batch("/v1/batchPlaceOrders", "orders", orders, lambda o, ts: self.build_place(o, ts, expiry))

    async def batch_modify(self, modifies: list[ArcusModify]) -> dict:
        if not modifies:
            return {"responses": []}
        await self._log.debug(f"modifying {len(modifies)} orders")
        return await self.batch("/v1/batchModifyOrders", "modifies", modifies, self.build_modify)

    async def batch_cancel(self, cancels: list[ArcusCancel]) -> dict:
        if not cancels:
            return {"responses": []}
        return await self.batch("/v1/batchCancelOrders", "cancels", cancels, self.build_cancel)

    async def signed(self, action: str, body: dict) -> Any:
        ts = self.now_ns()
        signature = self.signer.sign_legacy(ts, action, body)
        return await self.api.post(f"/v1/{action}", body, self.headers(ts, signature), {"address": self.address})

    async def cancel_all(self, market: int | None = None) -> Any:
        body: dict = {"address": self.address, "accountIndex": self.index}
        if market is not None:
            body["marketId"] = market
        return await self.signed("cancelAllOrders", body)

    async def schedule_cancel(self, seconds: float | None, market: int | None = None) -> Any:
        body: dict = {"address": self.address, "accountIndex": self.index}
        if seconds is not None:
            body["time"] = int((time.time() + seconds) * 1_000_000)
        if market is not None:
            body["marketId"] = market
        return await self.signed("scheduleCancel", body)

    async def set_leverage(self, market: int, leverage: int, isolated: bool | None = None) -> Any:
        body: dict = {"address": self.address, "accountIndex": self.index, "marketId": market, "leverage": int(leverage)}
        if isolated is not None:
            body["isolated"] = isolated
        return await self.signed("setLeverage", body)
