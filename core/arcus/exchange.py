import time
from typing import Any
from core.arcus.api import ArcusApi
from core.arcus.info import ArcusInfo
from core.arcus.utils.constants import EXPIRY_DAYS, MAX_BATCH, SIDE, TIF
from core.arcus.utils.signing import Signer, cancel_payload, modify_payload, place_payload
from core.arcus.utils.types import ArcusCancel, ArcusModify, ArcusOrder, fmt
from log.logger import Logger
from log.messenger import Messenger


class ArcusExchange:
    def __init__(self, messenger: Messenger, api: ArcusApi, info: ArcusInfo, signer: Signer, address: str, index: int = 0):
        self._log = Logger("Arcus Exchange", messenger)
        self.api = api
        self.info = info
        self.signer = signer
        self.address = address
        self.index = index

    @staticmethod
    def expiry() -> int:
        return int(time.time() * 1_000_000) + EXPIRY_DAYS * 86_400 * 1_000_000

    def headers(self, ts: int, signature: str) -> dict:
        return {"X-API-Key": self.signer.public, "X-Timestamp": str(ts), "X-Signature": signature}

    def build_place(self, o: ArcusOrder, ts: int, expiry: int) -> dict:
        m = o.market
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
                                 m.to_quantums(mo.size), mo.reduce, SIDE[side], TIF[mo.tif], mo.client)
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

    async def batch(self, path: str, key: str, items: list, build) -> dict:
        responses: list[dict] = []
        limits = None
        for i in range(0, len(items), MAX_BATCH):
            ts = time.time_ns()
            elements = [build(x, ts) for x in items[i:i + MAX_BATCH]]
            res = await self.api.post(path, {key: elements}, self.headers(ts, elements[0]["signature"]), {"address": self.address})
            responses += res.get("responses", [])
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
        ts = time.time_ns()
        signature = self.signer.sign_legacy(ts, action, body)
        return await self.api.post(f"/v1/{action}", body, self.headers(ts, signature), {"address": self.address})

    async def cancel_all(self, market: int | None = None) -> Any:
        body: dict = {"address": self.address, "accountIndex": self.index}
        if market is not None:
            body["marketId"] = market
        return await self.signed("cancelAllOrders", body)

    async def set_leverage(self, market: int, leverage: int, isolated: bool | None = None) -> Any:
        body: dict = {"address": self.address, "accountIndex": self.index, "marketId": market, "leverage": int(leverage)}
        if isolated is not None:
            body["isolated"] = isolated
        return await self.signed("setLeverage", body)
