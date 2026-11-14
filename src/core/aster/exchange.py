import asyncio
import json
from typing import Any
from core.aster.api import AsterApi, AsterError
from core.aster.utils.constants import MAX_BATCH_CANCEL, MAX_BATCH_MODIFY, MAX_BATCH_PLACE
from log.logger import Logger
from log.messenger import Messenger


def compact(x) -> str:
    return json.dumps(x, separators=(",", ":"))


class AsterExchange:
    """order entry. Aster has no order entry over the websocket: every order is one HTTPS request, signed (see
    utils/signing.py). A list is sent in batches (5 orders, 5 modifies or 10 cancels per request), the batches of one
    call concurrently; an element refused inside a batch comes back as {"code", "msg"} at its place. A single order
    is sent alone (lighter request, and the outcome of an IOC comes in the reply)"""

    def __init__(self, messenger: Messenger, api: AsterApi):
        self._log = Logger("Aster Exchange", messenger)
        self.api = api
        self.transport = "rest"     # the only one; kept so that a strategy setting it finds the attribute

    @staticmethod
    def error(e: AsterError) -> dict:
        return {"code": e.code if e.code is not None else -e.status, "msg": e.message or str(e)}

    async def one(self, verb: str, path: str, params: dict) -> dict:
        """a single order request: a business refusal becomes {"code", "msg"}; a rate limit or an unknown outcome
        (5xx, timeout) is raised, the caller must not conclude the order was refused"""
        try:
            return await self.api.request(verb, path, params, signed=True)
        except AsterError as e:
            if e.status in (418, 429) or e.unknown:
                raise
            return self.error(e)

    async def batches(self, verb: str, params: list[dict], size: int, key: str = "batchOrders") -> list[dict]:
        async def send(chunk: list[dict]) -> list[dict]:
            try:
                res = await self.api.request(verb, "/fapi/v3/batchOrders", {key: compact(chunk)}, signed=True, items=len(chunk))
            except AsterError as e:
                if (e.status in (418, 429) or e.unknown) and len(params) <= size:
                    raise
                return [self.error(e)] * len(chunk)
            rows = res if isinstance(res, list) else []
            return rows + [{"code": -1, "msg": "missing response"}] * (len(chunk) - len(rows))
        chunks = [params[i:i + size] for i in range(0, len(params), size)]
        out: list[dict] = []
        for rows in await asyncio.gather(*(send(c) for c in chunks)):
            out += rows
        return out

    async def place(self, orders: list[dict]) -> list[dict]:
        if not orders:
            return []
        if len(orders) == 1:
            return [await self.one("POST", "/fapi/v3/order", orders[0])]
        return await self.batches("POST", orders, MAX_BATCH_PLACE)

    async def modify(self, modifies: list[dict]) -> list[dict]:
        if not modifies:
            return []
        if len(modifies) == 1:
            return [await self.one("PUT", "/fapi/v3/order", modifies[0])]
        return await self.batches("PUT", modifies, MAX_BATCH_MODIFY)

    async def cancel(self, symbol: str, oids: list[int], clients: list[str]) -> tuple[list[dict], list[dict]]:
        """orders of ONE symbol, by exchange id and / or by client id; results in the order given"""
        async def send(key: str, ids: list) -> list[dict]:
            if not ids:
                return []
            if len(ids) == 1:
                field = "orderId" if key == "orderIdList" else "origClientOrderId"
                return [await self.one("DELETE", "/fapi/v3/order", {"symbol": symbol, field: ids[0]})]
            out: list[dict] = []
            for i in range(0, len(ids), MAX_BATCH_CANCEL):
                chunk = ids[i:i + MAX_BATCH_CANCEL]
                try:
                    res = await self.api.request("DELETE", "/fapi/v3/batchOrders", {"symbol": symbol, key: compact(chunk)},
                                                 signed=True, items=len(chunk))
                except AsterError as e:
                    if (e.status in (418, 429) or e.unknown) and len(ids) <= MAX_BATCH_CANCEL:
                        raise
                    res = [self.error(e)] * len(chunk)
                rows = res if isinstance(res, list) else []
                out += rows + [{"code": -1, "msg": "missing response"}] * (len(chunk) - len(rows))
            return out
        a, b = await asyncio.gather(send("orderIdList", oids), send("origClientOrderIdList", clients))
        return a, b

    async def cancel_all(self, symbol: str) -> Any:
        return await self.api.request("DELETE", "/fapi/v3/allOpenOrders", {"symbol": symbol}, signed=True)

    async def countdown(self, symbol: str, ms: int) -> Any:
        """dead man's switch of ONE symbol: its orders are cancelled if no new call comes within `ms` (0 disarms)"""
        return await self.api.request("POST", "/fapi/v3/countdownCancelAll", {"symbol": symbol, "countdownTime": int(ms)}, signed=True)

    async def set_leverage(self, symbol: str, leverage: int) -> Any:
        return await self.api.request("POST", "/fapi/v3/leverage", {"symbol": symbol, "leverage": int(leverage)}, signed=True)

    async def set_margin_type(self, symbol: str, isolated: bool) -> Any:
        return await self.api.request("POST", "/fapi/v3/marginType",
                                      {"symbol": symbol, "marginType": "ISOLATED" if isolated else "CROSSED"}, signed=True)

    async def add_margin(self, symbol: str, usd: float) -> Any:
        return await self.api.request("POST", "/fapi/v3/positionMargin", {"symbol": symbol, "amount": f"{usd:.6f}", "type": 1},
                                      signed=True)

    async def listen_key(self, verb: str = "POST") -> str | None:
        res = await self.api.request(verb, "/fapi/v3/listenKey", {}, signed=True)
        return res.get("listenKey") if isinstance(res, dict) else None
