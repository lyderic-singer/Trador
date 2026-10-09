import time
from typing import Any
from core.arcus.api import ArcusApi, ArcusError
from core.arcus.utils.types import Market
from log.logger import Logger
from log.messenger import Messenger


class ArcusInfo:
    def __init__(self, messenger: Messenger, api: ArcusApi):
        self.api = api
        self._log = Logger("Arcus Info", messenger)
        self.by_id: dict[int, Market] = {}
        self.by_name: dict[str, Market] = {}

    async def start(self):
        await self.load_markets()

    async def load_markets(self):
        res = await self.markets()
        if isinstance(res, list):
            rows = res
        elif isinstance(res, dict) and "marketId" in res:
            rows = [res]
        else:
            rows = res.get("markets", [])
        for row in rows:
            market = self.by_id.get(int(row["marketId"]))
            if market:
                market.update(row)
            else:
                market = Market(row)
            self.by_id[market.id] = market
            self.by_name[market.name.upper()] = market
            self.by_name[market.base.upper()] = market

    def market(self, x: str | int) -> Market:
        if isinstance(x, int) or x.isdigit():
            m = self.by_id.get(int(x))
        else:
            m = self.by_name.get(x.upper())
        if m is None:
            raise ValueError(f"unknown Arcus market: {x}")
        return m

    async def markets(self, market: str | int | None = None) -> Any:
        return await self.api.get("/v1/markets", {"market": market})

    async def bbo(self, market: str) -> Any:
        return await self.api.get(f"/v1/bbo/{market}")

    async def l2_book(self, market: str, levels: int = 20) -> Any:
        return await self.api.get(f"/v1/l2OrderBook/{market}", {"nLevels": levels})

    async def mids(self, market: str | int | None = None) -> Any:
        return await self.api.get("/v1/mids", {"market": market})

    async def mid(self, market: str) -> float | None:
        res = await self.mids(market)
        value = res.get(market) if isinstance(res, dict) else None
        if value is None and isinstance(res, dict) and len(res) == 1:
            value = next(iter(res.values()))
        return float(value) if value not in (None, "") else None

    async def fee_tiers(self) -> Any:
        return await self.api.get("/v1/feetiers")

    async def candles(self, market: str, timeframe: str, count: int | None = None,
                      start: int | None = None, end: int | None = None) -> list[dict]:
        params = {"market": market, "timeframe": timeframe, "to": end or int(time.time() * 1_000_000)}
        if count is not None:
            params["countback"] = count
        elif start is not None:
            params["from"] = start
        res = await self.api.get("/v1/candles", params)
        return res.get("candles", [])

    async def account(self, address: str, index: int = 0) -> dict | None:
        try:
            return await self.api.get("/v1/account", {"address": address, "accountIndex": index})
        except ArcusError as e:
            if e.status == 404:
                return None
            raise

    async def positions(self, address: str, index: int = 0) -> Any:
        return await self.api.get("/v1/positions", {"address": address, "accountIndex": index})

    async def open_orders(self, address: str, index: int = 0, market: str | int | None = None) -> list[dict]:
        res = await self.api.get("/v1/openOrders", {"address": address, "accountIndex": index, "market": market})
        return res.get("orders", [])

    async def order(self, address: str, oid: str, index: int = 0) -> Any:
        return await self.api.get(f"/v1/order/{oid}", {"address": address, "accountIndex": index})

    async def fills(self, address: str, index: int = 0, market: str | None = None) -> Any:
        return await self.api.get("/v1/fills", {"address": address, "accountIndex": index, "market": market})

    async def fills_since(self, address: str, index: int = 0, start: int | None = None, limit: int = 1000) -> list[dict]:
        res = await self.api.get("/v1/fills", {"address": address, "accountIndex": index, "from": start, "limit": limit})
        return res.get("fills", []) if isinstance(res, dict) else res

    async def funding_rates(self, market: str, start: int | None = None, limit: int = 1000) -> list[dict]:
        """hourly funding rates of a market, newest first; start in epoch microseconds"""
        res = await self.api.get("/v1/fundingRates", {"market": market, "from": start, "limit": limit})
        return res.get("fundingRates", []) if isinstance(res, dict) else res

    async def funding_payments(self, address: str, index: int = 0, start: int | None = None, limit: int = 1000) -> list[dict]:
        """funding paid / received by the account, newest first; payment > 0 = received"""
        res = await self.api.get("/v1/funding", {"address": address, "accountIndex": index, "from": start, "limit": limit})
        return res.get("fundingPayments", []) if isinstance(res, dict) else res

    async def rate_limit(self, address: str, index: int = 0) -> Any:
        return await self.api.get("/v1/rateLimit", {"address": address, "accountIndex": index})

    async def api_keys(self, address: str) -> list[dict]:
        res = await self.api.get("/v1/apiKeys", {"address": address})
        return res.get("apiKeys", []) if isinstance(res, dict) else res

    async def leverages(self, address: str, index: int = 0, market: str | int | None = None) -> list[dict]:
        res = await self.api.get("/v1/leverages", {"address": address, "accountIndex": index, "market": market})
        return res.get("leverages", [])
