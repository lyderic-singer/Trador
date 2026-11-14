import time
from typing import Any
from core.lighter.api import LighterApi
from core.lighter.utils.types import Market
from log.logger import Logger
from log.messenger import Messenger


class LighterInfo:
    def __init__(self, messenger: Messenger, api: LighterApi):
        self.api = api
        self._log = Logger("Lighter Info", messenger)
        self.by_id: dict[int, Market] = {}
        self.by_name: dict[str, Market] = {}

    async def start(self):
        await self.load_markets()

    async def load_markets(self):
        """perps only (spot and binary option markets are not traded by the bot)"""
        res = await self.api.get("/api/v1/orderBookDetails", {"filter": "perp"})
        for row in res.get("order_book_details") or []:
            if (row.get("market_type") or "perp") != "perp":
                continue
            market = self.by_id.get(int(row["market_id"]))
            if market:
                market.update(row)
            else:
                market = Market(row)
            self.by_id[market.id] = market
            self.by_name[market.name.upper()] = market

    def market(self, x: str | int) -> Market:
        """by id, by symbol ("SPY"), or by a name of another venue for the same base ("SPY-USD", "SPY/USD", "SPYUSDT")"""
        if isinstance(x, int) or str(x).isdigit():
            m = self.by_id.get(int(x))
        else:
            name = str(x).upper()
            m = self.by_name.get(name)
            if m is None:
                for suffix in ("-USD", "/USD", "-USDC", "/USDC", "-PERP", "USDT", "USDC", "USD"):
                    if name.endswith(suffix) and len(name) > len(suffix):
                        m = self.by_name.get(name[:-len(suffix)])
                        if m:
                            break
        if m is None:
            raise ValueError(f"unknown Lighter market: {x}")
        return m

    async def account(self, index: int) -> dict | None:
        res = await self.api.get("/api/v1/account", {"by": "index", "value": str(index)})
        rows = res.get("accounts") or []
        return rows[0] if rows else None

    async def accounts(self, address: str) -> list[dict]:
        """the accounts of an L1 address (the first one is the master account)"""
        res = await self.api.get("/api/v1/accountsByL1Address", {"l1_address": address})
        return res.get("sub_accounts") or []

    async def next_nonce(self, account: int, key: int) -> int:
        res = await self.api.get("/api/v1/nextNonce", {"account_index": account, "api_key_index": key})
        return int(res["nonce"])

    async def limits(self, account: int) -> dict:
        """tier, fee ticks and volume quota (auth)"""
        return await self.api.get("/api/v1/accountLimits", {"account_index": account}, auth=True)

    async def active_orders(self, account: int, market: int | None = None) -> list[dict]:
        res = await self.api.get("/api/v1/accountActiveOrders", {"account_index": account, "market_id": market,
                                                                 "market_type": "perp"}, auth=True)
        return res.get("orders") or []

    async def trades(self, account: int, since_ms: int | None = None, limit: int = 100, cursor: str | None = None,
                     market: int | None = None) -> tuple[list[dict], str | None]:
        """fills of the account, newest first (desc is the only order): `since_ms` is exclusive"""
        res = await self.api.get("/api/v1/trades", {"account_index": account, "sort_by": "timestamp", "limit": limit,
                                                    "from": since_ms if cursor is None else None, "cursor": cursor,
                                                    "market_id": market, "market_type": "perp"}, auth=True)
        return res.get("trades") or [], res.get("next_cursor") or None

    async def candles(self, market: int, resolution: str, count: int, end: float | None = None) -> list[dict]:
        end_ms = int((end or time.time()) * 1000)
        res = await self.api.get("/api/v1/candles", {"market_id": market, "resolution": resolution, "start_timestamp": 0,
                                                     "end_timestamp": end_ms, "count_back": count})
        return res.get("c") or res.get("candlesticks") or []

    async def fundings(self, market: int, start: float, end: float | None = None, count: int = 750) -> list[dict]:
        """hourly funding of a market (at most 750 rows per call)"""
        res = await self.api.get("/api/v1/fundings", {"market_id": market, "resolution": "1h", "count_back": count,
                                                      "start_timestamp": int(start * 1000),
                                                      "end_timestamp": int((end or time.time()) * 1000)})
        return res.get("fundings") or []

    async def funding_rates(self) -> list[dict]:
        res = await self.api.get("/api/v1/funding-rates")
        return res.get("funding_rates") or []

    async def position_funding(self, account: int, start: float, limit: int = 100,
                               cursor: str | None = None) -> tuple[list[dict], str | None]:
        res = await self.api.get("/api/v1/positionFunding", {"account_index": account, "limit": limit, "cursor": cursor,
                                                             "start_timestamp": int(start * 1000)}, auth=True)
        return res.get("position_fundings") or [], res.get("next_cursor") or None

    async def book(self, market: int, limit: int = 5) -> dict:
        return await self.api.get("/api/v1/orderBookOrders", {"market_id": market, "limit": limit})

    async def api_keys(self, account: int, key: int = 255) -> list[dict]:
        res = await self.api.get("/api/v1/apikeys", {"account_index": account, "api_key_index": key})
        return res.get("api_keys") or []

    async def tx(self, hash: str) -> Any:
        return await self.api.get("/api/v1/tx", {"by": "hash", "value": hash})
