import time
from typing import Any
from core.aster.api import AsterApi
from core.aster.utils.types import Market, number
from log.logger import Logger
from log.messenger import Messenger

QUOTES = ("USDT", "USD1", "USDC", "USD", "USDF")   # preferred quote when a market is named by its base only


class AsterInfo:
    def __init__(self, messenger: Messenger, api: AsterApi):
        self.api = api
        self._log = Logger("Aster Info", messenger)
        self.by_name: dict[str, Market] = {}
        self.by_base: dict[str, Market] = {}
        self.limits: dict[str, int] = {}      # REQUEST_WEIGHT / ORDERS per minute (exchangeInfo)

    async def start(self):
        await self.load_markets()
        try:
            await self.load_prices()
        except Exception as e:
            await self._log.warning(f"mark prices not loaded: {e}", send_tg=False)

    async def load_markets(self):
        res = await self.api.get("/fapi/v3/exchangeInfo")
        for row in res.get("rateLimits") or []:
            if row.get("interval") == "MINUTE" and int(row.get("intervalNum") or 1) == 1:
                self.limits[row.get("rateLimitType")] = int(row.get("limit") or 0)
        for row in res.get("symbols") or []:
            if (row.get("contractType") or "PERPETUAL") != "PERPETUAL":
                continue
            market = self.by_name.get(row["symbol"].upper())
            if market:
                market.update(row)
            else:
                market = Market(row)
            self.by_name[market.name.upper()] = market
        self.by_base = {}
        for market in self.by_name.values():
            known = self.by_base.get(market.base.upper())
            rank = QUOTES.index(market.quote) if market.quote in QUOTES else len(QUOTES)
            if known is None or rank < (QUOTES.index(known.quote) if known.quote in QUOTES else len(QUOTES)):
                self.by_base[market.base.upper()] = market

    async def load_prices(self):
        """mark, index and funding of every market (one request of weight 1), 24 h volumes (weight 40)"""
        now = time.time()
        for row in await self.api.get("/fapi/v3/premiumIndex") or []:
            m = self.by_name.get(str(row.get("symbol", "")).upper())
            if m:
                self.price(m, row.get("markPrice"), row.get("indexPrice"), row.get("lastFundingRate"),
                           row.get("nextFundingTime"), now)
        for row in await self.api.get("/fapi/v3/fundingInfo") or []:
            m = self.by_name.get(str(row.get("symbol", "")).upper())
            if m and number(row.get("fundingIntervalHours")):
                m.interval = float(row["fundingIntervalHours"])

    async def load_volumes(self):
        for row in await self.api.get("/fapi/v3/ticker/24hr") or []:
            m = self.by_name.get(str(row.get("symbol", "")).upper())
            if m:
                m.volume = number(row.get("quoteVolume")) or 0.0

    @staticmethod
    def price(m: Market, mark, index, funding, next_funding, now: float):
        m.mark = number(mark) or m.mark
        m.index = number(index) or m.index
        if number(funding) is not None:
            m.funding = number(funding)
        if number(next_funding):
            m.next_funding = float(next_funding) / 1000
        m.priced = now

    def market(self, x: str) -> Market:
        """by symbol ("SPYUSDT"), by base ("SPY"), or by a name of another venue for the same base ("SPY-USD")"""
        name = str(x).upper()
        m = self.by_name.get(name) or self.by_base.get(name)
        if m is None:
            for sep in ("-", "/", ":"):
                if sep in name:
                    head, tail = name.split(sep)[0], name.split(sep)[-1]
                    m = self.by_name.get(name.replace(sep, "")) or self.by_base.get(head) or self.by_base.get(tail)
                    if m:
                        break
        if m is None:
            raise ValueError(f"unknown Aster market: {x}")
        return m

    async def server_time(self) -> int | None:
        res = await self.api.get("/fapi/v3/time")
        return int(res["serverTime"]) if isinstance(res, dict) and res.get("serverTime") else None

    async def book_ticker(self, symbol: str) -> dict:
        return await self.api.get("/fapi/v3/ticker/bookTicker", {"symbol": symbol})

    async def premium(self, symbol: str) -> dict:
        return await self.api.get("/fapi/v3/premiumIndex", {"symbol": symbol})

    async def klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        return await self.api.get("/fapi/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    async def funding_rates(self, symbol: str, start_ms: int, limit: int = 1000) -> list[dict]:
        return await self.api.get("/fapi/v3/fundingRate", {"symbol": symbol, "startTime": start_ms, "limit": limit})

    # ---- signed (USER_DATA) ----

    async def account(self) -> dict:
        return await self.api.get("/fapi/v3/account", signed=True)

    async def position_risk(self, symbol: str | None = None) -> list[dict]:
        return await self.api.get("/fapi/v3/positionRisk", {"symbol": symbol}, signed=True)

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        return await self.api.get("/fapi/v3/openOrders", {"symbol": symbol}, signed=True)

    async def trades(self, symbol: str, start_ms: int | None = None, limit: int = 1000) -> list[dict]:
        return await self.api.get("/fapi/v3/userTrades", {"symbol": symbol, "startTime": start_ms, "limit": limit}, signed=True)

    async def income(self, kind: str, start_ms: int, limit: int = 1000) -> list[dict]:
        return await self.api.get("/fapi/v3/income", {"incomeType": kind, "startTime": start_ms, "limit": limit}, signed=True)

    async def commission(self, symbol: str) -> dict:
        return await self.api.get("/fapi/v3/commissionRate", {"symbol": symbol}, signed=True)

    async def brackets(self, symbol: str) -> Any:
        return await self.api.get("/fapi/v3/leverageBrackets", {"symbol": symbol}, signed=True)

    async def position_mode(self) -> dict:
        return await self.api.get("/fapi/v3/positionSide/dual", signed=True)
