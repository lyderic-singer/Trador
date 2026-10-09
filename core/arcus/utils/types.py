import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal

Rounding = Literal["down", "up", "nearest"]


def D(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def fmt(x: Decimal) -> str:
    s = format(x.normalize(), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


class Market:
    def __init__(self, info: dict):
        self.update(info)

    def update(self, info: dict):
        self.raw = info
        self.id: int = int(info["marketId"])
        self.name: str = info["marketDisplayName"]
        self.base: str = info.get("baseAsset", self.name.split("-")[0])
        self.quote: str = info.get("quoteAsset", "USD")
        self.tick = D(info["tickSize"])
        self.step = D(info["stepSize"])
        self.min_size = D(info.get("minOrderSize") or info["stepSize"])
        self.max_size = D(info["maxOrderSize"]) if info.get("maxOrderSize") else None
        self.min_notional = D(info.get("minOrderNotional") or 0)
        tiers = []
        for t in info.get("tickTiers") or []:
            up = t.get("upToPrice")
            tiers.append((D(up) if up not in (None, "") else None, D(t["tick"])))
        tiers.sort(key=lambda x: (x[0] is None, x[0] or 0))
        self.tiers = tiers
        self.status: str = info.get("status", "ONLINE")
        self.oracle = float(info["oraclePrice"]) if info.get("oraclePrice") else None
        self.mark = float(info["markPrice"]) if info.get("markPrice") else None
        self.imf = float(info.get("initialMarginFraction") or 0) or None
        self.category: str = info.get("category", "")
        self.closed: bool = bool(info.get("isOutsideRth"))
        self.lower = float(info["lowerTradingBound"]) if info.get("lowerTradingBound") else None
        self.upper = float(info["upperTradingBound"]) if info.get("upperTradingBound") else None
        self.hours = info.get("regularTradingHours")
        self.mmf = float(info.get("maintenanceMarginFraction") or 0) or None
        self.offhours_imf = float(info.get("offHoursInitialMarginFraction") or 0) or None
        self.funding = float(info["fundingRate"]) if info.get("fundingRate") not in (None, "") else None
        self.next_funding = float(info["nextFundingRate"]) if info.get("nextFundingRate") not in (None, "") else None
        self.next_funding_at = float(info["nextFundingAt"]) if info.get("nextFundingAt") else None
        self.updated = time.time()

    @property
    def online(self) -> bool:
        return self.status == "ONLINE"

    @property
    def max_leverage(self) -> int | None:
        return int(1 / self.imf) if self.imf else None

    def tick_at(self, price: Decimal) -> Decimal:
        for limit, tick in self.tiers:
            if limit is None or price <= limit:
                return max(tick, self.tick)
        return self.tick

    def round_price(self, price: float | Decimal, mode: Rounding = "nearest") -> Decimal:
        px = D(price)
        tick = self.tick_at(px)
        rounding = {"down": ROUND_FLOOR, "up": ROUND_CEILING}.get(mode)
        n = (px / tick).to_integral_value(rounding=rounding) if rounding else (px / tick).quantize(Decimal(1))
        out = n * tick
        if out <= 0:
            out = tick
        coarser = self.tick_at(out)
        if coarser != tick:
            out = (out / coarser).to_integral_value(rounding=rounding or ROUND_FLOOR) * coarser
        return out

    def round_size(self, size: float | Decimal) -> Decimal:
        out = (D(size) / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step
        if self.max_size is not None and out > self.max_size:
            out = (self.max_size / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step
        return out

    def to_ticks(self, price: Decimal) -> int:
        n = D(price) / self.tick
        if n != n.to_integral_value():
            raise ValueError(f"{self.name}: price {price} is not a multiple of tickSize {self.tick}")
        return int(n)

    def to_quantums(self, size: Decimal) -> int:
        n = D(size) / self.step
        if n != n.to_integral_value():
            raise ValueError(f"{self.name}: size {size} is not a multiple of stepSize {self.step}")
        return int(n)

    def __str__(self):
        return (f"Market({self.name} id={self.id} tick={self.tick} step={self.step} "
                f"min_size={self.min_size} min_notional={self.min_notional} status={self.status})")


class ArcusOrder:
    def __init__(self, market: Market, is_buy: bool, size: Decimal, price: Decimal, tif: str = "ALO",
                 reduce: bool = False, client: str | None = None, kind: str = "LIMIT", expiry: int | None = None):
        self.market = market
        self.expiry = expiry
        self.is_buy = is_buy
        self.size = D(size)
        self.price = D(price)
        self.tif = tif
        self.reduce = reduce
        self.client = client
        self.kind = kind


class ArcusModify:
    def __init__(self, market: Market, oid: str, is_buy: bool, size: Decimal, price: Decimal, tif: str,
                 reduce: bool, expiry: int, client: str | None = None):
        self.market = market
        self.oid = oid
        self.is_buy = is_buy
        self.size = D(size)
        self.price = D(price)
        self.tif = tif
        self.reduce = reduce
        self.expiry = int(expiry)
        self.client = client


class ArcusCancel:
    def __init__(self, market: Market, oid: str | None = None, client: str | None = None):
        if (oid is None) == (client is None):
            raise ValueError("cancel needs exactly one of oid / client")
        self.market = market
        self.oid = oid
        self.client = client
