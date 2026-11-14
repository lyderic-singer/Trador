import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal

Rounding = Literal["down", "up", "nearest"]


def D(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def fmt(x: Decimal) -> str:
    s = format(D(x).normalize(), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def number(x) -> float | None:
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


class Market:
    """a symbol of GET /fapi/v3/exchangeInfo. tickSize / stepSize come from the filters (pricePrecision and
    quantityPrecision are display precisions, not steps)"""

    def __init__(self, info: dict):
        self.mark: float | None = None
        self.index: float | None = None
        self.funding: float | None = None       # rate of the running funding period (per period, not per hour)
        self.next_funding: float | None = None  # unix s
        self.interval = 8.0                     # hours between two funding payments (fundingInfo)
        self.priced = 0.0                       # time.time() of the last mark
        self.volume = 0.0                       # 24 h quote volume
        self.brackets: list[dict] = []          # leverage brackets (signed endpoint), lowest notional first
        self.update(info)

    def update(self, info: dict):
        self.raw = info
        self.name: str = info["symbol"]
        self.base: str = info.get("baseAsset") or self.name
        self.quote: str = info.get("quoteAsset") or ""
        self.kind: str = info.get("contractType") or "PERPETUAL"
        self.status: str = info.get("status", "TRADING")
        self.category: str = info.get("underlyingType") or ""
        filters = {f.get("filterType"): f for f in info.get("filters") or []}
        price, lot = filters.get("PRICE_FILTER") or {}, filters.get("LOT_SIZE") or {}
        self.tick = D(price.get("tickSize") or Decimal(1).scaleb(-int(info.get("pricePrecision") or 2)))
        self.min_price = D(price.get("minPrice") or 0)
        self.max_price = D(price["maxPrice"]) if price.get("maxPrice") not in (None, "", "0") else None
        self.step = D(lot.get("stepSize") or Decimal(1).scaleb(-int(info.get("quantityPrecision") or 3)))
        self.min_size = D(lot.get("minQty") or self.step)
        self.max_size = D(lot["maxQty"]) if lot.get("maxQty") not in (None, "", "0") else None
        self.min_notional = D((filters.get("MIN_NOTIONAL") or {}).get("notional") or 5)
        percent = filters.get("PERCENT_PRICE") or {}
        # a BUY must be priced <= mark x up, a SELL >= mark x down
        self.up = number(percent.get("multiplierUp"))
        self.down = number(percent.get("multiplierDown"))
        self.max_orders = int((filters.get("MAX_NUM_ORDERS") or {}).get("limit") or 0) or None
        self.updated = time.time()

    @property
    def online(self) -> bool:
        return self.status == "TRADING"

    @property
    def max_leverage(self) -> int | None:
        return max((int(b.get("initialLeverage") or 0) for b in self.brackets), default=0) or None

    @property
    def mmf(self) -> float | None:
        return number(self.brackets[0].get("maintMarginRatio")) if self.brackets else None

    def round_price(self, price: float | Decimal, mode: Rounding = "nearest") -> Decimal:
        rounding = {"down": ROUND_FLOOR, "up": ROUND_CEILING}.get(mode)
        n = (D(price) / self.tick).to_integral_value(rounding=rounding) if rounding else (D(price) / self.tick).quantize(Decimal(1))
        return max(n, Decimal(1)) * self.tick

    def round_size(self, size: float | Decimal) -> Decimal:
        out = (D(size) / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step
        if self.max_size is not None and out > self.max_size:
            out = (self.max_size / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step
        return out

    def __str__(self):
        return f"Market({self.name} tick={self.tick} step={self.step} min={self.min_size} / {self.min_notional} $ {self.status})"
