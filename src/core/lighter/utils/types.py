import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal
from core.lighter.utils.constants import MAX_PRICE

Rounding = Literal["down", "up", "nearest"]


def D(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def number(x) -> float | None:
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


class Market:
    """a perp of GET /api/v1/orderBookDetails. Prices and sizes travel as integers: price x 10^price_decimals,
    size x 10^size_decimals; the tick and the size step are therefore 10^-decimals"""

    def __init__(self, info: dict):
        self.stats: dict = {}       # last market_stats of the websocket (mark, index, funding)
        self.stats_ts = 0.0
        self.update(info)

    def update(self, info: dict):
        self.raw = info
        self.id: int = int(info["market_id"])
        self.name: str = str(info["symbol"])
        self.kind: str = info.get("market_type") or "perp"
        self.price_decimals = int(info.get("price_decimals", info.get("supported_price_decimals") or 0))
        self.size_decimals = int(info.get("size_decimals", info.get("supported_size_decimals") or 0))
        self.tick = Decimal(1).scaleb(-self.price_decimals)
        self.step = Decimal(1).scaleb(-self.size_decimals)
        self.min_base = D(info.get("min_base_amount") or self.step)
        self.min_quote = D(info.get("min_quote_amount") or 0)
        self.max_quote = D(info["order_quote_limit"]) if info.get("order_quote_limit") not in (None, "", "0") else None
        self.status: str = info.get("status", "active")
        # margin fractions are integers out of 10 000 (500 = 5 % = 20x), like the fraction of updateLeverage
        self.imf = (number(info.get("default_initial_margin_fraction")) or 0) / 1e4 or None
        self.min_imf = (number(info.get("min_initial_margin_fraction")) or 0) / 1e4 or None
        self.mmf = (number(info.get("maintenance_margin_fraction")) or 0) / 1e4 or None
        self.mark = number(info.get("mark_price")) or getattr(self, "mark", None)
        self.index = number(info.get("index_price")) or getattr(self, "index", None)
        self.last = number(info.get("last_trade_price"))
        self.volume = number(info.get("daily_quote_token_volume")) or 0.0
        config = info.get("market_config") or {}
        self.isolated = int(config.get("market_margin_mode") or 0) == 1     # isolated margin only
        self.reduce_only = bool(config.get("force_reduce_only"))
        self.hours = config.get("trading_hours")
        self.updated = time.time()

    @property
    def online(self) -> bool:
        return self.status == "active" and not self.reduce_only

    @property
    def max_leverage(self) -> int | None:
        return int(round(1 / self.min_imf)) if self.min_imf else None

    def round_price(self, price: float | Decimal, mode: Rounding = "nearest") -> Decimal:
        rounding = {"down": ROUND_FLOOR, "up": ROUND_CEILING}.get(mode)
        n = (D(price) / self.tick).to_integral_value(rounding=rounding) if rounding else (D(price) / self.tick).quantize(Decimal(1))
        return max(n, Decimal(1)) * self.tick

    def round_size(self, size: float | Decimal) -> Decimal:
        return (D(size) / self.step).to_integral_value(rounding=ROUND_FLOOR) * self.step

    def price_int(self, price: Decimal) -> int:
        n = D(price) / self.tick
        if n != n.to_integral_value():
            raise ValueError(f"{self.name}: price {price} is not a multiple of {self.tick}")
        if not 0 < n <= MAX_PRICE:
            raise ValueError(f"{self.name}: price {price} out of the 32-bit range of the exchange")
        return int(n)

    def base_int(self, size: Decimal) -> int:
        n = D(size) / self.step
        if n != n.to_integral_value():
            raise ValueError(f"{self.name}: size {size} is not a multiple of {self.step}")
        return int(n)

    def minimum(self, price: Decimal) -> Decimal:
        """smallest size of a resting order at this price: the larger of min_base_amount and min_quote_amount"""
        if price <= 0 or not self.min_quote:
            return self.min_base
        need = (self.min_quote / price / self.step).to_integral_value(rounding=ROUND_CEILING) * self.step
        return max(self.min_base, need)

    def __str__(self):
        return f"Market({self.name} id={self.id} tick={self.tick} step={self.step} min={self.min_base} / {self.min_quote} $ {self.status})"


class Tx:
    """a signed transaction: what sendTx / sendTxBatch take, plus what the caller wants to know back"""
    __slots__ = ("type", "info", "hash", "client")

    def __init__(self, type: int, info: str, hash: str = "", client: int | None = None):
        self.type = int(type)
        self.info = info
        self.hash = hash
        self.client = client
