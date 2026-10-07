from __future__ import annotations
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def is_buy(self) -> bool:
        return self is Side.BUY

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY

    @staticmethod
    def of(is_buy: bool) -> Side:
        return Side.BUY if is_buy else Side.SELL


class TIF(str, Enum):
    GTC = "GTC"
    ALO = "ALO"
    IOC = "IOC"


class MarginMode(str, Enum):
    CROSS = "cross"
    ISOLATED = "isolated"


@dataclass
class Instrument:
    exchange: str
    symbol: str
    maker_fee: float
    taker_fee: float
    min_notional: float = 0.0
    min_size: float = 0.0
    pricer: Callable[[float, Side | None, bool], float] | None = None
    sizer: Callable[[float], float] | None = None
    stepper: Callable[[float], float] | None = None
    closed: bool = False
    lower: float | None = None
    upper: float | None = None
    raw: Any = None
    max_leverage: float | None = None

    def round_price(self, px: float, side: Side | None = None, passive: bool = True) -> float:
        return self.pricer(px, side, passive) if self.pricer else px

    def round_size(self, sz: float) -> float:
        return self.sizer(sz) if self.sizer else sz

    def tick(self, px: float) -> float:
        return self.stepper(px) if self.stepper else 0.0

    def bounded(self, px: float) -> bool:
        return (self.lower is None or px >= self.lower) and (self.upper is None or px <= self.upper)


@dataclass
class Order:
    symbol: str
    side: Side
    size: float
    price: float
    tif: TIF = TIF.ALO
    reduce: bool = False
    client: str | None = None

    @property
    def value(self) -> float:
        return self.size * self.price


@dataclass
class OpenOrder:
    symbol: str
    oid: str
    side: Side
    price: float
    size: float
    tif: TIF = TIF.GTC
    reduce: bool = False
    client: str | None = None
    raw: Any = None

    @property
    def is_buy(self) -> bool:
        return self.side.is_buy

    @property
    def value(self) -> float:
        return self.size * self.price


@dataclass
class Modify:
    order: OpenOrder
    size: float
    price: float
    tif: TIF | None = None

    @property
    def side(self) -> Side:
        return self.order.side

    @property
    def value(self) -> float:
        return self.size * self.price


@dataclass
class Cancel:
    symbol: str
    oid: str
    client: str | None = None


@dataclass
class Result:
    ok: bool
    oid: str | None = None
    status: str = ""
    filled: float = 0.0
    price: float | None = None
    error: str | None = None
    raw: Any = None


@dataclass
class Fill:
    symbol: str
    side: Side
    size: float
    price: float
    oid: str | None = None
    client: str | None = None
    id: str | None = None
    maker: bool | None = None
    remaining: float | None = None
    ts: float = field(default_factory=time.time)
    fee: float | None = None  # fee paid in $ when the exchange reports it (negative = rebate)

    @property
    def value(self) -> float:
        return self.size * self.price


@dataclass
class OrderUpdate:
    symbol: str
    oid: str
    side: Side
    price: float
    size: float
    remaining: float
    status: str
    client: str | None = None
    ts: float = field(default_factory=time.time)
    raw: Any = None

    @property
    def closed(self) -> bool:
        return self.status in ("filled", "canceled", "rejected", "replaced")


@dataclass
class BBO:
    symbol: str
    bid: float | None
    ask: float | None
    bid_size: float | None = None
    ask_size: float | None = None
    ts: float = field(default_factory=time.time)

    @property
    def mid(self) -> float | None:
        return (self.bid + self.ask) / 2 if self.bid and self.ask else None

    @property
    def spread(self) -> float | None:
        return (self.ask / self.bid - 1) if self.bid and self.ask else None


@dataclass
class Print:
    """a public trade on a market (anyone's)"""
    symbol: str
    price: float
    size: float
    side: Side | None = None  # aggressor side when the exchange gives it
    ts: float = field(default_factory=time.time)        # exchange time
    received: float = field(default_factory=time.time)  # when we got it


@dataclass
class Candle:
    time: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Position:
    symbol: str
    size: float = 0.0
    entry: float | None = None
    value: float = 0.0
    leverage: float = 1.0
    mode: MarginMode | None = None
    pnl: float = 0.0
    liquidation: float | None = None
    margin: float = 0.0

    @property
    def signed_value(self) -> float:
        return self.value if self.size >= 0 else -self.value


@dataclass
class AccountState:
    exchange: str
    equity: float = 0.0
    free: float = 0.0
    margin: float = 0.0
    positions: dict[str, Position] = field(default_factory=dict)
    leverages: dict[str, float] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def position(self, symbol: str) -> Position:
        return self.positions.get(symbol) or Position(symbol, leverage=self.leverages.get(symbol, 1.0))

    def copy(self) -> AccountState:
        return AccountState(self.exchange, self.equity, self.free, self.margin, dict(self.positions), dict(self.leverages), self.ts)


@dataclass
class Listing:
    symbol: str
    base: str
    category: str = ""
    dex: str = ""
    online: bool = True
    volume: float = 0.0


@dataclass
class Portfolio:
    exchange: str
    equity: float = 0.0
    free: float = 0.0
    pnl: float = 0.0
    notional: float = 0.0
    details: dict[str, float] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    error: str | None = None

    @staticmethod
    def of(state: AccountState, label: str | None = None) -> Portfolio:
        positions = {s: p for s, p in state.positions.items() if p.size}
        return Portfolio(state.exchange, state.equity, state.free, sum(p.pnl for p in positions.values()),
                         sum(abs(p.value) for p in positions.values()), {label or "account": state.equity}, positions)


WINDOWS = (("24h", 86_400), ("7j", 7 * 86_400), ("30j", 30 * 86_400))


@dataclass
class Volume:
    """traded volume of one account over rolling windows (24h, 7j, 30j) and since inception"""
    exchange: str
    volume: dict[str, float] = field(default_factory=dict)  # window -> $ traded
    maker: dict[str, float] = field(default_factory=dict)   # window -> $ traded as maker
    fills: dict[str, int] = field(default_factory=dict)     # window -> number of fills
    fees: dict[str, float] = field(default_factory=dict)    # window -> $ of fees paid
    total: float | None = None                              # since inception
    estimated: bool = False                                 # total derived, not summed
    truncated: bool = False                                 # the exchange did not return every fill of the window
    label: str = ""
    error: str | None = None

    @staticmethod
    def of(exchange: str, rows: list[tuple[float, float, bool | None, float]], now: float | None = None,
           label: str = "") -> Volume:
        """rows: (timestamp s, notional $, maker or None, fee $)"""
        now = now or time.time()
        out = Volume(exchange, label=label)
        for name, span in WINDOWS:
            picked = [r for r in rows if now - r[0] <= span]
            out.volume[name] = sum(r[1] for r in picked)
            out.maker[name] = sum(r[1] for r in picked if r[2])
            out.fills[name] = len(picked)
            out.fees[name] = sum(r[3] for r in picked)
        return out
