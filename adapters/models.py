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
    raw: Any = None

    def round_price(self, px: float, side: Side | None = None, passive: bool = True) -> float:
        return self.pricer(px, side, passive) if self.pricer else px

    def round_size(self, sz: float) -> float:
        return self.sizer(sz) if self.sizer else sz


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
