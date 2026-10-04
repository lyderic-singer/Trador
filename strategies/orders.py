import secrets
import time
from dataclasses import dataclass, field
from typing import Any
from adapters.models import Fill, OpenOrder, OrderUpdate, Result, Side, TIF

GONE = ("NOT_FOUND", "ALREADY_FILLED", "UNKNOWN ORDER", "SIZE_ALREADY_FILLED", "SUPERSEDED", "CANNOT MODIFY", "NEVER PLACED")


def gone(result: Result) -> bool:
    text = f"{result.error or ''} {result.status or ''}".upper()
    return any(g in text for g in GONE)


@dataclass
class Working:
    client: str
    symbol: str
    side: Side
    level: int
    price: float
    size: float
    remaining: float
    oid: str | None = None
    state: str = "pending"
    sent: float = field(default_factory=time.time)
    raw: dict = field(default_factory=dict)
    target: tuple[float, float] | None = None
    rejects: int = 0
    hold: float = 0.0

    @property
    def busy(self) -> bool:
        return self.state != "open"

    @property
    def value(self) -> float:
        return self.remaining * self.price

    def open_order(self) -> OpenOrder:
        return OpenOrder(self.symbol, self.oid or "", self.side, self.price, self.remaining, TIF.ALO, False, self.client, self.raw)


class OrderTracker:
    def __init__(self):
        self.orders: dict[str, Working] = {}
        self.oids: dict[str, str] = {}

    def new(self, symbol: str, side: Side, level: int, price: float, size: float) -> Working:
        w = Working("0x" + secrets.token_hex(16), symbol, side, level, price, size, size)
        self.orders[w.client] = w
        return w

    def find(self, client: str | None, oid: str | None) -> Working | None:
        if client and client in self.orders:
            return self.orders[client]
        if oid and oid in self.oids:
            return self.orders.get(self.oids[oid])
        return None

    def remove(self, w: Working):
        self.orders.pop(w.client, None)
        if w.oid:
            self.oids.pop(w.oid, None)

    def link(self, w: Working, oid: str | None):
        if not oid or oid == w.oid:
            return
        if w.oid:
            self.oids.pop(w.oid, None)
        w.oid = oid
        self.oids[oid] = w.client

    def placed(self, w: Working, result: Result):
        if w.client not in self.orders:
            return
        if not result.ok:
            return self.remove(w)
        if isinstance(result.raw, dict):
            w.raw.update(result.raw)
        self.link(w, result.oid)
        w.state = "open"

    def moved(self, w: Working, result: Result):
        if w.client not in self.orders:
            return
        target, w.target = w.target, None
        if not result.ok:
            if gone(result):
                return self.remove(w)
            w.state = "open"
            w.rejects += 1
            w.hold = time.time() + min(2 ** (w.rejects - 1), 30)
            return
        w.rejects, w.hold = 0, 0.0
        if target:
            w.price, w.size = target
            w.remaining = target[1]
        if isinstance(result.raw, dict):
            w.raw.update({k: v for k, v in result.raw.items() if v not in (None, "")})
        self.link(w, result.oid)
        w.state = "open"
        w.sent = time.time()

    def canceled(self, w: Working, result: Result):
        if w.client not in self.orders:
            return
        if result.ok or gone(result):
            return self.remove(w)
        w.state = "open"

    def failed(self, w: Working):
        if w.state == "pending":
            w.state = "open"
        elif w.state in ("moving", "canceling"):
            w.target = None
            w.state = "open"

    def update(self, u: OrderUpdate) -> Working | None:
        w = self.find(u.client, u.oid)
        if w is None:
            return None
        if u.status == "replaced":
            return w
        if w.oid and u.oid != w.oid and w.state != "pending":
            return w
        self.link(w, u.oid)
        if u.closed:
            self.remove(w)
            return w
        w.remaining = u.remaining
        if w.state == "pending":
            w.state = "open"
        return w

    def fill(self, f: Fill) -> Working | None:
        w = self.find(f.client, f.oid)
        if w is None:
            return None
        w.remaining = f.remaining if f.remaining is not None else max(w.remaining - f.size, 0.0)
        if w.remaining <= w.size * 1e-6:
            self.remove(w)
        return w

    def live(self, symbol: str, side: Side | None = None) -> list[Working]:
        return sorted((w for w in self.orders.values() if w.symbol == symbol and (side is None or w.side == side)),
                      key=lambda w: w.level)

    def size_at(self, symbol: str, side: Side, price: float, tick: float) -> float:
        return sum(w.remaining for w in self.orders.values()
                   if w.symbol == symbol and w.side == side and w.state != "pending" and abs(w.price - price) < tick / 2)

    def exposure(self, symbol: str, side: Side) -> float:
        return sum(w.value for w in self.orders.values() if w.symbol == symbol and w.side == side)

    def sync(self, orders: list[OpenOrder], symbols: set[str], grace: float = 3.0) -> list[OpenOrder]:
        now = time.time()
        seen: set[str] = set()
        orphans = []
        for o in orders:
            if o.symbol not in symbols:
                continue
            w = self.find(o.client, o.oid)
            if w is None:
                orphans.append(o)
                continue
            seen.add(w.client)
            if w.state == "open" and (not w.oid or w.oid == o.oid):
                self.link(w, o.oid)
                w.remaining = o.size
        for w in list(self.orders.values()):
            if w.symbol in symbols and w.client not in seen and not w.busy and now - w.sent > grace:
                self.remove(w)
        return orphans

    def snapshot(self) -> dict[str, Any]:
        return {"orders": len(self.orders), "busy": sum(1 for w in self.orders.values() if w.busy)}
