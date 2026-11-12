"""Round trip of every request sent to an exchange, measured where it is sent (core), read by the strategies.

One process-wide recorder (LATENCY): the core records, a strategy subscribes (`listen`) to journal every sample and
reads rolling percentiles (`stats`) for its reports. Clock offsets measured against the exchanges (`clock`) let a
strategy turn an exchange timestamp into a feed delay.
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Sample:
    venue: str          # "arcus", "hyperliquid"
    method: str         # batchPlaceOrders, batchCancelOrders, order, cancel, scheduleCancel, health...
    transport: str      # "ws" or "rest"
    ms: float           # round trip, from just before the send to the parsed reply
    ok: bool = True
    items: int = 1      # orders / cancels in the request
    error: str = ""
    ts: float = field(default_factory=time.time)


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class Latency:
    def __init__(self, window: int = 4000):
        self.window = window
        self.samples: dict[tuple[str, str, str], deque[Sample]] = defaultdict(lambda: deque(maxlen=self.window))
        self.listeners: list[Callable[[Sample], None]] = []
        self.counts: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])  # [requests, errors] since start
        # venue -> (offset ms = exchange clock - local clock, round trip ms of the measure, unix s of the measure)
        self.clock: dict[str, tuple[float, float, float]] = {}

    def record(self, venue: str, method: str, transport: str, ms: float, ok: bool = True, items: int = 1,
               error: str = "") -> Sample:
        s = Sample(venue, method, transport, ms, ok, items, error[:120])
        key = (venue, method, transport)
        self.samples[key].append(s)
        self.counts[key][0] += 1
        if not ok:
            self.counts[key][1] += 1
        for listener in list(self.listeners):
            try:
                listener(s)
            except Exception:
                pass
        return s

    def listen(self, listener: Callable[[Sample], None]):
        if listener not in self.listeners:
            self.listeners.append(listener)

    def unlisten(self, listener: Callable[[Sample], None]):
        if listener in self.listeners:
            self.listeners.remove(listener)

    def offset(self, venue: str) -> float:
        """exchange clock - local clock in ms (0 when never measured)"""
        row = self.clock.get(venue)
        return row[0] if row else 0.0

    def stats(self, venue: str | None = None, since: float = 0.0, ok_only: bool = True) -> dict[tuple[str, str, str], dict]:
        out = {}
        for key, rows in self.samples.items():
            if venue and key[0] != venue:
                continue
            values = [s.ms for s in rows if s.ts >= since and (s.ok or not ok_only)]
            if not values:
                continue
            out[key] = {"n": len(values), "p50": percentile(values, 0.5), "p90": percentile(values, 0.9),
                        "p99": percentile(values, 0.99), "max": max(values), "mean": sum(values) / len(values),
                        "requests": self.counts[key][0], "errors": self.counts[key][1]}
        return out

    def line(self, venue: str | None = None, since: float = 0.0) -> str:
        parts = []
        for (v, method, transport), s in sorted(self.stats(venue, since).items()):
            parts.append(f"{v[:2]}:{method}/{transport} p50 {s['p50']:.0f} p90 {s['p90']:.0f} p99 {s['p99']:.0f} ms (n={s['n']})")
        return " | ".join(parts) or "no request yet"


LATENCY = Latency()


class Timer:
    """with Timer() as t: ...; t.ms"""

    def __init__(self):
        self.start = 0.0
        self.ms = 0.0

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self.start) * 1000
        return False
