import math


class Governor:
    def __init__(self, base: float = 0.2, efficiency: float = 10.0, ceiling: float = 4.0, reserve: int = 5000,
                 safe: int = 50000, halflife: float = 900.0):
        self.base = base
        self.efficiency = efficiency
        self.ceiling = ceiling
        self.reserve = reserve
        self.safe = safe
        self.decay = math.log(2) / halflife
        self.flows: dict[str, float] = {}
        self.stamps: dict[str, float] = {}

    def faded(self, symbol: str, now: float) -> float:
        value = self.flows.get(symbol, 0.0)
        stamp = self.stamps.get(symbol, now)
        return value * math.exp(-self.decay * max(now - stamp, 0.0))

    def earn(self, symbol: str, value: float, now: float):
        self.flows[symbol] = self.faded(symbol, now) + value
        self.stamps[symbol] = now

    def flow(self, symbol: str, now: float) -> float:
        return self.faded(symbol, now) * self.decay

    def factor(self, headroom: int | None) -> float:
        if headroom is None:
            return 1.0
        return max(0.0, min(1.0, (headroom - self.reserve) / max(self.safe - self.reserve, 1)))

    def rate(self, symbol: str, headroom: int | None, now: float) -> float:
        return min(self.ceiling, self.base + self.flow(symbol, now) / self.efficiency) * self.factor(headroom)
