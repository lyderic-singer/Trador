"""helpers shared by the adapters whose exchange does not serve everything natively"""
from adapters.models import Candle, Side


# every timeframe a strategy may ask for (strategies/market.py), in seconds
SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
           "8h": 28800, "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800}


def rounding(side: Side | None, passive: bool) -> str:
    """a passive price is rounded away from the market, an aggressive one through it"""
    if side is None:
        return "nearest"
    return ("down" if side.is_buy else "up") if passive else ("up" if side.is_buy else "down")


def source(timeframes: dict[str, int], seconds: int) -> str | None:
    """largest native timeframe that divides `seconds` (None when even the smallest does not)"""
    fits = [(s, name) for name, s in timeframes.items() if s <= seconds and seconds % s == 0]
    return max(fits)[1] if fits else None


def aggregate(candles: list[Candle], seconds: int) -> list[Candle]:
    """candles of a smaller timeframe merged into buckets of `seconds` aligned on the epoch, oldest first"""
    out: list[Candle] = []
    for c in sorted(candles, key=lambda c: c.time):
        start = c.time // seconds * seconds
        if out and out[-1].time == start:
            last = out[-1]
            last.high, last.low, last.close = max(last.high, c.high), min(last.low, c.low), c.close
            last.volume += c.volume
        else:
            out.append(Candle(start, c.open, c.high, c.low, c.close, c.volume))
    return out
