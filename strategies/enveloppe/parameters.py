from dataclasses import dataclass


@dataclass
class Parameters:
    MAKER_FEES: float | None = None
    SPREAD: float = 0.003
    OFFSET: float = 0.00001
    ALPHA: float = 1.1
    SKEW: float = 2
    LEVELS: int = 24
    CAP: float = 0.85
    MIN_VALUE: float = 10.5
    POLL_DELAY: float = 10.1
    THRESHOLD: float = 0.0001
    TIMEFRAME: str = "1m"
    WINDOW: int = 5
    INV: bool = False
    COOLDOWN: float = 10.0
    CLEANUP: bool = False
