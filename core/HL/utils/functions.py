import time
from decimal import Decimal, ROUND_DOWN

def _significant_digits(x: float) -> int:
    d = Decimal(str(x)).normalize()
    return len(d.as_tuple().digits)

def format_price(px: float, max_decimals: int, sz_decimals: int) -> float:
    if float(px).is_integer():
        return float(px)
    max_dp = max_decimals - sz_decimals
    q = Decimal("1e-" + str(max_dp))
    px_d = Decimal(str(px)).quantize(q, rounding=ROUND_DOWN)
    if _significant_digits(px_d) > 5:
        raise ValueError("price has more than 5 significant figures")
    return float(px_d)

def timestamp_ms():
    return int(time.time() * 1000)