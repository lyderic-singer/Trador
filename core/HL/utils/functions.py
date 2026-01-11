import time
from decimal import Decimal, ROUND_DOWN

def format_price(px: float, max_decimals: int, sz_decimals: int):
    if not isinstance(px, (float, int)):
        raise TypeError("px must be a float or int")
    if float(px).is_integer():
        return str(int(px))
    max_dp = max_decimals - sz_decimals
    if max_dp < 0:
        max_dp = 0
    q = Decimal("1e-" + str(max_dp))
    px_d = Decimal(str(px)).quantize(q, rounding=ROUND_DOWN)
    return str(px_d)

def timestamp_ms():
    return int(time.time() * 1000)