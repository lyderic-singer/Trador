import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any

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

def key(subscription: Dict[str, Any]) -> str:
    d = subscription.get("data")
    if d:
        type = subscription["channel"]
        subscription = d
    else:
        type = subscription["type"]
    parts = [type]
    if "user" in subscription:
        parts.append(subscription["user"])
    if "coin" in subscription:
        parts.append(subscription["coin"])
    if "interval" in subscription:
        parts.append(subscription["interval"])
    if "dex" in subscription:
        parts.append(subscription["dex"])
    return ":".join(parts)