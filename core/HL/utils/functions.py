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

def key(dict: Dict[str, Any]) -> str:
    d = dict.get("data")
    if d:
        c = dict.get("channel")
        if c == "allMids": return "allMids"
        if c == "l2Book": return f'l2Book:{d["coin"].lower()}'
        if c == "activeAssetCtx": return f'activeAssetCtx:{d["coin"].lower()}'
        if c == "trades": return f'trades:{d[0]["coin"].lower()}' if d else None
        if c == "user": return "userEvents"
        if c == "userFills": return f'userFills:{d["user"].lower()}'
        if c == "candle": return f'candle:{d["s"].lower()},{d["i"]}'
        if c == "orderUpdates": return "orderUpdates"
        if c == "userFundings": return f'userFundings:{d["user"].lower()}'
        if c == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{d["user"].lower()}'
        if c == "webData2": return f'webData2:{d["user"].lower()}'
        if c == "bbo": return f'bbo:{d["coin"].lower()}'
        raise ValueError(f"Unknown msg type: {t}")
    else:
        t = dict.get("type")
        if t == "allMids": return "allMids"
        if t == "l2Book": return f'l2Book:{dict["coin"].lower()}'
        if t == "activeAssetCtx": return f'activeAssetCtx:{dict["coin"].lower()}'
        if t == "trades": return f'trades:{dict["coin"].lower()}'
        if t == "userEvents": return "userEvents"
        if t == "userFills": return f'userFills:{dict["user"].lower()}'
        if t == "candle": return f'candle:{dict["coin"].lower()},{dict["interval"]}'
        if t == "orderUpdates": return "orderUpdates"
        if t == "userFundings": return f'userFundings:{dict["user"].lower()}'
        if t == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{dict["user"].lower()}'
        if t == "webData2": return f'webData2:{dict["user"].lower()}'
        if t == "bbo": return f'bbo:{dict["coin"].lower()}'
        raise ValueError(f"Unknown subscription type: {t}")