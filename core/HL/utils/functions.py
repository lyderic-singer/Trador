import time
import asyncio
import json
from .types import Instrument
from typing import Dict, Any

def format_price(px: float, max_decimals: int, sz_decimals: int):
    if (px.is_integer()) or (len(str(int(px))) > 4):
        return str(int(px))
    else:
        decimals = max_decimals - sz_decimals
        px = float(f"{px:.5g}")
        px = round(px, decimals)
        px = f"{px:.{decimals}f}"
        return px.rstrip("0").rstrip(".")

def timestamp_ms():
    return int(time.time() * 1000)

async def call(fn, *args, **kwargs):
    result = fn(*args, **kwargs)
    if asyncio.iscoroutine(result):
        return await result
    return result

def extract_subscription_error(s: str) -> dict:
    try:
        json_str = s[s.index("{"):]
        return json.loads(json_str)
    except Exception:
        return {}

def key(dict: Dict[str, Any]) -> str:
    d = dict.get("data")
    if d is not None: # dict is ws msg
        c = dict.get("channel")
        if c == "allMids": return f"allMids:{d.get("dex", "")}"
        if c == "notification": return "notification"
        if c == "webData3": return f'webData3:{d["userState"]["user"].lower()}'
        if c == "twapStates": return f'twapStates:{d["user"].lower()}:{d.get("dex", "")}'
        if c == "clearinghouseState": return f'clearinghouseState:{d["user"].lower()}:{d.get("dex", "")}'
        if c == "openOrders": return f'openOrders:{d["user"].lower()}:{d.get("dex", "")}'
        if c == "candle": return f'candle:{d["s"].lower()},{d["i"]}'
        if c == "l2Book": return f'l2Book:{d["coin"].lower()}'
        if c == "trades": return f'trades:{d[0]["coin"].lower()}' if d else "trades"
        if c == "orderUpdates": return "orderUpdates"
        if c == "user": return "userEvents"
        if c == "userFills": return f'userFills:{d["user"].lower()}'
        if c == "userFundings": return f'userFundings:{d["user"].lower()}'
        if c == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{d["user"].lower()}'
        if c == "activeAssetCtx": return f'activeAssetCtx:{d["coin"].lower()}'
        if c == "activeAssetData": return f'activeAssetData:{d["user"].lower()}:{d["coin"].lower()}'
        if c == "userTwapSliceFills": return f'userTwapSliceFills:{d["user"].lower()}'
        if c == "userTwapHistory": return f'userTwapHistory:{d["user"].lower()}'
        if c == "bbo": return f'bbo:{d["coin"].lower()}'
        raise ValueError(f"Unknown WS channel: {c}")
    else: # dict is a sub
        t = dict.get("type")
        if t == "allMids": return f"allMids:{dict.get("dex", "")}"
        if t == "notification": return "notification"
        if t == "webData3": return f'webData3:{dict["user"].lower()}'
        if t == "twapStates": return f'twapStates:{dict["user"].lower()}:{dict.get("dex", "")}'
        if t == "clearinghouseState": return f'clearinghouseState:{dict["user"].lower()}:{dict.get("dex", "")}'
        if t == "openOrders": return f'openOrders:{dict["user"].lower()}:{dict.get("dex", "")}'
        if t == "candle": return f'candle:{dict["coin"].lower()},{dict["interval"]}'
        if t == "l2Book": return f'l2Book:{dict["coin"].lower()}'
        if t == "trades": return f'trades:{dict["coin"].lower()}'
        if t == "orderUpdates": return "orderUpdates"
        if t == "userEvents": return "userEvents"
        if t == "user": return "userEvents"
        if t == "userFills": return f'userFills:{dict["user"].lower()}'
        if t == "userFundings": return f'userFundings:{dict["user"].lower()}'
        if t == "userNonFundingLedgerUpdates": return f'userNonFundingLedgerUpdates:{dict["user"].lower()}'
        if t == "activeAssetCtx": return f'activeAssetCtx:{dict["coin"].lower()}'
        if t == "activeAssetData": return f'activeAssetData:{dict["user"].lower()}:{dict["coin"].lower()}'
        if t == "userTwapSliceFills": return f'userTwapSliceFills:{dict["user"].lower()}'
        if t == "userTwapHistory": return f'userTwapHistory:{dict["user"].lower()}'
        if t == "bbo": return f'bbo:{dict["coin"].lower()}'
        raise ValueError(f"Unknown subscription type: {t}")
    
async def cancel(task: asyncio.Task):
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

def get_order(instrument: Instrument, is_buy: bool, size: float, price: float, type: ExecutionType, reduce_only: bool = False, cloid: Optional[Cloid] = None) -> Order:
    order: Order = {
        "a": instrument.asset,
        "b": is_buy,
        "p": instrument.format_price(price),
        "s": instrument.format_size(size),
        "r": reduce_only,
        "t": type
    }
    if cloid:
        order["cloid"] = cloid
    return order