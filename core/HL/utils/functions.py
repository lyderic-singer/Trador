import time
import asyncio
import json
from typing import Any

from core.HL.utils.types import TIF, Cancel, ExecutionType, Modify, Order, Transfer

def timestamp_ms() -> int:
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

def key(dict: dict[str, Any]) -> str:
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
    
async def cancel(task: asyncio.Task | None):
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

def get_order(instrument, is_buy: bool, size: float, price: float, execution_type: ExecutionType,
              reduce_only: bool = False, cloid = None) -> Order:
    order: Order = {
        "a": instrument.asset,
        "b": is_buy,
        "p": instrument.format_price(price),
        "s": instrument.format_size(size),
        "r": reduce_only,
        "t": execution_type
    }
    if cloid:
        order["c"] = cloid
    return order

def get_cancel(instrument,  oid: int | None = None, cloid = None) -> Cancel:
    if (oid is None) == (cloid is None):
        raise ValueError("oid xor cloid")
    if oid is not None:
        return {"a": instrument.asset, "o": oid}
    else:
        return {"asset": instrument.asset, "cloid": cloid}

def get_modify(order, instrument, new_size: float | None = None, new_price: float | None = None, tif: TIF | None = None, new_order: Order | None = None) -> Modify:
    if (new_order is None) and ((new_price is None) or (new_size is None) or (tif is None)):
        raise ValueError("new_order or (new_price and new_size and tif) must be provided")
    if (not new_order) and new_size and new_price and tif:
        new_order = get_order(instrument, order['side'] == 'B', new_size, new_price, {"limit": {"tif": tif}})
    modify: Modify = {
        "oid": order['oid'],
        "order": new_order
    }
    return modify

def get_transfer(token: str, amount: float, source_dex: str, destination_dex: str, destination_address: str, nonce: int,
                 mainnet: bool = True, from_subaccount: str = "", signature_chain_id: str = "0xa4b1") -> Transfer:
    chain = "Mainnet" if mainnet else "Testnet"
    action: Transfer = {
        "type": "sendAsset",
        "hyperliquidChain": chain,
        "signatureChainId": signature_chain_id,
        "destination": destination_address,
        "sourceDex": source_dex,
        "destinationDex": destination_dex,
        "token": token,
        "amount": str(amount),
        "fromSubAccount": from_subaccount,
        "nonce": nonce
    }
    return action

def closests(l1: list[float], l2: list[float]): # both l1 and l2 in ascending order
    if not l1:
        return l2
    elif len(l2) <= len(l1):
        return []
    else:
        l = []
        j = 0
        for x in l1:
            while (j < len(l2)) and (l2[j] < x):
                l.append(l2[j])
                j += 1
            if l and ((j >= len(l2) - 1) or (abs(x - l2[j]) < abs(x - l2[j + 1]))):
                l.pop()
            else:
                j += 1
        return l + l2[j:]
