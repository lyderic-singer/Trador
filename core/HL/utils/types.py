from __future__ import annotations

from typing import List, Literal, Optional, Tuple, TypedDict, Union

AssetInfo = TypedDict("AssetInfo", {"name": str, "szDecimals": int})
Meta = TypedDict("Meta", {"universe": List[AssetInfo]})
SIDE = Literal["A", "B"]
TIF = Literal["Alo", "Ioc", "Gtc"]
TPSL = Literal["tp", "sl"]

InstrumentType = Literal["perp", "spot", "hip3", ""]

class LimitType(TypedDict):
    tif: TIF

class TriggerType(TypedDict):
    isMarket: bool
    triggerPx: str
    tpsl: TPSL

OrderType = TypedDict(
    "OrderType",
    {
        "limit": LimitType,
    },
    total=False,
)

TriggerOrderType = TypedDict(
    "TriggerOrderType",
    {
        "trigger": TriggerType,
    },
    total=False,
)

class Order(TypedDict):
    a: int
    b: bool # buy ?
    p: str
    s: str
    r: bool # reduceOnly ?
    t: Union[OrderType, TriggerOrderType]
    c: Optional[Cloid]

SpotAssetInfo = TypedDict(
    "SpotAssetInfo",
    {"name": str, "tokens": List[int], "index": int, "isCanonical": bool},
)

SpotTokenInfo = TypedDict(
    "SpotTokenInfo",
    {
        "name": str,
        "szDecimals": int,
        "weiDecimals": int,
        "index": int,
        "tokenId": str,
        "isCanonical": bool,
        "evmContract": Optional[str],
        "fullName": Optional[str],
    },
)

SpotMeta = TypedDict(
    "SpotMeta",
    {"universe": List[SpotAssetInfo], "tokens": List[SpotTokenInfo]},
)

SpotAssetCtx = TypedDict(
    "SpotAssetCtx",
    {
        "dayNtlVlm": str,
        "markPx": str,
        "midPx": Optional[str],
        "prevDayPx": str,
        "circulatingSupply": str,
        "coin": str,
    },
)

SpotMetaAndAssetCtxs = Tuple[SpotMeta, List[SpotAssetCtx]]

class Cloid:
    def __init__(self, raw_cloid: str):
        self._raw_cloid: str = raw_cloid
        self._validate()

    def _validate(self):
        if not self._raw_cloid[:2] == "0x":
            raise TypeError("cloid is not a hex string")
        if not len(self._raw_cloid[2:]) == 32:
            raise TypeError("cloid is not 16 bytes")

    def __str__(self):
        return str(self._raw_cloid)

    def __repr__(self):
        return str(self._raw_cloid)

    @staticmethod
    def from_int(cloid: int) -> Cloid:
        return Cloid(f"{cloid:#034x}")

    @staticmethod
    def from_str(cloid: str) -> Cloid:
        return Cloid(cloid)

    def to_raw(self):
        return self._raw_cloid

