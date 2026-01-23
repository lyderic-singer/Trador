from __future__ import annotations
from .constants import MAX_DECIMALS_PERP, MAX_DECIMALS_SPOT
from .functions import format_price
from typing import List, Literal, Optional, Tuple, TypedDict, Union, Any, NotRequired

AssetInfo = TypedDict("AssetInfo", {"name": str, "szDecimals": int})
Meta = TypedDict("Meta", {"universe": List[AssetInfo]})
SIDE = Literal["A", "B"]
TIF = Literal["Alo", "Ioc", "Gtc"]
TPSL = Literal["tp", "sl"]


InstrumentType = Literal["perp", "spot", "hip3", ""]

class Tif(TypedDict):
    tif: TIF

class TriggerType(TypedDict):
    isMarket: bool
    triggerPx: str
    tpsl: TPSL

LimitOrderType = TypedDict(
    "OrderType",
    {
        "limit": Tif,
    },
    total=False,
)

BuilderInfo = TypedDict("BuilderInfo", {"b": str, "f": int})

class Instrument:
    def __init__(self, name: str, coin: str, asset: int, szDecimals: int):
        self.name = name # appear in front, ex 'UBTC/USDC'
        self.coin = coin # real internal symbol ex: '@152'
        self.asset = asset # asset id ex: 10152
        self.szDecimals = szDecimals
        self.type: InstrumentType = ""
        if ('@' in name) or ('/' in name):
            self.type = "spot"
        elif ':' in name:
            self.type = "hip3"
        else:
            self.type = "perp"
        self.dex = name[:name.index(":")] if self.type == "hip3" else ""

    def format_price(self, price: float):
        max_decimals = MAX_DECIMALS_SPOT if self.type == "spot" else MAX_DECIMALS_PERP
        return format_price(price, max_decimals, self.szDecimals)

    def format_size(self, sz):
        return str(round(sz, self.szDecimals))
    
    def __str__(self):
        return f"Instrument(name={self.name}, coin={self.coin}, asset={self.asset}, type={self.type}, dex={self.dex if self.dex else '""'}, decimals={self.szDecimals})"

TriggerOrderType = TypedDict(
    "TriggerOrderType",
    {
        "trigger": TriggerType,
    },
    total=False,
)

ExecutionType = Union[LimitOrderType, TriggerOrderType]

class Order(TypedDict):
    a: int
    b: bool # buy ?
    p: str
    s: str
    r: bool # reduceOnly ?
    t: ExecutionType
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

OidOrCloid = Union[int, Cloid]
Modify = TypedDict(
    "ModifyRequest",
    {
        "oid": OidOrCloid,
        "order": Order,
    },
    total=False,
)
CancelRequest = TypedDict("CancelRequest", {"a": str, "o": int})
CancelByCloidRequest = TypedDict("CancelByCloidRequest", {"asset": str, "cloid": Cloid})
