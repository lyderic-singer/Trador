import math
from core.HL.utils.constants import MAX_DECIMALS_PERP, MAX_DECIMALS_SPOT
from typing import List, Literal, Optional, Tuple, TypedDict, Union, Any, NotRequired

class Cloid:
    def __init__(self, raw_cloid: str):
        self._raw_cloid: str = raw_cloid
        self._validate()

    def _validate(self):
        if not self._raw_cloid[:2] == "0x":
            raise TypeError("cloid is not a hex string")
        if not len(self._raw_cloid[2:]) == 32:
            raise TypeError("cloid is not 16 bytes")

class Instrument:
    def __init__(self, name: str, coin: str, asset: int, sz_decimals: int):
        self.name = name # appear in front, ex 'UBTC/USDC'
        self.coin = coin # real internal symbol ex: '@152'
        self.asset = asset # asset id ex: 10152
        self.type: InstrumentType = ""
        if ('@' in name) or ('/' in name):
            self.type = "spot"
        elif ':' in name:
            self.type = "hip3"
        else:
            self.type = "perp"
        self.dex = name[:name.index(":")] if self.type == "hip3" else ""
        self.max_decimals = MAX_DECIMALS_SPOT if self.type == "spot" else MAX_DECIMALS_PERP
        self.sz_decimals = sz_decimals
        self.sz_factor = 10 ** self.sz_decimals
        self.px_decimals = self.max_decimals - sz_decimals
        self.px_factor = 10 ** self.px_decimals
        self.max_leverage: int | None = None
        self.isolated_only = False
        self.delisted = False

    def format_price(self, price: float):
        if (price.is_integer()) or (len(str(int(price))) > 4):
            return str(int(price))
        else:
            pxint, pxdec = str(float(price))[:6].split('.')
            pxstr = pxint + '.' + (pxdec[:self.px_decimals])
            return pxstr.rstrip("0").rstrip(".")

    def format_size(self, size):
        return str(math.floor(size * self.sz_factor) / self.sz_factor)
    
    def __str__(self):
        return (
            f"Instrument(name={self.name}, coin={self.coin}, asset={self.asset}, type={self.type}, "
            f"dex={self.dex if self.dex else '""'}, sz_decimals={self.sz_decimals}, px_decimals={self.px_decimals})")

class Resting:
    def __init__(self, order: dict):
        self.sz = float(order["sz"])
        self.px = float(order["limitPx"])
        self.is_buy = order["side"] == "B"
        self.value = self.sz * self.px
        self.original = order

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

Transfer = TypedDict(
    "Transfer",
    {
        "type": str,
        "hyperliquidChain": str,
        "signatureChainId": str,
        "destination": str,
        "sourceDex": str,
        "destinationDex": str,
        "token": str,
        "amount": str,
        "fromSubAccount": str,
        "nonce": int
    }
)

AssetInfo = TypedDict("AssetInfo", {"name": str, "szDecimals": int})
Meta = TypedDict("Meta", {"universe": List[AssetInfo]})
SIDE = Literal["A", "B"]
TIF = Literal["Alo", "Ioc", "Gtc"]
TPSL = Literal["tp", "sl"]
InstrumentType = Literal["perp", "spot", "hip3", ""]
LimitOrder = TypedDict("LimitOrder", {"tif": TIF})
LimitOrderType = TypedDict("LimitOrderType", {"limit": LimitOrder})
TriggerType = TypedDict("TriggerType", {"isMarket": bool, "triggerPx": str, "tpsl": TPSL})
TriggerOrderType = TypedDict("TriggerOrderType", {"trigger": TriggerType})
ExecutionType = Union[LimitOrderType, TriggerOrderType]
BuilderInfo = TypedDict("BuilderInfo", {"b": str, "f": int})
SpotAssetInfo = TypedDict("SpotAssetInfo", {"name": str, "tokens": List[int], "index": int, "isCanonical": bool})
SpotMeta = TypedDict("SpotMeta", {"universe": List[SpotAssetInfo], "tokens": List[SpotTokenInfo]})
Sub = dict[str, Any]
OidOrCloid = Union[int, Cloid, None]
Order = TypedDict("Order", {"a": int, "b": bool, "p": str, "s": str, "r": bool, "t": ExecutionType, "c": NotRequired[Optional[Cloid]]})
Modify = TypedDict("Modify", {"oid": OidOrCloid,"order": Order})
CancelOid = TypedDict("CancelOid", {"a": str, "o": OidOrCloid})
CancelCloid = TypedDict("CancelCloid", {"asset": str, "cloid": OidOrCloid})
Cancel = CancelOid | CancelCloid
SpotMetaAndAssetCtxs = Tuple[SpotMeta, List[SpotAssetCtx]]
