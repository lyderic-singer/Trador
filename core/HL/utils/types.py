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

def format_price(px: float, max_decimals: int, sz_decimals: int) -> str:
    if (px.is_integer()) or (len(str(int(px))) > 4):
        return str(int(px))
    else:
        decimals = max_decimals - sz_decimals
        px = float(f"{px:.5g}")
        px = round(px, decimals)
        px_str = f"{px:.{decimals}f}"
        return px_str.rstrip("0").rstrip(".")

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
        return (
            f"Instrument(name={self.name}, coin={self.coin}, asset={self.asset},\ntype={self.type}, "
            f"dex={self.dex if self.dex else '""'}, decimals={self.szDecimals})")

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
LimitOrderType = TypedDict("LimitOrderType", {"limit": LimitOrder}, total=False)
TriggerType = TypedDict("TriggerType", {"isMarket": bool, "triggerPx": str, "tpsl": TPSL})
TriggerOrderType = TypedDict("TriggerOrderType", {"trigger": TriggerType}, total=False)
ExecutionType = Union[LimitOrderType, TriggerOrderType]
BuilderInfo = TypedDict("BuilderInfo", {"b": str, "f": int})
SpotAssetInfo = TypedDict("SpotAssetInfo", {"name": str, "tokens": List[int], "index": int, "isCanonical": bool})
SpotMeta = TypedDict("SpotMeta", {"universe": List[SpotAssetInfo], "tokens": List[SpotTokenInfo]})
Sub = dict[str, Any]
OidOrCloid = Union[int, Cloid, None]
Order = TypedDict("Order", {"a": int, "b": bool, "p": str, "s": str, "r": bool, "t": ExecutionType, "c": NotRequired[Optional[Cloid]]})
Modify = TypedDict("Modify", {"oid": OidOrCloid,"order": Order}, total=False)
CancelOid = TypedDict("CancelOid", {"a": str, "o": OidOrCloid}, total=False)
CancelCloid = TypedDict("CancelCloid", {"asset": str, "cloid": OidOrCloid}, total=False)
Cancel = CancelOid | CancelCloid
SpotMetaAndAssetCtxs = Tuple[SpotMeta, List[SpotAssetCtx]]
