import traceback as tb
from core.HL.api import API
from core.HL.utils.types import (Any, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Instrument)
from log.logger import Logger
from log.messenger import Messenger

class Info:
    def __init__(self, url, messenger: Messenger, api: API):
        self.api = api
        self.messenger = messenger
        self.url = url
        self._log = Logger("HL Info", messenger)
        self.instruments_table: dict[str | int, int] = {} # name | coin | asset -> asset
        self.instruments_dict: dict[int, Instrument] = {} # asset -> Instrument

    async def post(self, payload):
        return await self.api.post("/info", payload)
    
    def _register(self, name: str, coin: str, asset: int, szDecimals: int):
        inst = Instrument(name, coin, asset, szDecimals)
        self.instruments_dict[asset] = inst
        self.instruments_table[asset] = asset
        self.instruments_table[name] = asset
        self.instruments_table[coin] = asset

    def instrument(self, x: str | int) -> Instrument:
        asset = self.instruments_table.get(x, -1)
        instrument = self.instruments_dict.get(asset)
        if instrument is None:
            raise ValueError(f"The ID provided ({x}) does not correspond to any hyperliquid market")
        return instrument
    
    async def _load_perp(self, dex: str, offset: int):
        meta = await self.meta(dex=dex)
        for i, info in enumerate(meta["universe"]):
            self._register(info["name"], info["name"], offset + i, info["szDecimals"])
    
    async def _load_spot(self):
        spot_meta = await self.spot_meta()
        tokens = {t["index"]: t for t in spot_meta["tokens"]}
        for info in spot_meta["universe"]:
            asset = info["index"] + 10000
            base, quote = info["tokens"]
            base_info = tokens[base]
            quote_info = tokens[quote]
            self._register(f'{base_info["name"]}/{quote_info["name"]}', info['name'], asset, base_info["szDecimals"])

    async def start(self, dexs=None):
        try:
            await self._load_spot()
            await self._load_perp("", 0)
            perp_dexs = await self.perp_dexs()
            for i, dex in enumerate([d["name"] for d in perp_dexs[1:]]):
                offset = 110000 + i * 10000
                if (dexs is None) or (dexs and (dex in dexs)):
                    await self._load_perp(dex, offset)
            await self._log.success("fetched datas")
        except Exception as e:
            tb.print_exc()
            await self._log.error(f"{e}")
            raise

    async def all_perp_metas(self) -> Any:
        return await self.post({"type": "allPerpMetas"})

    async def spot_meta(self) -> SpotMeta:
        return await self.post({"type": "spotMeta"})

    async def outcome_meta(self) -> Any:
        return await self.post({"type": "outcomeMeta"})

    async def meta(self, dex: str = "", x: str | int | None = None) -> Meta:
        payload = {"type": "meta", "dex": dex}
        if x:
            coin = self.instrument(x).coin
            payload["coin"] = coin
        return await self.post(payload)

    async def meta_and_asset_ctxs(self) -> Any:
        return await self.post({"type": "metaAndAssetCtxs"})

    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        return await self.post({"type": "spotMetaAndAssetCtxs"})

    async def clearing_house_state(self, address: str, dex: str = "") -> Any:
        return await self.post({"type": "clearinghouseState", "user": address, "dex": dex})

    async def spot_clearing_house_state(self, address: str) -> Any:
        return await self.post({"type": "spotClearinghouseState", "user": address})

    async def perp_dexs(self) -> Any:
        return await self.post({"type": "perpDexs"})

    async def open_orders(self, address: str, dex: str = "") -> Any:
        return await self.post({"type": "openOrders", "user": address, "dex": dex})

    async def frontend_open_orders(self, address: str, dex: str = "") -> Any:
        return await self.post({"type": "frontendOpenOrders", "user": address, "dex": dex})

    async def all_mids(self, dex: str = "") -> Any:
        return await self.post({"type": "allMids", "dex": dex})

    async def user_fills(self, address: str) -> Any:
        return await self.post({"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        return await self.post({"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time})
    
    async def price(self, x: str | int, dex = ""):
        prices = await self.all_mids(dex)
        return float(prices[self.instrument(x).coin])

    async def funding_history(self, x: str | int, start_time: int, end_time: Optional[int] = None) -> Any:
        coin = self.instrument(x).coin
        payload = {"type": "fundingHistory", "coin": coin, "startTime": start_time}
        if end_time is not None: payload["endTime"] = end_time
        return await self.post(payload)

    async def user_funding(self, user: str, start_time: int, end_time: Optional[int] = None) -> Any:
        payload = {"type": "userFunding", "user": user, "startTime": start_time}
        if end_time is not None: payload["endTime"] = end_time
        return await self.post(payload)

    async def l2_book(self, x: str | int) -> Any:
        coin = self.instrument(x).coin
        return await self.post({"type": "l2Book", "coin": coin})

    async def candles_snapshot(self, x: str | int, interval: str, start_time: int, end_time: int) -> Any:
        coin = self.instrument(x).coin
        req = {"coin": coin, "interval": interval, "startTime": start_time, "endTime": end_time}
        return await self.post({"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        return await self.post({"type": "userFees", "user": address})

    async def delegator_summary(self, address: str) -> Any:
        return await self.post({"type": "delegatorSummary", "user": address})

    async def delegations(self, address: str) -> Any:
        return await self.post({"type": "delegations", "user": address})

    async def delegator_rewards(self, address: str) -> Any:
        return await self.post({"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        return await self.post({"type": "queryOrderByOid", "user": user, "oid": oid})

    async def order_status(self, user: str, oid: int) -> Any:
        return await self.post({"type": "orderStatus", "user": user, "oid": oid})

    async def referral(self, user: str) -> Any:
        return await self.post({"type": "referral", "user": user})

    async def sub_accounts(self, user: str) -> Any:
        return await self.post({"type": "subAccounts", "user": user})

    async def user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        return await self.post({"type": "userToMultiSigSigners", "user": multi_sig_user})
