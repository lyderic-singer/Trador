from .api import API
from .utils.types import (Any, Cloid, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Instrument)
from ..websocket import Websocket
from log.logger import Logger

class Info:
    def __init__(self, url, messenger, api: API):
        self.api = api
        self.messenger = messenger
        self.url = url
        self._log = Logger(self, messenger)
        self.instruments_dict: dict[int, Instrument] = {} # asset -> Instrument
        self.instruments_table: dict[str | int, int] = {} # name | coin | asset -> asset

    async def post(self, request):
        await self.post(request)
    
    def _register(self, name: str, coin: str, asset: int, szDecimals: int):
        inst = Instrument(name, coin, asset, szDecimals)
        self.instruments_dict[asset] = inst
        self.instruments_table[asset] = asset
        self.instruments_table[name] = asset
        self.instruments_table[coin] = asset

    def instrument(self, x: str | int) -> Instrument:
        asset = self.instruments_table[x]
        return self.instruments_dict[asset]
    
    async def _load_perp(self, dex: str, offset: int):
        meta = await self.meta(dex)
        for i, info in enumerate(meta["universe"]):
            self._register(info["name"], info["name"], offset + i, info["szDecimals"])
    
    async def _load_spot(self):
        spot_meta = await self.spot_meta()
        for info in spot_meta["universe"]:
            asset = info["index"] + 10000
            base, quote = info["tokens"]
            base_info = spot_meta["tokens"][base]
            quote_info = spot_meta["tokens"][quote]
            self._register(f'{base_info["name"]}/{quote_info["name"]}', info['name'], asset, base_info["szDecimals"])

    async def start(self):
        await self._load_spot()
        perp_dexs = await self.perp_dexs()
        for i, dex in enumerate([""] + [d["name"] for d in perp_dexs[1:]]):
            offset = 0 if dex == "" else 110000 + (i - 1) * 10000
            await self._load_perp(dex, offset)

    async def user_state(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel de user_state")
        return await self.post({"type": "clearinghouseState", "user": address, "dex": dex})

    async def spot_user_state(self, address: str) -> Any:
        await self._log.debug(f"Appel spot_user_state avec address={address}")
        return await self.post({"type": "spotClearinghouseState", "user": address})

    async def open_orders(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel open_orders avec address={address}, dex={dex}")
        return await self.post({"type": "openOrders", "user": address, "dex": dex})

    async def frontend_open_orders(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel frontend_open_orders avec address={address}, dex={dex}")
        return await self.post({"type": "frontendOpenOrders", "user": address, "dex": dex})

    async def all_mids(self, dex: str = "") -> Any:
        await self._log.debug(f"Appel all_mids avec dex={dex}")
        return await self.post({"type": "allMids", "dex": dex})

    async def user_fills(self, address: str) -> Any:
        await self._log.debug(f"Appel user_fills avec address={address}")
        return await self.post({"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        await self._log.debug(f"Appel user_fills_by_time avec address={address}, start_time={start_time}, end_time={end_time}")
        return await self.post({"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time})

    async def meta(self, dex: str = "") -> Meta:
        await self._log.debug(f"Appel meta")
        return await self.post({"type": "meta", "dex": dex})

    async def meta_and_asset_ctxs(self) -> Any:
        await self._log.debug("Appel meta_and_asset_ctxs")
        return await self.post({"type": "metaAndAssetCtxs"})

    async def perp_dexs(self) -> Any:
        await self._log.debug("Appel perp_dexs")
        return await self.post({"type": "perpDexs"})

    async def spot_meta(self) -> SpotMeta:
        await self._log.debug("Appel spot_meta")
        return await self.post({"type": "spotMeta"})

    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        await self._log.debug("Appel spot_meta_and_asset_ctxs")
        return await self.post({"type": "spotMetaAndAssetCtxs"})
    
    async def get_price(self, x: str | int, dex = ""):
        await self._log.info(f"prix demandé pour {x}")
        prices = await self.all_mids(dex)
        return float(prices[self.instrument(x).coin])

    async def funding_history(self, x: str | int, startTime: int, endTime: Optional[int] = None) -> Any:
        coin = self.instrument(x).coin
        await self._log.debug(f"Appel funding_history avec coin={coin}, startTime={startTime}, endTime={endTime}")
        if endTime is not None:
            return await self.post({"type": "fundingHistory", "coin": coin, "startTime": startTime, "endTime": endTime})
        return await self.post({"type": "fundingHistory", "coin": coin, "startTime": startTime})

    async def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        await self._log.debug(f"Appel user_funding_history avec user={user}, startTime={startTime}, endTime={endTime}")
        if endTime is not None:
            return await self.post({"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return await self.post({"type": "userFunding", "user": user, "startTime": startTime})

    async def l2_snapshot(self, x: str | int) -> Any:
        coin = self.instrument(x).coin
        await self._log.debug(f"Appel l2_snapshot avec coin={coin}")
        return await self.post({"type": "l2Book", "coin": coin})

    async def candles_snapshot(self, x: str | int, interval: str, startTime: int, endTime: int) -> Any:
        coin = self.instrument(x).coin
        await self._log.debug(f"Appel candles_snapshot avec coin={coin}, interval={interval}, startTime={startTime}, endTime={endTime}")
        req = {"coin": coin, "interval": interval, "startTime": startTime, "endTime": endTime}
        return await self.post({"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        await self._log.debug(f"Appel user_fees avec address={address}")
        return await self.post({"type": "userFees", "user": address})

    async def user_staking_summary(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_summary avec address={address}")
        return await self.post({"type": "delegatorSummary", "user": address})

    async def user_staking_delegations(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_delegations avec address={address}")
        return await self.post({"type": "delegations", "user": address})

    async def user_staking_rewards(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_rewards avec address={address}")
        return await self.post({"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        await self._log.debug(f"Appel query_order_by_oid avec user={user}, oid={oid}")
        return await self.post({"type": "queryOrderByOid", "user": user, "oid": oid})

    async def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        await self._log.debug(f"Appel query_order_by_cloid pour user={user}, cloid={cloid}")
        return await self.post({"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    async def query_referral_state(self, user: str) -> Any:
        await self._log.debug(f"Appel query_referral_state pour user={user}")
        return await self.post({"type": "referral", "user": user})

    async def query_sub_accounts(self, user: str) -> Any:
        await self._log.debug(f"Appel query_sub_accounts pour user={user}")
        return await self.post({"type": "subAccounts", "user": user})

    async def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        await self._log.debug(f"Appel query_user_to_multi_sig_signers pour multi_sig_user={multi_sig_user}")
        return await self.post({"type": "userToMultiSigSigners", "user": multi_sig_user})
    
    async def single_meta(self, x: str | int, dex: str = "") -> dict:
        coin = self.instrument(x).coin
        await self._log.info(f"Appel single_meta avec coin={coin}, dex={dex}")
        return await self.post({"type": "meta", "dex": dex, "coin": coin})
