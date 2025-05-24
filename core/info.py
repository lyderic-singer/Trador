from .api import API
import asyncio
from utils.types import (Any, Callable, Cloid, List, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Subscription, cast)
from .websocket_manager import WebsocketManager


class Info(API):
    def __init__(
        self,
        base_url: Optional[str] = None,
        skip_ws: Optional[bool] = False,
        meta: Optional[Meta] = None,
        spot_meta: Optional[SpotMeta] = None,
        perp_dexs: Optional[List[str]] = None,
    ):
        super().__init__(base_url)
        self.skip_ws = skip_ws
        self.meta_arg = meta
        self.spot_meta_arg = spot_meta
        self.perp_dexs_arg = perp_dexs

        self.ws_manager: Optional[WebsocketManager] = None
        self.coin_to_asset = {}
        self.name_to_coin = {}
        self.asset_to_sz_decimals = {}

    async def start(self):
        if not self.skip_ws:
            self.ws_manager = WebsocketManager(self.base_url)
            asyncio.create_task(self.ws_manager.start())

        spot_meta = self.spot_meta_arg or await self.spot_meta()

        for spot_info in spot_meta["universe"]:
            asset = spot_info["index"] + 10000
            self.coin_to_asset[spot_info["name"]] = asset
            self.name_to_coin[spot_info["name"]] = spot_info["name"]
            base, quote = spot_info["tokens"]
            base_info = spot_meta["tokens"][base]
            quote_info = spot_meta["tokens"][quote]
            self.asset_to_sz_decimals[asset] = base_info["szDecimals"]
            name = f'{base_info["name"]}/{quote_info["name"]}'
            if name not in self.name_to_coin:
                self.name_to_coin[name] = spot_info["name"]

        perp_dex_to_offset = {"": 0}
        perp_dexs = self.perp_dexs_arg or [""]

        if self.perp_dexs_arg is not None:
            perp_dex_list = await self.perp_dexs()
            for i, perp_dex in enumerate(perp_dex_list[1:]):
                perp_dex_to_offset[perp_dex["name"]] = 110000 + i * 10000

        for perp_dex in perp_dexs:
            offset = perp_dex_to_offset[perp_dex]
            if perp_dex == "" and self.meta_arg is not None:
                self.set_perp_meta(self.meta_arg, 0)
            else:
                fresh_meta = await self.meta(dex=perp_dex)
                self.set_perp_meta(fresh_meta, offset)
    
    def set_perp_meta(self, meta: Meta, offset: int) -> None:
        for asset, asset_info in enumerate(meta["universe"]):
            asset += offset
            self.coin_to_asset[asset_info["name"]] = asset
            self.name_to_coin[asset_info["name"]] = asset_info["name"]
            self.asset_to_sz_decimals[asset] = asset_info["szDecimals"]

    async def disconnect(self):
        if self.ws_manager is None:
            raise RuntimeError("Cannot call disconnect since skip_ws was used")
        await self.ws_manager.stop()
        if self._session:  # si tu utilises aussi ta session HTTP dans info_async
            await self._session.close()
            self._session = None

    async def user_state(self, address: str, dex: str = "") -> Any:
        return await self.post("/info", {"type": "clearinghouseState", "user": address, "dex": dex})

    async def spot_user_state(self, address: str) -> Any:
        return await self.post("/info", {"type": "spotClearinghouseState", "user": address})

    async def open_orders(self, address: str, dex: str = "") -> Any:
        return await self.post("/info", {"type": "openOrders", "user": address, "dex": dex})

    async def frontend_open_orders(self, address: str, dex: str = "") -> Any:
        return await self.post("/info", {"type": "frontendOpenOrders", "user": address, "dex": dex})
    
    async def all_mids(self, dex: str = "") -> Any:
        return await self.post("/info", {"type": "allMids", "dex": dex})

    async def user_fills(self, address: str) -> Any:
        return await self.post("/info", {"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        return await self.post("/info", {"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time})

    async def meta(self, dex: str = "") -> Meta:
        return cast(Meta, await self.post("/info", {"type": "meta", "dex": dex}))

    async def meta_and_asset_ctxs(self) -> Any:
        return await self.post("/info", {"type": "metaAndAssetCtxs"})

    async def perp_dexs(self) -> Any:
        return await self.post("/info", {"type": "perpDexs"})

    async def spot_meta(self) -> SpotMeta:
        return cast(SpotMeta, await self.post("/info", {"type": "spotMeta"}))
    
    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        return cast(SpotMetaAndAssetCtxs, await self.post("/info", {"type": "spotMetaAndAssetCtxs"}))

    async def funding_history(self, name: str, startTime: int, endTime: Optional[int] = None) -> Any:
        coin = self.name_to_coin[name]
        if endTime is not None:
            return await self.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime, "endTime": endTime})
        return await self.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime})

    async def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        if endTime is not None:
            return await self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return await self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime})

    async def l2_snapshot(self, name: str) -> Any:
        return await self.post("/info", {"type": "l2Book", "coin": self.name_to_coin[name]})

    async def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> Any:
        req = {"coin": self.name_to_coin[name], "interval": interval, "startTime": startTime, "endTime": endTime}
        return await self.post("/info", {"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        return await self.post("/info", {"type": "userFees", "user": address})

    async def user_staking_summary(self, address: str) -> Any:
        return await self.post("/info", {"type": "delegatorSummary", "user": address})

    async def user_staking_delegations(self, address: str) -> Any:
        return await self.post("/info", {"type": "delegations", "user": address})

    async def user_staking_rewards(self, address: str) -> Any:
        return await self.post("/info", {"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        return await self.post("/info", {"type": "orderStatus", "user": user, "oid": oid})

    async def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        return await self.post("/info", {"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    async def query_referral_state(self, user: str) -> Any:
        return await self.post("/info", {"type": "referral", "user": user})

    async def query_sub_accounts(self, user: str) -> Any:
        return await self.post("/info", {"type": "subAccounts", "user": user})

    async def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        return await self.post("/info", {"type": "userToMultiSigSigners", "user": multi_sig_user})

    def _remap_coin_subscription(self, subscription: Subscription) -> None:
        if subscription["type"] in {"l2Book", "trades", "candle", "bbo"}:
            subscription["coin"] = self.name_to_coin[subscription["coin"]]

    async def subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler subscribe sans ws manager")
        return await self.ws_manager.subscribe(subscription, callback)

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler unsubscribe sans ws manager")
        return await self.ws_manager.unsubscribe(subscription, subscription_id)
    
    async def unsubscribe_all(self) -> None:
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler unsubscribe_all sans ws manager")
        await self.ws_manager.unsubscribe_all()

    def name_to_asset(self, name: str) -> int:
        return self.coin_to_asset[self.name_to_coin[name]]