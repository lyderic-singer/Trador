from .api import API
import asyncio
from .utils.types import (Any, Callable, List, Cloid, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Subscription, cast)
from .websocket_manager import WebsocketManager
from logs.message_manager import Logger

class Info:
    def __init__(self, api: API, message_manager, skip_ws: Optional[bool] = False, meta: Optional[Meta] = None, spot_meta: Optional[SpotMeta] = None, perp_dexs: Optional[List[str]] = None,):
        self.message_manager = message_manager
        self.skip_ws = skip_ws
        self.meta_arg = meta
        self.spot_meta_arg = spot_meta
        self.base_url = api.base_url
        self.perp_dexs_arg = perp_dexs
        self._logging = Logger(self, message_manager)
        self.api = api
        self.ws_manager: Optional[WebsocketManager] = None
        self.coin_to_asset = {}
        self.name_to_coin = {}
        self.asset_to_sz_decimals = {}

    async def start(self):
        await self._logging.info("Démarrage de la connexion websocket...")

        if not self.skip_ws:
            self.ws_manager = WebsocketManager(self.base_url, self.message_manager)
            asyncio.create_task(self.ws_manager.start())
            await self._logging.success("WebsocketManager lancé avec succès.")
        else:
            await self._logging.warning("skip_ws est activé, websocket non lancé.")

        spot_meta = self.spot_meta_arg or await self.spot_meta()
        await self._logging.info("Récupération du spot_meta terminée.")

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
        await self._logging.debug("Mapping coin_to_asset et name_to_coin initialisés.")

        perp_dex_to_offset = {"": 0}
        perp_dexs = self.perp_dexs_arg or [""]

        if self.perp_dexs_arg is not None:
            perp_dex_list = await self.perp_dexs()
            await self._logging.info(f"Récupération des perp_dexs")
            for i, perp_dex in enumerate(perp_dex_list[1:]):
                perp_dex_to_offset[perp_dex["name"]] = 110000 + i * 10000
            await self._logging.debug(f"Offsets perp_dexs calculés")

        for perp_dex in perp_dexs:
            offset = perp_dex_to_offset[perp_dex]
            if perp_dex == "" and self.meta_arg is not None:
                self.set_perp_meta(self.meta_arg, 0)
                await self._logging.success("Meta perp initialisée avec meta_arg et offset 0.")
            else:
                fresh_meta = await self.meta(dex=perp_dex)
                self.set_perp_meta(fresh_meta, offset)
                await self._logging.success(f"Meta perp mise à jour pour dex '{perp_dex}' avec offset {offset}.")

    def set_perp_meta(self, meta: Meta, offset: int) -> None:
        for asset, asset_info in enumerate(meta["universe"]):
            asset += offset
            self.coin_to_asset[asset_info["name"]] = asset
            self.name_to_coin[asset_info["name"]] = asset_info["name"]
            self.asset_to_sz_decimals[asset] = asset_info["szDecimals"]

    async def disconnect(self):
        if self.ws_manager is None:
            await self._logging.error("Tentative de disconnect alors que skip_ws est activé.")
            raise RuntimeError("Cannot call disconnect since skip_ws was used")
        await self._logging.info("Arrêt du WebSocketManager...")
        await self.ws_manager.stop()
        await self._logging.success("WebSocketManager arrêté.")
        await self._logging.info("Fermeture de l'API...")
        await self.api.close()
        await self._logging.success("API fermée.")

    async def user_state(self, address: str, dex: str = "") -> Any:
        await self._logging.debug(f"Appel de user_state")
        return await self.api.post("/info", {"type": "clearinghouseState", "user": address, "dex": dex})

    async def spot_user_state(self, address: str) -> Any:
        await self._logging.debug(f"Appel spot_user_state avec address={address}")
        return await self.api.post("/info", {"type": "spotClearinghouseState", "user": address})

    async def open_orders(self, address: str, dex: str = "") -> Any:
        await self._logging.debug(f"Appel open_orders avec address={address}, dex={dex}")
        return await self.api.post("/info", {"type": "openOrders", "user": address, "dex": dex})

    async def frontend_open_orders(self, address: str, dex: str = "") -> Any:
        await self._logging.debug(f"Appel frontend_open_orders avec address={address}, dex={dex}")
        return await self.api.post("/info", {"type": "frontendOpenOrders", "user": address, "dex": dex})

    async def all_mids(self, dex: str = "") -> Any:
        await self._logging.debug(f"Appel all_mids avec dex={dex}")
        return await self.api.post("/info", {"type": "allMids", "dex": dex})

    async def user_fills(self, address: str) -> Any:
        await self._logging.debug(f"Appel user_fills avec address={address}")
        return await self.api.post("/info", {"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        await self._logging.debug(f"Appel user_fills_by_time avec address={address}, start_time={start_time}, end_time={end_time}")
        return await self.api.post("/info", {"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time})

    async def meta(self, dex: str = "") -> Meta:
        await self._logging.debug(f"Appel meta")
        return cast(Meta, await self.api.post("/info", {"type": "meta", "dex": dex}))

    async def meta_and_asset_ctxs(self) -> Any:
        await self._logging.debug("Appel meta_and_asset_ctxs")
        return await self.api.post("/info", {"type": "metaAndAssetCtxs"})

    async def perp_dexs(self) -> Any:
        await self._logging.debug("Appel perp_dexs")
        return await self.api.post("/info", {"type": "perpDexs"})

    async def spot_meta(self) -> SpotMeta:
        await self._logging.debug("Appel spot_meta")
        return cast(SpotMeta, await self.api.post("/info", {"type": "spotMeta"}))

    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        await self._logging.debug("Appel spot_meta_and_asset_ctxs")
        return cast(SpotMetaAndAssetCtxs, await self.api.post("/info", {"type": "spotMetaAndAssetCtxs"}))

    async def funding_history(self, name: str, startTime: int, endTime: Optional[int] = None) -> Any:
        await self._logging.debug(f"Appel funding_history avec name={name}, startTime={startTime}, endTime={endTime}")
        coin = self.name_to_coin[name]
        if endTime is not None:
            return await self.api.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime, "endTime": endTime})
        return await self.api.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime})

    async def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        await self._logging.debug(f"Appel user_funding_history avec user={user}, startTime={startTime}, endTime={endTime}")
        if endTime is not None:
            return await self.api.post("/info", {"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return await self.api.post("/info", {"type": "userFunding", "user": user, "startTime": startTime})

    async def l2_snapshot(self, name: str) -> Any:
        await self._logging.debug(f"Appel l2_snapshot avec name={name}")
        return await self.api.post("/info", {"type": "l2Book", "coin": self.name_to_coin[name]})

    async def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> Any:
        await self._logging.debug(f"Appel candles_snapshot avec name={name}, interval={interval}, startTime={startTime}, endTime={endTime}")
        req = {"coin": self.name_to_coin[name], "interval": interval, "startTime": startTime, "endTime": endTime}
        return await self.api.post("/info", {"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        await self._logging.debug(f"Appel user_fees avec address={address}")
        return await self.api.post("/info", {"type": "userFees", "user": address})

    async def user_staking_summary(self, address: str) -> Any:
        await self._logging.debug(f"Appel user_staking_summary avec address={address}")
        return await self.api.post("/info", {"type": "delegatorSummary", "user": address})

    async def user_staking_delegations(self, address: str) -> Any:
        await self._logging.debug(f"Appel user_staking_delegations avec address={address}")
        return await self.api.post("/info", {"type": "delegations", "user": address})

    async def user_staking_rewards(self, address: str) -> Any:
        await self._logging.debug(f"Appel user_staking_rewards avec address={address}")
        return await self.api.post("/info", {"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        await self._logging.debug(f"Appel query_order_by_oid avec user={user}, oid={oid}")
        return await self.api.post("/info", {"type": "queryOrderByOid", "user": user, "oid": oid})

    async def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        await self._logging.debug(f"Appel query_order_by_cloid pour user={user}, cloid={cloid}")
        return await self.api.post("/info", {"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    async def query_referral_state(self, user: str) -> Any:
        await self._logging.debug(f"Appel query_referral_state pour user={user}")
        return await self.api.post("/info", {"type": "referral", "user": user})

    async def query_sub_accounts(self, user: str) -> Any:
        await self._logging.debug(f"Appel query_sub_accounts pour user={user}")
        return await self.api.post("/info", {"type": "subAccounts", "user": user})

    async def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        await self._logging.debug(f"Appel query_user_to_multi_sig_signers pour multi_sig_user={multi_sig_user}")
        return await self.api.post("/info", {"type": "userToMultiSigSigners", "user": multi_sig_user})

    def _remap_coin_subscription(self, subscription: Subscription) -> None:
        if subscription["type"] in {"l2Book", "trades", "candle", "bbo"}:
            subscription["coin"] = self.name_to_coin[subscription["coin"]]

    async def subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            await self._logging.error("subscribe appelé sans ws_manager")
            raise RuntimeError("impossible d'appeler subscribe sans ws manager")
        sub_id = await self.ws_manager.subscribe(subscription, callback)
        await self._logging.info(f"Abonnement réussi, subscription_id={sub_id}, subscription={subscription}")
        return sub_id

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            await self._logging.error("unsubscribe appelé sans ws_manager")
            raise RuntimeError("impossible d'appeler unsubscribe sans ws manager")
        success = await self.ws_manager.unsubscribe(subscription, subscription_id)
        if success:
            await self._logging.success(f"Désabonnement réussi, subscription_id={subscription_id}")
        else:
            await self._logging.warning(f"Échec du désabonnement, subscription_id={subscription_id}")
        return success

    async def unsubscribe_all(self) -> None:
        if self.ws_manager is None:
            await self._logging.error("unsubscribe_all appelé sans ws_manager")
            raise RuntimeError("impossible d'appeler unsubscribe_all sans ws manager")
        await self.ws_manager.unsubscribe_all()
        await self._logging.success("Tous les abonnements ont été désabonnés.")

    def name_to_asset(self, name: str) -> int:
        return self.coin_to_asset[self.name_to_coin[name]]
