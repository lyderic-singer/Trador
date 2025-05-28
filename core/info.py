from .api import API
import asyncio
import logging
from .utils.types import (Any, Callable, Cloid, List, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Subscription, cast)
from .websocket_manager import WebsocketManager

logger = logging.getLogger(__name__)

class Info:
    def __init__(
        self,
        api: API,
        skip_ws: Optional[bool] = False,
        meta: Optional[Meta] = None,
        spot_meta: Optional[SpotMeta] = None,
        perp_dexs: Optional[List[str]] = None,
    ):
        self.skip_ws = skip_ws
        self.meta_arg = meta
        self.spot_meta_arg = spot_meta
        self.base_url = api.base_url
        self.perp_dexs_arg = perp_dexs
        self.api = api
        self.ws_manager: Optional[WebsocketManager] = None
        self.coin_to_asset = {}
        self.name_to_coin = {}
        self.asset_to_sz_decimals = {}
        logger.debug("Info initialized")

    async def start(self):
        logger.info("Starting Info module")
        if not self.skip_ws:
            self.ws_manager = WebsocketManager(self.base_url)
            asyncio.create_task(self.ws_manager.start())
            logger.debug("WebSocketManager started")

        spot_meta = self.spot_meta_arg or await self.spot_meta()
        logger.debug("Spot meta loaded")

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
            logger.debug("Perp dexs loaded")

        for perp_dex in perp_dexs:
            offset = perp_dex_to_offset[perp_dex]
            if perp_dex == "" and self.meta_arg is not None:
                self.set_perp_meta(self.meta_arg, 0)
            else:
                fresh_meta = await self.meta(dex=perp_dex)
                self.set_perp_meta(fresh_meta, offset)
        logger.info("Info start complete")

    def set_perp_meta(self, meta: Meta, offset: int) -> None:
        for asset, asset_info in enumerate(meta["universe"]):
            asset += offset
            self.coin_to_asset[asset_info["name"]] = asset
            self.name_to_coin[asset_info["name"]] = asset_info["name"]
            self.asset_to_sz_decimals[asset] = asset_info["szDecimals"]
        logger.debug(f"Set perp meta with offset {offset}")

    async def disconnect(self):
        if self.ws_manager is None:
            raise RuntimeError("Cannot call disconnect since skip_ws was used")
        await self.ws_manager.stop()
        await self.api.close()
        logger.info("Disconnected")

    def _remap_coin_subscription(self, subscription: Subscription) -> None:
        if subscription["type"] in {"l2Book", "trades", "candle", "bbo"}:
            subscription["coin"] = self.name_to_coin[subscription["coin"]]

    async def subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler subscribe sans ws manager")
        sub_id = await self.ws_manager.subscribe(subscription, callback)
        logger.debug(f"Subscribed to {subscription} with id {sub_id}")
        return sub_id

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        self._remap_coin_subscription(subscription)
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler unsubscribe sans ws manager")
        result = await self.ws_manager.unsubscribe(subscription, subscription_id)
        logger.debug(f"Unsubscribed from {subscription} with id {subscription_id}")
        return result

    async def unsubscribe_all(self) -> None:
        if self.ws_manager is None:
            raise RuntimeError("impossible d'appeler unsubscribe_all sans ws manager")
        await self.ws_manager.unsubscribe_all()
        logger.debug("Unsubscribed all")

    def name_to_asset(self, name: str) -> int:
        return self.coin_to_asset[self.name_to_coin[name]]
