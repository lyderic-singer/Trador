from .api import API
import asyncio
from .utils.types import (Any, Callable, List, Cloid, Meta, Optional, SpotMeta, SpotMetaAndAssetCtxs, Instrument, Subscription)
from ..websocket import Websocket
from log.logger import Logger

class Info:
    def __init__(self, api: API, messenger, skip_ws: bool = False):
        self.api = api
        self.messenger = messenger
        self.skip_ws = skip_ws
        self.base_url = api.base_url
        self._log = Logger(self, messenger)
        self.ws = None
        self.instruments_dict: dict[int, Instrument] = {} # asset -> Instrument
        self.instruments_table: dict[str | int, int] = {} # name | coin | asset -> asset
    
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
        if not self.skip_ws:
            self.ws = Websocket(self.base_url, self.messenger)
            asyncio.create_task(self.ws.start())
        await self._load_spot()
        perp_dexs = await self.perp_dexs()
        for i, dex in enumerate([""] + [d["name"] for d in perp_dexs[1:]]):
            offset = 0 if dex == "" else 110000 + (i - 1) * 10000
            await self._load_perp(dex, offset)

    async def disconnect(self):
        if self.ws is None:
            await self._log.error("Tentative de disconnect alors que skip_ws est activé.")
            raise RuntimeError("Cannot call disconnect since skip_ws was used")
        await self.ws.stop()
        await self._log.debug("WebSocket arrêté.")
        await self.api.close()
        await self._log.debug("API fermée.")

    async def user_state(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel de user_state")
        return await self.api.post("/info", {"type": "clearinghouseState", "user": address, "dex": dex})

    async def spot_user_state(self, address: str) -> Any:
        await self._log.debug(f"Appel spot_user_state avec address={address}")
        return await self.api.post("/info", {"type": "spotClearinghouseState", "user": address})

    async def open_orders(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel open_orders avec address={address}, dex={dex}")
        return await self.api.post("/info", {"type": "openOrders", "user": address, "dex": dex})

    async def frontend_open_orders(self, address: str, dex: str = "") -> Any:
        await self._log.debug(f"Appel frontend_open_orders avec address={address}, dex={dex}")
        return await self.api.post("/info", {"type": "frontendOpenOrders", "user": address, "dex": dex})

    async def all_mids(self, dex: str = "") -> Any:
        await self._log.debug(f"Appel all_mids avec dex={dex}")
        return await self.api.post("/info", {"type": "allMids", "dex": dex})

    async def user_fills(self, address: str) -> Any:
        await self._log.debug(f"Appel user_fills avec address={address}")
        return await self.api.post("/info", {"type": "userFills", "user": address})

    async def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        await self._log.debug(f"Appel user_fills_by_time avec address={address}, start_time={start_time}, end_time={end_time}")
        return await self.api.post("/info", {"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time})

    async def meta(self, dex: str = "") -> Meta:
        await self._log.debug(f"Appel meta")
        return cast(Meta, await self.api.post("/info", {"type": "meta", "dex": dex}))

    async def meta_and_asset_ctxs(self) -> Any:
        await self._log.debug("Appel meta_and_asset_ctxs")
        return await self.api.post("/info", {"type": "metaAndAssetCtxs"})

    async def perp_dexs(self) -> Any:
        await self._log.debug("Appel perp_dexs")
        return await self.api.post("/info", {"type": "perpDexs"})

    async def spot_meta(self) -> SpotMeta:
        await self._log.debug("Appel spot_meta")
        return cast(SpotMeta, await self.api.post("/info", {"type": "spotMeta"}))

    async def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        await self._log.debug("Appel spot_meta_and_asset_ctxs")
        return cast(SpotMetaAndAssetCtxs, await self.api.post("/info", {"type": "spotMetaAndAssetCtxs"}))
    
    async def get_price(self, name: str, dex = ""):
        await self._log.info(f"prix demandé pour {name}")
        prices = await self.all_mids(dex)
        coin = self.name_to_coin[name]
        return float(prices[coin])

    async def funding_history(self, name: str, startTime: int, endTime: Optional[int] = None) -> Any:
        await self._log.debug(f"Appel funding_history avec name={name}, startTime={startTime}, endTime={endTime}")
        coin = self.name_to_coin[name]
        if endTime is not None:
            return await self.api.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime, "endTime": endTime})
        return await self.api.post("/info", {"type": "fundingHistory", "coin": coin, "startTime": startTime})

    async def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        await self._log.debug(f"Appel user_funding_history avec user={user}, startTime={startTime}, endTime={endTime}")
        if endTime is not None:
            return await self.api.post("/info", {"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return await self.api.post("/info", {"type": "userFunding", "user": user, "startTime": startTime})

    async def l2_snapshot(self, name: str) -> Any:
        await self._log.debug(f"Appel l2_snapshot avec name={name}")
        return await self.api.post("/info", {"type": "l2Book", "coin": self.name_to_coin[name]})

    async def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> Any:
        await self._log.debug(f"Appel candles_snapshot avec name={name}, interval={interval}, startTime={startTime}, endTime={endTime}")
        req = {"coin": self.name_to_coin[name], "interval": interval, "startTime": startTime, "endTime": endTime}
        return await self.api.post("/info", {"type": "candleSnapshot", "req": req})

    async def user_fees(self, address: str) -> Any:
        await self._log.debug(f"Appel user_fees avec address={address}")
        return await self.api.post("/info", {"type": "userFees", "user": address})

    async def user_staking_summary(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_summary avec address={address}")
        return await self.api.post("/info", {"type": "delegatorSummary", "user": address})

    async def user_staking_delegations(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_delegations avec address={address}")
        return await self.api.post("/info", {"type": "delegations", "user": address})

    async def user_staking_rewards(self, address: str) -> Any:
        await self._log.debug(f"Appel user_staking_rewards avec address={address}")
        return await self.api.post("/info", {"type": "delegatorRewards", "user": address})

    async def query_order_by_oid(self, user: str, oid: int) -> Any:
        await self._log.debug(f"Appel query_order_by_oid avec user={user}, oid={oid}")
        return await self.api.post("/info", {"type": "queryOrderByOid", "user": user, "oid": oid})

    async def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        await self._log.debug(f"Appel query_order_by_cloid pour user={user}, cloid={cloid}")
        return await self.api.post("/info", {"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    async def query_referral_state(self, user: str) -> Any:
        await self._log.debug(f"Appel query_referral_state pour user={user}")
        return await self.api.post("/info", {"type": "referral", "user": user})

    async def query_sub_accounts(self, user: str) -> Any:
        await self._log.debug(f"Appel query_sub_accounts pour user={user}")
        return await self.api.post("/info", {"type": "subAccounts", "user": user})

    async def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        await self._log.debug(f"Appel query_user_to_multi_sig_signers pour multi_sig_user={multi_sig_user}")
        return await self.api.post("/info", {"type": "userToMultiSigSigners", "user": multi_sig_user})
    
    async def single_meta(self, name: str, dex: str = "") -> dict:
        await self._log.info(f"Appel single_meta avec name={name}, dex={dex}")
        coin = self.name_to_coin.get(name, name)  # fallback au name brut
        return await self.api.post("/info", {"type": "meta", "dex": dex, "coin": coin})

    def _remap_coin_subscription(self, subscription: Subscription) -> None:
        if subscription["type"] in {"l2Book", "trades", "candle", "bbo"}:
            subscription["coin"] = self.name_to_coin[subscription["coin"]]

    async def subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int:
        self._remap_coin_subscription(subscription)
        if self.ws is None:
            await self._log.error("subscribe appelé sans websocket")
            raise RuntimeError("impossible d'appeler subscribe sans web socket init")
        sub_id = await self.ws.subscribe(subscription, callback)
        await self._log.info(f"Abonnement réussi, subscription_id={sub_id}, subscription={subscription}")
        return sub_id

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        self._remap_coin_subscription(subscription)
        if self.ws is None:
            await self._log.error("unsubscribe appelé sans web")
            raise RuntimeError("impossible d'appeler unsubscribe sans web socket init")
        success = await self.ws.unsubscribe(subscription, subscription_id)
        if success:
            await self._log.success(f"Désabonnement réussi, subscription_id={subscription_id}")
        else:
            await self._log.warning(f"Échec du désabonnement, subscription_id={subscription_id}")
        return success

    async def unsubscribe_all(self) -> None:
        if self.ws is None:
            await self._log.error("unsubscribe_all appelé sans web socket init")
            raise RuntimeError("impossible d'appeler unsubscribe_all sans web socket init")
        await self.ws.unsubscribe_all()

    def name_to_asset(self, name: str) -> int:
        return self.coin_to_asset[self.name_to_coin[name]]
