import json
import logging
import secrets

import eth_account
from eth_account.signers.local import LocalAccount

from .info import Info
from .utils.constants import MAINNET_API_URL
from .utils.signing import (
    CancelByCloidRequest, CancelRequest, ModifyRequest, OidOrCloid, OrderRequest,
    OrderType, OrderWire, ScheduleCancelAction, float_to_usd_int, get_timestamp_ms,
    order_request_to_order_wire, order_wires_to_order_action, sign_agent,
    sign_approve_builder_fee, sign_convert_to_multi_sig_user_action, sign_l1_action,
    sign_multi_sig_action, sign_perp_dex_class_transfer_action, sign_spot_transfer_action,
    sign_token_delegate_action, sign_usd_class_transfer_action, sign_usd_transfer_action,
    sign_withdraw_from_bridge_action
)
from .utils.types import Any, BuilderInfo, Cloid, Dict, List, Meta, Optional, PerpDexSchemaInput, SpotMeta, Tuple

class Exchange:
    DEFAULT_SLIPPAGE = 0.05

    def __init__(
        self,
        info: Info,
        wallet: LocalAccount,
        vault_address: Optional[str] = None,
        account_address: Optional[str] = None
    ):
        self.wallet = wallet
        self.base_url = info.base_url
        self.vault_address = vault_address
        self.account_address = account_address
        self.info = info
        self.expires_after: Optional[int] = None
        logging.debug(f"Exchange initialized with wallet={wallet.address}, vault_address={vault_address}, account_address={account_address}")

    async def _post_action(self, action, signature, nonce):
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.vault_address if action["type"] != "usdClassTransfer" else None,
            "expiresAfter": self.expires_after,
        }
        logging.debug(f"_post_action payload: {payload}")
        try:
            response = await self.info.api.post("/exchange", payload)
            logging.debug(f"_post_action response: {response}")
            return response
        except Exception as e:
            logging.error(f"_post_action failed: {e}", exc_info=True)
            raise

    async def _slippage_price(self, name: str, is_buy: bool, slippage: float, px: Optional[float] = None) -> float:
        try:
            coin = self.info.name_to_coin[name]
            if not px:
                px = float(await self.info.all_mids()[coin])
            asset = self.info.coin_to_asset[coin]
            is_spot = asset >= 10_000
            original_px = px
            px *= (1 + slippage) if is_buy else (1 - slippage)
            decimals = (6 if not is_spot else 8) - self.info.asset_to_sz_decimals[asset]
            price = round(float(f"{px:.5g}"), decimals)
            logging.debug(f"_slippage_price: name={name}, is_buy={is_buy}, slippage={slippage}, original_px={original_px}, adjusted_px={price}")
            return price
        except Exception as e:
            logging.error(f"_slippage_price failed for {name} with error: {e}", exc_info=True)
            raise

    def set_expires_after(self, expires_after: Optional[int]) -> None:
        self.expires_after = expires_after
        logging.debug(f"Expires after set to {expires_after}")

    async def order(
        self,
        name: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        order_type: OrderType,
        reduce_only: bool = False,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        order: OrderRequest = {
            "coin": name,
            "is_buy": is_buy,
            "sz": sz,
            "limit_px": limit_px,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        if cloid:
            order["cloid"] = cloid
        logging.debug(f"Placing single order: {order}")
        try:
            result = await self.bulk_orders([order], builder)
            logging.debug(f"order result: {result}")
            return result
        except Exception as e:
            logging.error(f"order failed: {e}", exc_info=True)
            raise

    async def bulk_orders(self, order_requests: List[OrderRequest], builder: Optional[BuilderInfo] = None) -> Any:
        try:
            order_wires = [
                order_request_to_order_wire(order, self.info.name_to_asset(order["coin"])) for order in order_requests
            ]
            timestamp = get_timestamp_ms()
            if builder:
                builder["b"] = builder["b"].lower()
            order_action = order_wires_to_order_action(order_wires, builder)
            signature = sign_l1_action(
                self.wallet, order_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL
            )
            logging.debug(f"bulk_orders: action={order_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(order_action, signature, timestamp)
            logging.debug(f"bulk_orders result: {result}")
            return result
        except Exception as e:
            logging.error(f"bulk_orders failed: {e}", exc_info=True)
            raise

    async def modify_order(
        self,
        oid: OidOrCloid,
        name: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        order_type: OrderType,
        reduce_only: bool = False,
        cloid: Optional[Cloid] = None,
    ) -> Any:
        modify: ModifyRequest = {
            "oid": oid,
            "order": {
                "coin": name,
                "is_buy": is_buy,
                "sz": sz,
                "limit_px": limit_px,
                "order_type": order_type,
                "reduce_only": reduce_only,
                "cloid": cloid,
            },
        }
        logging.debug(f"Modifying single order: {modify}")
        try:
            result = await self.bulk_modify_orders_new([modify])
            logging.debug(f"modify_order result: {result}")
            return result
        except Exception as e:
            logging.error(f"modify_order failed: {e}", exc_info=True)
            raise

    async def bulk_modify_orders_new(self, modify_requests: List[ModifyRequest]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            modify_wires = [
                {
                    "oid": modify["oid"].to_raw() if isinstance(modify["oid"], Cloid) else modify["oid"],
                    "order": order_request_to_order_wire(modify["order"], self.info.name_to_asset(modify["order"]["coin"])),
                }
                for modify in modify_requests
            ]
            modify_action = {"type": "batchModify", "modifies": modify_wires}
            signature = sign_l1_action(
                self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL
            )
            logging.debug(f"bulk_modify_orders_new: action={modify_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(modify_action, signature, timestamp)
            logging.debug(f"bulk_modify_orders_new result: {result}")
            return result
        except Exception as e:
            logging.error(f"bulk_modify_orders_new failed: {e}", exc_info=True)
            raise

    async def market_open(
        self,
        name: str,
        is_buy: bool,
        sz: float,
        px: Optional[float] = None,
        slippage: float = None,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        if slippage is None:
            slippage = self.DEFAULT_SLIPPAGE
        try:
            px = await self._slippage_price(name, is_buy, slippage, px)
            logging.debug(f"market_open with price {px}")
            result = await self.order(
                name, is_buy, sz, px, order_type={"limit": {"tif": "Ioc"}}, reduce_only=False, cloid=cloid, builder=builder
            )
            logging.debug(f"market_open result: {result}")
            return result
        except Exception as e:
            logging.error(f"market_open failed: {e}", exc_info=True)
            raise

    async def market_close(
        self,
        coin: str,
        sz: Optional[float] = None,
        px: Optional[float] = None,
        slippage: float = None,
        cloid: Optional[Cloid] = None,
        builder: Optional[BuilderInfo] = None,
    ) -> Any:
        if slippage is None:
            slippage = self.DEFAULT_SLIPPAGE
        try:
            address: str = self.wallet.address
            if self.account_address:
                address = self.account_address
            if self.vault_address:
                address = self.vault_address
            logging.debug(f"market_close for coin={coin} on address={address}")
            positions = await self.info.user_state(address)["assetPositions"]
            for position in positions:
                item = position["position"]
                if coin != item["coin"]:
                    continue
                szi = float(item["szi"])
                if not sz:
                    sz = abs(szi)
                is_buy = szi < 0
                px_adj = await self._slippage_price(coin, is_buy, slippage, px)
                logging.debug(f"market_close order details: coin={coin}, is_buy={is_buy}, sz={sz}, px={px_adj}")
                result = await self.order(
                    coin,
                    is_buy,
                    sz,
                    px_adj,
                    order_type={"limit": {"tif": "Ioc"}},
                    reduce_only=True,
                    cloid=cloid,
                    builder=builder,
                )
                logging.debug(f"market_close result: {result}")
                return result
            logging.debug(f"No position found to close for coin={coin}")
        except Exception as e:
            logging.error(f"market_close failed: {e}", exc_info=True)
            raise

    async def cancel(self, name: str, oid: int) -> Any:
        logging.debug(f"cancel order: coin={name}, oid={oid}")
        try:
            result = await self.bulk_cancel([{"coin": name, "oid": oid}])
            logging.debug(f"cancel result: {result}")
            return result
        except Exception as e:
            logging.error(f"cancel failed: {e}", exc_info=True)
            raise

    async def cancel_by_cloid(self, name: str, cloid: Cloid) -> Any:
        logging.debug(f"cancel_by_cloid: coin={name}, cloid={cloid}")
        try:
            result = await self.bulk_cancel_by_cloid([{"coin": name, "cloid": cloid}])
            logging.debug(f"cancel_by_cloid result: {result}")
            return result
        except Exception as e:
            logging.error(f"cancel_by_cloid failed: {e}", exc_info=True)
            raise

    async def bulk_cancel(self, cancel_requests: List[CancelRequest]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            cancel_action = {
                "type": "cancel",
                "cancels": [
                    {
                        "a": self.info.name_to_asset(cancel["coin"]),
                        "o": cancel["oid"],
                    }
                    for cancel in cancel_requests
                ],
            }
            signature = sign_l1_action(
                self.wallet,
                cancel_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            logging.debug(f"bulk_cancel: action={cancel_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(cancel_action, signature, timestamp)
            logging.debug(f"bulk_cancel result: {result}")
            return result
        except Exception as e:
            logging.error(f"bulk_cancel failed: {e}", exc_info=True)
            raise

    async def bulk_cancel_by_cloid(self, cancel_requests: List[CancelByCloidRequest]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            cancel_action = {
                "type": "cancelByCloid",
                "cancels": [
                    {
                        "asset": self.info.name_to_asset(cancel["coin"]),
                        "cloid": cancel["cloid"].to_raw(),
                    }
                    for cancel in cancel_requests
                ],
            }
            signature = sign_l1_action(
                self.wallet,
                cancel_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            logging.debug(f"bulk_cancel_by_cloid: action={cancel_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(cancel_action, signature, timestamp)
            logging.debug(f"bulk_cancel_by_cloid result: {result}")
            return result
        except Exception as e:
            logging.error(f"bulk_cancel_by_cloid failed: {e}", exc_info=True)
            raise

    async def schedule_cancel(self, time: Optional[int]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            schedule_cancel_action: ScheduleCancelAction = {
                "type": "scheduleCancel",
            }
            if time is not None:
                schedule_cancel_action["time"] = time
            signature = sign_l1_action(
                self.wallet,
                schedule_cancel_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            logging.debug(f"schedule_cancel: action={schedule_cancel_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(schedule_cancel_action, signature, timestamp)
            logging.debug(f"schedule_cancel result: {result}")
            return result
        except Exception as e:
            logging.error(f"schedule_cancel failed: {e}", exc_info=True)
            raise

    async def update_leverage(self, leverage: int, name: str, is_cross: bool = True) -> Any:
        try:
            timestamp = get_timestamp_ms()
            update_leverage_action = {
                "type": "updateLeverage",
                "asset": self.info.name_to_asset(name),
                "isCross": is_cross,
                "leverage": leverage,
            }
            signature = sign_l1_action(
                self.wallet,
                update_leverage_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            logging.debug(f"update_leverage: action={update_leverage_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(update_leverage_action, signature, timestamp)
            logging.debug(f"update_leverage result: {result}")
            return result
        except Exception as e:
            logging.error(f"update_leverage failed: {e}", exc_info=True)
            raise

    async def update_isolated_margin(self, amount: float, name: str) -> Any:
        try:
            timestamp = get_timestamp_ms()
            update_margin_action = {
                "type": "updateIsolatedMargin",
                "asset": self.info.name_to_asset(name),
                "amount": float_to_usd_int(amount),
            }
            signature = sign_l1_action(
                self.wallet,
                update_margin_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            logging.debug(f"update_isolated_margin: action={update_margin_action}, signature={signature.hex()}, timestamp={timestamp}")
            result = await self._post_action(update_margin_action, signature, timestamp)
            logging.debug(f"update_isolated_margin result: {result}")
            return result
        except Exception as e:
            logging.error(f"update_isolated_margin failed: {e}", exc_info=True)
            raise

