from eth_account.signers.local import LocalAccount
import time

from .info import Info
from .utils.constants import MAINNET_API_URL
from .utils.signing import CancelByCloidRequest, CancelRequest, ModifyRequest, OrderRequest, OrderType, order_request_to_order_wire, order_wires_to_order_action, sign_l1_action
from .utils.functions import timestamp_ms, format_price
from .utils.types import Any, BuilderInfo, Cloid, List, Optional
from log.logger import Logger

class Exchange:
    def __init__(self, info: Info, messenger, wallet: LocalAccount, vault_address: str = None, account_address: str = None):
        self.wallet = wallet
        self._log = Logger(self, messenger)
        self.base_url = info.base_url
        self.vault_address = vault_address
        self.account_address = account_address
        self.info = info
        self.expires_after: Optional[int] = None

    async def post(self, action, signature, nonce):
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.vault_address if action["type"] != "usdClassTransfer" else None,
            "expiresAfter": self.expires_after,
        }
        try:
            return await self.info.api.post("/exchange", payload)
        except Exception as e:
            await self._log.error(f"Failed to post action: {e}")
            raise e

    def set_expires_after(self, expires_after: Optional[int]) -> None:
        self.expires_after = expires_after

    def get_order(self, name: str, is_buy: bool, sz: float, limit_px: float, order_type: OrderType,
                    reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> OrderRequest:
        order: OrderRequest = {
            "coin": name,
            "is_buy": is_buy,
            "sz": round( sz, self.info.asset_to_sz_decimals[self.info.name_to_asset(name)]),
            "limit_px": self.format_price_spot(limit_px),
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        if cloid:
            order["cloid"] = cloid
        return order

    async def order(self, name: str, is_buy: bool, sz: float, limit_px: float, order_type: OrderType,
                    reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> Any:
        order = self.get_order(name, is_buy, sz, limit_px, order_type, reduce_only, cloid, builder)
        await self._log.debug(f"Placing single order: {order}")
        return await self.bulk_orders([order], builder)

    async def perp_order(self, coin: str, is_buy: bool, size: float, price: float) -> bool:
        size = round(size, self.info.sz_decimals[coin])
        price = self.format_price_perp(price)
        return await self.order(coin, is_buy, size, price, {"limit": {"tif": "Alo"}})

    async def spot_order(self, name: str, is_buy: bool, size: float, price: float) -> bool:
        coin = self.info.name_to_coin[name]
        price = format_price_spot(price)
        size = round( size, self.info.asset_to_sz_decimals[self.info.name_to_asset(name)])
        return await self.order( coin, is_buy, size, price, {"limit": {"tif": "Alo"}})

    async def bulk_orders(self, order_requests: List[OrderRequest], builder: Optional[BuilderInfo] = None) -> Any:
        try:
            order_wires = [order_request_to_order_wire(order, self.info.name_to_asset(order["coin"])) for order in order_requests]
            timestamp = timestamp_ms()
            if builder:
                builder["b"] = builder["b"].lower()
            order_action = order_wires_to_order_action(order_wires, builder)
            signature = sign_l1_action(self.wallet, order_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            await self._log.debug(f"Sending bulk orders: {order_requests}")
            return await self._post_action(order_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Failed bulk order: {e}")
            raise
    
    def get_modifyRequest(self, order, new_price, new_size):
        modifyrequest: ModifyRequest = {
            "oid": order['oid'],
            "order": {
                "coin": order['coin'],
                "is_buy": False if order['side'] == 'A' else True,
                "sz": round( new_size, self.info.asset_to_sz_decimals[self.info.name_to_asset(order['coin'])]),
                "limit_px": format_price_spot(new_price),
                "order_type": {"limit": {"tif": "Alo"}},
                "reduce_only": False,
                "cloid": None, },}
        return modifyrequest

    async def modify_order(self, order, new_price, new_size) -> Any:
        modifyrequest = self.get_modifyRequest(order, new_price, new_size)
        await self._log.debug(f"Modifying order: {modifyrequest}")
        return await self.bulk_modify_orders([modifyrequest])

    async def bulk_modify_orders(self, modify_requests: List[ModifyRequest]) -> Any:
        try: 
            timestamp = timestamp_ms()
            modify_wires = [{
                "oid": modify["oid"].to_raw() if isinstance(modify["oid"], Cloid) else modify["oid"],
                "order": order_request_to_order_wire(modify["order"], self.info.name_to_asset(modify["order"]["coin"]))
            } for modify in modify_requests]
            modify_action = {"type": "batchModify", "modifies": modify_wires}
            signature = sign_l1_action(self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            await self._log.debug(f"Bulk modifying orders: {modify_requests}")
            return await self._post_action(modify_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Failed bulk modify: {e}")
            raise

    async def cancel(self, name: str, oid: int) -> Any:
        try:
            await self._log.debug(f"Cancel order: coin={name}, oid={oid}")
            return await self.bulk_cancel([{"coin": name, "oid": oid}])
        except Exception as e:
            await self._log.error(f"Failed to cancel order: coin={name}, oid={oid}, error={e}")
            raise

    async def cancel_by_cloid(self, name: str, cloid: Cloid) -> Any:
        try:
            await self._log.debug(f"Cancel order by cloid: coin={name}, cloid={cloid}")
            return await self.bulk_cancel_by_cloid([{"coin": name, "cloid": cloid}])
        except Exception as e:
            await self._log.error(f"Failed to cancel by cloid: coin={name}, cloid={cloid}, error={e}")
            raise

    async def bulk_cancel(self, cancel_requests: List[CancelRequest]) -> Any:
        try:
            timestamp = timestamp_ms()
            await self._log.debug(f"Bulk cancel requests: {cancel_requests} at {timestamp}")
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
            return await self._post_action(cancel_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Bulk cancel failed: {e}")
            raise

    async def bulk_cancel_by_cloid(self, cancel_requests: List[CancelByCloidRequest]) -> Any:
        try:
            timestamp = timestamp_ms()
            await self._log.debug(f"Bulk cancel by cloid requests: {cancel_requests} at {timestamp}")
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
            return await self._post_action(cancel_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Bulk cancel by cloid failed: {e}")
            raise
