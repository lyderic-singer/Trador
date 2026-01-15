from eth_account.signers.local import LocalAccount
import time

from .info import Info
from .utils.constants import MAINNET_API_URL
from .utils.signing import OrderType, sign
from .utils.functions import timestamp_ms
from .utils.types import Any, BuilderInfo, Cloid, List, Optional, Tif, Order, ExecutionType, CancelByCloidRequest, CancelRequest, Modify
from log.logger import Logger

class Exchange:
    def __init__(self, url , messenger, wallet: LocalAccount, info: Info, vault_address: str = None, account_address: str = None):
        self.wallet = wallet
        self._log = Logger(self, messenger)
        self.url = url
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

    def get_order(self, x: str | int, is_buy: bool, size: float, price: float, type: ExecutionType, reduce_only: bool = False, cloid: Optional[Cloid] = None) -> Order:
        instrument = self.info.instrument(x)
        order: Order = {
            "a": instrument.asset,
            "b": is_buy,
            "p": instrument.format_price(price),
            "s": instrument.format_size(size),
            "r": reduce_only,
            "t": type
        }
        if cloid:
            order["cloid"] = cloid
        return order

    async def order(self, x: str | int, is_buy: bool, size: float, price: float, type: ExecutionType, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> Any:
        order = self.get_order(x, is_buy, size, price, type, reduce_only, cloid)
        return await self.bulk_orders([order], builder)

    async def limit(self, x: str | int, is_buy: bool, size: float, price: float, tif: Tif, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        type = {"limit": {"tif": tif}}
        return await self.order(x, is_buy, size, price, type, reduce_only, cloid, builder)

    async def market(self, x: str | int, is_buy: bool, size: float, price: float, slippage: float, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        price = price * ((1 + slippage) if is_buy else (1 - slippage))
        type = {"limit": {"tif": "Ioc"}}
        return await self.order(x, is_buy, size, price, type, reduce_only, cloid, builder)
    
    async def trigger(self, x: str | int, is_buy: bool, size: float, trigger_px: float, is_market: bool, tpsl: str, price: Optional[float] = None, reduce_only: bool = True, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        instr = self.info.instrument(x)
        type = {"trigger": {"isMarket": is_market, "triggerPx": instr.format_price(trigger_px), "tpsl": tpsl}}
        px = trigger_px if is_market else price
        return await self.order(x, is_buy, size, px, type, reduce_only, cloid, builder)

    async def bulk_orders(self, orders: List[Order], builder: Optional[BuilderInfo] = None) -> Any:
        try:
            timestamp = timestamp_ms()
            if builder:
                builder["b"] = builder["b"].lower()
            order_action = {
                "type": "order",
                "orders": orders,
                "grouping": "na",
            }
            if builder:
                order_action["builder"] = builder
            signature = sign(self.wallet, order_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
            await self._log.debug(f"Sending {len(orders)} orders")
            return await self.post(order_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Failed bulk order: {e}")
            raise e

    def get_modify(self, order, new_price, new_size, tif):
        type = {"limit": {"tif": tif}}
        modify = {
            "oid": order['oid'],
            "order": self.get_order(order['coin'],  order['side'] == 'B', new_size, new_price, type)
        }
        return modify

    async def modify(self, order, new_price, new_size, tif) -> Any:
        modify = self.get_modify(order, new_price, new_size, tif)
        return await self.bulk_modify_orders([modify])

    async def bulk_modify(self, modifies: List[Modify]) -> Any:
        try:
            timestamp = timestamp_ms()
            modify_action = {"type": "batchModify", "modifies": modifies}
            signature = sign(self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
            await self._log.debug(f"modifying {len(modifies)} orders")
            return await self.post(modify_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Failed bulk modify: {e}")
            raise e

    async def cancel(self, x: str | int, *, oid: int | None = None, cloid: Cloid | None = None) -> Any:
        if (oid is None) == (cloid is None):
            raise ValueError("oid xor cloid")
        instrument = self.info.instrument(x)
        cancel = {"a": instrument.asset, "o": oid} if oid is not None else {"asset": instrument.asset, "cloid": cloid}
        try:
            return await self.bulk_cancel(oid is not None, [cancel])
        except Exception as e:
            await self._log.error(f"Cancel failed: coin={instrument.coin}, oid={oid}, cloid={cloid}, error={e}")
            raise e

    async def bulk_cancel(self, is_oid, cancels: list[dict]) -> Any:
        try:
            timestamp = timestamp_ms()
            t = "cancel" if is_oid else "cancelByCloid"
            cancel_action = {"type": t, "cancels": cancels}
            signature = sign(self.wallet, cancel_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
            return await self.post(cancel_action, signature, timestamp)
        except Exception as e:
            await self._log.error(f"Bulk {t} failed: {e}")
            raise
