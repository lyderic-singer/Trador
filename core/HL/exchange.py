from eth_account.signers.local import LocalAccount
from .info import Info
from .utils.constants import MAINNET_API_URL
from .utils.signing import sign
from .utils.functions import timestamp_ms, get_order, get_modify
from .utils.types import TPSL, Any, BuilderInfo, Cloid, List, Optional, TIF, Order, ExecutionType, Modify, LimitOrderType, TriggerOrderType
from log.logger import Logger
from log.messenger import Messenger

class Exchange:
    def __init__(self, url , messenger: Messenger, wallet: LocalAccount, info: Info, vault_address: str | None = None):
        self.wallet = wallet
        self._log = Logger("HL Exchange", messenger)
        self.url = url
        self.vault_address = vault_address
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
        return await self.info.api.post("/exchange", payload)

    async def order(self, x: str | int, is_buy: bool, size: float, price: float, execution_type: ExecutionType, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> Any:
        instrument = self.info.instrument(x)
        order = get_order(instrument, is_buy, size, price, execution_type, reduce_only, cloid)
        return await self.bulk_orders([order], builder)

    async def limit(self, x: str | int, is_buy: bool, size: float, price: float, tif: TIF, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        execution_type: LimitOrderType = {"limit": {"tif": tif}}
        return await self.order(x, is_buy, size, price, execution_type, reduce_only, cloid, builder)

    async def market(self, x: str | int, is_buy: bool, size: float, price: float, slippage: float, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        price = price * ((1 + slippage) if is_buy else (1 - slippage))
        execution_type: LimitOrderType = {"limit": {"tif": "Ioc"}}
        return await self.order(x, is_buy, size, price, execution_type, reduce_only, cloid, builder)
    
    async def trigger(self, x: str | int, is_buy: bool, size: float, trigger_px: float, is_market: bool, tpsl: TPSL, price: Optional[float] = None, reduce_only: bool = True, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None):
        instrument = self.info.instrument(x)
        execution_type: TriggerOrderType = {"trigger": {"isMarket": is_market, "triggerPx": instrument.format_price(trigger_px), "tpsl": tpsl}}
        px = trigger_px if is_market else price
        return await self.order(x, is_buy, size, px or 0, execution_type, reduce_only, cloid, builder)

    async def bulk_orders(self, orders: List[Order], builder: Optional[BuilderInfo] = None) -> Any:
        timestamp = timestamp_ms()
        if builder:
            builder["b"] = builder["b"].lower()
        order_action = {"type": "order", "orders": orders, "grouping": "na"}
        if builder:
            order_action["builder"] = builder
        signature = sign(self.wallet, order_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        await self._log.debug(f"Sending {len(orders)} orders")
        return await self.post(order_action, signature, timestamp)

    async def modify(self, order, new_price, new_size, tif) -> Any:
        instrument = self.info.instrument(order["coin"])
        modify = get_modify(order, instrument, new_price, new_size, tif)
        return await self.bulk_modify([modify])

    async def bulk_modify(self, modifies: List[Modify]) -> Any:
        timestamp = timestamp_ms()
        modify_action = {"type": "batchModify", "modifies": modifies}
        signature = sign(self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        await self._log.debug(f"modifying {len(modifies)} orders")
        return await self.post(modify_action, signature, timestamp)

    async def cancel(self, x: str | int, *, oid: int | None = None, cloid: Cloid | None = None) -> Any:
        if (oid is None) == (cloid is None):
            raise ValueError("oid xor cloid")
        instrument = self.info.instrument(x)
        cancel = {"a": instrument.asset, "o": oid} if oid is not None else {"asset": instrument.asset, "cloid": cloid}
        return await self.bulk_cancel(oid is not None, [cancel])

    async def bulk_cancel(self, is_oid, cancels: list[dict]) -> Any:
        timestamp = timestamp_ms()
        t = "cancel" if is_oid else "cancelByCloid"
        cancel_action = {"type": t, "cancels": cancels}
        signature = sign(self.wallet, cancel_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        return await self.post(cancel_action, signature, timestamp)
