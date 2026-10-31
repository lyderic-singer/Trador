import time
from eth_account.signers.local import LocalAccount
from core.latency import LATENCY
from core.HL.info import HLInfo
from core.HL.utils.constants import MAINNET_API_URL
from core.HL.utils.signing import sign, sign_send_asset_action
from core.HL.utils.functions import get_cancel, timestamp_ms, get_order, get_modify, get_transfer
from core.HL.utils.types import TPSL, Any, BuilderInfo, Cancel, Cloid, List, Optional, TIF, Order, ExecutionType, Modify, LimitOrderType, TriggerOrderType
from log.logger import Logger
from log.messenger import Messenger

class HLExchange:
    def __init__(self, url , messenger: Messenger, wallet: LocalAccount, info: HLInfo, vault_address: str | None = None):
        self.wallet = wallet
        self._log = Logger("HL Exchange", messenger)
        self.url = url
        self.vault_address = vault_address
        self.info = info
        self.expires_after: Optional[int] = None
        self.last_nonce = 0
        # actions sent over the already open websocket ("post"): no HTTPS round trip; HTTP when the socket is down
        self.transport = "ws"
        self.ws = None          # HLWebSocket, wired by the client
        self.timeout = 5.0

    def nonce(self) -> int:
        # HL rejects a reused nonce: two actions signed in the same millisecond (concurrent strategies) must differ
        self.last_nonce = max(timestamp_ms(), self.last_nonce + 1)
        return self.last_nonce

    async def post(self, action, signature, nonce):
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.vault_address if action["type"] != "usdClassTransfer" else None,
            "expiresAfter": self.expires_after,
        }
        kind = action.get("type", "?")
        items = len(action.get("orders") or action.get("cancels") or action.get("modifies") or [None])
        ws = getattr(self, "ws", None)
        t = time.perf_counter()
        if getattr(self, "transport", "rest") == "ws" and ws is not None and ws.connected.is_set():
            try:
                response = await ws.post({"type": "action", "payload": payload}, timeout=getattr(self, "timeout", 5.0))
            except Exception as e:
                LATENCY.record("hyperliquid", kind, "ws", (time.perf_counter() - t) * 1000, False, items, type(e).__name__)
                raise
            ms = (time.perf_counter() - t) * 1000
            if response.get("type") == "error":
                LATENCY.record("hyperliquid", kind, "ws", ms, False, items, str(response.get("payload"))[:60])
                return {"status": "err", "response": response.get("payload")}
            out = response.get("payload")
            LATENCY.record("hyperliquid", kind, "ws", ms, isinstance(out, dict) and out.get("status") == "ok", items)
            return out
        try:
            out = await self.info.api.post("/exchange", payload)
        except Exception as e:
            LATENCY.record("hyperliquid", kind, "rest", (time.perf_counter() - t) * 1000, False, items, type(e).__name__)
            raise
        LATENCY.record("hyperliquid", kind, "rest", (time.perf_counter() - t) * 1000,
                       isinstance(out, dict) and out.get("status") == "ok", items)
        return out

    async def order(self, x: str | int, is_buy: bool, size: float, price: float, execution_type: ExecutionType, reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> Any:
        instrument = self.info.instrument(x)
        order = get_order(instrument, is_buy, size, price, execution_type, reduce_only, cloid)
        return await self.batch_order([order], builder)

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

    async def batch_order(self, orders: List[Order], builder: Optional[BuilderInfo] = None) -> Any:
        timestamp = self.nonce()
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
        return await self.batch_modify([modify])

    async def batch_modify(self, modifies: List[Modify]) -> Any:
        timestamp = self.nonce()
        modify_action = {"type": "batchModify", "modifies": modifies}
        signature = sign(self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        await self._log.debug(f"modifying {len(modifies)} orders")
        return await self.post(modify_action, signature, timestamp)

    async def cancel(self, x: str | int, oid: int | None = None, cloid: Cloid | None = None) -> Any:
        instrument = self.info.instrument(x)
        cancel = get_cancel(instrument, oid, cloid)
        return await self.batch_cancel(oid is not None, [cancel])

    async def batch_cancel(self, is_oid, cancels: list[Cancel]) -> Any:
        timestamp = self.nonce()
        t = "cancel" if is_oid else "cancelByCloid"
        cancel_action = {"type": t, "cancels": cancels}
        signature = sign(self.wallet, cancel_action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        return await self.post(cancel_action, signature, timestamp)

    async def action(self, action: dict) -> Any:
        """any L1 action (signed like an order): userOutcome (split / merge), reserveRequestWeight, scheduleCancel, noop"""
        timestamp = self.nonce()
        signature = sign(self.wallet, action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        return await self.post(action, signature, timestamp)

    async def update_leverage(self, asset: int, leverage: int, is_cross: bool = True) -> Any:
        timestamp = self.nonce()
        action = {"type": "updateLeverage", "asset": asset, "isCross": is_cross, "leverage": int(leverage)}
        signature = sign(self.wallet, action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        return await self.post(action, signature, timestamp)

    async def update_isolated_margin(self, asset: int, amount: float, is_buy: bool = True) -> Any:
        # adds (amount > 0) or removes (amount < 0) USDC on an isolated position; ntli = amount x 1e6
        timestamp = self.nonce()
        action = {"type": "updateIsolatedMargin", "asset": asset, "isBuy": is_buy, "ntli": int(round(amount * 1_000_000))}
        signature = sign(self.wallet, action, self.vault_address, timestamp, self.expires_after, self.url == MAINNET_API_URL)
        return await self.post(action, signature, timestamp)

    async def transfer(self, token: str, amount: float, source_dex: str, destination_dex: str, destination_address: str, from_subaccount: str = "") -> Any:
        # use token: "USDC:0x6d1e7cde53ba9467b783cb7c530ce054" for USDC on Mainnet
        timestamp = self.nonce()
        action = get_transfer(token, amount, source_dex, destination_dex, destination_address, nonce=timestamp, from_subaccount=from_subaccount, mainnet= self.url == MAINNET_API_URL)
        signature = sign_send_asset_action(self.wallet, action, self.url == MAINNET_API_URL,)
        return await self.post(action, signature, timestamp)
