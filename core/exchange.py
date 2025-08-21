import json
import logging
import secrets

import eth_account
from eth_account.signers.local import LocalAccount

from .info import Info
from .utils.constants import MAINNET_API_URL
from .utils.signing import CancelByCloidRequest, CancelRequest, ModifyRequest, OidOrCloid, OrderRequest, OrderType, ScheduleCancelAction, float_to_usd_int, get_timestamp_ms, order_request_to_order_wire, order_wires_to_order_action, sign_agent, sign_approve_builder_fee, sign_convert_to_multi_sig_user_action, sign_l1_action, sign_multi_sig_action, sign_perp_dex_class_transfer_action, sign_spot_transfer_action, sign_token_delegate_action, sign_usd_class_transfer_action, sign_usd_transfer_action, sign_withdraw_from_bridge_action
from .utils.types import Any, BuilderInfo, Cloid, Dict, List, Optional, PerpDexSchemaInput, Tuple
from logs.message_manager import Logger

max_decimal_perp = 6
max_decimal_spot = 5

def format_price(px, max_decimal):
    if px > 100_000:
        return round(px)
    px = round(float(f"{px:.5g}"), max_decimal)
    return px

def format_price_perp(px):
    return format_price(px, max_decimal_perp)

def format_price_spot(px):
    return format_price(px, max_decimal_spot)

class Exchange:
    DEFAULT_SLIPPAGE = 0.05
    def __init__(self, info: Info, message_manager, wallet: LocalAccount, vault_address: str = None, account_address: str = None):
        self.wallet = wallet
        self._logging = Logger(self, message_manager)
        self.base_url = info.base_url
        self.vault_address = vault_address
        self.account_address = account_address
        self.info = info
        self.expires_after: Optional[int] = None

    async def _post_action(self, action, signature, nonce):
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.vault_address if action["type"] != "usdClassTransfer" else None,
            "expiresAfter": self.expires_after,
        }
        await self._logging.debug(f"Posting action: {payload}")
        try:
            return await self.info.api.post("/exchange", payload)
        except Exception as e:
            await self._logging.error(f"Failed to post action: {e}")
            raise

    async def _slippage_price(self, name: str, is_buy: bool, slippage: float, px: Optional[float] = None) -> float:
        try:
            coin = self.info.name_to_coin[name]
            if not px:
                px = float(await self.info.all_mids()[coin])
            asset = self.info.coin_to_asset[coin]
            is_spot = asset >= 10_000
            px *= (1 + slippage) if is_buy else (1 - slippage)
            result = round(float(f"{px:.5g}"), (6 if not is_spot else 8) - self.info.asset_to_sz_decimals[asset])
            await self._logging.debug(f"Calculated slippage price: {result}")
            return result
        except Exception as e:
            await self._logging.error(f"Error in slippage price calc: {e}")
            raise

    def set_expires_after(self, expires_after: Optional[int]) -> None:
        self.expires_after = expires_after

    def get_order(self, name: str, is_buy: bool, sz: float, limit_px: float, order_type: OrderType,
                    reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> OrderRequest:
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
        return order

    async def order(self, name: str, is_buy: bool, sz: float, limit_px: float, order_type: OrderType,
                    reduce_only: bool = False, cloid: Optional[Cloid] = None, builder: Optional[BuilderInfo] = None) -> Any:
        order = self.get_order(name, is_buy, sz, limit_px, order_type, reduce_only, cloid, builder)
        await self._logging.debug(f"Placing single order: {order}")
        return await self.bulk_orders([order], builder)

    async def perp_order(self, coin: str, is_buy: bool, size: float, price: float) -> bool:
        size = round(size, self.info.sz_decimals[coin])
        price = format_price_perp(price)
        return await self.order(coin, is_buy, size, price, {"limit": {"tif": "Alo"}})

    async def spot_order(self, name: str, is_buy: bool, size: float, price: float) -> bool:
        coin = self.info.name_to_coin[name]
        price = format_price_spot(price)
        size = round( size, self.info.asset_to_sz_decimals[self.info.name_to_asset(name)])
        return await self.order( coin, is_buy, size, price, {"limit": {"tif": "Alo"}})

    async def bulk_orders(self, order_requests: List[OrderRequest], builder: Optional[BuilderInfo] = None) -> Any:
        try:
            order_wires = [order_request_to_order_wire(order, self.info.name_to_asset(order["coin"])) for order in order_requests]
            timestamp = get_timestamp_ms()
            if builder:
                builder["b"] = builder["b"].lower()
            order_action = order_wires_to_order_action(order_wires, builder)
            signature = sign_l1_action(self.wallet, order_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            await self._logging.debug(f"Sending bulk orders: {order_requests}")
            return await self._post_action(order_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Failed bulk order: {e}")
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
        await self._logging.debug(f"Modifying order: {modifyrequest}")
        return await self.bulk_modify_order([modifyrequest])

    async def bulk_modify_order(self, modify_requests: List[ModifyRequest]) -> Any:
        try: 
            timestamp = get_timestamp_ms()
            modify_wires = [{
                "oid": modify["oid"].to_raw() if isinstance(modify["oid"], Cloid) else modify["oid"],
                "order": order_request_to_order_wire(modify["order"], self.info.name_to_asset(modify["order"]["coin"]))
            } for modify in modify_requests]
            modify_action = {"type": "batchModify", "modifies": modify_wires}
            signature = sign_l1_action(self.wallet, modify_action, self.vault_address, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            await self._logging.debug(f"Bulk modifying orders: {modify_requests}")
            return await self._post_action(modify_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Failed bulk modify: {e}")
            raise

    async def market_open(self, name: str, is_buy: bool, sz: float, slippage: float = DEFAULT_SLIPPAGE, cloid: Optional[Cloid] = None) -> Any:
        try:
            limit_px = await self._slippage_price(name, is_buy, slippage)
            return await self.order(name, is_buy, sz, limit_px, "ioc", cloid=cloid)
        except Exception as e:
            await self._logging.error(f"Failed market open: {e}")
            raise

    async def market_close(self, name: str, is_buy: bool, slippage: float = DEFAULT_SLIPPAGE, cloid: Optional[Cloid] = None) -> Any:
        try:
            pos = await self.info.position(name)
            side_sz = pos["szBuy"] if is_buy else pos["szSell"]
            sz = float(side_sz)
            if sz == 0:
                await self._logging.warning(f"No position to close for {name}, is_buy={is_buy}")
                return None
            limit_px = await self._slippage_price(name, not is_buy, slippage)
            return await self.order(name, not is_buy, sz, limit_px, "ioc", reduce_only=True, cloid=cloid)
        except Exception as e:
            await self._logging.error(f"Failed market close: {e}")
            raise

    async def cancel(self, name: str, oid: int) -> Any:
        try:
            await self._logging.debug(f"Cancel order: coin={name}, oid={oid}")
            return await self.bulk_cancel([{"coin": name, "oid": oid}])
        except Exception as e:
            await self._logging.error(f"Failed to cancel order: coin={name}, oid={oid}, error={e}")
            raise

    async def cancel_by_cloid(self, name: str, cloid: Cloid) -> Any:
        try:
            await self._logging.debug(f"Cancel order by cloid: coin={name}, cloid={cloid}")
            return await self.bulk_cancel_by_cloid([{"coin": name, "cloid": cloid}])
        except Exception as e:
            await self._logging.error(f"Failed to cancel by cloid: coin={name}, cloid={cloid}, error={e}")
            raise

    async def bulk_cancel(self, cancel_requests: List[CancelRequest]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Bulk cancel requests: {cancel_requests} at {timestamp}")
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
            await self._logging.error(f"Bulk cancel failed: {e}")
            raise

    async def bulk_cancel_by_cloid(self, cancel_requests: List[CancelByCloidRequest]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Bulk cancel by cloid requests: {cancel_requests} at {timestamp}")
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
            await self._logging.error(f"Bulk cancel by cloid failed: {e}")
            raise

    async def schedule_cancel(self, time: Optional[int]) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Schedule cancel at time: {time} with timestamp {timestamp}")
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
            return await self._post_action(schedule_cancel_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Schedule cancel failed: {e}")
            raise

    async def update_leverage(self, leverage: int, name: str, is_cross: bool = True) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Update leverage: asset={name}, leverage={leverage}, is_cross={is_cross}")
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
            return await self._post_action(update_leverage_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Update leverage failed: {e}")
            raise

    async def update_isolated_margin(self, amount: float, name: str) -> Any:
        try:
            timestamp = get_timestamp_ms()
            amount_int = float_to_usd_int(amount)
            await self._logging.debug(f"Update isolated margin: asset={name}, amount={amount} (int={amount_int})")
            update_isolated_margin_action = {
                "type": "updateIsolatedMargin",
                "asset": self.info.name_to_asset(name),
                "isBuy": True,
                "ntli": amount_int,
            }
            signature = sign_l1_action(
                self.wallet,
                update_isolated_margin_action,
                self.vault_address,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            return await self._post_action(update_isolated_margin_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Update isolated margin failed: {e}")
            raise

    async def set_referrer(self, code: str) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Set referrer code: {code}")
            set_referrer_action = {
                "type": "setReferrer",
                "code": code,
            }
            signature = sign_l1_action(
                self.wallet,
                set_referrer_action,
                None,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            return await self._post_action(set_referrer_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Set referrer failed: {e}")
            raise

    async def create_sub_account(self, name: str) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Create sub account: {name}")
            create_sub_account_action = {
                "type": "createSubAccount",
                "name": name,
            }
            signature = sign_l1_action(
                self.wallet,
                create_sub_account_action,
                None,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            return await self._post_action(create_sub_account_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Create sub account failed: {e}")
            raise

    async def usd_class_transfer(self, amount: float, to_perp: bool) -> Any:
        try:
            timestamp = get_timestamp_ms()
            str_amount = str(amount)
            if self.vault_address:
                str_amount += f" subaccount:{self.vault_address}"
            await self._logging.debug(f"USD class transfer: amount={str_amount}, to_perp={to_perp}")
            action = {
                "type": "usdClassTransfer",
                "amount": str_amount,
                "toPerp": to_perp,
                "nonce": timestamp,
            }
            signature = sign_usd_class_transfer_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"USD class transfer failed: {e}")
            raise

    async def perp_dex_class_transfer(self, dex: str, token: str, amount: float, to_perp: bool) -> Any:
        try:
            timestamp = get_timestamp_ms()
            str_amount = str(amount)
            if self.vault_address:
                str_amount += f" subaccount:{self.vault_address}"
            await self._logging.debug(f"PerpDex class transfer: dex={dex}, token={token}, amount={str_amount}, to_perp={to_perp}")
            action = {
                "type": "PerpDexClassTransfer",
                "dex": dex,
                "token": token,
                "amount": str_amount,
                "toPerp": to_perp,
                "nonce": timestamp,
            }
            signature = sign_perp_dex_class_transfer_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"PerpDex class transfer failed: {e}")
            raise

    async def sub_account_transfer(self, sub_account_user: str, is_deposit: bool, usd: int) -> Any:
        try:
            timestamp = get_timestamp_ms()
            await self._logging.debug(f"Sub account transfer: user={sub_account_user}, is_deposit={is_deposit}, usd={usd}")
            sub_account_transfer_action = {
                "type": "subAccountTransfer",
                "subAccountUser": sub_account_user,
                "isDeposit": is_deposit,
                "usd": usd,
            }
            signature = sign_l1_action(
                self.wallet,
                sub_account_transfer_action,
                None,
                timestamp,
                self.expires_after,
                self.base_url == MAINNET_API_URL,
            )
            return await self._post_action(sub_account_transfer_action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"Sub account transfer failed: {e}")
            raise

    async def sub_account_spot_transfer(self, sub_account_user: str, is_deposit: bool, token: str, amount: float) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "subAccountSpotTransfer",
            "subAccountUser": sub_account_user,
            "isDeposit": is_deposit,
            "token": token,
            "amount": str(amount),
        }
        try:
            await self._logging.debug(f"sub_account_spot_transfer: {action}")
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"sub_account_spot_transfer failed: {e}")
            raise

    async def vault_usd_transfer(self, vault_address: str, is_deposit: bool, usd: int) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "vaultTransfer",
            "vaultAddress": vault_address,
            "isDeposit": is_deposit,
            "usd": usd,
        }
        try:
            await self._logging.debug(f"vault_usd_transfer: {action}")
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"vault_usd_transfer failed: {e}")
            raise

    async def usd_transfer(self, amount: float, destination: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {"destination": destination, "amount": str(amount), "time": timestamp, "type": "usdSend"}
        try:
            await self._logging.debug(f"usd_transfer: {action}")
            signature = sign_usd_transfer_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"usd_transfer failed: {e}")
            raise

    async def spot_transfer(self, amount: float, destination: str, token: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "destination": destination,
            "amount": str(amount),
            "token": token,
            "time": timestamp,
            "type": "spotSend",
        }
        try:
            await self._logging.debug(f"spot_transfer: {action}")
            signature = sign_spot_transfer_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"spot_transfer failed: {e}")
            raise

    async def token_delegate(self, validator: str, wei: int, is_undelegate: bool) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "validator": validator,
            "wei": wei,
            "isUndelegate": is_undelegate,
            "nonce": timestamp,
            "type": "tokenDelegate",
        }
        try:
            await self._logging.debug(f"token_delegate: {action}")
            signature = sign_token_delegate_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"token_delegate failed: {e}")
            raise

    async def withdraw_from_bridge(self, amount: float, destination: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {"destination": destination, "amount": str(amount), "time": timestamp, "type": "withdraw3"}
        try:
            await self._logging.debug(f"withdraw_from_bridge: {action}")
            signature = sign_withdraw_from_bridge_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"withdraw_from_bridge failed: {e}")
            raise

    async def approve_agent(self, name: Optional[str] = None) -> Tuple[Any, str]:
        agent_key = "0x" + secrets.token_hex(32)
        account = eth_account.Account.from_key(agent_key)
        timestamp = get_timestamp_ms()
        action = {
            "type": "approveAgent",
            "agentAddress": account.address,
            "agentName": name or "",
            "nonce": timestamp,
        }
        try:
            if name is None:
                del action["agentName"]
            await self._logging.debug(f"approve_agent: {action}")
            signature = sign_agent(self.wallet, action, self.base_url == MAINNET_API_URL)
            result = await self._post_action(action, signature, timestamp)
            return result, agent_key
        except Exception as e:
            await self._logging.error(f"approve_agent failed: {e}")
            raise

    async def approve_builder_fee(self, builder: str, max_fee_rate: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {"maxFeeRate": max_fee_rate, "builder": builder, "nonce": timestamp, "type": "approveBuilderFee"}
        try:
            await self._logging.debug(f"approve_builder_fee: {action}")
            signature = sign_approve_builder_fee(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"approve_builder_fee failed: {e}")
            raise

    async def convert_to_multi_sig_user(self, authorized_users: List[str], threshold: int) -> Any:
        timestamp = get_timestamp_ms()
        authorized_users = sorted(authorized_users)
        signers = {"authorizedUsers": authorized_users, "threshold": threshold}
        action = {
            "type": "convertToMultiSigUser",
            "signers": json.dumps(signers),
            "nonce": timestamp,
        }
        try:
            await self._logging.debug(f"convert_to_multi_sig_user: {action}")
            signature = sign_convert_to_multi_sig_user_action(self.wallet, action, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"convert_to_multi_sig_user failed: {e}")
            raise

    async def spot_deploy_register_token(self, token_name: str, sz_decimals: int, wei_decimals: int, max_gas: int, full_name: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "spotDeploy",
            "registerToken2": {
                "spec": {"name": token_name, "szDecimals": sz_decimals, "weiDecimals": wei_decimals},
                "maxGas": max_gas,
                "fullName": full_name,
            },
        }
        try:
            await self._logging.debug(f"spot_deploy_register_token: {action}")
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"spot_deploy_register_token failed: {e}")
            raise

    async def spot_deploy_user_genesis(self, token: int, user_and_wei: List[Tuple[str, str]], existing_token_and_wei: List[Tuple[int, str]]) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "spotDeploy",
            "userGenesis": {
                "token": token,
                "userAndWei": [(user.lower(), wei) for (user, wei) in user_and_wei],
                "existingTokenAndWei": existing_token_and_wei,
            },
        }
        try:
            await self._logging.debug(f"spot_deploy_user_genesis: {action}")
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"spot_deploy_user_genesis failed: {e}")
            raise

    async def spot_deploy_enable_freeze_privilege(self, token: int) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "spotDeploy",
            "enableFreezePrivilege": {"token": token},
        }
        try:
            await self._logging.debug(f"spot_deploy_enable_freeze_privilege: {action}")
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"spot_deploy_enable_freeze_privilege failed: {e}")
            raise

    async def spot_deploy_freeze_user(self, token: int, user: str, freeze: bool) -> Any:
        timestamp = get_timestamp_ms()
        action = {"type": "spotDeploy", "freezeUser": {"token": token, "user": user.lower(), "freeze": freeze}}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.freezeUser sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.freezeUser failed: {e}")
            return None

    async def spot_deploy_revoke_freeze_privilege(self, token: int) -> Any:
        timestamp = get_timestamp_ms()
        action = {"type": "spotDeploy", "revokeFreezePrivilege": {"token": token}}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.revokeFreezePrivilege sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.revokeFreezePrivilege failed: {e}")
            return None

    async def spot_deploy_genesis(self, token: int, max_supply: str, no_hyperliquidity: bool) -> Any:
        timestamp = get_timestamp_ms()
        genesis = {"token": token, "maxSupply": max_supply}
        if no_hyperliquidity:
            genesis["noHyperliquidity"] = True
        action = {"type": "spotDeploy", "genesis": genesis}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.genesis sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.genesis failed: {e}")
            return None

    async def spot_deploy_register_spot(self, base_token: int, quote_token: int) -> Any:
        timestamp = get_timestamp_ms()
        action = {"type": "spotDeploy", "registerSpot": {"tokens": [base_token, quote_token]}}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.registerSpot sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.registerSpot failed: {e}")
            return None

    async def spot_deploy_register_hyperliquidity(self, spot: int, start_px: float, order_sz: float, n_orders: int, n_seeded_levels: Optional[int]) -> Any:
        timestamp = get_timestamp_ms()
        h = {"spot": spot, "startPx": str(start_px), "orderSz": str(order_sz), "nOrders": n_orders}
        if n_seeded_levels is not None:
            h["nSeededLevels"] = n_seeded_levels
        action = {"type": "spotDeploy", "registerHyperliquidity": h}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.registerHyperliquidity sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.registerHyperliquidity failed: {e}")
            return None

    async def spot_deploy_set_deployer_trading_fee_share(self, token: int, share: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {"type": "spotDeploy", "setDeployerTradingFeeShare": {"token": token, "share": share}}
        await self._logging.debug(f"{timestamp=} {action=}")
        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after, self.base_url == MAINNET_API_URL)
            res = await self._post_action(action, signature, timestamp)
            await self._logging.info("spotDeploy.setDeployerTradingFeeShare sent")
            return res
        except Exception as e:
            await self._logging.error(f"spotDeploy.setDeployerTradingFeeShare failed: {e}")
            return None

    async def perp_deploy_register_asset(self, dex: str, max_gas: Optional[int], coin: str, sz_decimals: int, oracle_px: str, margin_table_id: int, only_isolated: bool, schema: Optional[PerpDexSchemaInput]) -> Any:
        timestamp = get_timestamp_ms()
        schema_wire = {
            "fullName": schema["fullName"],
            "collateralToken": schema["collateralToken"],
            "oracleUpdater": schema["oracleUpdater"].lower() if schema["oracleUpdater"] else None,
        } if schema else None

        action = {
            "type": "perpDeploy",
            "registerAsset": {
                "maxGas": max_gas,
                "assetRequest": {
                    "coin": coin,
                    "szDecimals": sz_decimals,
                    "oraclePx": oracle_px,
                    "marginTableId": margin_table_id,
                    "onlyIsolated": only_isolated,
                },
                "dex": dex,
                "schema": schema_wire,
            },
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"perp_deploy_register_asset failed: {e}")
            raise

    async def perp_deploy_set_oracle(self, dex: str, oracle_pxs: Dict[str, str], mark_pxs: Optional[Dict[str, str]]) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "perpDeploy",
            "setOracle": {
                "dex": dex,
                "oraclePxs": sorted(oracle_pxs.items()),
                "markPxs": sorted(mark_pxs.items()) if mark_pxs else None,
            },
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"perp_deploy_set_oracle failed: {e}")
            raise

    async def c_signer_unjail_self(self) -> Any:
        return await self.c_signer_inner("unjailSelf")

    async def c_signer_jail_self(self) -> Any:
        return await self.c_signer_inner("jailSelf")

    async def c_signer_inner(self, variant: str) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "CSignerAction",
            variant: None,
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"c_signer_inner({variant}) failed: {e}")
            raise

    async def c_validator_register(self, node_ip: str, name: str, description: str, delegations_disabled: bool,
                                commission_bps: int, signer: str, unjailed: bool, initial_wei: int) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "CValidatorAction",
            "register": {
                "profile": {
                    "node_ip": {"Ip": node_ip},
                    "name": name,
                    "description": description,
                    "delegations_disabled": delegations_disabled,
                    "commission_bps": commission_bps,
                    "signer": signer,
                },
                "unjailed": unjailed,
                "initial_wei": initial_wei,
            },
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"c_validator_register failed: {e}")
            raise

    async def c_validator_change_profile(self, node_ip: Optional[str], name: Optional[str], description: Optional[str],
                                        unjailed: bool, disable_delegations: Optional[bool],
                                        commission_bps: Optional[int], signer: Optional[str]) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "CValidatorAction",
            "changeProfile": {
                "node_ip": {"Ip": node_ip} if node_ip else None,
                "name": name,
                "description": description,
                "unjailed": unjailed,
                "disable_delegations": disable_delegations,
                "commission_bps": commission_bps,
                "signer": signer,
            },
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"c_validator_change_profile failed: {e}")
            raise

    async def c_validator_unregister(self) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "CValidatorAction",
            "unregister": None,
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"c_validator_unregister failed: {e}")
            raise

    async def multi_sig(self, multi_sig_user, inner_action, signatures, nonce, vault_address=None):
        multi_sig_user = multi_sig_user.lower()
        action = {
            "type": "multiSig",
            "signatureChainId": "0x66eee",
            "signatures": signatures,
            "payload": {
                "multiSigUser": multi_sig_user,
                "outerSigner": self.wallet.address.lower(),
                "action": inner_action,
            },
        }

        try:
            signature = sign_multi_sig_action(
                self.wallet, action, self.base_url == MAINNET_API_URL,
                vault_address, nonce, self.expires_after,
            )
            return await self._post_action(action, signature, nonce)
        except Exception as e:
            await self._logging.error(f"multi_sig failed: {e}")
            raise

    async def use_big_blocks(self, enable: bool) -> Any:
        timestamp = get_timestamp_ms()
        action = {
            "type": "evmUserModify",
            "usingBigBlocks": enable,
        }

        try:
            signature = sign_l1_action(self.wallet, action, None, timestamp, self.expires_after,
                                    self.base_url == MAINNET_API_URL)
            return await self._post_action(action, signature, timestamp)
        except Exception as e:
            await self._logging.error(f"use_big_blocks({enable}) failed: {e}")
            raise
