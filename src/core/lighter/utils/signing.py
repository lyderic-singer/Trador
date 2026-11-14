"""Signature of Lighter transactions.

Lighter does not sign with an Ethereum key: an API key is a key pair on its own curve (Schnorr over ECgFp5, Poseidon2
hash), and the only reference implementation is the Go signer of the exchange, shipped compiled inside the official
Python SDK (`lighter-sdk`, a ctypes shared library). The engine only needs what is below, so everything that depends on
the SDK lives in `SdkSigner`; the rest of core/lighter (REST, websocket, nonces) is ours and never imports it. The SDK
is not a dependency of the project: `uv add lighter-sdk` on the machines that trade on Lighter (see lighter.md).
"""
import asyncio
import time
from typing import Protocol
from core.lighter.utils.constants import (ORDER_LIMIT, TX_CANCEL_ALL_ORDERS, TX_CANCEL_ORDER, TX_CREATE_ORDER,
                                          TX_MODIFY_ORDER, TX_UPDATE_LEVERAGE, TX_UPDATE_MARGIN)
from core.lighter.utils.types import Tx

INSTALL = ("the Lighter exchange needs the official signer: run `uv add lighter-sdk` (1.1.8 or later) at the project "
           "root, commit pyproject.toml and uv.lock, then restart")


class Signer(Protocol):
    def create_order(self, key: int, nonce: int, market: int, client: int, base: int, price: int, is_ask: bool,
                     tif: int, reduce: bool, expiry: int) -> Tx: ...

    def modify_order(self, key: int, nonce: int, market: int, index: int, base: int, price: int) -> Tx: ...

    def cancel_order(self, key: int, nonce: int, market: int, index: int) -> Tx: ...

    def cancel_all(self, key: int, nonce: int, tif: int, at_ms: int) -> Tx: ...

    def update_leverage(self, key: int, nonce: int, market: int, fraction: int, mode: int) -> Tx: ...

    def update_margin(self, key: int, nonce: int, market: int, usdc: int, direction: int) -> Tx: ...

    def auth(self, key: int, ttl: int) -> str: ...

    def check(self) -> str | None: ...


class SigningError(Exception):
    pass


class SdkSigner:
    """the Go signer of `lighter-sdk`, through the sync `sign_*` methods of its SignerClient (explicit nonce and api
    key: the SDK's own nonce manager and HTTP client are never used on the hot path)"""

    def __init__(self, url: str, account: int, keys: dict[int, str]):
        try:
            import lighter
        except ImportError as e:
            raise RuntimeError(INSTALL) from e
        self.account = account
        self.keys = {int(k): (v[2:] if v.startswith("0x") else v) for k, v in keys.items()}
        # the constructor loads the shared library and registers each key in it (it may read the nonces over HTTP)
        self.client = lighter.SignerClient(url=url, account_index=account, api_private_keys=dict(self.keys))

    @classmethod
    async def create(cls, url: str, account: int, keys: dict[int, str]) -> "SdkSigner":
        """built inside the running event loop (the SDK creates its own HTTP client there); it may block for the time
        of one HTTP read per key: only at start"""
        return cls(url, account, keys)

    @staticmethod
    def tx(out, kind: int, client: int | None = None) -> Tx:
        """(tx_type, tx_info, tx_hash, error) since SDK 1.x; older versions returned (tx_info, error)"""
        if not isinstance(out, (tuple, list)):
            raise SigningError(f"unexpected signer result: {out!r}")
        error = out[-1]
        if error:
            raise SigningError(str(error))
        if len(out) >= 4:
            return Tx(out[0] if out[0] is not None else kind, out[1], out[2] or "", client)
        if len(out) == 2 and out[0]:
            return Tx(kind, out[0], "", client)
        raise SigningError(f"unexpected signer result: {out!r}")

    def create_order(self, key, nonce, market, client, base, price, is_ask, tif, reduce, expiry) -> Tx:
        return self.tx(self.client.sign_create_order(
            market_index=market, client_order_index=client, base_amount=base, price=price, is_ask=int(is_ask),
            order_type=ORDER_LIMIT, time_in_force=tif, reduce_only=int(reduce), trigger_price=0, order_expiry=expiry,
            nonce=nonce, api_key_index=key), TX_CREATE_ORDER, client)

    def modify_order(self, key, nonce, market, index, base, price) -> Tx:
        return self.tx(self.client.sign_modify_order(
            market_index=market, order_index=index, base_amount=base, price=price, trigger_price=0,
            nonce=nonce, api_key_index=key), TX_MODIFY_ORDER, index)

    def cancel_order(self, key, nonce, market, index) -> Tx:
        return self.tx(self.client.sign_cancel_order(market_index=market, order_index=index, nonce=nonce,
                                                      api_key_index=key), TX_CANCEL_ORDER, index)

    def cancel_all(self, key, nonce, tif, at_ms) -> Tx:
        return self.tx(self.client.sign_cancel_all_orders(time_in_force=tif, timestamp_ms=at_ms, nonce=nonce,
                                                          api_key_index=key), TX_CANCEL_ALL_ORDERS)

    def update_leverage(self, key, nonce, market, fraction, mode) -> Tx:
        return self.tx(self.client.sign_update_leverage(market_index=market, fraction=fraction, margin_mode=mode,
                                                        nonce=nonce, api_key_index=key), TX_UPDATE_LEVERAGE)

    def update_margin(self, key, nonce, market, usdc, direction) -> Tx:
        sign = getattr(self.client, "sign_update_margin", None)
        if sign is None:
            raise SigningError("this lighter-sdk version cannot sign updateMargin")
        return self.tx(sign(market_index=market, usdc_amount=usdc, direction=direction, nonce=nonce,
                            api_key_index=key), TX_UPDATE_MARGIN)

    def auth(self, key: int, ttl: int) -> str:
        token, error = self.client.create_auth_token_with_expiry(ttl, timestamp=int(time.time()), api_key_index=key)
        if error or not token:
            raise SigningError(f"auth token: {error}")
        return token

    def check(self) -> str | None:
        """None when every key is the one registered on the exchange for this account (one HTTP call per key)"""
        return self.client.check_client()

    async def close(self):
        close = getattr(self.client, "close", None)
        if close:
            try:
                out = close()
                if asyncio.iscoroutine(out):
                    await out
            except Exception:
                pass
