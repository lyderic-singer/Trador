import json
from typing import Any
from cryptography.hazmat.primitives.asymmetric import ed25519
from core.arcus.utils.constants import OP_CANCEL, OP_MODIFY, OP_PLACE, PAYLOAD_VERSION


def canonical(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


class Signer:
    def __init__(self, private_key: str):
        key = private_key[2:] if private_key.startswith("0x") else private_key
        self.key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(key))
        self.public = self.key.public_key().public_bytes_raw().hex()

    def sign(self, message: str | bytes) -> str:
        data = message.encode() if isinstance(message, str) else message
        return self.key.sign(data).hex()

    def sign_payload(self, payload: dict) -> str:
        return self.sign(canonical(payload))

    def sign_legacy(self, timestamp: int, action: str, body: dict | None = None) -> str:
        return self.sign(f"{timestamp}{action}" + (canonical(body) if body is not None else ""))


def place_payload(address: str, account: int, ct: int, expiry: int, market: int, p: int, q: int,
                  reduce: bool, side: int, tif: int, client: str | None = None) -> dict:
    payload = {
        "ad": address.lower(), "ai": account, "ct": ct, "g": expiry, "m": market,
        "op": OP_PLACE, "p": p, "q": q, "r": 1 if reduce else 0, "s": side, "t": tif,
        "v": PAYLOAD_VERSION,
    }
    if client:
        payload["c"] = client
    return payload


def cancel_payload(address: str, account: int, ct: int, market: int,
                   oid: str | None = None, client: str | None = None) -> dict:
    if (oid is None) == (client is None):
        raise ValueError("cancel needs exactly one of oid / client")
    payload = {"ad": address.lower(), "ai": account, "ct": ct, "m": market, "op": OP_CANCEL, "v": PAYLOAD_VERSION}
    if oid is not None:
        payload["id"] = oid
    else:
        payload["c"] = client
    return payload


def modify_payload(address: str, account: int, ct: int, expiry: int, oid: str, market: int,
                   p: int, q: int, reduce: bool, side: int, tif: int, client: str | None = None) -> dict:
    payload = {
        "ad": address.lower(), "ai": account, "ct": ct, "g": expiry, "id": oid, "m": market,
        "op": OP_MODIFY, "p": p, "q": q, "r": 1 if reduce else 0, "s": side, "t": tif,
        "v": PAYLOAD_VERSION,
    }
    if client:
        payload["c"] = client
    return payload
