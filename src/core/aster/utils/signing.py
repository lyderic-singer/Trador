"""Aster futures API v3 ("Pro API"): every private request is signed by an API wallet (the "agent" created on
asterdex.com/en/api-wallet), never by the main wallet.

The request parameters, then `nonce` (microseconds) and `signer` (the agent address), are url-encoded in that order;
the resulting string is the `msg` of an EIP-712 message (domain AsterSignTransaction, version 1, chainId 1666 on
mainnet / 714 on testnet, primary type Message{string msg}) signed with the agent key. `signature` is appended to the
very same string, which is sent as it is (query string of a GET / DELETE, form body of a POST / PUT): the server
verifies the bytes it receives, so nothing may re-encode them on the way.
"""
import threading
import time
from urllib.parse import urlencode
from eth_account import Account
from eth_account.messages import encode_typed_data

ZERO = "0x0000000000000000000000000000000000000000"


def typed(msg: str, chain: int) -> dict:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Message": [{"name": "msg", "type": "string"}],
        },
        "primaryType": "Message",
        "domain": {"name": "AsterSignTransaction", "version": "1", "chainId": chain, "verifyingContract": ZERO},
        "message": {"msg": msg},
    }


class Signer:
    def __init__(self, private_key: str, chain: int, user: str | None = None, send_user: bool = False):
        self.wallet = Account.from_key(private_key)
        self.address: str = self.wallet.address          # the agent ("signer")
        self.chain = chain
        self.user = user                                 # the main wallet the agent trades for
        self.send_user = send_user and bool(user)
        self.clock = 0                                   # µs added to the local clock (set when it is off the exchange's)
        self.last = 0
        self.lock = threading.Lock()

    def nonce(self) -> int:
        """microseconds, strictly increasing: the exchange keeps the 100 latest nonces of an agent, refuses a
        duplicate and anything more than 60 s away from its clock"""
        with self.lock:
            self.last = max(time.time_ns() // 1000 + self.clock, self.last + 1)
            return self.last

    def message(self, params: dict, nonce: int | None = None) -> str:
        fields = {k: v for k, v in params.items() if v is not None}
        if self.send_user:
            fields["user"] = self.user
        fields["nonce"] = str(nonce if nonce is not None else self.nonce())
        fields["signer"] = self.address
        return urlencode(fields)

    def signature(self, msg: str) -> str:
        signed = self.wallet.sign_message(encode_typed_data(full_message=typed(msg, self.chain)))
        text = signed.signature.hex()
        return text if text.startswith("0x") else "0x" + text

    def sign(self, params: dict, nonce: int | None = None) -> str:
        """the exact string to send: `<params>&nonce=..&signer=..&signature=0x..`"""
        msg = self.message(params, nonce)
        return f"{msg}&signature={self.signature(msg)}"
