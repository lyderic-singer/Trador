from decimal import Decimal
from cryptography.hazmat.primitives.asymmetric import ed25519
from adapters.arcus import Arcus
from core.arcus.exchange import ArcusExchange
from core.arcus.utils.signing import Signer, canonical, cancel_payload, modify_payload, place_payload
from core.arcus.utils.types import ArcusCancel, ArcusModify, ArcusOrder, Market

KEY = "11" * 32
ADDRESS = "0xAbCdEf0000000000000000000000000000000001"
LOWER = ADDRESS.lower()

MARKET = {
    "marketId": 1, "marketDisplayName": "BTC-USD", "baseAsset": "BTC", "quoteAsset": "USD",
    "tickSize": "1", "stepSize": "0.0001", "minOrderSize": "0.0001", "maxOrderSize": "100",
    "minOrderNotional": "10", "oraclePrice": "63455.8", "markPrice": "63460", "status": "ONLINE",
    "initialMarginFraction": "0.05",
    "tickTiers": [{"upToPrice": "50000", "tick": "1"}, {"upToPrice": None, "tick": "5"}],
}

BTC = {"marketDisplayName": "BTC-USD", "marketId": 1, "status": "ONLINE", "baseAsset": "BTC", "quoteAsset": "USD",
       "tickSize": "0.1", "stepSize": "0.00000001",
       "tickTiers": [{"upToPrice": "500000", "tick": "0.1"}, {"upToPrice": "1000000", "tick": "0.2"},
                     {"upToPrice": "2000000", "tick": "0.5"}, {"upToPrice": "5000000", "tick": "1"},
                     {"upToPrice": "10000000", "tick": "2"}, {"tick": "5"}],
       "minOrderNotional": "5", "minOrderSize": "0.0001", "maxOrderSize": "10000",
       "oraclePrice": "86264.3", "markPrice": "86274.3", "initialMarginFraction": "0.025"}


def verify(signer: Signer, message: str, signature: str):
    ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(signer.public)).verify(bytes.fromhex(signature), message.encode())


def test_payloads():
    assert canonical(place_payload(ADDRESS, 0, 123, 456000, 1, 63000, 10, False, 0, 3, "my-id")) == \
        f'{{"ad":"{LOWER}","ai":0,"c":"my-id","ct":123,"g":456000,"m":1,"op":1,"p":63000,"q":10,"r":0,"s":0,"t":3,"v":1}}'
    bare = canonical(place_payload(ADDRESS, 2, 1, 2, 1, 3, 4, True, 1, 2))
    assert '"c"' not in bare and '"r":1' in bare and '"ai":2' in bare
    assert canonical(cancel_payload(ADDRESS, 0, 9, 1, oid="abc")) == \
        f'{{"ad":"{LOWER}","ai":0,"ct":9,"id":"abc","m":1,"op":2,"v":1}}'
    assert canonical(modify_payload(ADDRESS, 0, 9, 7000, "abc", 1, 5, 6, False, 1, 3)) == \
        f'{{"ad":"{LOWER}","ai":0,"ct":9,"g":7000,"id":"abc","m":1,"op":3,"p":5,"q":6,"r":0,"s":1,"t":3,"v":1}}'


def test_legacy():
    signer = Signer(KEY)
    body = {"address": ADDRESS, "accountIndex": 0, "marketId": 1, "leverage": 5}
    signature = signer.sign_legacy(1700, "setLeverage", body)
    verify(signer, '1700setLeverage{"accountIndex":0,"address":"' + ADDRESS + '","leverage":5,"marketId":1}', signature)


def test_rounding():
    m = Market(MARKET)
    assert m.round_price(49999.7, "down") == Decimal("49999")
    assert m.round_price(49999.2, "up") == Decimal("50000")
    assert m.round_price(63457, "down") == Decimal("63455")
    assert m.round_price(63457, "up") == Decimal("63460")
    assert m.to_ticks(Decimal("63455")) == 63455
    assert m.round_size(0.012345) == Decimal("0.0123")
    assert m.to_quantums(Decimal("0.0123")) == 123
    assert m.max_leverage == 20


def test_market():
    m = Market(BTC)
    assert m.round_price(86274.37, "down") == Decimal("86274.3")
    assert m.round_price(86274.31, "up") == Decimal("86274.4")
    assert m.to_ticks(Decimal("86274.3")) == 862743
    assert m.round_price(750000.3, "down") == Decimal("750000.2")
    assert m.to_ticks(m.round_price(750000.3, "down")) == 7500002
    assert m.round_size(0.000123456789) == Decimal("0.00012345")
    assert m.max_leverage == 40 and m.min_notional == 5


def test_elements():
    signer = Signer(KEY)
    exchange = ArcusExchange.__new__(ArcusExchange)
    exchange.signer, exchange.address, exchange.index = signer, ADDRESS, 0
    m = Market(MARKET)
    ts, expiry = 1712345678000000000, 4102444800000000
    place = exchange.build_place(ArcusOrder(m, True, Decimal("0.0123"), Decimal("63455"), "ALO"), ts, expiry)
    assert place["orderSide"] == "BUY" and place["quantity"] == "0.0123" and place["price"] == "63455"
    assert place["goodTilTime"] == str(expiry) and place["timestamp"] == ts and place["timeInForce"] == "ALO"
    verify(signer, canonical(place_payload(ADDRESS, 0, ts, expiry * 1000, 1, 63455, 123, False, 0, 3)), place["signature"])
    modify = exchange.build_modify(ArcusModify(m, "oid1", False, Decimal("0.01"), Decimal("63460"), "ALO", False, expiry), ts)
    assert modify["side"] == "SELL" and modify["orderId"] == "oid1" and modify["clientTime"] == str(ts)
    verify(signer, canonical(modify_payload(ADDRESS, 0, ts, expiry * 1000, "oid1", 1, 63460, 100, False, 1, 3)), modify["signature"])
    cancel = exchange.build_cancel(ArcusCancel(m, oid="oid1"), ts)
    assert cancel["kind"] == "orderId"
    verify(signer, canonical(cancel_payload(ADDRESS, 0, ts, 1, oid="oid1")), cancel["signature"])


def test_parsing():
    raw = {"equity": "1250.5", "freeCollateral": "1000", "positions": {"1": {
        "marketId": 1, "marketDisplayName": "BTC-USD", "side": "SHORT", "size": "-1000000",
        "averageEntryPrice": "50000000000", "leverage": "10", "marginMode": "CROSS", "marginUsed": "50",
        "positionValueNotional": "500", "unrealizedPnl": "-5", "markPx": "50000"}}}
    state = Arcus.parse_account(raw, {"BTC-USD": 10}, lambda i: "BTC-USD")
    p = state.positions["BTC-USD"]
    assert abs(p.size + 0.01) < 1e-12 and p.value == 500 and p.signed_value == -500
    assert abs(p.entry - 49500) < 1e-6
    assert state.equity == 1250.5 and Arcus.parse_account(None, {}, str).equity == 0
    res = {"responses": [{"orderId": "a", "status": "ACK"},
                         {"orderId": "b", "status": "REJECTED", "rejectionReason": "POST_ONLY_WOULD_CROSS"},
                         {"status": "ERROR", "error": "invalid order signature"}]}
    out = Arcus.parse_responses(res, 3)
    assert [r.ok for r in out] == [True, False, False] and out[1].error == "POST_ONLY_WOULD_CROSS"
