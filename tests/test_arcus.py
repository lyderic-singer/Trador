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
    tagged = exchange.build_modify(ArcusModify(m, "oid1", False, Decimal("0.01"), Decimal("63460"), "ALO", False, expiry, "0xabc"), ts)
    assert "clientId" not in tagged and tagged["orderId"] == "oid1"
    verify(signer, canonical(modify_payload(ADDRESS, 0, ts, expiry * 1000, "oid1", 1, 63460, 100, False, 1, 3)), tagged["signature"])
    assert '"c"' not in canonical(modify_payload(ADDRESS, 0, ts, expiry * 1000, "oid1", 1, 63460, 100, False, 1, 3))
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


def test_refused():
    import asyncio, json
    from core.arcus.websocket import ArcusWebSocket
    from log.messenger import Messenger

    class Socket:
        def __init__(self, ws):
            self.ws = ws

        async def send(self, raw):
            msg = json.loads(raw)
            if msg["channel"] == "bbo":
                await self.ws.handle(json.dumps({"type": "subscribed", "channel": "bbo", "id": msg["id"], "contents": {}}))
            else:
                await self.ws.handle(json.dumps({"type": "error", "message": "address not on access whitelist"}))

    async def go():
        ws = ArcusWebSocket("wss://x", Messenger(None))
        ws.ws = Socket(ws)
        ws.connected.set()
        await ws.bbo(lambda m: None, "SPY-USD")
        try:
            await ws.positions(lambda m: None, ADDRESS.lower())
            raise AssertionError("should be refused")
        except ValueError as e:
            assert "positions" in str(e) and "whitelist" in str(e)
    asyncio.run(go())


def test_check():
    import asyncio
    from core.arcus.client import ArcusClient
    from log.messenger import Messenger

    def client(rows):
        c = ArcusClient.__new__(ArcusClient)
        c.signer, c.address, c.index, c.mainnet = Signer(KEY), LOWER, 0, True
        c._log = __import__("log.logger", fromlist=["Logger"]).Logger("t", Messenger(None))
        c.exchange = type("E", (), {"index": 0})()

        class Info:
            async def api_keys(self, address):
                return rows
        c.info = Info()
        return c

    async def go():
        public = Signer(KEY).public
        good = client([{"apiKey": public, "accountIndex": 2}])
        await good.check()
        assert good.index == 2 and good.exchange.index == 2
        try:
            await client([{"apiKey": "ab" * 32}]).check()
            raise AssertionError("should fail")
        except ValueError as e:
            assert "not registered" in str(e)
    asyncio.run(go())


def test_fallback():
    import asyncio
    from adapters.models import AccountState
    from log.messenger import Messenger

    class Socket:
        async def refuse(self, *args, **kwargs):
            raise ValueError("subscription positions:0xabc:0 refused: address not on access whitelist")
        positions = orders = account = refuse

    class Client:
        messenger, address, index, running = Messenger(None), "0xabc", 0, True
        websocket = Socket()

    async def go():
        arcus = Arcus(Client())
        arcus.running = True
        async def account(symbols):
            return AccountState("arcus", equity=100)
        arcus.account = account
        arcus.market = lambda s: Market(MARKET)
        got = []
        await arcus.subscribe_account(["BTC-USD"], got.append)
        assert got and got[0].equity == 100 and arcus.poll == 2.0
        await arcus.stop()
    asyncio.run(go())


class Stub:
    def __init__(self):
        from log.messenger import Messenger
        from core.arcus.utils.types import Market as M
        self.messenger, self.address, self.index, self.running = Messenger(None), LOWER, 0, True
        markets = {1: M(BTC), 23: M({**MARKET, "marketId": 23, "marketDisplayName": "SPY-USD", "baseAsset": "SPY",
                                     "isOutsideRth": True, "lowerTradingBound": "760", "upperTradingBound": "780"})}

        class Info:
            def market(self, x):
                return markets[int(x)] if isinstance(x, int) or str(x).isdigit() else next(m for m in markets.values() if m.name == x)
        self.info = Info()


def test_routing():
    import asyncio, json
    from core.arcus.websocket import ArcusSubscription, ArcusWebSocket
    from log.messenger import Messenger

    async def go():
        ws = ArcusWebSocket("wss://x", Messenger(None))
        got = []
        for market in ("SPY-USD", "QQQ-USD"):
            s = ArcusSubscription("oraclePrices", market, got.append)
            ws.subscriptions[s.key] = s
        orders = ArcusSubscription("orders", LOWER, got.append, {"accountIndex": 2})
        ws.subscriptions[orders.key] = orders
        await ws.handle(json.dumps({"type": "channel_data", "channel": "oraclePrices", "contents": {"prices": []}}))
        assert len(got) == 1
        await ws.handle(json.dumps({"type": "channel_data", "channel": "orders", "id": ADDRESS, "contents": {"orderId": "x"}}))
        assert len(got) == 2 and got[1]["contents"]["orderId"] == "x"
    asyncio.run(go())


def test_streams():
    import asyncio
    from adapters.models import Side

    async def go():
        arcus = Arcus(Stub())
        updates, fills, prices = [], [], []
        arcus.order_listeners.append(updates.append)
        arcus.fill_listeners.append(fills.append)
        arcus.oracle_listeners["SPY-USD"] = [lambda name, px: prices.append((name, px))]
        row = {"orderId": "o2", "clientId": "0xabc", "marketId": 23, "side": "SELL", "status": "OPEN", "state": "PARTIALLY_FILLED",
               "price": "770.5", "originalSize": "2", "remainingSize": "1.5"}
        await arcus.on_orders({"type": "channel_data", "contents": row})
        await arcus.on_orders({"type": "channel_data", "contents": {**row, "orderId": "o1", "status": "CANCELED", "cancelReason": "MODIFY_CANCELED"}})
        await arcus.on_orders({"type": "channel_data", "contents": {**row, "status": "FILLED", "remainingSize": "0"}})
        await arcus.on_orders({"type": "subscribed", "contents": {"isSnapshot": True, "orders": [row]}})
        assert [u.status for u in updates] == ["open", "replaced", "filled"]
        assert updates[0].symbol == "SPY-USD" and updates[0].side == Side.SELL and updates[0].remaining == 1.5
        fill = {"tradeId": "t1", "orderId": "o2", "clientId": "0xabc", "marketId": 23, "side": "SELL", "size": "0.5",
                "price": "770.5", "role": "MAKER", "remainingSize": "1.5", "createdAt": 1785801600123456}
        await arcus.on_fills({"type": "subscribed", "contents": {"fills": [{**fill, "tradeId": "old"}]}})
        await arcus.on_fills({"type": "channel_data", "contents": fill})
        await arcus.dispatch([fill])
        assert len(fills) == 1 and fills[0].maker and fills[0].value == 385.25 and fills[0].remaining == 1.5
        await arcus.on_oracle({"type": "channel_data", "contents": {"prices": [{"marketDisplayName": "SPY-USD", "price": "770.1"},
                                                                                {"marketDisplayName": "BTC-USD", "price": "1"}]}})
        assert prices == [("SPY-USD", 770.1)]
        inst = arcus.instrument("SPY-USD")
        assert inst.closed and inst.lower == 760 and inst.upper == 780 and inst.tick(700) == 1.0 and not inst.bounded(781)
    asyncio.run(go())


def test_placement():
    import asyncio
    from adapters.models import Order, Side, TIF

    async def go():
        stub = Stub()

        class Exchange:
            async def batch_place(self, orders):
                return {"responses": [{"orderId": "n1", "status": "ACK"}], "rateLimit": {"pool": "order", "remaining": 1234}}
        stub.exchange = Exchange()
        arcus = Arcus(stub)
        result = (await arcus.place([Order("BTC-USD", Side.BUY, 0.01, 86000.0, TIF.ALO, False, "0xabc")]))[0]
        assert result.ok and result.raw["goodTilTime"] > time_us() and result.raw["timeInForce"] == "ALO"
        assert result.raw["clientId"] == "0xabc" and arcus.budget() == {"order": 1234}
    asyncio.run(go())


def time_us() -> int:
    import time
    return int(time.time() * 1_000_000)


def test_deadman():
    import asyncio, json, time

    async def go():
        signer = Signer(KEY)
        exchange = ArcusExchange.__new__(ArcusExchange)
        exchange.signer, exchange.address, exchange.index = signer, LOWER, 0
        sent = {}

        class Api:
            async def post(self, path, body, headers, params):
                sent.update(path=path, body=body, headers=headers)
                return {"status": "scheduled"}
        exchange.api = Api()
        await exchange.schedule_cancel(15)
        body = sent["body"]
        assert sent["path"] == "/v1/scheduleCancel" and 14e6 < body["time"] - time.time() * 1e6 < 16e6
        verify(signer, sent["headers"]["X-Timestamp"] + "scheduleCancel" + canonical(body), sent["headers"]["X-Signature"])
        await exchange.schedule_cancel(None)
        assert "time" not in sent["body"]
    asyncio.run(go())


def test_replay():
    import asyncio, time

    async def go():
        arcus = Arcus(Stub())
        fills = []
        arcus.fill_listeners.append(fills.append)
        now = time.time()
        row = {"tradeId": "a", "orderId": "o", "marketId": 23, "side": "BUY", "size": "1", "price": "770",
               "createdAt": int((now - 600) * 1e6)}
        await arcus.on_fills({"type": "subscribed", "contents": {"fills": [row]}})
        assert not fills
        fresh = {**row, "tradeId": "b", "createdAt": int((now + 1) * 1e6)}
        await arcus.on_fills({"type": "subscribed", "contents": {"fills": [row, fresh]}})
        assert [f.id for f in fills] == ["b:o"]
        await arcus.on_fills({"type": "subscribed", "contents": {"fills": [row, fresh]}})
        assert len(fills) == 1
    asyncio.run(go())
