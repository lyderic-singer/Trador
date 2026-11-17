import asyncio
import json
import time
from urllib.parse import parse_qsl
import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data
from adapters import ADAPTERS, CLIENTS
from adapters.aster import Aster
from adapters.models import Cancel, Modify, Order, Side, TIF
from core.aster.api import AsterApi, AsterError
from core.aster.client import AsterClient, credentials
from core.aster.exchange import AsterExchange
from core.aster.utils.signing import Signer, typed
from core.aster.utils.types import fmt
from core.aster.websocket import AsterMarketSocket, AsterUserSocket
from core.session import Session
from log.messenger import Messenger
from strategies.enveloppe import Enveloppe, Parameters as EnveloppeParameters
from strategies.orders import OrderTracker, gone
from tests.aster_sim import KEY, USER, AsterSim

CLOID = "0x" + "ab" * 16          # a strategy's client id: 34 characters, accepted as it is by Aster


def run(coroutine):
    return asyncio.run(coroutine)


async def setup(monkeypatch, **options):
    sim = AsterSim(monkeypatch)
    await sim.load()
    aster = Aster(sim.client, **options)
    await aster.start()
    return sim, aster


async def settle():
    for _ in range(6):
        await asyncio.sleep(0)


def calls(sim, verb, name):
    return [p for v, n, p in sim.calls if (v, n) == (verb, name)]


def test_registered():
    assert CLIENTS["aster"] is AsterClient and ADAPTERS["aster"] is Aster


# ---- signature ----

def test_signature_is_the_documented_eip712_message():
    signer = Signer(KEY, 1666, user=USER)
    assert signer.address == Account.from_key(KEY).address
    params = {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "timeInForce": "GTX", "quantity": "0.01", "price": "100000.5",
              "newClientOrderId": CLOID, "skipped": None}
    text = signer.sign(params, nonce=1_800_000_000_000_000)
    msg, signature = text.rsplit("&signature=", 1)
    # the parameters in the order given, then nonce and signer; `user` is not sent (the agent identifies the account)
    assert msg == ("symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTX&quantity=0.01&price=100000.5"
                   f"&newClientOrderId={CLOID}&nonce=1800000000000000&signer={signer.address}")
    data = typed(msg, 1666)
    assert data["primaryType"] == "Message" and data["message"] == {"msg": msg}
    assert data["domain"] == {"name": "AsterSignTransaction", "version": "1", "chainId": 1666,
                              "verifyingContract": "0x0000000000000000000000000000000000000000"}
    # the signature is the agent's over exactly that string
    assert signature.startswith("0x") and len(signature) == 132
    assert Account.recover_message(encode_typed_data(full_message=data), signature=signature) == signer.address
    assert Account.recover_message(encode_typed_data(full_message=typed(msg, 714)), signature=signature) != signer.address
    # values that need escaping are signed escaped, exactly as they travel
    batch = signer.message({"batchOrders": '[{"symbol":"BTCUSDT"}]'}, nonce=1)
    assert batch.startswith("batchOrders=%5B%7B%22symbol%22%3A%22BTCUSDT%22%7D%5D&nonce=1&signer=")
    assert dict(parse_qsl(batch))["batchOrders"] == '[{"symbol":"BTCUSDT"}]'
    # opt-in: the main wallet as `user`
    with_user = Signer(KEY, 1666, user=USER, send_user=True).message({"symbol": "X"}, nonce=1)
    assert f"symbol=X&user={USER}&nonce=1&signer=" in with_user


def test_nonces_strictly_increase_and_follow_the_exchange_clock():
    signer = Signer(KEY, 714)
    nonces = [signer.nonce() for _ in range(2000)]
    assert nonces == sorted(set(nonces)) and abs(nonces[0] / 1e6 - time.time()) < 2      # microseconds
    signer.clock = 30_000_000                                                             # local clock 30 s late
    assert signer.nonce() / 1e6 - time.time() > 29


def test_credentials(monkeypatch):
    for field in ("PRIVATE_KEY", "ADDRESS", "SIGNER", "SEND_USER"):
        monkeypatch.delenv(f"ASTER_TESTNET_{field}", raising=False)
    assert credentials(False) is None                                       # no key: read-only
    monkeypatch.setenv("ASTER_TESTNET_PRIVATE_KEY", KEY)
    monkeypatch.setenv("ASTER_TESTNET_ADDRESS", USER)
    signer = credentials(False)
    assert signer.chain == 714 and signer.user == USER and not signer.send_user
    assert credentials(True) is None                                        # mainnet has its own variables
    monkeypatch.setenv("ASTER_TESTNET_SIGNER", USER)                        # not the address of the key
    with pytest.raises(ValueError, match="SIGNER"):
        credentials(False)
    monkeypatch.setenv("ASTER_TESTNET_SIGNER", signer.address.lower())
    assert credentials(False).address == signer.address
    client = AsterClient(False, Messenger(None), Session(Messenger(None)))
    assert not client.readonly and client.address == USER and client.url == "https://fapi.asterdex-testnet.com"
    assert client.websocket.url == "wss://fstream.asterdex-testnet.com/ws"


class Response:
    def __init__(self, status, body, headers=None):
        self.status, self.body, self.headers = status, body, headers or {}

    async def text(self):
        return self.body if isinstance(self.body, str) else json.dumps(self.body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class Http:
    def __init__(self, *responses):
        self.responses, self.requests = list(responses), []

    async def get(self):
        return self

    def request(self, verb, url, data=None, headers=None):
        self.requests.append((verb, str(url), data, headers))
        return self.responses.pop(0)


def test_requests_send_exactly_what_was_signed():
    async def go():
        http = Http(Response(200, {"orderId": 1}, {"X-MBX-USED-WEIGHT-1M": "12", "X-MBX-ORDER-COUNT-1M": "3"}),
                    Response(200, [{"orderId": 1}]), Response(200, {"serverTime": 5}),
                    Response(400, {"code": -2019, "msg": "Margin is insufficient."}),
                    Response(429, {"code": -1003, "msg": "Too many requests."}, {"Retry-After": "7"}),
                    Response(503, "Service Unavailable"), Response(418, {"code": -1003, "msg": "banned"}),
                    Response(200, {"code": -1121, "msg": "Invalid symbol."}))
        signer = Signer(KEY, 714)
        api = AsterApi("https://x", Messenger(None), http, signer)
        assert await api.request("POST", "/fapi/v3/order", {"symbol": "BTCUSDT", "reduceOnly": True, "none": None}, signed=True) == {"orderId": 1}
        verb, url, body, headers = http.requests[-1]
        # POST / PUT: a form body; the bytes sent are the signed string followed by its signature
        assert (verb, url, headers["Content-Type"]) == ("POST", "https://x/fapi/v3/order", "application/x-www-form-urlencoded")
        msg, signature = body.decode().rsplit("&signature=", 1)
        assert msg.startswith("symbol=BTCUSDT&reduceOnly=true&nonce=") and msg.endswith(f"&signer={signer.address}")
        assert Account.recover_message(encode_typed_data(full_message=typed(msg, 714)), signature=signature) == signer.address
        assert (api.weight, api.orders) == (12, 3)
        # GET / DELETE: the same string as the query, not re-encoded
        await api.request("DELETE", "/fapi/v3/batchOrders", {"symbol": "BTCUSDT", "orderIdList": "[1,2]"}, signed=True)
        verb, url, body, _ = http.requests[-1]
        assert verb == "DELETE" and body is None and url.startswith("https://x/fapi/v3/batchOrders?symbol=BTCUSDT&orderIdList=%5B1%2C2%5D&nonce=")
        await api.get("/fapi/v3/time")
        assert http.requests[-1][1] == "https://x/fapi/v3/time"            # public: nothing added
        with pytest.raises(AsterError) as e:
            await api.request("POST", "/fapi/v3/order", {}, signed=True)
        assert (e.value.status, e.value.code, e.value.retry, e.value.unknown) == (400, -2019, None, False)
        with pytest.raises(AsterError) as e:
            await api.request("POST", "/fapi/v3/order", {}, signed=True)
        assert (e.value.status, e.value.retry) == (429, 7000)              # Retry-After, in ms for the Gateway
        with pytest.raises(AsterError) as e:
            await api.request("POST", "/fapi/v3/order", {}, signed=True)
        assert e.value.status == 503 and e.value.unknown                   # the order may have been executed
        with pytest.raises(AsterError) as e:
            await api.get("/fapi/v3/time")
        assert e.value.retry == 120_000
        with pytest.raises(AsterError) as e:                               # an error code in a 200
            await api.get("/fapi/v3/klines")
        assert e.value.code == -1121
        with pytest.raises(AsterError, match="read-only"):
            await AsterApi("https://x", Messenger(None), http).request("GET", "/fapi/v3/account", signed=True)
    run(go())


# ---- markets ----

def test_markets_and_instrument(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        assert set(sim.client.info.by_name) == {"BTCUSDT", "SPYUSDT", "SPYUSD1"}       # perpetuals only
        # by symbol, by base (USDT first), or by the name another venue gives the same base
        for name in ("SPYUSDT", "spyusdt", "SPY", "SPY-USD", "SPY/USD", "xyz:SPY"):
            assert aster.market(name).name == "SPYUSDT", name
        assert aster.market("SPYUSD1").name == "SPYUSD1" and aster.canonical("BTC-USD") == "BTCUSDT"
        with pytest.raises(ValueError):
            aster.market("GLD-USD")
        inst = aster.instrument("SPY-USD")
        assert (inst.exchange, inst.symbol, inst.min_size, inst.min_notional) == ("aster", "SPYUSDT", 0.01, 5.0)
        assert inst.tick(700) == 0.01 and inst.round_size(0.0399) == 0.03 and aster.instrument("BTC").tick(1) == 0.1
        assert inst.round_price(700.004, Side.BUY) == 700.0 and inst.round_price(700.004, Side.SELL) == 700.01
        assert (inst.maker_fee, inst.taker_fee) == (0.0001, 0.00035)                   # configured until the account's are read
        await aster.subscribe_account(["SPY-USD"], lambda s: None)
        inst = aster.instrument("SPY-USD")
        assert (inst.maker_fee, inst.taker_fee) == (0.00005, 0.0004)                   # commissionRate of the account
        assert (inst.max_leverage, inst.maintenance, inst.initial) == (25, 0.01, 0.04)  # leverage brackets
        assert fmt(aster.market("BTC").round_price(100000.04)) == "100000" and fmt(aster.market("SPY").round_size(1.5)) == "1.5"
        listing = {x.symbol: x for x in aster.listings()}
        assert listing["BTCUSDT"].volume == 5000000.5 and listing["SPYUSDT"].category == "STOCK" and aster.tradable("SPY")
        assert aster.transport == "rest" and aster.budget() is None
        await aster.stop()
    run(go())


def test_start_checks_the_key_and_the_position_mode(monkeypatch):
    async def go():
        sim = AsterSim(monkeypatch)
        await sim.load()
        await sim.client.check()
        sim.hedge_mode = True
        with pytest.raises(ValueError, match="One-way"):
            await sim.client.check()
        sim.hedge_mode = False
        sim.fail.append(AsterError(400, -1022, "Signature for this request is not valid.", "/x"))
        with pytest.raises(ValueError, match="refused the API wallet"):
            await sim.client.check()
        offset, rtt = await sim.client.sync_clock(2)
        assert abs(offset) < 1000 and sim.client.signer.clock == 0
    run(go())


# ---- orders ----

def test_place_single(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        r, = await aster.place([Order("SPY-USD", Side.BUY, 0.0599, 699.004, TIF.ALO, client=CLOID)])
        sent, = calls(sim, "POST", "order")
        # post-only is GTX; numbers as plain strings on the step; RESULT so that the reply carries the outcome
        assert sent == {"symbol": "SPYUSDT", "side": "BUY", "type": "LIMIT", "timeInForce": "GTX", "quantity": "0.05",
                        "price": "699", "newClientOrderId": CLOID, "newOrderRespType": "RESULT"}
        assert r.ok and r.oid == "1001" and r.status == "NEW" and r.filled == 0
        r, = await aster.place([Order("SPY", Side.SELL, 0.05, 701.001, TIF.GTC, reduce=True)])
        assert not r.ok and "ReduceOnly" in r.error and "-2022" in r.error    # no position: refused by the exchange
        assert calls(sim, "POST", "order")[-1]["reduceOnly"] == "true" and calls(sim, "POST", "order")[-1]["price"] == "701.01"
        # a post-only order that would cross comes back EXPIRED: a refusal
        r, = await aster.place([Order("SPY", Side.BUY, 0.05, 700.50, TIF.ALO)])
        assert not r.ok and "post only would cross" in r.error and r.oid
        orders = await aster.open_orders("SPY-USD")
        assert [(o.symbol, o.oid, o.side, o.price, o.size, o.tif, o.client) for o in orders] == \
            [("SPYUSDT", "1001", Side.BUY, 699.0, 0.05, TIF.ALO, CLOID)]
        await aster.stop()
    run(go())


def test_place_refused_locally(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        results = await aster.place([
            Order("SPY", Side.BUY, 0.004, 700, TIF.ALO),              # under the minimum quantity
            Order("BTC", Side.BUY, 0.001, 4000, TIF.ALO),             # 4 $ < MIN_NOTIONAL (and under the price limit...)
            Order("SPY", Side.BUY, 0.05, 740, TIF.GTC),               # above mark x 1.05
            Order("SPY", Side.SELL, 0.05, 660, TIF.GTC),              # under mark x 0.95
            Order("SPY", Side.BUY, 0.05, 699, TIF.ALO, client="bad id!"),
            Order("GLD-USD", Side.BUY, 1, 300, TIF.ALO),
        ])
        assert [r.ok for r in results] == [False] * 6 and not calls(sim, "POST", "order") and not calls(sim, "POST", "batchOrders")
        assert "< min" in results[0].error and "notional" in results[1].error and "price limit" in results[2].error
        assert "price limit" in results[3].error and "client id" in results[4].error and "unknown Aster market" in results[5].error
        # a sell ABOVE the cap or a buy BELOW the floor is legal (the limits are one-sided)
        r, = await aster.place([Order("SPY", Side.SELL, 0.05, 760, TIF.ALO)])
        assert r.ok
        await aster.stop()
    run(go())


def test_ioc_outcome_is_in_the_reply(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        r, = await aster.place([Order("SPY", Side.BUY, 0.05, 700.201, TIF.IOC)])
        assert calls(sim, "POST", "order")[-1]["price"] == "700.21"            # aggressive: rounded through the market
        assert r.ok and r.filled == 0.05 and r.price == pytest.approx(700.10) and r.status == "FILLED"
        r, = await aster.place([Order("SPY", Side.BUY, 0.05, 699.00, TIF.IOC)])
        assert not r.ok and "IOC could not match" in r.error and r.filled == 0
        # rate limit and unknown outcomes are raised, never turned into "refused"
        sim.fail.append(AsterError(429, -1003, "Too many requests.", "/fapi/v3/order", "2"))
        with pytest.raises(AsterError) as e:
            await aster.place([Order("SPY", Side.BUY, 0.05, 700.2, TIF.IOC)])
        assert e.value.status == 429 and e.value.retry == 2000
        sim.fail.append(AsterError(503, None, "Service Unavailable", "/fapi/v3/order"))
        with pytest.raises(AsterError):
            await aster.place([Order("SPY", Side.BUY, 0.05, 700.2, TIF.IOC)])
        sim.fail.append(asyncio.TimeoutError())
        with pytest.raises(asyncio.TimeoutError):
            await aster.place([Order("SPY", Side.BUY, 0.05, 700.2, TIF.IOC)])
        await aster.stop()
    run(go())


def test_batches(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        orders = [Order("SPY", Side.BUY, 0.05, 690 + i * 0.01, TIF.ALO) for i in range(11)]
        orders[3] = Order("SPY", Side.BUY, 0.05, 701, TIF.ALO)                 # would cross
        orders[7] = Order("SPY", Side.BUY, 900, 695, TIF.ALO)                  # not enough margin
        results = await aster.place(orders)
        # 5 orders per request, the requests of one call together; each element answered at its place
        assert [len(json.loads(p["batchOrders"])) for p in calls(sim, "POST", "batchOrders")] == [5, 5, 1]
        assert [r.ok for r in results] == [True, True, True, False, True, True, True, False, True, True, True]
        assert "post only would cross" in results[3].error and "Margin is insufficient" in results[7].error
        assert json.loads(calls(sim, "POST", "batchOrders")[0]["batchOrders"])[0]["quantity"] == "0.05"
        live = await aster.open_orders()
        assert len(live) == 9
        # modifies by 5, cancels by 10 per symbol
        mods = await aster.modify([Modify(o, 0.06, o.price - 1, TIF.ALO) for o in live])
        assert all(m.ok for m in mods) and [len(json.loads(p["batchOrders"])) for p in calls(sim, "PUT", "batchOrders")] == [5, 4]
        assert [m.oid for m in mods] == [o.oid for o in live]                  # a modify keeps the order id
        await aster.place([Order("BTC", Side.SELL, 0.001, 100500, TIF.ALO, client=CLOID)])
        done = await aster.cancel([Cancel(o.symbol, o.oid) for o in live] + [Cancel("BTC-USD", None, CLOID), Cancel("SPY", "424242")])
        assert [r.ok for r in done] == [True] * 10 + [False]
        assert gone(done[-1]) and "NOT_FOUND" in done[-1].error                # the trackers drop an order the exchange no longer has
        batch, = calls(sim, "DELETE", "batchOrders")
        assert batch["symbol"] == "SPYUSDT" and len(json.loads(batch["orderIdList"])) == 10
        assert calls(sim, "DELETE", "order")[-1] == {"symbol": "BTCUSDT", "origClientOrderId": CLOID}
        assert not sim.orders
        await aster.stop()
    run(go())


def test_modify_takes_the_total_quantity(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        updates = []
        await aster.subscribe_orders(["SPY"], updates.append)
        r, = await aster.place([Order("SPY", Side.BUY, 0.10, 699, TIF.ALO, client=CLOID)])
        await sim.fill(int(r.oid), 0.03)
        order = (await aster.open_orders("SPY"))[0]
        assert order.size == pytest.approx(0.07)
        # the strategy wants 0.05 LEFT on the book; Aster wants the new total: 0.05 + the 0.03 already filled
        m, = await aster.modify([Modify(order, 0.05, 698.996, TIF.ALO)])
        sent = calls(sim, "PUT", "order")[-1]
        assert m.ok and m.oid == r.oid and sent == {"symbol": "SPYUSDT", "side": "BUY", "quantity": "0.08", "price": "698.99", "orderId": int(r.oid)}
        await settle()
        assert (updates[-1].status, updates[-1].price, updates[-1].remaining) == ("open", 698.99, pytest.approx(0.05))
        # an order the exchange no longer has: reported so that the trackers forget it
        await aster.cancel([Cancel("SPY", r.oid)])
        m, = await aster.modify([Modify(order, 0.05, 698, TIF.ALO)])
        assert not m.ok and gone(m)
        bad, = await aster.modify([Modify(type(order)("SPY", "", Side.BUY, 698, 0.05), 0.05, 698, TIF.ALO)])
        assert not bad.ok and "NEVER PLACED" in bad.error
        await aster.stop()
    run(go())


def test_stream_updates_fills_and_tracker(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        tracker, updates, fills = OrderTracker(), [], []

        def on_update(u):
            updates.append(u)
            tracker.update(u)

        def on_fill(f):
            fills.append(f)
            tracker.fill(f)
        assert await aster.subscribe_orders(["SPY-USD"], on_update)
        await aster.subscribe_fills(["SPY-USD"], on_fill)
        assert sim.user.running and len(sim.user.listeners) == 1               # one user stream for everything
        w = tracker.new("SPYUSDT", Side.SELL, 0, 701, 0.06)
        tracker.placed(w, (await aster.place([Order("SPY", Side.SELL, 0.06, 701, TIF.ALO, client=w.client)]))[0])
        await settle()
        u = updates[-1]
        assert (u.symbol, u.oid, u.side, u.price, u.size, u.remaining, u.status, u.client) == \
            ("SPYUSDT", w.oid, Side.SELL, 701.0, 0.06, 0.06, "open", w.client)
        await sim.fill(int(w.oid), 0.02)
        f = fills[-1]
        assert (f.symbol, f.side, f.size, f.price, f.oid, f.client, f.maker, f.remaining) == \
            ("SPYUSDT", Side.SELL, 0.02, 701.0, w.oid, w.client, True, pytest.approx(0.04))
        assert f.fee == pytest.approx(0.02 * 701 * 0.00005) and w.remaining == pytest.approx(0.04)
        trade = await sim.fill(int(w.oid))
        assert updates[-1].status == "filled" and not tracker.live("SPYUSDT") and len(fills) == 2
        # a GTX order expired on arrival is a rejection; a commission paid in another asset is not a dollar fee
        await aster.place([Order("SPY", Side.BUY, 0.05, 700.5, TIF.ALO)])
        await settle()
        assert updates[-1].status == "rejected" and updates[-1].raw["rejectionReason"] == "GTX_WOULD_CROSS"
        await sim.user.push({"e": "ORDER_TRADE_UPDATE", "T": 1, "o": {"s": "SPYUSDT", "i": 77, "S": "BUY", "q": "1", "z": "1", "l": "1", "L": "700",
                                                                      "x": "TRADE", "X": "FILLED", "t": 999, "n": "0.001", "N": "ASTER", "m": False}})
        assert fills[-1].fee is None and not fills[-1].maker
        # after a reconnection of the stream, the fills missed are read by REST, once
        sim.trades.append({**trade, "id": 555, "qty": "0.01", "time": int(time.time() * 1000) + 50})
        await aster.reconnected()
        await asyncio.sleep(0.1)
        assert fills[-1].id == "SPYUSDT:555" and fills[-1].size == 0.01
        n = len(fills)
        await aster.recover()
        assert len(fills) == n
        await aster.stop()
    run(go())


def test_account(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch, poll=60.0)
        states = []
        await aster.subscribe_account(["SPY-USD"], states.append)
        s = states[-1]
        assert (s.equity, s.free, s.margin, s.positions) == (10_000.0, 10_000.0, 0.0, {}) and s.leverages["SPYUSDT"] == 10.0
        r, = await aster.place([Order("SPY", Side.BUY, 1.0, 699, TIF.ALO)])
        await sim.fill(int(r.oid))
        await asyncio.sleep(0.5)                        # the stream says "something changed": the account is read again
        p = states[-1].position("SPYUSDT")
        assert (p.size, p.entry, p.leverage, p.mode.value, p.liquidation) == (1.0, 699.0, 10.0, "cross", 650.5)
        assert p.value == pytest.approx(700.0) and p.pnl == pytest.approx(1.0) and p.margin == pytest.approx(70.0)
        assert states[-1].equity == pytest.approx(10_001.0) and states[-1].margin == pytest.approx(70.0)
        # leverage and margin mode
        assert await aster.set_leverage("SPY", 50, cross=False)               # capped at the first bracket (25x)
        assert sim.leverage["SPYUSDT"] == 25 and sim.margin_type["SPYUSDT"] == "ISOLATED"
        assert await aster.set_leverage("SPY", 5, cross=False) and sim.leverage["SPYUSDT"] == 5   # "no need to change": fine
        assert (await aster.account([])).position("SPYUSDT").mode.value == "isolated"
        assert await aster.add_margin("SPY", 12.5) and calls(sim, "POST", "positionMargin")[-1] == {"symbol": "SPYUSDT", "amount": "12.500000", "type": 1}
        await sim.user.push({"e": "ACCOUNT_CONFIG_UPDATE", "ac": {"s": "BTCUSDT", "l": 7}})
        assert aster.leverages["BTCUSDT"] == 7.0
        assert list((await aster.portfolio()).details) == ["futures"]
        await aster.stop()
    run(go())


def test_cancel_all_and_dead_man_switch(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        assert await aster.schedule_cancel(30) and not sim.countdowns          # nothing followed yet: nothing to arm
        await aster.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO), Order("BTC", Side.SELL, 0.001, 100500, TIF.ALO),
                           Order("SPY", Side.SELL, 0.05, 701, TIF.ALO)])
        # the switch is per symbol on Aster: armed on every market the adapter trades
        assert await aster.schedule_cancel(30) and sim.countdowns == {"BTCUSDT": 30_000, "SPYUSDT": 30_000}
        assert await aster.schedule_cancel(None) and sim.countdowns == {"BTCUSDT": 0, "SPYUSDT": 0}
        sim.fail.append(AsterError(400, -1000, "x", "/x"))
        assert not await aster.schedule_cancel(30)
        await aster.cancel_all("SPY-USD")
        assert [o["symbol"] for o in sim.orders.values()] == ["BTCUSDT"]
        await aster.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)])
        await aster.purge()                                                    # every symbol that has an order
        assert not sim.orders and sorted(p["symbol"] for p in calls(sim, "DELETE", "allOpenOrders")[1:]) == ["BTCUSDT", "SPYUSDT"]
        await aster.stop()
    run(go())


# ---- market data ----

def test_market_data(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        bbos, prints, books, oracle = [], [], [], []
        await aster.subscribe_bbo("SPY-USD", bbos.append)
        assert (bbos[-1].symbol, bbos[-1].bid, bbos[-1].ask, bbos[-1].bid_size, bbos[-1].ask_size) == ("SPYUSDT", 699.9, 700.1, 5.0, 4.0)
        assert list(sim.market.streams) == ["spyusdt@bookTicker"] and abs(bbos[-1].ts - time.time()) < 5
        await sim.quote("SPYUSDT", 700.0, 700.2)
        assert await aster.mid("SPY") == pytest.approx(700.1) and (await aster.book("BTC")).mid == pytest.approx(100000.0)
        assert await aster.subscribe_trades("SPY", prints.append)
        await sim.market.push("spyusdt@aggTrade", {"e": "aggTrade", "s": "SPYUSDT", "a": 5, "p": "700.2", "q": "0.5", "T": 1_800_000_000_123, "m": False})
        await sim.market.push("spyusdt@aggTrade", {"e": "aggTrade", "s": "SPYUSDT", "a": 6, "p": "700.0", "q": "0.1", "T": 1_800_000_000_500, "m": True})
        # m = the buyer was the maker: the taker sold
        assert [(p.side, p.price, p.size, p.id) for p in prints] == [(Side.BUY, 700.2, 0.5, "5"), (Side.SELL, 700.0, 0.1, "6")]
        assert prints[0].ts == pytest.approx(1_800_000_000.123)
        assert await aster.subscribe_depth("SPY", lambda b, a, ts: books.append((b, a)), levels=7)
        assert "spyusdt@depth10@100ms" in sim.market.streams                   # 5, 10 or 20 levels exist
        await sim.market.push("spyusdt@depth10@100ms", {"e": "depthUpdate", "s": "SPYUSDT", "T": 1, "b": [["699.8", "2"], ["699.9", "1"]], "a": [["700.1", "1"]]})
        assert books[-1] == ([(699.9, 1.0), (699.8, 2.0)], [(700.1, 1.0)])
        assert await aster.subscribe_oracle("SPY", lambda s, p: oracle.append((s, p)))
        await sim.market.push("spyusdt@markPrice@1s", {"e": "markPriceUpdate", "s": "SPYUSDT", "p": "700.02", "i": "700.07", "r": "0.00080000", "T": 1_800_000_000_000})
        assert oracle[-1] == ("SPYUSDT", 700.07) and aster.mark("SPY") == 700.02
        # funding: the rate of a 4 h period on SPY, 8 h on BTC, both given per hour
        f = await aster.fundings(["SPY-USD", "BTC"])
        assert f["SPYUSDT"].rate == pytest.approx(0.0008 / 4) and f["SPYUSDT"].next_at == 1_800_000_000.0 and not f["SPYUSDT"].closed
        assert f["BTCUSDT"].rate == pytest.approx(0.0004 / 8)
        assert await aster.funding_history("SPY", 0) == [(1_700_000_000.0, pytest.approx(0.0001)), (1_700_014_400.0, pytest.approx(-0.00005))]
        pay, = await aster.funding_payments(0)
        assert (pay.symbol, pay.usd, pay.ts) == ("SPYUSDT", -0.42, 1_700_000_000.0)
        sim.market.refused.add("aggTrade")
        assert not await aster.subscribe_trades("BTC", prints.append)
        assert await aster.volume() is None                                    # no account-wide volume endpoint
        await aster.stop()
    run(go())


def test_candles(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        base = 1_800_000_000 // 3600 * 3600
        sim.klines = [[(base + 60 * i) * 1000, str(100 + i), str(101 + i), str(99 + i), str(100.5 + i), "2", 0, "0", 0, "0", "0", "0"] for i in range(10)]
        candles = await aster.candles("SPY", "3m", 3)
        assert calls(sim, "GET", "klines")[-1] == {"symbol": "SPYUSDT", "interval": "3m", "limit": 4} and len(candles) == 3   # native on Aster
        candles = await aster.candles("SPY-USD", "1m", 2)
        assert [(c.time, c.open, c.high, c.low, c.close, c.volume) for c in candles][-1] == (base + 540, 109.0, 110.0, 108.0, 109.5, 2.0)
        with pytest.raises(ValueError):
            await aster.candles("SPY", "15s", 2)
        await aster.stop()
    run(go())


# ---- sockets ----

class Wire:
    def __init__(self):
        self.sent, self.closed = [], False

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def close(self):
        self.closed = True


def test_market_socket():
    async def go():
        ws = AsterMarketSocket("wss://x/", Messenger(None))
        assert ws.url == "wss://x/ws"
        ws.ws, ws.running = Wire(), True
        ws.connected.set()
        got = []
        task = asyncio.create_task(ws.subscribe("spyusdt@bookTicker", got.append))
        await asyncio.sleep(0.01)
        assert ws.ws.sent == [{"method": "SUBSCRIBE", "params": ["spyusdt@bookTicker"], "id": 1}]
        await ws.handle('{"result":null,"id":1}')
        await task
        # raw events are routed by type and symbol, wrapped ones by their stream name
        await ws.handle('{"e":"bookTicker","s":"SPYUSDT","b":"1","a":"2"}')
        await ws.handle('{"stream":"spyusdt@bookTicker","data":{"e":"bookTicker","s":"SPYUSDT","b":"3","a":"4"}}')
        await ws.handle('{"e":"bookTicker","s":"BTCUSDT","b":"9","a":"9"}')
        assert [m["b"] for m in got] == ["1", "3"]
        # a refused subscription is reported and forgotten
        task = asyncio.create_task(ws.subscribe("nope@bookTicker", got.append))
        await asyncio.sleep(0.3)
        await ws.handle('{"error":{"code":2,"msg":"Invalid request"},"id":2}')
        with pytest.raises(ValueError, match="refused"):
            await task
        assert list(ws.streams) == ["spyusdt@bookTicker"]
        # no answer: not acknowledged
        with pytest.raises(ValueError, match="not acknowledged"):
            await ws.subscribe("btcusdt@aggTrade", got.append, timeout=0.2)
        # everything is subscribed again after a reconnection, in one message
        ws.ws.sent.clear()
        await ws.opened()
        assert ws.ws.sent == [{"method": "SUBSCRIBE", "params": ["spyusdt@bookTicker"], "id": 4}]
    run(go())


def test_user_socket_listen_key(monkeypatch):
    async def go():
        sim = AsterSim(monkeypatch)
        user = AsterUserSocket("wss://x", Messenger(None), AsterExchange(Messenger(None), sim.client.api))
        # a new key at every (re)connection
        assert await user.endpoint() == "wss://x/ws/key1" and await user.endpoint() == "wss://x/ws/key2"
        got = []
        user.listeners.append(got.append)
        await user.handle('{"e":"ACCOUNT_UPDATE","a":{}}')
        assert got == [{"e": "ACCOUNT_UPDATE", "a": {}}]
        user.ws = Wire()
        await user.handle('{"e":"listenKeyExpired"}')                          # the stream is dead: reconnect with a new key
        assert user.ws.closed and len(got) == 1
    run(go())


def test_user_stream_unavailable_falls_back_to_polling(monkeypatch):
    async def go():
        sim, aster = await setup(monkeypatch)
        sim.user.fail = ConnectionError("no listenKey")
        states = []
        await aster.subscribe_account(["SPY"], states.append)
        assert states and aster.poll == 2.0
        sim.wallet = 12_000.0
        await asyncio.sleep(2.5)
        assert states[-1].equity == pytest.approx(12_000.0)
        await aster.stop()
    run(go())


def test_enveloppe_runs_on_aster(monkeypatch):
    """a real strategy, unchanged, with its Arcus market name, on the adapter"""
    async def go():
        sim, aster = await setup(monkeypatch)
        now = int(time.time()) // 60 * 60
        sim.klines = [[(now - 60 * (5 - i)) * 1000, "701", "701.5", "700.5", "701", "10"] for i in range(6)]
        env = Enveloppe(aster, "SPY-USD", EnveloppeParameters(LEVELS=4, SPREAD=0.01, COOLDOWN=0, POLL_DELAY=0.2, TIMEFRAME="1m", WINDOW=3))
        assert env.symbol == "SPYUSDT"
        await env.start()
        await asyncio.sleep(1.0)
        rows = list(sim.orders.values())
        assert len(rows) == 4 and all(o["side"] == "SELL" and o["timeInForce"] == "GTX" for o in rows)
        assert env.placed == 4 and not env.placed_error and env.modified >= 4 and not env.modified_error
        assert sum(float(o["origQty"]) * float(o["price"]) for o in rows) == pytest.approx(0.85 * 100_000, rel=0.01)
        ids = {o["orderId"] for o in rows}
        await sim.quote("SPYUSDT", 702.9, 703.1)
        await asyncio.sleep(1.0)
        rows = list(sim.orders.values())
        assert len(rows) == 8 and ids == {o["orderId"] for o in rows if o["side"] == "SELL"} and not env.placed_error
        await sim.fill(next(o["orderId"] for o in rows if o["side"] == "BUY"))
        await asyncio.sleep(0.5)
        assert env.account.position("SPYUSDT").size > 0
        await env.stop()
        await aster.stop()
    run(go())


def test_sockets_report_like_the_others():
    from tests.test_ws_reporting import scenario
    scenario(AsterMarketSocket("wss://x", Messenger(None)))
    scenario(AsterUserSocket("wss://x", Messenger(None), None))


def test_launcher_variants(monkeypatch):
    import main
    built = []
    monkeypatch.setattr(main, "Enveloppe", lambda exchange, market, parameters: built.append((exchange, market, parameters)))

    class Orchester:
        def __init__(self):
            self.asked = []

        def exchange(self, name, **options):
            self.asked.append((name, options))
            return name

    assert main.EXCHANGES["enveloppe_aster"] == ("aster",) and main.EXCHANGES["enveloppe_lighter"] == ("lighter",)
    o = Orchester()
    main.build(o, "enveloppe_aster")
    assert o.asked == [("aster", main.ASTER)] and built[0][:2] == ("aster", "SPY-USD") and built[0][2].COOLDOWN == 1.0
