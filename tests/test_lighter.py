import asyncio
import json
import time
import pytest
from adapters import ADAPTERS, CLIENTS
from adapters.lighter import Lighter, leverage_of, stamp
from adapters.models import Cancel, Modify, Order, Side, TIF
from adapters.utils import aggregate, source
from core.lighter.api import LighterError
from core.lighter.client import LighterClient, credentials
from core.lighter.utils.constants import (CANCEL_ALL_ABORT, CANCEL_ALL_IMMEDIATE, CANCEL_ALL_SCHEDULED, MAX_CLIENT_INDEX,
                                          TIF_GTT, TIF_IOC, TIF_POST_ONLY, TIMEFRAMES, TX_CANCEL_ALL_ORDERS,
                                          TX_CANCEL_ORDER, TX_CREATE_ORDER, TX_MODIFY_ORDER)
from core.lighter.utils.signing import SdkSigner, SigningError
from core.lighter.websocket import LighterWebSocket
from core.session import Session
from log.messenger import Messenger
from strategies.enveloppe import Enveloppe, Parameters as EnveloppeParameters
from strategies.orders import OrderTracker
from tests.lighter_sim import ACCOUNT, LighterSim

CLOID = "0x" + "ab" * 10 + "0123456789ab"          # a strategy's client id: 128 bits of hex


def run(coroutine):
    return asyncio.run(coroutine)


async def setup(monkeypatch, **options):
    sim = LighterSim(monkeypatch, **{k: options.pop(k) for k in ("taker_key", "tier", "account") if k in options})
    await sim.load()
    lighter = Lighter(sim.client, **options)
    await lighter.start()
    return sim, lighter


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


def test_registered():
    assert CLIENTS["lighter"] is LighterClient and ADAPTERS["lighter"] is Lighter


def test_markets_and_instrument(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        assert set(sim.client.info.by_name) == {"BTC", "SPY"}             # spot markets are left out
        # a market is found by its own symbol, its id, or the name another venue gives the same base
        for name in ("SPY", "spy", "SPY-USD", "SPY/USD", "SPYUSDT", 7):
            assert lighter.market(name).name == "SPY"
        assert lighter.canonical("SPY-USD") == "SPY"
        with pytest.raises(ValueError):
            lighter.market("GLD-USD")
        inst = lighter.instrument("SPY-USD")
        assert (inst.exchange, inst.symbol, inst.min_size, inst.min_notional) == ("lighter", "SPY", 0.02, 10.0)
        assert inst.tick(700) == 0.01 and inst.round_size(0.0399) == 0.03
        # passive prices are rounded away from the market, aggressive ones through it
        assert inst.round_price(700.004, Side.BUY) == 700.0 and inst.round_price(700.004, Side.SELL) == 700.01
        assert inst.round_price(700.004, Side.BUY, passive=False) == 700.01
        assert (inst.max_leverage, inst.maintenance, inst.initial) == (10, 0.06, 0.1)
        assert lighter.instrument("BTC").max_leverage == 50
        # fees of the account tier unless given
        assert (inst.maker_fee, inst.taker_fee) == (0.00004, 0.00028)
        sim.client.tier = "standard"
        assert lighter.instrument("SPY").maker_fee == 0.0 and lighter.instrument("SPY").taker_fee == 0.0
        assert Lighter(sim.client, maker_fee=0.00002, taker_fee=0.0002).fees == (0.00002, 0.0002)
        assert [x.symbol for x in lighter.listings()] == ["BTC", "SPY"] and lighter.tradable("SPY")
        await lighter.stop()
    run(go())


def test_credentials(monkeypatch):
    for field in ("ACCOUNT", "API_KEY", "PRIVATE_KEY", "TAKER_API_KEY", "TAKER_PRIVATE_KEY"):
        monkeypatch.delenv(f"LIGHTER_TESTNET_{field}", raising=False)
    assert credentials(False) == (None, {}, {})                           # nothing: read-only, public data
    monkeypatch.setenv("LIGHTER_TESTNET_ACCOUNT", "12")
    assert credentials(False) == (12, {}, {})
    monkeypatch.setenv("LIGHTER_TESTNET_PRIVATE_KEY", "0xabc")
    monkeypatch.setenv("LIGHTER_TESTNET_API_KEY", "4")
    assert credentials(False) == (12, {4: "0xabc"}, {"maker": 4, "taker": 4})
    monkeypatch.setenv("LIGHTER_TESTNET_TAKER_PRIVATE_KEY", "0xdef")
    monkeypatch.setenv("LIGHTER_TESTNET_TAKER_API_KEY", "9")
    assert credentials(False) == (12, {4: "0xabc", 9: "0xdef"}, {"maker": 4, "taker": 9})
    monkeypatch.setenv("LIGHTER_TESTNET_API_KEY", "2")                    # 0-3 belong to the Lighter apps
    with pytest.raises(ValueError):
        credentials(False)
    client = None
    monkeypatch.delenv("LIGHTER_TESTNET_PRIVATE_KEY")
    client = LighterClient(False, Messenger(None), Session(Messenger(None)))
    assert client.readonly and client.token() is None


def test_signer_missing_sdk_and_result_shapes(monkeypatch):
    import builtins
    real = builtins.__import__

    def no_lighter(name, *args, **kwargs):
        if name == "lighter":
            raise ImportError("No module named 'lighter'")
        return real(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", no_lighter)
    with pytest.raises(RuntimeError, match="uv add"):
        SdkSigner("https://x", 1, {5: "ab"})
    monkeypatch.setattr(builtins, "__import__", real)
    # (tx_type, tx_info, tx_hash, error) since SDK 1.x; (tx_info, error) before
    tx = SdkSigner.tx((14, '{"a":1}', "0xhash", None), 14, 77)
    assert (tx.type, tx.info, tx.hash, tx.client) == (14, '{"a":1}', "0xhash", 77)
    assert SdkSigner.tx(('{"a":1}', None), 15).type == 15
    with pytest.raises(SigningError, match="bad key"):
        SdkSigner.tx((None, None, None, "bad key"), 14)


def test_sdk_signer_calls_the_documented_functions(monkeypatch):
    """the SDK is not installed for the tests: a stand-in with the exact signatures of lighter-python's SignerClient
    (sync sign_* methods, keyword-only nonce / api_key_index) checks every call the engine makes"""
    import sys
    import types
    seen = []

    class SignerClient:
        def __init__(self, url, account_index, api_private_keys, nonce_management_type=None):
            self.args = (url, account_index, api_private_keys)

        def sign_create_order(self, market_index, client_order_index, base_amount, price, is_ask, order_type, time_in_force,
                              reduce_only=False, trigger_price=0, order_expiry=-1, *, integrator_account_index=0,
                              integrator_taker_fee=0, integrator_maker_fee=0, skip_nonce=0, nonce=-1, api_key_index=255):
            seen.append(("create", market_index, client_order_index, base_amount, price, is_ask, order_type, time_in_force,
                         reduce_only, trigger_price, order_expiry, nonce, api_key_index))
            return 14, '{"x":1}', "0xh", None

        def sign_modify_order(self, market_index, order_index, base_amount, price, trigger_price=0, *, integrator_account_index=0,
                              integrator_taker_fee=0, integrator_maker_fee=0, skip_nonce=0, nonce=-1, api_key_index=255):
            seen.append(("modify", market_index, order_index, base_amount, price, trigger_price, nonce, api_key_index))
            return 17, "{}", "0xh", None

        def sign_cancel_order(self, market_index, order_index, *, skip_nonce=0, nonce=-1, api_key_index=255):
            seen.append(("cancel", market_index, order_index, nonce, api_key_index))
            return 15, "{}", "0xh", None

        def sign_cancel_all_orders(self, time_in_force, timestamp_ms, *, skip_nonce=0, nonce=-1, api_key_index=255):
            seen.append(("cancel_all", time_in_force, timestamp_ms, nonce, api_key_index))
            return 16, "{}", "0xh", None

        def sign_update_leverage(self, market_index, fraction, margin_mode, *, skip_nonce=0, nonce=-1, api_key_index=255):
            seen.append(("leverage", market_index, fraction, margin_mode, nonce, api_key_index))
            return None, None, None, "leverage too high"

        def create_auth_token_with_expiry(self, deadline=-1, *, timestamp=None, api_key_index=255):
            seen.append(("auth", deadline, api_key_index))
            return f"{timestamp + deadline}:1:{api_key_index}:sig", None

        def check_client(self):
            return None
    monkeypatch.setitem(sys.modules, "lighter", types.SimpleNamespace(SignerClient=SignerClient))
    signer = SdkSigner("https://x", 42, {5: "0xabcd"})
    assert signer.client.args == ("https://x", 42, {5: "abcd"})                       # keys without 0x
    tx = signer.create_order(5, 10, 7, 123, 5, 69900, False, TIF_POST_ONLY, True, 99)
    assert (tx.type, tx.info, tx.hash, tx.client) == (14, '{"x":1}', "0xh", 123)
    assert seen[-1] == ("create", 7, 123, 5, 69900, 0, 0, TIF_POST_ONLY, 1, 0, 99, 10, 5)
    assert signer.modify_order(5, 11, 7, 123, 4, 69800).type == 17 and seen[-1] == ("modify", 7, 123, 4, 69800, 0, 11, 5)
    assert signer.cancel_order(5, 12, 7, 123).type == 15 and seen[-1] == ("cancel", 7, 123, 12, 5)
    assert signer.cancel_all(5, 13, CANCEL_ALL_SCHEDULED, 1234).type == 16 and seen[-1] == ("cancel_all", CANCEL_ALL_SCHEDULED, 1234, 13, 5)
    with pytest.raises(SigningError, match="leverage too high"):
        signer.update_leverage(5, 14, 7, 500, 0)
    with pytest.raises(SigningError, match="updateMargin"):                           # not in this SDK version
        signer.update_margin(5, 15, 7, 1_000_000, 1)
    token = signer.auth(5, 3600)
    assert token.endswith(":1:5:sig") and abs(int(token.split(":")[0]) - time.time() - 3600) < 5 and signer.check() is None


def test_place_post_only(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        before = time.time() * 1000
        r, = await lighter.place([Order("SPY-USD", Side.BUY, 0.0599, 699.004, TIF.ALO, client=CLOID)])
        index = int(CLOID, 16) & MAX_CLIENT_INDEX
        assert r.ok and r.oid == str(index) and r.status == "accepted" and r.filled == 0
        call = sim.signer.calls[-1]
        # integers on the wire: price x 100, size x 100, floored; post-only; expires in 28 days
        assert (call["market"], call["client"], call["base"], call["price"], call["is_ask"], call["tif"], call["reduce"]) == \
            (7, index, 5, 69900, False, TIF_POST_ONLY, False)
        assert 27.9 * 86_400_000 < call["expiry"] - before < 28.1 * 86_400_000
        assert (call["key"], call["nonce"]) == (5, 10)                    # the nonce read from the exchange once
        path, form = sim.posts[-1]
        assert path == "/api/v1/sendTx" and form["tx_type"] == TX_CREATE_ORDER and json.loads(form["tx_info"])["nonce"] == 10
        await settle()
        orders = await lighter.open_orders("SPY")
        assert [(o.symbol, o.oid, o.side, o.price, o.size, o.tif, o.client) for o in orders] == \
            [("SPY", str(index), Side.BUY, 699.0, 0.05, TIF.ALO, CLOID)]
        # GTC and reduce-only; a sell is rounded up
        r, = await lighter.place([Order("SPY", Side.SELL, 0.05, 701.001, TIF.GTC, reduce=True)])
        call = sim.signer.calls[-1]
        assert r.ok and (call["tif"], call["reduce"], call["is_ask"], call["price"], call["nonce"]) == (TIF_GTT, True, True, 70101, 11)
        assert 0 < int(r.oid) <= MAX_CLIENT_INDEX                         # no client id given: a random 48-bit index
        await lighter.stop()
    run(go())


def test_place_refused_locally(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        results = await lighter.place([
            Order("SPY", Side.BUY, 0.004, 700, TIF.ALO),        # rounds to 0
            Order("SPY", Side.BUY, 0.01, 700, TIF.ALO),         # under min_base_amount
            Order("BTC", Side.BUY, 0.00005, 100000, TIF.ALO),   # 5 $ < min_quote_amount
            Order("GLD-USD", Side.BUY, 1, 300, TIF.ALO),        # not listed
        ])
        assert [r.ok for r in results] == [False] * 4 and not sim.posts
        assert "rounds to 0" in results[0].error and "< min" in results[1].error and "< min" in results[2].error
        assert "unknown Lighter market" in results[3].error
        # the minimums are those of resting orders: a reduce-only order may be smaller
        r, = await lighter.place([Order("SPY", Side.SELL, 0.01, 701, TIF.ALO, reduce=True)])
        assert r.ok
        sim.client.info.market("SPY").status = "frozen"
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)])
        assert not r.ok and "frozen" in r.error
        await lighter.stop()
    run(go())


def test_batch_nonces_and_chunks(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        orders = [Order("SPY", Side.BUY, 0.05, 690 + i * 0.01, TIF.ALO) for i in range(32)]
        results = await lighter.place(orders)
        assert all(r.ok for r in results) and len({r.oid for r in results}) == 32
        # REST batches of 15, sent one after the other on the same key, consecutive nonces
        assert [(p, len(json.loads(f["tx_infos"]))) for p, f in sim.posts if p.endswith("Batch")] == \
            [("/api/v1/sendTxBatch", 15), ("/api/v1/sendTxBatch", 15), ("/api/v1/sendTxBatch", 2)]
        assert [c["nonce"] for c in sim.signer.calls] == list(range(10, 42))
        assert len([r for r in sim.requests if r[0].endswith("nextNonce")]) == 1
        assert lighter.budget() == {"order": 1000 - 32, "cancel": 10**9}  # one unit of volume quota per order
        await lighter.stop()
    run(go())


def test_ioc_reports_its_real_outcome(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        # the API only says "accepted": the adapter waits for the order to end and answers like Hyperliquid
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 700.20, TIF.IOC, client=CLOID)])
        assert r.ok and r.filled == 0.05 and r.price == pytest.approx(700.10) and r.status == "filled"
        assert sim.signer.calls[-1]["tif"] == TIF_IOC and sim.signer.calls[-1]["expiry"] == 0
        # aggressive price rounded through the market: a buy up
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 700.101, TIF.IOC)])
        assert sim.signer.calls[-1]["price"] == 70011
        # nothing matched: a failure (the hedge then retries deeper), never an "accepted" order
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699.00, TIF.IOC)])
        assert not r.ok and "IOC could not match" in r.error and "slippage" in r.error and r.filled == 0
        assert not lighter.waiters
        await lighter.stop()
    run(go())


def test_ioc_unknown_outcome_is_not_a_failure(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch, ioc_timeout=0.3)
        sim.ws.callbacks_backup = None
        original = sim.push_order

        async def mute(row):          # the order channel stays silent
            return None
        sim.push_order = mute
        r, = await lighter.place([Order("SPY", Side.SELL, 0.05, 699.00, TIF.IOC)])
        # ok with nothing filled: strategies.hedging counts it as "unknown, filled until its fills come"
        assert r.ok and r.filled == 0 and r.status == "accepted" and not lighter.waiters
        sim.push_order = original
        # a network error: raised, never turned into "refused" (the order may be in)
        sim.fail.append(ConnectionError("reset"))
        with pytest.raises(ConnectionError):
            await lighter.place([Order("SPY", Side.SELL, 0.05, 699.00, TIF.IOC)])
        assert not lighter.waiters and sim.client.exchange.lane("taker").dirty
        await lighter.stop()
    run(go())


def test_taker_key_has_its_own_lane(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch, taker_key=True)
        results = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO), Order("SPY", Side.BUY, 0.05, 700.2, TIF.IOC),
                                       Order("SPY", Side.SELL, 0.05, 701, TIF.ALO)])
        assert [r.ok for r in results] == [True, True, True] and results[1].filled == 0.05
        calls = {(c["key"], c["nonce"], c["tif"]) for c in sim.signer.calls}
        # quotes on key 5, the IOC on key 6 with its own nonces: a taker order (speed bump) never delays the quotes
        assert calls == {(5, 10, TIF_POST_ONLY), (5, 11, TIF_POST_ONLY), (6, 100, TIF_IOC)}
        await lighter.stop()
    run(go())


def test_nonce_recovery(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        order = Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)
        assert (await lighter.place([order]))[0].ok
        sim.nonces[5] = 50                                # another process used the key: ours is wrong
        r, = await lighter.place([order])
        assert not r.ok and "invalid nonce" in r.error
        r, = await lighter.place([order])                 # read again from the exchange, never guessed
        assert r.ok and sim.signer.calls[-1]["nonce"] == 50
        # a business refusal of the API: reported, and the nonce (not used) is read again too
        sim.fail.append(LighterError(400, 21739, "not enough margin", "sendTx"))
        r, = await lighter.place([order])
        assert not r.ok and "not enough margin" in r.error
        assert (await lighter.place([order]))[0].ok and sim.signer.calls[-1]["nonce"] == 51
        # a rate limit is raised with its status: the Gateway holds the next sends
        sim.fail.append(LighterError(429, 23000, "Too Many Requests!", "sendTx"))
        with pytest.raises(LighterError) as e:
            await lighter.place([order])
        assert e.value.status == 429 and e.value.retry == 1000
        assert LighterError(400, 23000, "x", "ws").status == 429          # the websocket codes too
        await lighter.stop()
    run(go())


def test_reconcile_sees_dropped_transactions(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        sim.drop = True                                   # accepted by the API (200), never executed
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)])
        lane = sim.client.exchange.lane()
        assert r.ok and lane.nonce == 11 and sim.nonces[5] == 10
        await sim.client.exchange.reconcile(quiet=0.0)    # the idle check compares with the exchange
        assert lane.nonce == 10
        sim.drop = False
        assert (await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)]))[0].ok
        await lighter.stop()
    run(go())


def test_order_updates_and_fills(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        updates, fills = [], []
        assert await lighter.subscribe_orders(["SPY"], updates.append)
        await lighter.subscribe_fills(["SPY"], fills.append)
        assert sim.ws.private == {f"account_all_orders:{ACCOUNT}": True, f"account_tx:{ACCOUNT}": True,
                                  f"account_all_trades:{ACCOUNT}": False}
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO, client=CLOID)])
        await settle()
        u = updates[-1]
        assert (u.symbol, u.oid, u.side, u.price, u.size, u.remaining, u.status, u.client) == \
            ("SPY", r.oid, Side.BUY, 699.0, 0.05, 0.05, "open", CLOID)
        order_index = next(iter(sim.orders))
        await sim.fill(order_index, 0.02)
        f = fills[-1]
        # our side, maker, the id the strategy knows, and the fee: a rate in millionths of the notional
        assert (f.symbol, f.side, f.size, f.price, f.oid, f.client, f.maker) == ("SPY", Side.BUY, 0.02, 699.0, r.oid, CLOID, True)
        assert f.fee == pytest.approx(0.02 * 699 * 40e-6)
        assert updates[-1].status == "open" and updates[-1].remaining == pytest.approx(0.03)
        trade = await sim.fill(order_index)
        assert updates[-1].status == "filled" and updates[-1].remaining == 0 and len(fills) == 2
        # the same trade again (snapshot, REST recovery): not published twice
        await sim.ws.push(f"account_all_trades/{ACCOUNT}", {"trades": {"7": [trade]}})
        assert len(fills) == 2
        # a post-only order that would cross is cancelled on arrival: a refusal for the strategies
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 700.50, TIF.ALO)])
        await settle()
        assert r.ok and updates[-1].status == "rejected" and updates[-1].raw["rejectionReason"] == "canceled-post-only"
        # an order of another origin keeps the exchange index as id
        row = sim.row({"market": 7, "client": 0, "base": 5, "price": 69000, "is_ask": False, "reduce": False,
                       "tif": TIF_POST_ONLY, "expiry": 0}, "open")
        await sim.push_order(row)
        assert updates[-1].oid == str(row["order_index"]) and updates[-1].client is None
        await lighter.stop()
    run(go())


def test_tracker_follows_the_whole_life_of_an_order(monkeypatch):
    """the id given by place is the one of every later update and fill: the shared OrderTracker never loses the order"""
    async def go():
        sim, lighter = await setup(monkeypatch)
        tracker = OrderTracker()
        fills = []

        def on_update(u):
            tracker.update(u)

        def on_fill(f):
            fills.append(tracker.fill(f))
        await lighter.subscribe_orders(["SPY"], on_update)
        await lighter.subscribe_fills(["SPY"], on_fill)
        w = tracker.new("SPY", Side.SELL, 0, 701, 0.06)
        tracker.placed(w, (await lighter.place([Order("SPY", Side.SELL, 0.06, 701, TIF.ALO, client=w.client)]))[0])
        await settle()
        assert w.state == "open" and tracker.live("SPY") == [w] and w.oid in lighter.orders
        await sim.fill(next(iter(sim.orders)), 0.02)
        assert fills[-1] is w and w.remaining == pytest.approx(0.04)
        # partly filled: the modify is a cancel + a new order, the tracker follows the new id under the same client id
        w.state, w.target = "moving", (701.5, 0.04)
        res, = await lighter.modify([Modify(w.open_order(), 0.04, 701.5, TIF.ALO)])
        tracker.moved(w, res)
        await settle()
        assert res.ok and w.state == "open" and w.price == 701.5 and w.oid == res.oid and tracker.live("SPY") == [w]
        assert [float(o["price"]) for o in sim.orders.values()] == [701.5]
        res, = await lighter.cancel([Cancel("SPY", w.oid, w.client)])
        tracker.canceled(w, res)
        await settle()
        assert res.ok and not tracker.live("SPY") and not sim.orders
        await lighter.stop()
    run(go())


def test_modify(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        updates = []
        await lighter.subscribe_orders(["SPY"], updates.append)
        r, = await lighter.place([Order("SPY", Side.BUY, 0.10, 699, TIF.ALO, client=CLOID)])
        await settle()
        order = (await lighter.open_orders("SPY"))[0]
        # untouched order: a native modify, same id
        m, = await lighter.modify([Modify(order, 0.08, 698.996, TIF.ALO)])
        call = sim.signer.calls[-1]
        assert m.ok and m.oid == r.oid and (call["type"], call["index"], call["base"], call["price"]) == (TX_MODIFY_ORDER, int(r.oid), 8, 69899)
        await settle()
        assert updates[-1].price == 698.99 and updates[-1].remaining == 0.08
        # partly filled: what "base_amount" means then is not documented, so cancel + new order in ONE batch
        await sim.fill(next(iter(sim.orders)), 0.03)
        order = (await lighter.open_orders("SPY"))[0]
        assert order.size == pytest.approx(0.05)
        m, = await lighter.modify([Modify(order, 0.05, 698.50, TIF.ALO)])
        cancel, create = sim.signer.calls[-2:]
        assert m.ok and m.oid != r.oid
        assert (cancel["type"], cancel["index"], create["type"], create["base"], create["price"], create["tif"]) == \
            (TX_CANCEL_ORDER, int(r.oid), TX_CREATE_ORDER, 5, 69850, TIF_POST_ONLY)
        assert cancel["nonce"] + 1 == create["nonce"] and sim.posts[-1][0].endswith("sendTxBatch")
        await settle()
        old = [u for u in updates if u.oid == r.oid][-1]
        new = [u for u in updates if u.oid == m.oid][-1]
        # the old one is "replaced" (the trackers ignore it), the new one carries the strategy's client id
        assert old.status == "replaced" and (new.status, new.client, new.remaining) == ("open", CLOID, 0.05)
        # local refusals
        bad = await lighter.modify([Modify(order, 0.001, 698, TIF.ALO),
                                    Modify(type(order)("SPY", None, Side.BUY, 698, 0.05), 0.05, 698, TIF.ALO)])
        assert [b.ok for b in bad] == [False, False] and "NEVER PLACED" in bad[1].error
        await lighter.stop()
    run(go())


def test_cancel(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        a, b = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO, client=CLOID), Order("BTC", Side.SELL, 0.001, 100500, TIF.ALO)])
        quota = sim.quota
        # by client id alone (its index is derived, no table needed), and by id
        results = await lighter.cancel([Cancel("SPY", None, CLOID), Cancel("BTC", b.oid)])
        assert [r.ok for r in results] == [True, True] and [r.oid for r in results] == [a.oid, b.oid]
        await settle()
        assert not sim.orders and sim.quota == quota                      # a cancel uses no volume quota
        # cancel_all of one market = the cancels of its orders; without market = one cancel-all transaction
        await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO), Order("BTC", Side.SELL, 0.001, 100500, TIF.ALO)])
        await lighter.cancel_all("SPY")
        assert [o["market_index"] for o in sim.orders.values()] == [0] and sim.signer.calls[-1]["type"] == TX_CANCEL_ORDER
        await lighter.cancel_all()
        assert not sim.orders and (sim.signer.calls[-1]["type"], sim.signer.calls[-1]["tif"]) == (TX_CANCEL_ALL_ORDERS, CANCEL_ALL_IMMEDIATE)
        # dead man's switch: scheduled cancel-all, replaced by each call, aborted by None
        now = time.time() * 1000
        assert await lighter.schedule_cancel(30)
        assert sim.signer.calls[-1]["tif"] == CANCEL_ALL_SCHEDULED and 29_000 < sim.cancel_at - now < 31_000
        assert await lighter.schedule_cancel(None) and sim.signer.calls[-1]["tif"] == CANCEL_ALL_ABORT and sim.cancel_at is None
        await lighter.stop()
    run(go())


def test_failed_transaction_becomes_a_rejection(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        updates = []
        await lighter.subscribe_orders(["SPY"], updates.append)
        sim.drop = True
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO, client=CLOID)])
        assert r.ok                                                       # accepted by the API...
        await sim.ws.push(f"account_tx/{ACCOUNT}", {"txs": [{"hash": r.raw["tx_hash"], "type": 14, "status": 0,
                                                             "api_key_index": 5, "event_info": '{"ae":"margin"}'}]})
        assert not updates                                                # believed only if the order never shows up
        await asyncio.sleep(1.2)
        u = updates[-1]                                                   # ...refused by the sequencer
        assert (u.oid, u.status, u.client) == (r.oid, "rejected", CLOID) and "margin" in u.raw["rejectionReason"]
        assert sim.client.exchange.lane().dirty
        # an order that DID appear is never rejected by a transaction status
        sim.drop = False
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO)])
        await settle()
        n = len(updates)
        await sim.ws.push(f"account_tx/{ACCOUNT}", {"txs": [{"hash": r.raw["tx_hash"], "type": 14, "status": 0, "api_key_index": 5}]})
        await asyncio.sleep(1.2)
        assert len(updates) == n and updates[-1].status == "open"
        await lighter.stop()
    run(go())


def test_account(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch, account_interval=0.5)
        states = []
        await lighter.subscribe_account(["SPY"], states.append)
        assert (states[-1].equity, states[-1].free, states[-1].positions) == (10_000.0, 10_000.0, {})
        r, = await lighter.place([Order("SPY", Side.BUY, 1.0, 699, TIF.ALO)])
        await settle()
        await sim.fill(next(iter(sim.orders)))
        await asyncio.sleep(0.2)                           # pushed by the websocket, no REST read
        p = states[-1].position("SPY")
        assert (p.size, p.entry, p.leverage, p.value) == (1.0, 699.0, 10.0, 699.0) and states[-1].leverages["SPY"] == 10.0
        reads = len([x for x in sim.requests if x[0].endswith("/account")])
        # user_stats: equity and free margin
        await sim.ws.push(f"user_stats/{ACCOUNT}", {"stats": {"portfolio_value": "10050.5", "available_balance": "9000.25"}})
        await asyncio.sleep(0.2)
        assert (states[-1].equity, states[-1].free, states[-1].margin) == (10050.5, 9000.25, pytest.approx(1050.25))
        assert len([x for x in sim.requests if x[0].endswith("/account")]) == reads
        # a short position, isolated, with a liquidation price; a closed position disappears
        await sim.ws.push(f"account_all_positions/{ACCOUNT}", {"positions": {"0": {
            "market_id": 0, "sign": -1, "position": "0.01", "avg_entry_price": "100000", "position_value": "1000",
            "unrealized_pnl": "-5", "initial_margin_fraction": "0.05", "margin_mode": 1, "allocated_margin": "50",
            "liquidation_price": "104000"}, "7": {**sim.positions[7], "position": "0"}}})
        await asyncio.sleep(0.2)
        btc = states[-1].position("BTC")
        assert (btc.size, btc.leverage, btc.mode.value, btc.liquidation, btc.margin, btc.pnl) == (-0.01, 20.0, "isolated", 104000.0, 50.0, -5.0)
        assert "SPY" not in states[-1].positions
        # account(): REST at most once per account_interval, the stream in between
        sim.collateral = 20_000.0
        await asyncio.sleep(0.6)
        assert (await lighter.account([])).equity == pytest.approx(20_000.0)
        sim.collateral = 30_000.0
        assert (await lighter.account([])).equity == pytest.approx(20_000.0)
        assert list((await lighter.portfolio()).details) == [f"account {ACCOUNT}"]
        assert leverage_of("10.00") == 10 and leverage_of("0.1") == 10 and leverage_of("") is None
        await lighter.stop()
    run(go())


def test_leverage_margin_budget(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        assert await lighter.set_leverage("SPY", 25) and sim.leverage[7] == (1000, 0)      # capped at the market max (10x)
        assert await lighter.set_leverage("BTC", 20, cross=False) and sim.leverage[0] == (500, 1)
        assert await lighter.add_margin("BTC", 12.5) and sim.margin == [(0, 12_500_000)]
        assert lighter.budget() == {"order": sim.quota, "cancel": 10**9}
        sim.client.tier = "standard"                                       # no volume quota on a Standard account
        assert lighter.budget() is None
        await lighter.stop()
    run(go())


def test_market_data(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        bbos, prints, books, oracle = [], [], [], []
        await lighter.subscribe_bbo("SPY-USD", bbos.append)
        assert (bbos[-1].symbol, bbos[-1].bid, bbos[-1].ask, bbos[-1].bid_size, bbos[-1].ask_size) == ("SPY", 699.9, 700.1, 5.0, 4.0)
        assert abs(bbos[-1].ts - time.time()) < 5                          # microseconds on the wire
        await sim.quote(7, 700.0, 700.2)
        assert await lighter.mid("SPY") == pytest.approx(700.1)
        assert (await lighter.book("BTC")).mid == pytest.approx(100000.0)
        # trades: the taker's side; the snapshot of past trades is not replayed
        assert await lighter.subscribe_trades("SPY", prints.append)
        row = {"trade_id": 9, "size": "0.5", "price": "700.2", "is_maker_ask": True, "ask_account_id": 1, "bid_account_id": 2,
               "ask_id": 11, "bid_id": 12, "timestamp": 1_800_000_000_123}
        await sim.ws.push("trade/7", {"trades": [row]}, "subscribed")
        assert not prints
        await sim.ws.push("trade/7", {"trades": [row, {**row, "trade_id": 10, "is_maker_ask": False}]})
        assert [(p.side, p.price, p.size, p.maker, p.taker, p.id) for p in prints] == \
            [(Side.BUY, 700.2, 0.5, "1", "2", "9"), (Side.SELL, 700.2, 0.5, "2", "1", "10")]
        assert prints[0].ts == pytest.approx(1_800_000_000.123)
        # depth: snapshot, then deltas chained by nonce; a gap resubscribes
        assert await lighter.subscribe_depth("SPY", lambda b, a, ts: books.append((b, a)), levels=2)
        await sim.ws.push("order_book/7", {"order_book": {"bids": [{"price": "699.9", "size": "1"}, {"price": "699.8", "size": "2"}, {"price": "699.7", "size": "3"}],
                                                          "asks": [{"price": "700.1", "size": "1"}], "nonce": 5}}, "subscribed")
        assert books[-1] == ([(699.9, 1.0), (699.8, 2.0)], [(700.1, 1.0)])
        await sim.ws.push("order_book/7", {"order_book": {"bids": [{"price": "699.9", "size": "0"}], "asks": [{"price": "700.0", "size": "7"}],
                                                          "begin_nonce": 5, "nonce": 6}})
        assert books[-1] == ([(699.8, 2.0), (699.7, 3.0)], [(700.0, 7.0), (700.1, 1.0)])
        n = len(books)
        await sim.ws.push("order_book/7", {"order_book": {"bids": [{"price": "1", "size": "1"}], "asks": [], "begin_nonce": 9, "nonce": 10}})
        await settle()
        assert len(books) == n                                             # an update was missed: nothing published from it
        # oracle = index price; funding in PERCENT per hour on the wire -> fraction per hour
        assert await lighter.subscribe_oracle("SPY", lambda s, p: oracle.append((s, p)))
        assert oracle[-1] == ("SPY", 700.05) and lighter.mark("SPY") == 700.02
        f = (await lighter.fundings(["SPY-USD"]))["SPY"]
        assert f.rate == pytest.approx(0.000008) and f.predicted == pytest.approx(0.000012) and not f.closed
        assert f.next_at % 3600 == 0 and 0 < f.next_at - time.time() <= 3600
        assert await lighter.funding_history("SPY", 0) == [(1_700_000_000.0, pytest.approx(0.00001)), (1_700_003_600.0, pytest.approx(-0.00002))]
        pay, = await lighter.funding_payments(0)
        assert (pay.symbol, pay.usd, pay.rate) == ("SPY", -0.42, pytest.approx(0.00001))
        volume = await lighter.volume()
        assert volume.volume == {"24h": 1000.5, "7j": 7000.0, "30j": 30000.0} and volume.total == 123456.0
        await lighter.stop()
    run(go())


def test_candles(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        sim.candles = [{"t": (1_800_000_000 // 180 * 180 + 60 * i) * 1000, "o": 100 + i, "h": 101 + i, "l": 99 + i, "c": 100.5 + i, "v": 1.0}
                       for i in range(9)]
        candles = await lighter.candles("SPY", "1m", 3)
        assert [c.open for c in candles] == [106, 107, 108] and candles[0].time < candles[-1].time
        assert sim.requests[-1][1]["resolution"] == "1m"
        # 3m does not exist on Lighter: built from 1m, aligned
        three = await lighter.candles("SPY", "3m", 2)
        assert sim.requests[-1][1]["resolution"] == "1m" and sim.requests[-1][1]["count_back"] == 9
        assert [(c.open, c.high, c.low, c.close, c.volume) for c in three] == [(103, 106, 102, 105.5, 3.0), (106, 109, 105, 108.5, 3.0)]
        assert source(TIMEFRAMES, 7200) == "1h" and source(TIMEFRAMES, 30) is None
        assert aggregate([], 60) == []
        with pytest.raises(ValueError):
            await lighter.candles("SPY", "15s", 2)
        assert stamp(1_800_000_000) == stamp(1_800_000_000_000) == stamp(1_800_000_000_000_000) == 1_800_000_000
        await lighter.stop()
    run(go())


def test_websocket_transport(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch, transport="ws")
        assert lighter.transport == "ws"
        orders = [Order("SPY", Side.BUY, 0.05, 690 + i * 0.01, TIF.ALO) for i in range(60)]
        results = await lighter.place(orders)
        assert all(r.ok for r in results) and not sim.posts
        # 50 per websocket message, then the rest; the batch fields are JSON strings like the REST form
        assert [(k, len(json.loads(d["tx_infos"]))) for k, d in sim.ws.frames] == [("jsonapi/sendtxbatch", 50), ("jsonapi/sendtxbatch", 10)]
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 689, TIF.ALO)])
        kind, data = sim.ws.frames[-1]
        assert r.ok and kind == "jsonapi/sendtx" and data["tx_type"] == TX_CREATE_ORDER and isinstance(data["tx_info"], dict)
        # a refusal over the websocket is reported like the REST one, and the nonce is read again
        sim.nonces[5] += 3
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 689, TIF.ALO)])
        assert not r.ok and "invalid nonce" in r.error
        assert (await lighter.place([Order("SPY", Side.BUY, 0.05, 689, TIF.ALO)]))[0].ok
        # no reply: unknown outcome, raised (not "refused"), nothing left in flight
        sim.silent, sim.client.exchange.timeout = True, 0.2
        with pytest.raises(asyncio.TimeoutError):
            await lighter.place([Order("SPY", Side.BUY, 0.05, 689, TIF.ALO)])
        lane = sim.client.exchange.lane()
        assert lane.dirty and lane.inflight == 0
        sim.silent = False
        # socket down when sending: REST takes it (nothing was sent on the socket)
        sim.ws.connected.clear()
        assert lighter.transport == "rest"
        r, = await lighter.place([Order("SPY", Side.BUY, 0.05, 689, TIF.ALO)])
        assert r.ok and sim.posts[-1][0] == "/api/v1/sendTx"
        await lighter.stop()
    run(go())


def test_reconnection_recovers_what_was_missed(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        updates, fills = [], []
        await lighter.subscribe_orders(["SPY"], updates.append)
        await lighter.subscribe_fills(["SPY"], fills.append)
        a, b = await lighter.place([Order("SPY", Side.BUY, 0.05, 699, TIF.ALO, client=CLOID), Order("SPY", Side.SELL, 0.05, 701, TIF.ALO)])
        await settle()
        # the socket drops; meanwhile the first order fills
        saved, sim.ws.callbacks = sim.ws.callbacks, {}
        await sim.fill(next(i for i, o in sim.orders.items() if not o["is_ask"]))
        assert not fills
        sim.ws.callbacks = saved
        for listener in sim.ws.connect_listeners:
            await listener()
        await sim.snapshot(f"account_all_orders/{ACCOUNT}")               # every channel sends a fresh snapshot
        await asyncio.sleep(0.1)
        # the fill is read by REST first, then the order that vanished is closed: fill before close, like live
        assert [(f.oid, f.size, f.client) for f in fills] == [(a.oid, 0.05, CLOID)]
        assert [(u.oid, u.status) for u in updates if u.oid == a.oid][-1] == (a.oid, "canceled")
        assert b.oid in lighter.orders and a.oid not in lighter.orders
        await lighter.stop()
    run(go())


class Wire:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text))


def socket():
    ws = LighterWebSocket("wss://x", Messenger(None))
    ws.ws, ws.running = Wire(), True
    ws.connected.set()
    return ws


def test_socket_subscriptions_and_routing():
    async def go():
        ws = socket()
        ws.token = lambda: "tok"
        got = []
        task = asyncio.create_task(ws.subscribe("account_all_orders/7", got.append, private=True))
        await asyncio.sleep(0)
        assert ws.ws.sent == [{"type": "subscribe", "channel": "account_all_orders/7", "auth": "tok"}]
        # requests name the channel with slashes, messages with colons
        await ws.handle(json.dumps({"type": "subscribed/account_all_orders", "channel": "account_all_orders:7", "orders": {}}))
        await task
        await ws.handle(json.dumps({"type": "update/account_all_orders", "channel": "account_all_orders:7", "orders": {"1": []}}))
        assert [m["type"] for m in got] == ["subscribed/account_all_orders", "update/account_all_orders"]
        # a refused subscription fails at once with the reason
        task = asyncio.create_task(ws.subscribe("ticker/999", got.append))
        await asyncio.sleep(0)
        await ws.handle(json.dumps({"error": {"code": 30003, "message": "invalid channel"}}))
        with pytest.raises(ValueError, match="invalid channel"):
            await task
        assert "ticker:999" not in ws.subscriptions
        # after a reconnection every channel is asked again, private ones with the current token
        ws.ws.sent.clear()
        await ws.opened()
        assert ws.ws.sent == [{"type": "subscribe", "channel": "account_all_orders/7", "auth": "tok"}]
        await ws.handle('{"type":"ping"}')
        assert ws.ws.sent[-1] == {"type": "pong"}
        # a private channel without token (read-only client) is refused before anything is sent
        ws.token = lambda: None
        with pytest.raises(ValueError, match="auth token"):
            await ws.subscribe("account_tx/7", got.append, private=True)
    run(go())


def test_socket_transaction_replies():
    async def go():
        ws = socket()
        first = await ws.post("jsonapi/sendtx", {"tx_type": 14, "tx_info": {}})
        second = await ws.post("jsonapi/sendtx", {"tx_type": 14, "tx_info": {}})
        assert [m["data"]["id"] for m in ws.ws.sent] == ["1", "2"]
        # matched by the id we sent, wherever the server echoes it
        await ws.handle(json.dumps({"type": "jsonapi/sendtx", "data": {"id": "2", "code": 200, "tx_hash": "b"}}))
        await ws.handle(json.dumps({"id": "1", "code": 200, "tx_hash": "a"}))
        assert ws.parse(first.result(), "sendTx")["tx_hash"] == "a" and ws.parse(second.result(), "sendTx")["tx_hash"] == "b"
        # no id echoed: replies come in the order of the requests
        third = await ws.post("jsonapi/sendtx", {"tx_type": 14, "tx_info": {}})
        fourth = await ws.post("jsonapi/sendtx", {"tx_type": 14, "tx_info": {}})
        await ws.handle(json.dumps({"type": "jsonapi/sendtx", "data": {"code": 200, "tx_hash": "c"}}))
        await ws.handle(json.dumps({"error": {"code": 21104, "message": "invalid nonce"}}))
        assert ws.parse(third.result(), "sendTx")["tx_hash"] == "c"
        with pytest.raises(LighterError) as e:
            ws.parse(fourth.result(), "sendTx")
        assert e.value.code == 21104
        with pytest.raises(LighterError):
            ws.parse({"data": {"code": 21500, "message": "bad tx"}}, "sendTx")
        # a cut fails what was waiting
        fifth = await ws.post("jsonapi/sendtx", {"tx_type": 14, "tx_info": {}})
        ws.closed()
        with pytest.raises(ConnectionError):
            fifth.result()
        ws.connected.clear()
        with pytest.raises(ConnectionError):
            await ws.post("jsonapi/sendtx", {})
    run(go())


def test_auth_token_renewal(monkeypatch):
    async def go():
        sim, lighter = await setup(monkeypatch)
        client = sim.client
        first = client.token()
        assert client.token() == first and sim.signer.tokens == 1          # one token for 6 hours
        client.tokens = (first, time.time() - 7 * 3600)
        second = client.token()
        assert second != first and client.renewed                          # the private channels must subscribe again
        await lighter.stop()
    run(go())


def test_enveloppe_runs_on_lighter(monkeypatch):
    """a real strategy, unchanged, with its Arcus market name, on the adapter"""
    async def go():
        sim, lighter = await setup(monkeypatch, account_interval=0.0)
        now = int(time.time()) // 60 * 60
        sim.candles = [{"t": (now - 60 * (5 - i)) * 1000, "o": 701, "h": 701.5, "l": 700.5, "c": 701, "v": 10} for i in range(6)]
        env = Enveloppe(lighter, "SPY-USD", EnveloppeParameters(LEVELS=4, SPREAD=0.01, COOLDOWN=0, POLL_DELAY=0.2, TIMEFRAME="1m", WINDOW=3))
        assert env.symbol == "SPY"
        await env.start()
        await asyncio.sleep(1.0)
        # the mid (700) is under the average (701): the grid of asks goes in, post-only, sized on the 10x of the market
        rows = list(sim.orders.values())
        assert len(rows) == 4 and all(o["is_ask"] and o["time_in_force"] == "post-only" for o in rows)
        assert min(float(o["price"]) for o in rows) > 700.1 and env.placed == 4 and not env.placed_error
        assert sum(float(o["initial_base_amount"]) * float(o["price"]) for o in rows) == pytest.approx(0.85 * 100_000, rel=0.01)
        ids = {o["client_order_index"] for o in rows}
        # the cycle after a placement re-prices the grid: modifies of the SAME orders (native: none is partly filled)
        assert env.modified >= 4 and not env.modified_error
        # the market goes over the average: the other side goes in, the asks are still the same orders
        await sim.quote(7, 702.9, 703.1)
        await asyncio.sleep(1.0)
        rows = list(sim.orders.values())
        assert len(rows) == 8 and ids == {o["client_order_index"] for o in rows if o["is_ask"]}
        assert max(float(o["price"]) for o in rows if not o["is_ask"]) < 702.9 and not env.placed_error and not env.modified_error
        # a fill reaches the strategy's account tracker through the adapter
        await sim.fill(next(i for i, o in sim.orders.items() if not o["is_ask"]))
        await asyncio.sleep(0.3)
        assert env.account.position("SPY").size > 0
        await env.stop()
        await lighter.stop()
    run(go())
