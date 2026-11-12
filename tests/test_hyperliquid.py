import asyncio
import json
from decimal import Decimal
from pathlib import Path
from adapters.models import AccountState, Side, TIF
from adapters.hyperliquid import Hyperliquid, round_price, round_size
from core.HL.utils.types import Instrument as Asset

DATA = Path(__file__).parent.parent / "src/core/HL/utils/data"


def test_rounding():
    btc = Asset("BTC", "BTC", 0, 5)
    assert round_price(btc, 65432.7, "down") == Decimal("65432")
    assert round_price(btc, 65432.2, "up") == Decimal("65433")
    assert round_price(btc, 123456.7, "down") == Decimal("123456")
    sol = Asset("SOL", "SOL", 5, 2)
    assert round_price(sol, 116.9012, "down") == Decimal("116.90")
    assert round_price(sol, 116.9012, "up") == Decimal("116.91")
    small = Asset("X", "X", 9, 0)
    assert round_price(small, 0.0123456, "down") == Decimal("0.012345")
    assert round_size(btc, 0.0012349) == Decimal("0.00123")


def test_statuses():
    out = Hyperliquid.parse_statuses(json.loads((DATA / "batchOrder.json").read_text()), 13)
    assert sum(r.ok for r in out) == 11 and out[0].oid == "47850680969" and "Post only" in out[-1].error
    cancels = Hyperliquid.parse_statuses(json.loads((DATA / "batchCancel.json").read_text()), 13)
    assert [r.ok for r in cancels][-2:] == [True, True] and not cancels[0].ok
    filled = Hyperliquid.parse_statuses({"status": "ok", "response": {"type": "order", "data": {"statuses": [
        {"filled": {"totalSz": "0.02", "avgPx": "1891.4", "oid": 77738308}}]}}}, 1)
    assert filled[0].ok and filled[0].filled == 0.02
    assert not Hyperliquid.parse_statuses({"status": "err", "response": "boom"}, 2)[1].ok


def test_encode():
    hl = Hyperliquid.__new__(Hyperliquid)
    btc = Asset("BTC", "BTC", 0, 5)
    hl.asset = lambda s: btc
    order = hl.encode("BTC", Side.SELL, 0.0012349, 65432.2, TIF.ALO, False)
    assert order == {"a": 0, "b": False, "p": "65433", "s": "0.00123", "r": False, "t": {"limit": {"tif": "Alo"}}}
    assert list(order) == ["a", "b", "p", "s", "r", "t"]
    assert isinstance(hl.encode("BTC", Side.BUY, 0.000001, 65000, TIF.IOC, False), str)


def test_account():
    hl = Hyperliquid.__new__(Hyperliquid)
    hl.name, hl.state, hl.ready, hl.listeners = "hyperliquid", AccountState("hyperliquid"), False, []
    assets = {"SOL": Asset("SOL", "SOL", 5, 2), "BTC": Asset("BTC", "BTC", 0, 5)}
    hl.asset = lambda s: assets[s]
    hl.parse_state(json.loads((DATA / "clearingHouseState2.json").read_text())["clearinghouseState"])
    state = hl.state
    assert state.equity == 112.11382 and state.free == 111.816905
    assert state.positions["BTC"].size == -0.00003 and state.positions["BTC"].signed_value == -2.1045
    assert state.positions["SOL"].leverage == 10 and state.positions["SOL"].liquidation is None
    got = []
    hl.listeners.append(got.append)
    hl.ready = True
    asyncio.run(hl.push())
    assert got and got[0].positions["SOL"].entry == 116.9012


def test_streams():
    hl = Hyperliquid.__new__(Hyperliquid)
    hl.fill_listeners, hl.order_listeners, hl.seen = [], [], set()
    hl.asset = lambda s: Asset("rrrrr:PTEST", "rrrrr:PTEST", 110000, 3)
    fills, updates = [], []
    hl.fill_listeners.append(fills.append)
    hl.order_listeners.append(updates.append)
    data = json.loads((DATA / "userFills.json").read_text())
    asyncio.run(hl.on_fills({**data, "isSnapshot": True}))
    asyncio.run(hl.on_fills(data))
    assert not fills
    data["fills"][0]["tid"] = 1
    asyncio.run(hl.on_fills(data))
    assert len(fills) == 1 and fills[0].side == Side.SELL and fills[0].maker and fills[0].size == 6.784
    asyncio.run(hl.on_orders(json.loads((DATA / "orderUpdates.json").read_text())))
    assert [u.status for u in updates][:3] == ["rejected", "open", "canceled"]


def stub(assets: dict):
    """Hyperliquid adapter over a fake client: info answers from the recorded payloads"""
    from log.messenger import Messenger

    class Info:
        instruments_dict = {i: a for i, a in enumerate(assets.values())}

        def instrument(self, x):
            if x in assets:
                return assets[x]
            raise ValueError(x)

        async def perp_dexs(self):
            return [None, {"name": "xyz"}, {"name": "km"}]

        async def clearing_house_state(self, address, dex=""):
            if dex == "km":
                raise ConnectionError("boom")
            data = json.loads((DATA / ("clearingHouseState2.json" if dex == "" else "clearingHouseState1.json")).read_text())
            return data["clearinghouseState"]

        async def spot_clearing_house_state(self, address):
            return {"balances": [{"coin": "USDC", "total": "12.5", "hold": "0", "entryNtl": "0"},
                                 {"coin": "HYPE", "total": "2", "hold": "0", "entryNtl": "60"},
                                 {"coin": "DUST", "total": "0.0", "hold": "0", "entryNtl": "0"}]}

        async def all_mids(self, dex=""):
            return {"@107": "40.0"}

        async def l2_book(self, x):
            return json.loads((DATA / "l2Book.json").read_text())

    class Exchange:
        def __init__(self):
            self.calls = []

        async def update_leverage(self, asset, leverage, is_cross=True):
            self.calls.append((asset, leverage, is_cross))
            return {"status": "ok"} if not is_cross or asset != 110000 else {"status": "err", "response": "cross not allowed"}

    class Client:
        messenger, address, running = Messenger(None), "0xabc", True
        info, exchange = Info(), Exchange()
    hl = Hyperliquid(Client(), fees={"xyz": (0.00003, 0.00009)})
    return hl


def test_portfolio():
    btc, sol = Asset("BTC", "BTC", 0, 5), Asset("SOL", "SOL", 5, 2)
    test = Asset("rrrrr:PTEST", "rrrrr:PTEST", 110000, 3)
    hype = Asset("HYPE/USDC", "@107", 10107, 2)
    hl = stub({"BTC": btc, "SOL": sol, "rrrrr:PTEST": test, "HYPE/USDC": hype})
    pf = asyncio.run(hl.portfolio())
    assert pf.details["perp"] == 112.11382 and pf.details["xyz"] == 3.461931
    assert pf.details["spot USDC"] == 12.5 and pf.details["spot HYPE"] == 80.0 and "spot DUST" not in pf.details
    assert abs(pf.equity - (112.11382 + 3.461931 + 12.5 + 80)) < 1e-9
    assert set(pf.positions) == {"SOL", "BTC", "rrrrr:PTEST"} and pf.positions["BTC"].size < 0
    assert "km" in pf.error


def test_xyz():
    nvda = Asset("xyz:NVDA", "xyz:NVDA", 110002, 3)
    nvda.max_leverage, nvda.isolated_only = 10, True
    old = Asset("xyz:OLD", "xyz:OLD", 110003, 2)
    old.delisted = True
    hl = stub({"xyz:NVDA": nvda, "xyz:OLD": old, "BTC": Asset("BTC", "BTC", 0, 5), "HYPE/USDC": Asset("HYPE/USDC", "@107", 10107, 2)})
    hl.state = AccountState("hyperliquid")
    inst = hl.instrument("xyz:NVDA")
    assert inst.taker_fee == 0.00009 and inst.max_leverage == 10 and hl.instrument("BTC").taker_fee == 0.00045
    listings = {l.symbol: l for l in hl.listings()}
    assert set(listings) == {"xyz:NVDA", "xyz:OLD", "BTC"} and listings["xyz:NVDA"].base == "NVDA"
    assert listings["xyz:NVDA"].dex == "xyz" and not listings["xyz:OLD"].online
    assert asyncio.run(hl.set_leverage("xyz:NVDA", 25))
    assert hl.client.exchange.calls == [(110002, 10, False)] and hl.state.leverages["xyz:NVDA"] == 10
    bbo = asyncio.run(hl.book("xyz:NVDA"))
    assert bbo.bid == 89006.0 and bbo.ask == 89033.0 and bbo.bid_size == 0.01846


def test_nonce():
    from core.HL.exchange import HLExchange
    ex = HLExchange.__new__(HLExchange)
    ex.last_nonce = 0
    nonces = [ex.nonce() for _ in range(50)]
    assert len(set(nonces)) == 50 and nonces == sorted(nonces)


def test_meta():
    from core.HL.info import HLInfo
    info = HLInfo.__new__(HLInfo)
    info.instruments_table, info.instruments_dict = {}, {}
    info.register("xyz:NVDA", "xyz:NVDA", 110000, 3, {"name": "xyz:NVDA", "szDecimals": 3, "maxLeverage": 10,
                                                     "marginMode": "strictIsolated"})
    info.register("BTC", "BTC", 0, 5, {"name": "BTC", "szDecimals": 5, "maxLeverage": 40})
    assert info.instrument("xyz:NVDA").isolated_only and info.instrument("xyz:NVDA").max_leverage == 10
    assert not info.instrument("BTC").isolated_only and info.instrument("BTC").max_leverage == 40
