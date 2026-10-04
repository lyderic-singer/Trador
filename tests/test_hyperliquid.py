import asyncio
import json
from decimal import Decimal
from pathlib import Path
from adapters.models import AccountState, Side, TIF
from adapters.hyperliquid import Hyperliquid, round_price, round_size
from core.HL.utils.types import Instrument as Asset

DATA = Path(__file__).parent.parent / "core/HL/utils/data"


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
