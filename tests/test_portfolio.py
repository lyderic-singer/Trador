import asyncio
from adapters.models import Portfolio, Position
from log.telegram import format_portfolio
from tests.fake import MultiFake


def test_format():
    hl = Portfolio("hyperliquid", 506.1, 400, 1.2, 300, {"xyz": 500.0, "spot USDC": 6.1},
                   {"xyz:NVDA": Position("xyz:NVDA", 0.5, value=90, pnl=1.2), "xyz:TSLA": Position("xyz:TSLA", -0.2, value=84, pnl=-0.4)})
    ar = Portfolio("arcus", 493.9, 350, -0.8, 174, {"sub-account 0": 493.9}, {}, "partial read")
    text = format_portfolio([hl, ar])
    assert text.splitlines()[0] == "💼 Portfolio $1,000.00"
    assert "🏦 hyperliquid: $506.10 (51%)" in text and "  · xyz: $500.00" in text and "  · spot USDC: $6.10" in text
    assert "xyz:NVDA long $90.00 (+$1.20)" in text and "xyz:TSLA short $84.00 (-$0.40)" in text
    assert "upnl -$0.80" in text and "⚠️ partial read" in text
    assert text.index("xyz:NVDA") < text.index("xyz:TSLA")


def test_orchester():
    from core.orchester import Orchester

    class Broken(MultiFake):
        async def portfolio(self):
            raise RuntimeError("api down")

    async def go():
        orchester = Orchester.__new__(Orchester)
        good = MultiFake("hyperliquid", {"xyz:NVDA": 180.0})
        good.qty["xyz:NVDA"], good.cash = 1.0, -180.0
        orchester.clients = {"hyperliquid": object(), "arcus": object()}
        orchester.adapters = {"hyperliquid": good, "arcus": Broken("arcus", {"NVDA-USD": 180.0})}
        pfs = await orchester.portfolios()
        assert pfs[0].exchange == "hyperliquid" and abs(pfs[0].equity - 500) < 0.1 and pfs[0].notional > 179
        assert pfs[1].exchange == "arcus" and "api down" in pfs[1].error
    asyncio.run(go())
