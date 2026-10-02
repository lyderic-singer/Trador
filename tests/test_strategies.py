import asyncio
from strategies.arbitrage import Arbitrage, Parameters as ArbitrageParameters
from strategies.enveloppe import Enveloppe, Parameters as EnveloppeParameters
from tests.fake import FakeExchange


def test_enveloppe():
    async def go():
        fake = FakeExchange("fake", "TEST", 99.99, 100.01, equity=1000, leverage=5)
        env = Enveloppe(fake, "TEST", EnveloppeParameters(LEVELS=6, SPREAD=0.01, COOLDOWN=0, TIMEFRAME="1m", WINDOW=3))
        await env.market.start()
        await env.account.start()
        await env.executor.start()
        (bids, asks), power, (_, value), (mid, ema, bid, ask) = await env.state()
        assert power == 5000 and not bids and not asks
        await env.place(bids, asks, power, value, mid, ema, bid, ask)
        placed = list(fake.orders.values())
        assert env.placed == len(placed) == 6 and env.to_modify
        assert len({o.side for o in placed}) == 1
        assert all(o.value > env.MIN_VALUE for o in placed)
        assert sum(o.size for o in placed) <= env.p.CAP * 5000 / mid * 1.0001
        prices = sorted(o.price for o in placed)
        assert prices[-1] / prices[0] - 1 <= 0.0102
        await fake.set_bbo(99.49, 99.51)
        (bids, asks), power, (_, value), (mid, ema, bid, ask) = await env.state()
        await env.modify(bids, asks, power, value, mid, ema, bid, ask)
        assert env.modified >= 1 and not env.to_modify
        await env.market.stop()
        await env.executor.stop()
    asyncio.run(go())


def pair(**parameters):
    a = FakeExchange("A", "X", 100.00, 100.02, equity=10_000, leverage=5)
    b = FakeExchange("B", "X-USD", 100.50, 100.52, equity=10_000, leverage=5)
    p = ArbitrageParameters(CLIP=100, MAX_CLIPS=2, BASIS_MODE="zero", SETTLE=0, COOLDOWN=0, **parameters)
    return a, b, Arbitrage(a, "X", b, "X-USD", p)


async def started(**parameters):
    a, b, arb = pair(**parameters)
    await arb.a.start()
    await arb.b.start()
    return a, b, arb


def test_cycle():
    async def go():
        a, b, arb = await started(EXIT_EDGE=-0.0012)
        d, notional, edge = arb.decide()
        assert d == 1 and notional == 100 and edge > 0.003
        await arb.trade(d, notional, edge)
        assert a.position.size > 0 and b.position.size < 0
        assert abs(arb.a.exposure() + arb.b.exposure()) < 2
        await arb.trade(*arb.decide())
        assert arb.decide() is None
        await b.set_bbo(100.00, 100.02)
        d, notional, edge = arb.decide()
        assert d == -1 and edge >= -0.0012
        await arb.trade(d, notional, edge)
        await arb.trade(*arb.decide())
        assert abs(a.position.size) < 1e-9 and abs(arb.b.exposure()) < 2
        assert arb.trades == 4
    asyncio.run(go())


def test_repair():
    async def go():
        a, b, arb = await started()
        b.rejects = 1
        await arb.trade(*arb.decide())
        assert arb.hedges == 1 and abs(arb.a.exposure() + arb.b.exposure()) < 20 and arb.failures == 0
    asyncio.run(go())


def test_kill():
    async def go():
        a, b, arb = await started(HEDGE_RETRIES=2)
        arb.running = True
        b.rejects = 10
        await arb.trade(*arb.decide())
        await arb.check_hedge()
        assert not arb.running and arb.failures == 2
    asyncio.run(go())


def test_basis():
    async def go():
        a, b, arb = await started()
        arb.p.BASIS_MODE = "ema"
        arb.update_basis(0.0)
        assert arb.decide() is None
        await b.set_bbo(101.20, 101.22)
        assert arb.decide()[0] == 1
    asyncio.run(go())


def test_loops():
    async def go():
        a = FakeExchange("A", "X", 100.00, 100.02, equity=10_000)
        b = FakeExchange("B", "X-USD", 100.50, 100.52, equity=10_000)
        p = ArbitrageParameters(CLIP=100, WARMUP=0.3, HALFLIFE=60, SETTLE=0, COOLDOWN=0.1, POLL=0.05)
        arb = Arbitrage(a, "X", b, "X-USD", p)
        await arb.start()
        await asyncio.sleep(0.5)
        assert arb.trades == 0
        await b.set_bbo(101.30, 101.32)
        await asyncio.sleep(1.0)
        assert arb.trades >= 1 and abs(arb.a.exposure() + arb.b.exposure()) < 5
        await arb.stop()
        fake = FakeExchange("F", "T", 99.99, 100.01)
        env = Enveloppe(fake, "T", EnveloppeParameters(LEVELS=4, POLL_DELAY=0.1, COOLDOWN=0, TIMEFRAME="1m", WINDOW=3))
        await env.start()
        await asyncio.sleep(0.5)
        await env.stop()
        assert env.cycles >= 3
    asyncio.run(go())
