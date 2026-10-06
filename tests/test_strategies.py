import asyncio
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


def test_loops():
    async def go():
        fake = FakeExchange("F", "T", 99.99, 100.01)
        env = Enveloppe(fake, "T", EnveloppeParameters(LEVELS=4, POLL_DELAY=0.1, COOLDOWN=0, TIMEFRAME="1m", WINDOW=3))
        await env.start()
        await asyncio.sleep(0.5)
        await env.stop()
        assert env.cycles >= 3
    asyncio.run(go())
