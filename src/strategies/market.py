import asyncio
import math
import time
from collections import deque
from adapters.exchange import Exchange
from adapters.models import BBO
from core.utils import cancel
from log.logger import Logger

TIMEFRAMES = {"1s": 1, "3s": 3, "15s": 15, "30s": 30, "1m": 60, "3m": 180, "5m": 300, "15m": 900,
              "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400}


class MarketData:
    def __init__(self, exchange: Exchange, symbol: str, window: int = 9, timeframe: str = "1m"):
        self.exchange = exchange
        self.symbol = symbol
        self.window = window
        self.timeframe = timeframe
        self.delay = TIMEFRAMES[timeframe]
        self._log = Logger(f"{exchange.name}:{symbol} Market", exchange.messenger)
        self.running = False
        self.received = asyncio.Event()
        self.task = None
        self.bbo: BBO | None = None
        self.best_bid: float | None = None
        self.best_ask: float | None = None
        self.mid: float | None = None
        self.spread: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.last_bbo: float | None = None
        n = max(self.window - 1, 1)
        self.last_closes: deque = deque(maxlen=n)
        self.sum_closes = 0.0
        self.last_highs: deque = deque(maxlen=n)
        self.sum_highs = 0.0
        self.last_lows: deque = deque(maxlen=n)
        self.sum_lows = 0.0
        self.alpha = 2 / (self.window + 1)
        self.ema_closes: float | None = None
        self.ema_highs: float | None = None
        self.ema_lows: float | None = None

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self.exchange.subscribe_bbo(self.symbol, self.on_bbo)
        now = time.time()
        candles = await self.exchange.candles(self.symbol, self.timeframe, self.window + 1)
        closed = [c for c in candles if c.time + self.delay <= now + 1][-(self.window - 1):] if self.window > 1 else []
        for c in closed:
            self.last_closes.append(c.close)
            self.last_highs.append(c.high)
            self.last_lows.append(c.low)
            self.sum_closes += c.close
            self.sum_highs += c.high
            self.sum_lows += c.low
        mid = await self.exchange.mid(self.symbol)
        if mid is None and candles:
            mid = candles[-1].close
        if mid is None:
            raise ValueError(f"{self.symbol}: cannot initialise mid price")
        self.mid, self.high, self.low = mid, mid, mid
        n = len(closed)
        if n:
            self.ema_closes, self.ema_highs, self.ema_lows = self.sum_closes / n, self.sum_highs / n, self.sum_lows / n
        else:
            self.ema_closes, self.ema_highs, self.ema_lows = mid, mid, mid
        self.running = True
        self.task = asyncio.create_task(self.run())
        await asyncio.wait_for(self.received.wait(), timeout=30)
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self._log.success("Stopped")

    async def on_bbo(self, bbo: BBO):
        if not (bbo.bid and bbo.ask):
            return
        self.bbo = bbo
        self.best_bid, self.best_ask = bbo.bid, bbo.ask
        self.mid = bbo.mid
        self.spread = bbo.spread
        if self.high and self.low:
            self.high = max(self.high, self.mid)
            self.low = min(self.low, self.mid)
        self.last_bbo = time.time()
        self.received.set()

    def age(self) -> float:
        return time.time() - self.last_bbo if self.last_bbo else float("inf")

    def boundary(self) -> float:
        return math.ceil(time.time() / self.delay + 1e-6) * self.delay - time.time()

    @staticmethod
    def roll(values: deque, total: float, value: float) -> float:
        if len(values) == values.maxlen:
            total -= values.popleft()
        values.append(value)
        return total + value

    async def run(self):
        try:
            await asyncio.sleep(self.boundary())
            while self.running:
                if self.mid and self.ema_closes:
                    self.ema_closes = self.ema_close()
                    self.sum_closes = self.roll(self.last_closes, self.sum_closes, self.mid)
                if self.high and self.ema_highs:
                    self.ema_highs = self.ema_high()
                    self.sum_highs = self.roll(self.last_highs, self.sum_highs, self.high)
                    self.high = self.mid
                if self.low and self.ema_lows:
                    self.ema_lows = self.ema_low()
                    self.sum_lows = self.roll(self.last_lows, self.sum_lows, self.low)
                    self.low = self.mid
                await asyncio.sleep(self.boundary())
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
            self.running = False

    @staticmethod
    def sma(values: deque, total: float, value: float | None) -> float:
        if value is None:
            raise ValueError("must start before request sma")
        return (total + value) / (len(values) + 1)

    def ema(self, previous: float | None, value: float | None) -> float:
        if not (previous and value):
            raise ValueError("must start before request ema")
        return self.alpha * value + (1 - self.alpha) * previous

    def sma_close(self): return self.sma(self.last_closes, self.sum_closes, self.mid)
    def sma_high(self): return self.sma(self.last_highs, self.sum_highs, self.high)
    def sma_low(self): return self.sma(self.last_lows, self.sum_lows, self.low)
    def ema_close(self): return self.ema(self.ema_closes, self.mid)
    def ema_high(self): return self.ema(self.ema_highs, self.high)
    def ema_low(self): return self.ema(self.ema_lows, self.low)

    def __str__(self):
        return (f"<MarketData {self.exchange.name}:{self.symbol} mid={self.mid} bid={self.best_bid} ask={self.best_ask} "
                f"ema={self.ema_closes} sma_n={len(self.last_closes)}>")
