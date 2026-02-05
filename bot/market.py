import asyncio
from collections import deque
from time import time
from core.HL.utils.functions import cancel
from log.logger import Logger

MIN_MS = 60 * 1000
TIMEFRAME_MAP_MINUTE = {"1m":1,"3m":3,"5m":5,"15m":15,"1h":60}
TIMEFRAME_MAP_SECOND = {"1s":1,"3s":3,"15s":15,"30s":30,"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600}

class MarketHL:
    def __init__(self, instrument, hl, sma_period = 9, sma_candles = "1m"):
        self.instrument = instrument
        self.hl = hl
        self.sma_candles = sma_candles
        self.sma_period = sma_period
        self._log = Logger(f"{self.instrument.coin} Market", hl.messenger)
        self.running = asyncio.Event()
        self.task = None
        # ==== bbo ====
        self.best_bid = None
        self.best_ask = None
        self.mid = None
        self.spread = None
        self.high = None
        self.low = None
        self.last_bbo = None
        # ==== activeAssetCtx ====
        self.funding = None
        self.oracle = None
        self.mark = None
        self.last_ctx = None
        # ==== sma ====
        self.last_closes = deque(maxlen=self.sma_period - 1)
        self.sum_closes = 0
        self.last_highs = deque(maxlen=self.sma_period - 1)
        self.sum_highs = 0
        self.last_lows = deque(maxlen=self.sma_period - 1)
        self.sum_lows = 0

    async def start(self):
        await self.hl.websocket.bbo(self.on_bbo, self.instrument.coin)
        await self.hl.websocket.active_asset_ctx(self.on_activeAssetCtx, self.instrument.coin)
        self.task = asyncio.create_task(self.run())
        await self.running.wait()
        await self._log.success("Started")

    async def stop(self):
        self.running.clear()
        await cancel(task=self.task)

    async def on_bbo(self, msg):
        try:
            best_bid = float(msg["bbo"][0]["px"])
            best_ask = float(msg["bbo"][1]["px"])
            self.best_bid = best_bid
            self.best_ask = best_ask
            mid = (best_ask + best_bid) / 2
            spread = (best_ask / best_bid) - 1
            self.mid = mid
            if self.high and self.low:
                self.high = max(self.high, mid)
                self.low = min(self.low, mid)
            self.spread = spread
            self.last_bbo = time()
        except Exception as e:
            await self._log.error(f"{e}")

    async def on_activeAssetCtx(self, msg):
        try:
            ctx = msg["ctx"]
            self.funding = float(ctx["funding"])
            self.oracle = float(ctx["oraclePx"])
            self.mark = float(ctx["markPx"])
            self.last_ctx = time()
        except Exception as e:
            await self._log.error(f"{e}")

    def update(self, last_values: deque, sum_values: float, value: float):
        if len(last_values) == last_values.maxlen:
            sum_values -= last_values.popleft()
        last_values.append(value)
        return sum_values + value

    async def run(self):
        now = int(time()) * 1000
        first_candles = await self.hl.info.candles_snapshot( self.instrument.coin, self.sma_candles, now - (self.sma_period * TIMEFRAME_MAP_MINUTE[self.sma_candles] * MIN_MS), now)
        first_candles = first_candles[-(self.sma_period - 1):]
        for c in first_candles:
            close = float(c['c'])
            high = float(c['h'])
            low = float(c['l'])
            self.last_closes.append(close)
            self.last_highs.append(high)
            self.last_lows.append(low)
            self.sum_closes += close
            self.sum_highs += high
            self.sum_lows += low
        mid = await self.hl.info.price(self.instrument.coin, self.instrument.dex)
        self.mid, self.high, self.low = mid, mid, mid
        self.running.set()
        while self.running.is_set():
            await asyncio.sleep(TIMEFRAME_MAP_SECOND[self.sma_candles])
            if self.mid is not None:
                self.sum_closes = self.update(self.last_closes, self.sum_closes, self.mid)
            if self.high is not None:
                self.sum_highs = self.update(self.last_highs, self.sum_highs, self.high)
                self.high = self.mid
            if self.low is not None:
                self.sum_lows = self.update(self.last_lows, self.sum_lows, self.low)
                self.low = self.mid

    def sma(self, last_values: deque, sum_values: float | None, value: float | None):
        if not last_values:
            raise ValueError("must start before request sma")
        if sum_values and value:
            total = sum_values + value
            return total / (len(last_values) + 1)
        return

    def sma_close(self):
        return self.sma(self.last_closes, self.sum_closes, self.mid)

    def sma_high(self):
        return self.sma(self.last_highs, self.sum_highs, self.high)

    def sma_low(self):
        return self.sma(self.last_lows, self.sum_lows, self.low)

    def __str__(self):
        return (
            f"<MarketHL {self.instrument.coin} | "
            f"mid={self.mid} | "
            f"best_bid={self.best_bid} | best_ask={self.best_ask} | spread={self.spread} | "
            f"sma close={self.sma_close() if self.last_closes else 'N/A'}"
            f"sma high={self.sma_high() if self.last_highs else 'N/A'}"
            f"sma low={self.sma_low() if self.last_lows else 'N/A'}>"
        )
