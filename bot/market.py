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
        self.last_bbo = None
        # ==== activeAssetCtx ====
        self.funding = None
        self.oracle = None
        self.mark = None
        self.last_ctx = None
        # ==== sma ====
        self.last_closes = None
        self.sum_closes = None

    async def start(self):
        await self.hl.websocket.bbo(self.on_bbo, self.instrument.coin)
        await self.hl.websocket.active_asset_ctx(self.on_activeAssetCtx, self.instrument.coin)
        self.task = asyncio.create_task(self.handle_sma())
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
            self.last_bbo = time()
            self.mid = mid
            self.spread = spread
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

    async def handle_sma(self):
        now = int(time()) * 1000
        first_candles = await self.hl.info.candles_snapshot( self.instrument.coin, self.sma_candles, now - (self.sma_period * TIMEFRAME_MAP_MINUTE[self.sma_candles] * MIN_MS), now)
        first_candles = first_candles[-(self.sma_period - 1):]
        self.last_closes = deque((float(c['c']) for c in first_candles), maxlen=(self.sma_period - 1))
        self.sum_closes = sum(self.last_closes)
        self.mid = await self.hl.info.price(self.instrument.coin, self.instrument.dex)
        self.running.set()
        while self.running.is_set():
            await asyncio.sleep(TIMEFRAME_MAP_SECOND[self.sma_candles])
            if self.mid is not None:
                if len(self.last_closes) == self.last_closes.maxlen:
                    old = self.last_closes.popleft()
                    self.sum_closes -= old
                self.last_closes.append(self.mid)
                self.sum_closes += self.mid

    def sma(self):
        if self.last_closes is None:
            raise ValueError("must start before request sma")
        if self.sum_closes and self.mid:
            total = self.sum_closes + self.mid
            return total / (len(self.last_closes) + 1)
        return

    def __str__(self):
        return (
            f"<MarketHL {self.instrument.coin} | "
            f"mid={self.mid} | "
            f"best_bid={self.best_bid} | best_ask={self.best_ask} | spread={self.spread} | "
            f"sma={self.sma() if self.last_closes else 'N/A'}>"
        )
