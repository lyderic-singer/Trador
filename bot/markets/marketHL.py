import asyncio
import math
from collections import deque
from time import time
from logs.logger import Logger

TIMEFRAME_MAP_MINUTE = {"1m":1,"3m":3,"5m":5,"15m":15,"1h":60}
TIMEFRAME_MAP_SECOND = {"1s":1,"3s":3,"15s":15,"30s":30,"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600}

class MarketHL:
    def __init__(self, x: str | int, sma_period = 9, sma_candles = "1m"):
        self.best_bid = None
        self.best_ask = None
        self.spread = None
        self.x = x
        self.instrument = None
        self.sma_candles = sma_candles
        self.sma_period = sma_period
        self.last_bbo = None
        self.last_l2Book = None
        self.last_closes = None
        self._sum = None
        self.running = asyncio.Event()
        self.orchester = None
        self._log = None

    def sma(self):
        total = self._sum + self.mid
        return total / (len(self.last_closes) + 1)

    async def start(self, orchester):
        self.orchester = orchester
        self.instrument = orchester.hl.info.instrument(self.x)
        self._log = Logger(f"{self.x} HL", orchester.messenger)
        await orchester.hl.websocket.bbo(self.on_bbo, self.x)
        await orchester.hl.websocket.l2_book(self.on_l2Book, self.x)
        asyncio.create_task(self.handle_sma())
        await self.running.wait()
        await self._log.success("Started")

    def on_bbo(self, msg):
        best_bid = float(msg["data"]["bbo"][0]["px"])
        best_ask = float(msg["data"]["bbo"][1]["px"])
        self.best_bid = best_bid
        self.best_ask = best_ask
        mid = (best_ask + best_bid) / 2
        spread = (best_ask / best_bid) - 1
        self.last_bbo = time()
        self.mid = mid
        self.spread = spread

    async def on_l2Book(self, msg):
        

    async def handle_sma(self):
        now = int(time()) * 1000
        min = 60 * 1000
        first_candles = await self.core.info.candles_snapshot(self.instrument.coin, self.sma_candles, now - (self.sma_period * TIMEFRAME_MAP_MINUTE[self.sma_candles] * min), now)
        first_candles = first_candles[-(self.sma_period - 1):]
        self.last_closes = deque((float(c['c']) for c in first_candles), maxlen=(self.sma_period - 1))
        self._sum = sum(self.last_closes)
        self.running.set()
        while True:
            await asyncio.sleep(TIMEFRAME_MAP_SECOND[self.sma_candles])
            if self.mid is not None:
                if len(self.last_closes) == self.last_closes.maxlen:
                    old = self.last_closes.popleft()
                    self._sum -= old
                self.last_closes.append(self.mid)
                self._sum += self.mid
