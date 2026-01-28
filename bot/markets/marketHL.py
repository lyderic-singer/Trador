import asyncio
import math
from collections import deque
from time import time
from core.HL.utils.functions import cancel
from log.logger import Logger
from bot.markets.ob import L2BookAnalyzer

MIN_MS = 60 * 1000
TIMEFRAME_MAP_MINUTE = {"1m":1,"3m":3,"5m":5,"15m":15,"1h":60}
TIMEFRAME_MAP_SECOND = {"1s":1,"3s":3,"15s":15,"30s":30,"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600}

class MarketHL:
    def __init__(self, instrument, hl, sma_period=9, sma_candles="1m"):
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
        
        # ==== L2 Book ====
        self.l2 = L2BookAnalyzer(wall_threshold=3.0, imbalance_threshold=8.0)
        self.l2_enabled = False

    async def start(self):
        """Start market data feeds"""
        try:
            # Subscribe to BBO
            await self.hl.websocket.bbo(self.on_bbo, self.instrument.coin)
            
            # Subscribe to active asset context
            await self.hl.websocket.active_asset_ctx(self.on_activeAssetCtx, self.instrument.coin)
            
            # Subscribe to L2 book
            await self.hl.websocket.l2_book(self.on_l2Book, self.instrument.coin)
            
            # Start SMA calculation task
            self.task = asyncio.create_task(self.handle_sma())
            
            # Wait for running to be set by handle_sma
            await self.running.wait()
            
            await self._log.success("Started")
            
        except Exception as e:
            await self._log.error(f"Failed to start: {e}")
            raise

    async def stop(self):
        """Stop market data feeds"""
        try:
            self.running.clear()
            await cancel(task=self.task)
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")

    async def on_bbo(self, msg):
        """Handle BBO updates"""
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
            await self._log.error(f"Error in on_bbo: {e}")

    async def on_activeAssetCtx(self, msg):
        """Handle active asset context updates"""
        try:
            ctx = msg["ctx"]
            self.funding = float(ctx["funding"])
            self.oracle = float(ctx["oraclePx"])
            self.mark = float(ctx["markPx"])
            self.last_ctx = time()
        except Exception as e:
            await self._log.error(f"Error in on_activeAssetCtx: {e}")

    async def on_l2Book(self, msg):
        """Handle L2 book updates"""
        try:
            self.l2.update(msg)
            self.l2_enabled = True
        except Exception as e:
            await self._log.error(f"Error in on_l2Book: {e}")

    async def handle_sma(self):
        """Calculate and maintain SMA"""
        try:
            # Get initial candles
            now = int(time()) * 1000
            first_candles = await self.hl.info.candles_snapshot(
                self.instrument.coin, 
                self.sma_candles, 
                now - (self.sma_period * TIMEFRAME_MAP_MINUTE[self.sma_candles] * MIN_MS), 
                now
            )
            
            first_candles = first_candles[-(self.sma_period - 1):]
            self.last_closes = deque((float(c['c']) for c in first_candles), maxlen=(self.sma_period - 1))
            self.sum_closes = sum(self.last_closes)
            
            # Get initial mid price
            self.mid = await self.hl.info.price(self.instrument.coin, self.instrument.dex)
            
            self.running.set()
            
            # Update SMA periodically
            while self.running.is_set():
                await asyncio.sleep(TIMEFRAME_MAP_SECOND[self.sma_candles])
                
                if self.mid is not None:
                    if len(self.last_closes) == self.last_closes.maxlen:
                        old = self.last_closes.popleft()
                        self.sum_closes -= old
                    self.last_closes.append(self.mid)
                    self.sum_closes += self.mid
                    
        except Exception as e:
            await self._log.error(f"Error in handle_sma: {e}")
            self.running.set()  # Still mark as running even if SMA fails

    def sma(self):
        """Get current SMA value"""
        if self.sum_closes and self.mid:
            total = self.sum_closes + self.mid
            return total / (len(self.last_closes) + 1)
        return None
    
    def get_l2_optimal_prices(self, is_bid, nb_levels):
        """Get optimal prices from L2 book"""
        if self.l2_enabled and self.l2.mid:
            return self.l2.get_optimal_prices(is_bid, nb_levels)
        return None
    
    def should_cancel_orders(self, side):
        """Check if orders should be cancelled based on L2 book"""
        if self.l2_enabled:
            return self.l2.should_cancel(side)
        return False, ""
    
    def get_imbalance(self):
        """Get L2 book imbalance"""
        if self.l2_enabled:
            return self.l2.get_imbalance()
        return None, False, "BALANCED"