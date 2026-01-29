from time import time
import asyncio
from log.logger import Logger
from collections import defaultdict, deque

class AccountPerpHL:
    def __init__(self, instruments, hl):
        self.instruments = instruments
        self.dexs = list({i.dex for i in instruments})
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.running = asyncio.Event()
        
        # ==== clearinghouseState ====
        self.margin_summary = None
        self.cross_margin_summary = None
        self.withdrawable = None
        self.positions = {}  # coin -> position dict
        self.last_clearinghouseState = None
        
        # ==== activeAssetData ====
        self.leverage = {}
        self.availableSzLong = {}
        self.availableSzShort = {}
        self.availableDolLong = {}
        self.availableDolShort = {}
        self.markPx = {}
        self.last_activeAssetData = {}
        
        # ==== orders / fills ====
        self.open_orders = defaultdict(list)   # coin -> [orders]
        self.order_updates = deque(maxlen=500)
        self.user_fills = deque(maxlen=500)
        
        # ==== Volume tracking ====
        self.cumulative_volume_usd = 0
        self.daily_volume_usd = 0
        self.hourly_volume_usd = 0
        self.volume_by_coin = defaultdict(float)
        self.last_volume_reset = time()
        self.last_hourly_reset = time()
        self.volume_stats_interval = 300  # Log every 5 min

    async def start(self):
        """Start account monitoring"""
        try:
            subscriptions = []
            
            # Subscribe to clearinghouse state for each dex
            for dex in self.dexs:
                subscriptions.append(
                    self.hl.websocket.clearinghouse_state(
                        self.on_clearinghouseState, 
                        self.hl.address, 
                        dex
                    )
                )
                subscriptions.append(
                    self.hl.websocket.open_orders(
                        self.on_openOrders, 
                        self.hl.address, 
                        dex
                    )
                )
            
            # Subscribe to active asset data
            subscriptions += [
                self.hl.websocket.active_asset_data(
                    self.on_activeAssetData, 
                    self.hl.address, 
                    instrument.coin
                ) 
                for instrument in self.instruments
            ]
            
            # Subscribe to order updates and fills
            subscriptions.append(
                self.hl.websocket.order_updates(
                    self.on_orderUpdates, 
                    self.hl.address
                )
            )
            subscriptions.append(
                self.hl.websocket.user_fills(
                    self.on_userFills, 
                    self.hl.address
                )
            )
            
            await asyncio.gather(*subscriptions)
            
            self.running.set()
            await self._log.success("Started")
            
        except Exception as e:
            await self._log.error(f"Failed to start: {e}")
            raise
    
    async def stop(self):
        """Stop account monitoring"""
        self.running.clear()
        await self._log.success("Stopped")

    async def on_clearinghouseState(self, msg):
        """Handle clearinghouse state updates"""
        try:
            ch = msg["clearinghouseState"]
            self.margin_summary = ch["marginSummary"]
            self.cross_margin_summary = ch["crossMarginSummary"]
            self.withdrawable = float(ch["withdrawable"])
            
            # Update positions
            self.positions.clear()
            for p in ch["assetPositions"]:
                pos = p["position"]
                self.positions[pos["coin"]] = {
                    "szi": float(pos["szi"]),
                    "entryPx": float(pos["entryPx"]),
                    "positionValue": float(pos["positionValue"]),
                    "unrealizedPnl": float(pos["unrealizedPnl"]),
                    "roe": float(pos["returnOnEquity"]),
                    "liqPx": float(pos["liquidationPx"] or 0),
                    "marginUsed": float(pos["marginUsed"]),
                    "leverage": pos["leverage"],
                }
            
            self.last_clearinghouseState = time()
            
        except Exception as e:
            await self._log.error(f"Error in on_clearinghouseState: {e}")

    async def on_activeAssetData(self, msg):
        """Handle active asset data updates"""
        try:
            data = msg
            coin = data["coin"]
            self.leverage[coin] = data["leverage"]
            self.availableSzLong[coin] = float(data["maxTradeSzs"][0])
            self.availableSzShort[coin] = float(data["maxTradeSzs"][1])
            self.availableDolLong[coin] = float(data["availableToTrade"][0])
            self.availableDolShort[coin] = float(data["availableToTrade"][1])
            self.markPx[coin] = float(data["markPx"])
            self.last_activeAssetData[coin] = time()
            
        except Exception as e:
            await self._log.error(f"Error in on_activeAssetData: {e}")

    async def on_openOrders(self, msg):
        """Handle open orders updates"""
        try:
            orders = msg["orders"]
            self.open_orders.clear()
            
            for o in orders:
                self.open_orders[o["coin"]].append({
                    "side": o["side"],
                    "px": float(o["limitPx"]),
                    "sz": float(o["sz"]),
                    "oid": o["oid"],
                    "reduceOnly": o["reduceOnly"],
                    "tif": o["tif"],
                    "ts": o["timestamp"],
                })
                
        except Exception as e:
            await self._log.error(f"Error in on_openOrders: {e}")

    async def on_orderUpdates(self, msg):
        """Handle order update events"""
        try:
            for upd in msg:
                self.order_updates.append({
                    "coin": upd["order"]["coin"],
                    "oid": upd["order"]["oid"],
                    "status": upd["status"],
                    "ts": upd["statusTimestamp"],
                })
                
        except Exception as e:
            await self._log.error(f"Error in on_orderUpdates: {e}")

    async def on_userFills(self, msg):
        """Handle user fill events and track volume"""
        try:
            if msg.get("isSnapshot"):
                return
                
            for f in msg["fills"]:
                fill = {
                    "coin": f["coin"],
                    "px": float(f["px"]),
                    "sz": float(f["sz"]),
                    "side": f["side"],
                    "pnl": float(f["closedPnl"]),
                    "fee": float(f["fee"]),
                    "time": f["time"],
                    "oid": f["oid"],
                }
                
                self.user_fills.append(fill)
                
                # Track volume
                volume_usd = fill["px"] * fill["sz"]
                self.cumulative_volume_usd += volume_usd
                self.daily_volume_usd += volume_usd
                self.hourly_volume_usd += volume_usd
                self.volume_by_coin[fill["coin"]] += volume_usd
                
                # Log volume stats periodically
                current_time = time()
                if current_time - self.last_volume_reset > self.volume_stats_interval:
                    await self._log_volume_stats()
                    
        except Exception as e:
            await self._log.error(f"Error in on_userFills: {e}")
    
    async def _log_volume_stats(self):
        """Log volume statistics"""
        try:
            current_time = time()
            
            # Hourly rate
            hourly_elapsed = (current_time - self.last_hourly_reset) / 3600
            hourly_rate = self.hourly_volume_usd / hourly_elapsed if hourly_elapsed > 0 else 0
            
            stats = (
                f"📊 Volume - "
                f"Total: ${self.cumulative_volume_usd:,.0f} | "
                f"Daily: ${self.daily_volume_usd:,.0f} | "
                f"Rate: ${hourly_rate:,.0f}/h"
            )
            
            await self._log.info(stats, send_tg=False)
            
            # Per-coin volumes
            for coin, volume in sorted(self.volume_by_coin.items(), key=lambda x: -x[1]):
                if volume > 100:
                    await self._log.debug(f"  {coin}: ${volume:,.0f}")
            
            # Reset hourly
            if (current_time - self.last_hourly_reset) > 3600:
                self.hourly_volume_usd = 0
                self.last_hourly_reset = current_time
            
            # Reset daily if 24h passed
            if (current_time - self.last_volume_reset) > 86400:
                self.daily_volume_usd = 0
                self.volume_by_coin.clear()
                self.last_volume_reset = current_time
                await self._log.info("Daily volume counters reset")
                
        except Exception as e:
            await self._log.error(f"Error logging volume stats: {e}")
    
    def get_volume_stats(self):
        """Get current volume statistics"""
        hourly_elapsed = (time() - self.last_hourly_reset) / 3600
        return {
            "cumulative_usd": self.cumulative_volume_usd,
            "daily_usd": self.daily_volume_usd,
            "hourly_usd": self.hourly_volume_usd,
            "hourly_rate": self.hourly_volume_usd / hourly_elapsed if hourly_elapsed > 0 else 0,
            "by_coin": dict(self.volume_by_coin)
        }