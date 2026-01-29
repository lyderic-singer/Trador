import asyncio
from time import time
from log.logger import Logger

class RiskLimits:
    """Configuration for risk limits"""
    def __init__(self):
        # Position limits
        self.max_position_usd_per_coin = 10000  # Max $10k per coin
        self.max_total_position_usd = 50000  # Max $50k total
        self.max_leverage = 3  # Max 3x leverage
        
        # Order limits
        self.max_order_size_usd = 1000  # Max $1k per order
        self.max_orders_per_coin = 20  # Max 20 orders per coin (was 100)
        
        # Loss limits
        self.max_loss_per_coin = -500  # Max -$500 loss per coin
        self.max_total_loss = -2000  # Max -$2k total loss
        self.daily_loss_limit = -1000  # Max -$1k per day
        
        # Emergency stops
        self.min_margin_ratio = 0.2  # Stop if margin < 20%
        self.max_drawdown_percent = 0.1  # 10% max drawdown
        
        # Spreads and slippage
        self.max_spread = 0.05  # 5% max spread
        self.max_slippage = 0.02  # 2% max slippage


class RiskState:
    """Tracks current risk state"""
    def __init__(self):
        self.daily_pnl = 0
        self.daily_start_balance = 0
        self.peak_balance = 0
        self.current_drawdown = 0
        self.emergency_stop = False
        self.last_reset = time()
        
        # Per-coin tracking
        self.coin_pnl = {}
        self.coin_positions_usd = {}


class RiskManager:
    """
    Manages risk and validates all orders before execution
    """
    def __init__(self, hl, messenger, account, executor):
        self.hl = hl
        self._log = Logger(self, messenger)
        self.account = account
        self.executor = executor
        self.running = asyncio.Event()
        
        # Risk configuration
        self.limits = RiskLimits()
        self.state = RiskState()
        
        # Data manager reference (set after initialization)
        self.data = None
        
        # Monitoring tasks
        self.monitor_task = None
        self.event_task = None
        
    async def start(self, data=None):
        """Start risk manager"""
        try:
            self.data = data
            self.running.set()
            
            # Initialize state
            await self._initialize_state()
            
            # Start monitoring
            self.monitor_task = asyncio.create_task(self._monitor_loop())
            
            # Start event processing if data manager is available
            if self.data:
                self.event_task = asyncio.create_task(self._process_events())
            
            await self._log.success("Started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise
    
    async def stop(self):
        """Stop risk manager"""
        try:
            self.running.clear()
            
            if self.monitor_task:
                self.monitor_task.cancel()
                try:
                    await self.monitor_task
                except asyncio.CancelledError:
                    pass
            
            if self.event_task:
                self.event_task.cancel()
                try:
                    await self.event_task
                except asyncio.CancelledError:
                    pass
            
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")
    
    async def _initialize_state(self):
        """Initialize risk state from account"""
        if self.account.margin_summary:
            balance = float(self.account.margin_summary.get("accountValue", 0))
            self.state.daily_start_balance = balance
            self.state.peak_balance = balance
        
        self.state.last_reset = time()
        await self._log.info(f"Risk initialized: balance=${self.state.daily_start_balance:.2f}")
    
    async def _monitor_loop(self):
        """Monitor risk metrics continuously"""
        while self.running.is_set():
            try:
                await asyncio.sleep(5)  # Check every 5s
                
                # Update state
                await self._update_state()
                
                # Check emergency conditions
                await self._check_emergency_conditions()
                
                # Reset daily if needed
                if time() - self.state.last_reset > 86400:  # 24 hours
                    await self._reset_daily()
                
            except Exception as e:
                await self._log.error(f"Error in risk monitor: {e}")
                await asyncio.sleep(1)
    
    async def _process_events(self):
        """Process events from data manager (consumes risk queue)"""
        while self.running.is_set():
            try:
                event = await asyncio.wait_for(
                    self.data.risk_events.get(),
                    timeout=0.1
                )
                
                # Process event based on type
                if event.type == "fill":
                    pass
                elif event.type == "position_update":
                    pass
                elif event.type == "book_update":
                    pass
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                await self._log.error(f"Error processing risk event: {e}")
                await asyncio.sleep(0.1)
    
    async def _update_state(self):
        """Update risk state from account"""
        if not self.account.margin_summary:
            return
        
        # Update balance and PnL
        current_balance = float(self.account.margin_summary.get("accountValue", 0))
        self.state.daily_pnl = current_balance - self.state.daily_start_balance
        
        # Update peak and drawdown
        if current_balance > self.state.peak_balance:
            self.state.peak_balance = current_balance
        
        self.state.current_drawdown = (current_balance - self.state.peak_balance) / self.state.peak_balance if self.state.peak_balance > 0 else 0
        
        # Update per-coin positions and PnL
        for coin, pos in self.account.positions.items():
            self.state.coin_pnl[coin] = pos.get("unrealizedPnl", 0)
            self.state.coin_positions_usd[coin] = abs(pos.get("positionValue", 0))
    
    async def _check_emergency_conditions(self):
        """Check if emergency stop conditions are met"""
        if self.state.emergency_stop:
            return
        
        should_stop = False
        reason = ""
        
        # Check daily loss limit
        if self.state.daily_pnl < self.limits.daily_loss_limit:
            should_stop = True
            reason = f"Daily loss limit hit: ${self.state.daily_pnl:.2f}"
        
        # Check drawdown
        if self.state.current_drawdown < -self.limits.max_drawdown_percent:
            should_stop = True
            reason = f"Max drawdown exceeded: {self.state.current_drawdown*100:.1f}%"
        
        # Check margin ratio
        if self.account.margin_summary:
            margin_used = float(self.account.margin_summary.get("totalMarginUsed", 0))
            account_value = float(self.account.margin_summary.get("accountValue", 0))
            if account_value > 0:
                margin_ratio = margin_used / account_value
                if margin_ratio > (1 - self.limits.min_margin_ratio):
                    should_stop = True
                    reason = f"Low margin ratio: {margin_ratio*100:.1f}%"
        
        # Check per-coin losses
        for coin, pnl in self.state.coin_pnl.items():
            if pnl < self.limits.max_loss_per_coin:
                should_stop = True
                reason = f"Max loss on {coin}: ${pnl:.2f}"
                break
        
        if should_stop:
            await self._trigger_emergency_stop(reason)
    
    async def _trigger_emergency_stop(self, reason):
        """Trigger emergency stop - cancel all orders and close positions"""
        self.state.emergency_stop = True
        await self._log.critical(f"🚨 EMERGENCY STOP: {reason}", send_tg=True)
        
        try:
            # Cancel all open orders
            await self._cancel_all_orders()
            
            # Close all positions (market orders)
            await self._close_all_positions()
            
            await self._log.critical("Emergency stop executed", send_tg=True)
            
        except Exception as e:
            await self._log.critical(f"Error in emergency stop: {e}", send_tg=True)
    
    async def _cancel_all_orders(self):
        """Cancel all open orders"""
        try:
            all_cancels = []
            for coin, orders in self.account.open_orders.items():
                for order in orders:
                    instrument = self.hl.info.instrument(coin)
                    all_cancels.append({"a": instrument.asset, "o": order["oid"]})
            
            if all_cancels:
                await self.executor.submit_cancels(all_cancels, priority=0)
                await self._log.warning(f"Cancelled {len(all_cancels)} orders")
                
        except Exception as e:
            await self._log.error(f"Error cancelling orders: {e}")
    
    async def _close_all_positions(self):
        """Close all positions with market orders"""
        try:
            for coin, pos in self.account.positions.items():
                size = pos.get("szi", 0)
                if abs(size) < 0.001:
                    continue
                
                instrument = self.hl.info.instrument(coin)
                is_buy = size < 0  # Opposite side to close
                size = abs(size)
                
                # Get current price
                mid = self.account.markPx.get(coin, 0)
                if mid <= 0:
                    continue
                
                # Market order with 1% slippage
                await self.hl.exchange.market(
                    instrument.coin,
                    is_buy,
                    size,
                    mid,
                    0.01,
                    reduce_only=True
                )
                
                await self._log.warning(f"Closed position on {coin}: {size}")
                
        except Exception as e:
            await self._log.error(f"Error closing positions: {e}")
    
    async def _reset_daily(self):
        """Reset daily counters"""
        self.state.daily_pnl = 0
        if self.account.margin_summary:
            self.state.daily_start_balance = float(
                self.account.margin_summary.get("accountValue", 0)
            )
        self.state.last_reset = time()
        await self._log.info("Daily risk counters reset")
    
    # ===== Order Validation Methods =====
    
    async def validate_orders(self, orders, coin):
        """
        Validate a batch of orders before execution
        Returns: (is_valid, reason)
        """
        if self.state.emergency_stop:
            return False, "Emergency stop active"
        
        # Check total position size
        total_position_usd = sum(self.state.coin_positions_usd.values())
        if total_position_usd > self.limits.max_total_position_usd:
            return False, f"Total position limit exceeded: ${total_position_usd:.2f}"
        
        # Check per-coin position
        coin_position_usd = self.state.coin_positions_usd.get(coin, 0)
        if coin_position_usd > self.limits.max_position_usd_per_coin:
            return False, f"Coin position limit exceeded: ${coin_position_usd:.2f}"
        
        # Check number of orders
        existing_orders = len(self.account.open_orders.get(coin, []))
        new_orders = len(orders)
        if existing_orders + new_orders > self.limits.max_orders_per_coin:
            return False, f"Too many orders: {existing_orders + new_orders}"
        
        # Validate each order
        for order in orders:
            size = float(order.get("s", 0))
            price = float(order.get("p", 0))
            order_value = size * price
            
            if order_value > self.limits.max_order_size_usd:
                return False, f"Order too large: ${order_value:.2f}"
        
        return True, "OK"
    
    async def should_reduce_position(self, coin):
        """Check if we should reduce position on a coin"""
        # Check PnL
        pnl = self.state.coin_pnl.get(coin, 0)
        if pnl < self.limits.max_loss_per_coin * 0.8:  # 80% of limit
            return True, f"High loss: ${pnl:.2f}"
        
        # Check position size
        position_usd = self.state.coin_positions_usd.get(coin, 0)
        if position_usd > self.limits.max_position_usd_per_coin * 0.9:  # 90% of limit
            return True, f"Large position: ${position_usd:.2f}"
        
        return False, ""
    
    def can_increase_position(self, coin, direction):
        """Check if we can increase position"""
        if self.state.emergency_stop:
            return False
        
        position = self.account.positions.get(coin, {})
        current_size = position.get("szi", 0)
        
        # Check if we're trying to increase in the same direction
        if direction == "long" and current_size > 0:
            position_usd = self.state.coin_positions_usd.get(coin, 0)
            return position_usd < self.limits.max_position_usd_per_coin
        elif direction == "short" and current_size < 0:
            position_usd = self.state.coin_positions_usd.get(coin, 0)
            return position_usd < self.limits.max_position_usd_per_coin
        
        return True
    
    def get_max_order_size(self, coin, price):
        """Get maximum order size for a coin"""
        max_usd = self.limits.max_order_size_usd
        max_size = max_usd / price if price > 0 else 0
        
        # Consider available balance
        available_long = self.account.availableDolLong.get(coin, 0)
        available_short = self.account.availableDolShort.get(coin, 0)
        available = min(available_long, available_short)
        
        if available > 0 and price > 0:
            max_size = min(max_size, available / price)
        
        return max_size