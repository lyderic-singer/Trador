# ici tu respecte les rate limit tu dois modifier le account.py pour compter ton volume :

import asyncio
from time import time
from log.logger import Logger

class RateLimiter:
    """Handles rate limiting per IP and per address"""
    def __init__(self):
        # IP-based limits
        self.ip_weight = 0
        self.ip_reset_time = time() + 60
        self.ip_max_weight = 1200  # per minute
        
        # Address-based limits
        self.address_requests = 0
        self.address_reset_time = time() + 60
        
        # Volume tracking for address limit calculation
        self.cumulative_volume_usd = 0
        self.initial_buffer = 10000
        
    def calculate_request_weight(self, batch_length=1):
        """Calculate weight for a request"""
        return 1 + (batch_length // 40)
    
    def can_send_ip(self, weight=1):
        """Check if we can send based on IP limits"""
        now = time()
        if now >= self.ip_reset_time:
            self.ip_weight = 0
            self.ip_reset_time = now + 60
        
        return (self.ip_weight + weight) <= self.ip_max_weight
    
    def can_send_address(self):
        """Check if we can send based on address limits"""
        now = time()
        if now >= self.address_reset_time:
            self.address_requests = 0
            self.address_reset_time = now + 60
        
        # Calculate allowed requests based on volume
        allowed = self.initial_buffer + int(self.cumulative_volume_usd)
        
        return self.address_requests < allowed
    
    def record_request(self, weight=1, volume_usd=0):
        """Record a request"""
        self.ip_weight += weight
        self.address_requests += 1
        if volume_usd > 0:
            self.cumulative_volume_usd += volume_usd
    
    async def wait_if_needed(self, weight=1):
        """Wait if rate limits are hit"""
        max_wait = 60
        waited = 0
        
        while not (self.can_send_ip(weight) and self.can_send_address()):
            if waited >= max_wait:
                return False  # Timeout
            await asyncio.sleep(0.1)
            waited += 0.1
        
        return True


class OrderRequest:
    """Represents an order request"""
    def __init__(self, orders, priority=5, callback=None):
        self.orders = orders  # List of orders to send
        self.priority = priority  # 0-10, lower = higher priority
        self.callback = callback  # Async callback when done
        self.timestamp = time()
        self.result = None
        self.error = None
        self.done = asyncio.Event()


class ExecutorHL:
    """
    Handles order execution with rate limiting and prioritization
    """
    def __init__(self, hl, messenger):
        self.hl = hl
        self._log = Logger(self, messenger)
        self.running = asyncio.Event()
        
        # Rate limiting
        self.rate_limiter = RateLimiter()
        
        # Request queue (priority queue simulation)
        self.request_queue = asyncio.Queue(maxsize=5000)
        
        # Execution tasks
        self.executor_task = None
        
        # Stats
        self.orders_sent = 0
        self.orders_filled = 0
        self.orders_rejected = 0
        self.total_volume_usd = 0
        self.last_stats_time = time()
        
        # Account reference (set in start)
        self.account = None
        
    async def start(self, account):
        """Start the executor"""
        try:
            self.account = account
            self.running.set()
            self.executor_task = asyncio.create_task(self._execute_loop())
            await self._log.success("Started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise
    
    async def stop(self):
        """Stop the executor"""
        try:
            self.running.clear()
            if self.executor_task:
                self.executor_task.cancel()
                try:
                    await self.executor_task
                except asyncio.CancelledError:
                    pass
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")
    
    async def submit_orders(self, orders, priority=5, callback=None):
        """
        Submit orders for execution
        orders: list of order dicts
        priority: 0-10, lower = higher priority
        callback: async function to call when done
        Returns: OrderRequest object
        """
        request = OrderRequest(orders, priority, callback)
        
        try:
            await self.request_queue.put(request)
            return request
        except Exception as e:
            await self._log.error(f"Failed to submit orders: {e}")
            request.error = str(e)
            request.done.set()
            return request
    
    async def submit_cancels(self, cancels, priority=0):
        """
        Submit cancel requests (high priority)
        cancels: list of {"a": asset, "o": oid}
        """
        # Cancels have 2x limit, so they're prioritized
        request = OrderRequest(cancels, priority=priority)
        request.is_cancel = True
        
        try:
            await self.request_queue.put(request)
            return request
        except Exception as e:
            await self._log.error(f"Failed to submit cancels: {e}")
            return None
    
    async def _execute_loop(self):
        """Main execution loop"""
        while self.running.is_set():
            try:
                # Get next request (blocks if empty)
                request = await asyncio.wait_for(
                    self.request_queue.get(), 
                    timeout=0.1
                )
                
                # Execute the request
                await self._execute_request(request)
                
                # Stats
                if time() - self.last_stats_time > 60:
                    await self._log_stats()
                    self.last_stats_time = time()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                await self._log.error(f"Error in execution loop: {e}")
                await asyncio.sleep(0.1)
    
    async def _execute_request(self, request):
        """Execute a single request"""
        try:
            orders = request.orders
            batch_length = len(orders)
            
            # Calculate weight
            weight = self.rate_limiter.calculate_request_weight(batch_length)
            
            # Wait for rate limits if needed
            can_send = await self.rate_limiter.wait_if_needed(weight)
            
            if not can_send:
                request.error = "Rate limit timeout"
                request.done.set()
                await self._log.warning("Rate limit timeout")
                return
            
            # Calculate volume for this request
            volume_usd = 0
            for order in orders:
                size = float(order.get("s", 0))
                price = float(order.get("p", 0))
                volume_usd += size * price
            
            # Send the orders
            if hasattr(request, 'is_cancel') and request.is_cancel:
                result = await self.hl.exchange.bulk_cancel(True, orders)
            else:
                result = await self.hl.exchange.bulk_orders(orders)
            
            # Record the request
            self.rate_limiter.record_request(weight, volume_usd)
            
            # Update stats
            self.orders_sent += batch_length
            self.total_volume_usd += volume_usd
            
            # Parse result and check individual order statuses
            if result.get("status") == "ok":
                request.result = result
                
                # Parse statuses array to count successes and errors
                response = result.get("response", {})
                data = response.get("data", {})
                statuses = data.get("statuses", [])
                
                success_count = 0
                error_count = 0
                errors = []
                
                for status in statuses:
                    if "resting" in status or "filled" in status:
                        success_count += 1
                    elif "error" in status:
                        error_count += 1
                        errors.append(status["error"])
                
                # Update stats
                if error_count > 0:
                    self.orders_rejected += error_count
                    # Only log if ALL orders failed
                    if error_count == len(statuses):
                        await self._log.info(f"All {error_count} orders rejected: {errors[0]}")
                    else:
                        await self._log.debug(f"{error_count}/{len(statuses)} orders rejected")
                
                # Log success only at debug level to reduce spam
                if success_count > 0:
                    await self._log.debug(f"{success_count}/{len(statuses)} orders accepted")
                    
            else:
                # API-level error (not individual order errors)
                request.error = result
                self.orders_rejected += batch_length
                await self._log.error(f"API error: {result}")
            
            # Call callback if provided
            if request.callback:
                try:
                    await request.callback(request)
                except Exception as e:
                    await self._log.error(f"Callback error: {e}")
            
            # Mark as done
            request.done.set()
            
        except Exception as e:
            request.error = str(e)
            request.done.set()
            await self._log.error(f"Failed to execute request: {e}")
    
    async def _log_stats(self):
        """Log execution statistics"""
        await self._log.info(
            f"Stats: sent={self.orders_sent}, filled={self.orders_filled}, "
            f"rejected={self.orders_rejected}, volume=${self.total_volume_usd:.2f}"
        )
        
        # Reset counters
        self.orders_sent = 0
        self.orders_filled = 0
        self.orders_rejected = 0
        self.total_volume_usd = 0
    
    def get_rate_limit_info(self):
        """Get current rate limit status"""
        return {
            "ip_weight": self.rate_limiter.ip_weight,
            "ip_max": self.rate_limiter.ip_max_weight,
            "ip_available": self.rate_limiter.ip_max_weight - self.rate_limiter.ip_weight,
            "address_requests": self.rate_limiter.address_requests,
            "cumulative_volume": self.rate_limiter.cumulative_volume_usd,
            "queue_size": self.request_queue.qsize()
        }