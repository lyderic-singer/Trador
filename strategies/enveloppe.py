import asyncio
from logs.message_manager import Logger
from time import time
import random
from data.market import Market

class Enveloppe:
    def __init__(self, asset, spread_min=0.0005, spread_max = 0.003, period = "1m", sma_period = 9, levels = 4, min_order_value = 10.5):
        self.asset = asset
        self.name = asset + "/USDC"
        self.coin = None
        self.core = None
        self.exchange = None
        self._logging = None
        self.period = period
        self.market = Market(asset, sma_period, period)
        self.spread_min = spread_min
        self.spread_max = spread_max
        self.min_order_value = min_order_value
        self.levels = levels
        self._stop = asyncio.Event()
        self.last = time()
        self.orders = []

    async def init(self, core):
        self.core = core
        await self.market.start(core)
        self.coin = core.info.name_to_coin[self.name]
        self.exchange = core.exchange
        self._logging = Logger(self, core.message_manager)

    def get_balances(self, data):
        balances = {}
        for d in data:
            balances[d["coin"]] = float(d["total"])
        try:
            usd_balance = balances["USDC"]
        except Exception:
            usd_balance = 0
        try:
            coin_balance = balances[self.asset]
        except Exception:
            coin_balance = 0
        return usd_balance, coin_balance

    def new_orders(self, sma, mid, usd_balance, coin_balance_value):
        def _orders(balance, is_bid):
            value_per_order = balance / self.levels
            size = value_per_order / mid
            orders = []
            if value_per_order > self.min_order_value:
                for i in range(self.levels):
                    spread = ((self.spread_max - self.spread_min) * (i / (self.levels - 1))) + self.spread_min
                    price = sma * (1 - spread if is_bid else 1 + spread)
                    price = min(price, mid) if is_bid else max(price, mid)
                    orders.append(self.core.exchange.get_order(self.coin, is_bid, size, price, {"limit": {"tif": "Alo"}}))
            else:
                nb_orders = min(int(balance // self.min_order_value), self.levels)
                min_size = self.min_order_value / mid
                for i in range(self.levels - nb_orders, self.levels):
                    spread = ((self.spread_max - self.spread_min) * (i / (self.levels - 1))) + self.spread_min
                    price = sma * (1 - spread if is_bid else 1 + spread)
                    price = min(price, mid) if is_bid else max(price, mid)
                    orders.append(self.core.exchange.get_order(self.coin, is_bid, min_size, price, {"limit": {"tif": "Alo"}}))
            return orders
        new_asks = _orders(coin_balance_value, False)
        new_bids = _orders(usd_balance, True)
        return new_asks[::-1], new_bids[::-1]

    def get_orders(self, prev_orders, usd_balance, coin_balance_value):
        sma = self.market.sma()
        mid = self.market.mid
        new_asks, new_bids = self.new_orders(sma, mid, usd_balance, coin_balance_value)
        modified_orders = []
        for order in prev_orders:
            if order['side'] == 'A':
                new_order = new_asks.pop()
            else:
                new_order = new_bids.pop()
            modify_request = self.core.exchange.get_modifyRequest(order, new_order[''])
        return new_orders, modified_orders

    async def run(self):
        await asyncio.sleep(3)
        while not self._stop.is_set():
            try:
                data, orders = await asyncio.gather(self.core.info.spot_user_state(self.core.address), self.core.info.open_orders(self.core.address))
                usd_balance, coin_balance = self.get_balance(data["balances"])
                coin_balance_value = self.market.mid * coin_balance

                usd_per_order = 5

            except Exception as e:
                await self._logging.error(f"probleme dans l'execution : {e}")
            finally:
                await asyncio.sleep(5)

    async def stop(self):
        self._stop.set()