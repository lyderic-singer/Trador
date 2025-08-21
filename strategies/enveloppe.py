import asyncio
from logs.message_manager import Logger
from time import time
import random
from data.market import Market

class Enveloppe:
    def __init__(self, asset, spread_min=0.0005, spread_max = 0.003, period = "1m", sma_period = 9, levels = 4, min_order_value = 10):
        self.core = None
        self.exchange = None
        self._logging = None
        self.period = period
        self.asset = asset
        self.market = Market(asset, sma_period, period)
        self.name = asset + "/USDC"
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

    def get_orders(self):
        sma = self.market.sma()
        orders = []
        if self.levels > 1:
            for i in range(self.levels):
                ask = sma * (1 + (((self.spread_max - self.spread_min) * (i / (self.levels - 1))) + self.spread_min))
                bid = sma * (1 - (((self.spread_max - self.spread_min) * (i / (self.levels - 1))) + self.spread_min))
                orders.append(ask)
                orders.append(bid)
        else:
            ask = sma * (1 + self.spread_max)
            bid = sma * (1 - self.spread_max)
            orders.append(ask)
            orders.append(bid)
        return orders

    async def run(self):
        await asyncio.sleep(3)
        while not self._stop.is_set():
            try:
                data, orders = await asyncio.gather(self.core.info.spot_user_state(self.core.address), self.core.info.open_orders(self.core.address))
                usd_balance, coin_balance = self.get_balance(data["balances"])
                value_coin_balance = self.market.mid * coin_balance
                value = usd_balance + value_coin_balance
                usd_per_order = 5

            except Exception as e:
                await self._logging.error(f"probleme dans l'execution : {e}")
            finally:
                await asyncio.sleep(5)

    async def stop(self):
        self._stop.set()