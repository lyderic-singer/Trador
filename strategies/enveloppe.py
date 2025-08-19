import asyncio
from logs.message_manager import Logger
from time import time
import random
from data.market import Market
from data.portfolio import Portfolio

class Enveloppe:
    def __init__(self, asset, spread_min=0.001, spread_max = 0.01, period = "1m", sma_period = 9, levels = 4):
        self.core = None
        self.exchange = None
        self._logging = None
        self.period = period
        self.asset = asset
        self.market = Market(asset, sma_period, period)
        self.name = asset + "/USDC"
        self.spread_min = spread_min
        self.spread_max = spread_max
        self.levels = levels
        self._stop = asyncio.Event()
        self.last = time()
        self.orders = 

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

    async def run(self):
        await asyncio.sleep(3)
        while not self._stop.is_set():
            try:
                data = await self.core.info.spot_user_state(self.core.address)
                usd, coin_balance = self.get_balance(data["balances"])
                   # la liquidité doit etre partagée par niveau
                await self.core.exchange.spot_order(self.coin, True, size, bid_price) # methode de placage ordre
                await self.core.exchange.edit(a, b) # edit l'ordre a en b
            except Exception as e:
                await self._logging.error(f"probleme dans l'execution : {e}")
            finally:
                await asyncio.sleep(5)

    async def stop(self):
        self._stop.set()
