import asyncio
from logs.message_manager import Logger
from time import time
import random

def get_balance(data):
    balances = {}
    for d in data:
        balances[d["coin"]] = float(d["total"])
    return balances

class Enveloppe:
    def __init__(self, asset, spread_min=0.001, spread_max = 0.01, sma_period = 9, levels = 4):
        self.core = None
        self.exchange = None
        self._logging = None
        self.asset = asset
        self.name = asset + "/USDC"
        self.spread_min = spread_min
        self.spread_max = spread_max
        self.sma_period = sma_period
        self.levels = levels
        self.mid = None
        self._stop = asyncio.Event()
        self.last = time()
        self.last_maj_orders = None

    def init(self, core_manager):
        self.core = core_manager
        self.coin = core_manager.info.name_to_coin[self.name]
        self.exchange = core_manager.exchange
        self._logging = Logger(self, core_manager.message_manager)

    async def on_bbo(self, msg):
        best_bid = float(msg["data"]["bbo"][0]["px"])
        best_ask = float(msg["data"]["bbo"][1]["px"])
        self.best_bid = best_bid
        self.best_ask = best_bid
        mid = (best_ask + best_bid) / 2
        self.mid = mid
        return best_bid, best_ask, mid

    async def update(self):
        

    async def run(self):
        await self.core.info.subscribe({"type": "bbo", "coin": self.name}, self.on_bbo)
        await asyncio.sleep(3)
        while not self._stop.is_set():
            try:
                data = await self.core.info.spot_user_state(self.core.address)
                balances = get_balance(data["balances"])
                try:
                    usd = balances["USDC"]
                except Exception:
                    usd = 0
                try:
                    coin_balance = balances[self.asset]
                except Exception:
                    coin_balance = 0
                size = random.uniform(10.5, 12.5)/ self.mid
                a = self.mid * (1 - (self.spread/2))
                b = self.mid * (1 + (self.spread/2))
                bid_price = self.best_bid if self.best_bid <= a else a
                ask_price = self.best_ask if self.best_ask >= b else b
                if usd > bid_price * size:
                    await self.core.exchange.spot_order(self.coin, True, size, bid_price)
                if coin_balance > size:
                    await self.core.exchange.spot_order(self.coin, False, size, ask_price)
                await self.clear_orders()
            except Exception as e:
                await self._logging.error(f"probleme dans l'execution : {e}")
            finally:
                await asyncio.sleep(2)

    async def stop(self):
        self._stop.set()
