import asyncio
from typing import Iterable
from core.HL.utils.functions import cancel, get_order, get_modify, remaining
from bot.market import MarketHL
from bot.account import AccountPerpDexHL
from bot.executor import Executor
from core.HL.utils.types import Modify, Order
from log.logger import Logger

class Grid:
    def __init__(self, levels: int, spread: float, offset: float, alpha: float):
        self.running = False
        self.levels = levels
        self.spread = spread
        self.offset = offset
        self.alpha = alpha
        self.bids = []
        self.asks = []
        if self.levels <= 1:
            self.geo = 1.0
        elif self.alpha == 1:
            self.geo = self.levels - 1
        else:
            self.geo = (self.alpha ** (self.levels - 1) - 1) / (self.alpha - 1)

    def update(self, ema: float):
        raw = self.spread * ema / self.geo
        offset = self.offset * raw
        bids = []
        px = ema - offset
        for i in range(self.levels):
            bids.append(px)
            px -= raw * (self.alpha ** i)
        self.bids = list(reversed(bids))
        asks = []
        px = ema + offset
        for i in range(self.levels):
            asks.append(px)
            px += raw * (self.alpha ** i)
        self.asks = asks

class Enveloppe:
    def __init__(self, x: str | int, hl):
        # ==== CONSTANTS ====
        self.MAKER_FEES = 0.00003 # 0.003%
        self.TAKER_FEES = 0.00009 # 0.009%
        self.SPREAD = 0.02        # 4.5% | minimum distance between the first and the last placed orders
        self.OFFSET = 0.0001      # 0.01% spread from ema for first
        self.ALPHA = 3.6          # exponential price factor between levels (>1 -> more and more spaced)
        self.SKEW = 10            # size ratio between last and first order (SKEW=10 -> last order is 10x bigger than first)
        self.LEVELS = 3           # orders count of each side
        self.CAP = 1              # 85% | max capacity of portfolio used
        self.MIN_VALUE = 10.5     # minimum order notional value in $ to avoid overfragmentation / fees inefficiency
        self.POLL_DELAY = 10.1    # delay in seconds between each grid evaluation cycle
        self.THRESHOLD = 0.0001   # 0.01% | relative EMA change threshold to trigger rebalancing
        self.TIMEFRAME = "5m"     # market data candle timeframe used for indicators (SMA/EMA)
        self.WINDOW = 40          # rolling window size for indicator (SMA/EMA period)
        self.INV = True           # stop place / modify in the part against mean if activated
        # ==== STATS ====
        self.cycles = 0           # nb of cycles for stats
        self.placed = 0           # nb of place for stats
        self.modified = 0         # nb of modify for stats
        self.passed = 0           # nb of pass for stats
        self.placed_error = 0     # nb of errors while place for stats
        self.modified_error = 0   # nb of errors while modify for stats
        # ==== UTILS ====
        self.task = None
        self.running = False
        self.last_ema = None
        self.beta = self.SKEW ** (1 / (self.LEVELS - 1)) if self.LEVELS > 1 else 1.0
        self.geo = (1 - self.beta ** self.LEVELS) / (1 - self.beta) if self.beta != 1.0 else float(self.LEVELS)
        self.grid = Grid(self.LEVELS, self.SPREAD, self.OFFSET, self.ALPHA)
        # ==== CORE ====
        self.hl = hl
        self.address = hl.address
        self._log = Logger(self, hl.messenger)
        self.instrument = hl.info.instrument(x)
        self.dex = self.instrument.dex
        self.executor = Executor(hl)
        self.market = MarketHL(self.instrument, hl, self.WINDOW, self.TIMEFRAME)
        self.account = AccountPerpDexHL(self.dex, [self.instrument], hl)

    def available(self, leveraged_account_value, mid: float, pos_value, pos_size, bids_value: float = 0.0, asks_value: float = 0.0):
        position_value = pos_value if pos_size >= 0 else -pos_value
        longable = max(leveraged_account_value - position_value - bids_value, 0) # TODO adapt for multiple coins
        shortable = max(leveraged_account_value + position_value - asks_value, 0) # TODO adapt for multiple coins
        factor = self.CAP / mid # normalize $ to get [COIN]
        return factor * longable, factor * shortable # max_sz_bid, max_sz_ask

    def valid(self, is_buy, px, sz, best_bid, best_ask):
        value = px * sz
        if is_buy:
            return (value > self.MIN_VALUE) and (px <= best_ask)
        elif not is_buy:
            return (value > self.MIN_VALUE) and (px >= best_bid)
        else:
            return False

    def sizes(self, available: float) -> list[float]:
        if self.LEVELS == 0: return []
        if self.beta == 1.0 or self.LEVELS == 1:
            unit = available / self.LEVELS
            return [unit] * self.LEVELS
        s0 = available / self.geo
        return [s0 * (self.beta ** i) for i in range(self.LEVELS)]

    def compute_place(self, leveraged_account_value: float, bids: list, asks: list, best_bid, best_ask, ema, pos_value, pos_size):
        mid = (best_bid + best_ask) / 2
        bids_px, asks_px = [b["limitPx"] for b in bids], [a["limitPx"] for a in asks]
        self.grid.update(ema)
        longable, shortable = self.available(leveraged_account_value, mid, pos_value, pos_size, sum([sz * px for sz, px in bids]), sum([sz * px for sz, px in asks]))
        orders = []
        def process(existing, existing_px, grid_px, available, is_buy):
            sizes = self.sizes(available)
            size = 0
            nominal = list(reversed(sizes)) if is_buy else sizes
            missing_px = remaining(existing_px, grid_px)
            missing, ref, m = [], 0.0, 0
            for k, px in enumerate(grid_px):
                if m < len(missing_px) and px == missing_px[m]:
                    missing.append(k)
                    m += 1
                else:
                    ref += nominal[k]
            used = sum(o[0] for o in existing)
            scale = min(used / ref, 1.0) if ref > 0 else 1.0
            for k in missing:
                sz = scale * nominal[k]
                if self.valid(is_buy, grid_px[k], sz, best_bid, best_ask):
                    size += sz
                    orders.append((sz, grid_px[k]))
        if self.INV:
            if ema and ema < mid: process(bids, bids_px, self.grid.bids, longable, True)
            else: process(asks, asks_px, self.grid.asks, shortable, False)
        else:
            process(bids, bids_px, self.grid.bids, longable, True)
            process(asks, asks_px, self.grid.asks, shortable, False)
        return orders

    def compute_modify(self, leveraged_account_value: float, bids: list, asks: list, best_bid, best_ask, ema, pos_value, pos_size): # both bids and asks in ascending order
        mid = (best_bid + best_ask) / 2
        self.grid.update(ema)
        modifies = []
        longable, shortable = self.available(leveraged_account_value, mid, pos_value, pos_size, 0, 0) # disregard bids and asks value because they gonna be modified
        def process(orders: Iterable, n: int, available: float, price_fn, is_buy):
            sizes = self.sizes(available)
            size = 0
            scale = max(available / sum(sizes[-n:]), 1)
            for i, o in enumerate(orders):
                sz = scale * sizes[-(i+1)]
                size += sz
                if self.valid(is_buy, price_fn(i), sz, best_bid, best_ask): modifies.append((o, (sz, price_fn(i))))
            print(available, size, sum(sizes[-n:]))
        if self.INV:
            if ema and ema < mid: process(bids, len(bids), longable, lambda i: min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid), True)
            else: process(reversed(asks), len(asks), shortable, lambda i: max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid), False)
        else:
            process(bids, len(bids), longable, lambda i: min(self.grid.bids[i], (1 - self.MAKER_FEES) * mid), True)
            process(reversed(asks), len(asks), shortable, lambda i: max(self.grid.asks[-(i + 1)], (1 + self.MAKER_FEES) * mid), False)
        return modifies
