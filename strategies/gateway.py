import asyncio
import time
import traceback
from collections import Counter
from adapters.exchange import Exchange
from adapters.models import Cancel, Modify, Order, Result, TIF
from core.utils import cancel
from log.logger import Logger
from strategies.orders import OrderTracker, Working


class Gateway:
    def __init__(self, exchange: Exchange, tracker: OrderTracker, interval: float = 0.02, limit: int = 200):
        self.exchange = exchange
        self.tracker = tracker
        self.interval = interval
        self.limit = limit
        self._log = Logger(f"{exchange.name} Gateway", exchange.messenger)
        self.places: list[Working] = []
        self.modifies: list[Working] = []
        self.cancels: list[Working] = []
        self.wake = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.running = False
        self.task = None
        self.stats: Counter = Counter()
        self.hold = 0.0
        self.errors: Counter = Counter()

    async def start(self):
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self.run())

    async def stop(self):
        self.running = False
        await cancel(self.task)

    def place(self, w: Working):
        w.state, w.sent = "pending", time.time()
        self.places.append(w)
        self.wake.set()

    def modify(self, w: Working, price: float, size: float):
        w.state, w.sent, w.target = "moving", time.time(), (price, size)
        self.modifies.append(w)
        self.wake.set()

    def cancel(self, w: Working):
        w.state, w.sent = "canceling", time.time()
        self.cancels.append(w)
        self.wake.set()

    @property
    def queued(self) -> int:
        return len(self.places) + len(self.modifies) + len(self.cancels)

    async def flush(self, timeout: float = 5.0):
        self.wake.set()
        end = time.time() + timeout
        while (self.queued or not self.idle.is_set()) and time.time() < end:
            await asyncio.sleep(self.interval)

    def take(self, queue: list[Working], room: int) -> list[Working]:
        batch, queue[:] = queue[:room], queue[room:]
        return batch

    async def send(self, kind: str, batch: list[Working], call, apply):
        if not batch:
            return
        self.stats[kind] += len(batch)
        try:
            results: list[Result] = await call(batch)
        except Exception as e:
            retry = getattr(e, "retry", None) if getattr(e, "status", None) == 429 else None
            if retry is not None or getattr(e, "status", None) == 429:
                self.hold = max(self.hold, time.time() + (retry or 1000) / 1000)
                await self._log.warning(f"{kind} x{len(batch)} rate limited, holding {(retry or 1000) / 1000:.1f}s", send_tg=False)
            else:
                await self._log.error(f"{kind} x{len(batch)} failed: {e}\n{traceback.format_exc(limit=3)}")
            for w in batch:
                self.tracker.failed(w)
            self.errors[kind] += len(batch)
            return
        for w, r in zip(batch, results):
            if not r.ok:
                self.errors[f"{kind}:{(r.error or r.status or '?')[:40]}"] += 1
            apply(w, r)
        for w in batch[len(results):]:
            self.tracker.failed(w)

    async def round(self):
        room = self.limit
        cancels = self.take(self.cancels, room)
        modifies = self.take(self.modifies, max(room - len(cancels), 0))
        places = self.take(self.places, max(room - len(cancels) - len(modifies), 0))
        await asyncio.gather(
            self.send("cancel", cancels,
                      lambda b: self.exchange.cancel([Cancel(w.symbol, w.oid or "", w.client) for w in b]),
                      self.tracker.canceled),
            self.send("modify", modifies,
                      lambda b: self.exchange.modify([Modify(w.open_order(), w.target[1], w.target[0], TIF.ALO) for w in b]),
                      self.tracker.moved),
            self.send("place", places,
                      lambda b: self.exchange.place([Order(w.symbol, w.side, w.size, w.price, TIF.ALO, False, w.client) for w in b]),
                      self.tracker.placed))

    async def run(self):
        try:
            while self.running:
                await self.wake.wait()
                self.wake.clear()
                await asyncio.sleep(max(self.interval, self.hold - time.time()))
                self.idle.clear()
                try:
                    await self.round()
                finally:
                    self.idle.set()
                if self.queued:
                    self.wake.set()
        except asyncio.CancelledError:
            pass
