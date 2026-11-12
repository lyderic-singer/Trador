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
        # the target is frozen when the modify is queued: w.target can no longer be None at send time
        self.modifies: list[tuple[Working, tuple[float, float]]] = []
        self.cancels: list[Working] = []
        self.wake = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.running = False
        self.task = None
        self.stats: Counter = Counter()
        self.hold = 0.0
        self.errors: Counter = Counter()
        self.refused: Counter = Counter()   # (kind, symbol) -> requests failed: lets a strategy tell its markets apart

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
        self.modifies.append((w, (price, size)))
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

    @staticmethod
    def take(queue: list, room: int) -> list:
        batch, queue[:] = queue[:room], queue[room:]
        return batch

    async def send(self, kind: str, batch: list[Working], request, apply):
        if not batch:
            return
        self.stats[kind] += len(batch)
        try:
            results: list[Result] = await request()
        except Exception as e:
            retry = getattr(e, "retry", None) if getattr(e, "status", None) == 429 else None
            if retry is not None or getattr(e, "status", None) == 429:
                self.hold = max(self.hold, time.time() + (retry or 1000) / 1000)
                await self._log.warning(f"{kind} x{len(batch)} rate limited, holding {(retry or 1000) / 1000:.1f}s", send_tg=False)
            else:
                await self._log.error(f"{kind} x{len(batch)} failed: {e}\n{traceback.format_exc(limit=3)}")
            for w in batch:
                self.tracker.failed(w)
                self.refused[(kind, w.symbol)] += 1
            self.errors[kind] += len(batch)
            return
        for w, r in zip(batch, results):
            if not r.ok:
                self.errors[f"{kind}:{(r.error or r.status or '?')[:40]}"] += 1
                self.refused[(kind, w.symbol)] += 1
            apply(w, r)
        for w in batch[len(results):]:
            self.tracker.failed(w)

    async def round(self):
        room = self.limit
        cancels = self.take(self.cancels, room)
        # an order whose state changed while it was queued (failure, removal...) is no longer sent
        moves = [(w, t) for w, t in self.take(self.modifies, max(room - len(cancels), 0)) if w.state == "moving"]
        places = self.take(self.places, max(room - len(cancels) - len(moves), 0))
        for w, target in moves:
            w.target = target
        modifies = [w for w, _ in moves]
        # requests are built here, synchronously, before any await
        cancel_args = [Cancel(w.symbol, w.oid or "", w.client) for w in cancels]
        modify_args = [Modify(w.open_order(), size, price, TIF.ALO) for w, (price, size) in moves]
        place_args = [Order(w.symbol, w.side, w.size, w.price, TIF.ALO, False, w.client) for w in places]
        await asyncio.gather(
            self.send("cancel", cancels, lambda: self.exchange.cancel(cancel_args), self.tracker.canceled),
            self.send("modify", modifies, lambda: self.exchange.modify(modify_args), self.tracker.moved),
            self.send("place", places, lambda: self.exchange.place(place_args), self.tracker.placed))

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

