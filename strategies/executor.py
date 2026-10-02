import asyncio
import time
import traceback
from typing import Awaitable, Callable
from adapters.exchange import Exchange
from adapters.models import Cancel, Modify, Order, Result
from core.utils import cancel
from log.logger import Logger


class Request:
    def __init__(self, fn: Callable[[], Awaitable[list[Result]]], size: int, kind: str):
        self.fn = fn
        self.size = size
        self.kind = kind
        self.results: list[Result] = []
        self.completed = asyncio.Event()
        self.cancelled = False


class Executor:
    def __init__(self, exchange: Exchange, cooldown: float = 10.0, timeout: float = 20.0):
        self.exchange = exchange
        self.cooldown = cooldown
        self.timeout = timeout
        self._log = Logger(f"{exchange.name} Executor", exchange.messenger)
        self.requests: asyncio.Queue[Request] = asyncio.Queue()
        self.running = False
        self.task = None
        self.last = 0.0

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        self.running = True
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self._log.success("Stopped")

    async def run(self):
        try:
            while self.running:
                request = await self.requests.get()
                try:
                    if request.cancelled:
                        continue
                    wait = self.cooldown - (time.time() - self.last)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    try:
                        request.results = await request.fn()
                    except Exception as e:
                        await self._log.error(f"{request.kind} failed: {e}\n{traceback.format_exc()}")
                        request.results = [Result(False, error=str(e))] * request.size
                    self.last = time.time()
                    for r in request.results:
                        if not r.ok:
                            await self._log.error(f"{request.kind} rejected: {r.error or r.status}")
                finally:
                    request.completed.set()
                    self.requests.task_done()
        except asyncio.CancelledError:
            pass

    async def execute(self, fn, size: int, kind: str) -> list[Result]:
        if size == 0:
            return []
        request = Request(fn, size, kind)
        await self.requests.put(request)
        try:
            await asyncio.wait_for(request.completed.wait(), timeout=self.timeout + self.cooldown * (self.requests.qsize() + 1))
        except asyncio.TimeoutError:
            request.cancelled = True
            await self._log.critical(f"{kind} timed out, qsize={self.requests.qsize()}")
            return [Result(False, error="timeout")] * size
        if len(request.results) != size:
            await self._log.critical(f"{kind} returned {len(request.results)} results while expecting {size}")
        return request.results

    async def place(self, orders: list[Order]) -> list[Result]:
        return await self.execute(lambda: self.exchange.place(orders), len(orders), "place")

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        return await self.execute(lambda: self.exchange.modify(modifies), len(modifies), "modify")

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        return await self.execute(lambda: self.exchange.cancel(cancels), len(cancels), "cancel")
