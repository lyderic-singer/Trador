import time
import asyncio
from typing import Any
from core.HL.utils.functions import cancel
from log.logger import Logger
from core.HL.utils.types import Cancel, Modify, Order
from core.HL.exchange import Exchange
from core.HL.hl import HL

class Request:
    def __init__(self, task, actions: list[Any], expected_type: str):
        self.task = task
        self.actions = actions
        self.expected_type = expected_type
        self.res = {}
        self.completed = asyncio.Event()
        self.executed = [] # sorted as actions
        self.cancelled = False

class Executor:
    def __init__(self, hl: HL):
        self.COOLDOWN = 10 # wait 10s minimum between each requests
        self.exchange: Exchange = hl.exchange
        self._log = Logger(self, hl.messenger)
        self.requests: asyncio.Queue[Request] = asyncio.Queue()
        self.running = False
        self.task = None
        self.last = 0

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        await cancel(self.task)
        await self._log.success("Stopped")

    async def handle_order(self, statuses: list[Any], expected_type: str) -> tuple[list[bool], int]:
        executed = []
        cpt = 0
        for status in statuses:
            if not isinstance(status, dict):
                executed.append(False)
                await self._log.critical(f"{expected_type} unexpected return: {status}")
            elif status.get("resting") is not None:
                executed.append(True)
                cpt += 1
            elif status.get("error") is not None:
                executed.append(False)
                await self._log.error(f"{expected_type} failed: {status['error']}")
            else:
                executed.append(False)
                await self._log.warning(f"{expected_type} unexpected return: {status}")
        return (executed, cpt)
    
    async def handle_cancel(self, statuses: list[Any]) -> tuple[list[bool], int]:
        executed = []
        cpt = 0
        for status in statuses:
            if status == "success":
                executed.append(True)
                cpt += 1
            elif isinstance(status, dict) and (status.get("error") is not None):
                executed.append(False)
                await self._log.error(f"cancel failed: {status['error']}")
            else:
                executed.append(False)
                await self._log.warning(f"cancel unexpected return: {status}")
        return (executed, cpt)

    async def handle(self, request: Request) -> list[bool]:
        status = request.res.get("status")
        response = request.res.get("response", {})
        request_type = response.get("type")
        data = response.get("data", {})
        statuses = data.get("statuses", [])
        if not (status == "ok" and request_type and statuses):
            await self._log.critical(f"{request.res}")
            return [False] * len(request.actions)
        check_type = "order" if request.expected_type == "modify" else request.expected_type
        if request_type != check_type:
            await self._log.critical(f"{request.expected_type} request returned {request_type}: {request.res}")
        if request_type == "order":
            executed, cpt = await self.handle_order(statuses, request.expected_type)
        elif request_type == "cancel":
            executed, cpt = await self.handle_cancel(statuses)
        else:
            executed, cpt = [False] * len(request.actions), 0
            await self._log.warning(f"unknown method: {request_type} | {request.res}")
        if len(request.actions) > cpt:
            await self._log.warning(f"{len(request.actions) - cpt} {request.expected_type} not executed")
        if len(request.actions) != len(executed):
            await self._log.critical(f"{request.expected_type} request returned {len(executed)} while expecting {len(request.actions)}")
        return executed

    async def run(self):
        try:
            while self.running:
                request = await self.requests.get()
                try:
                    if request.cancelled:
                        await self._log.info("request cancelled")
                    else:
                        request.res = await request.task
                        request.executed = await self.handle(request)
                        self.last = time.time()
                        await asyncio.sleep(self.COOLDOWN)
                finally:
                    request.completed.set()
                    self.requests.task_done()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
        finally: return

    async def execute(self, task, actions, expected_type, timeout=30):
        request = Request(task, actions, expected_type)
        await self.requests.put(request)
        try:
            await asyncio.wait_for(request.completed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            request.cancelled = True
            await self._log.critical(f"{expected_type} timed out for {actions}")
            return [False] * len(actions)
        return request.executed

    async def batch_order(self, orders: list[Order]) -> list[bool]:
        task = self.exchange.batch_order(orders)
        return await self.execute(task, orders, "order")

    async def batch_modify(self, modifies: list[Modify]):
        task = self.exchange.batch_modify(modifies)
        return await self.execute(task, modifies, "modify")

    async def batch_cancel(self, cancels: list[Cancel]):
        task = self.exchange.batch_cancel(True, cancels)
        return await self.execute(task, cancels, "cancel")
