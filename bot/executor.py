import time
import asyncio
from typing import Any
from log.logger import Logger
from core.HL.utils.types import Order
from core.HL.exchange import Exchange
from core.HL.hl import HL

class Action:
    def __init__(self, action, res):
        self.action = action
        self.res = None
        self.completed = asyncio.Event()

class Executor:
    def __init__(self, hl: HL):
        self.COOLDOWN = 10 # wait 10s minimum between each requests
        self.exchange: Exchange = hl.exchange
        self._log = Logger(self, hl.messenger)
        self.actions = asyncio.Queue()
        self.running = False
        self.task = None
        self.last = 0

    async def start(self):
        self.task = asyncio.create_task(self.run())
        await self._log.success("Started")

    async def stop(self):


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
                await self._log.error(f"{expected_type} failed: {status["error"]}")
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
                await self._log.error(f"cancel failed: {status["error"]}")
            else:
                executed.append(False)
                await self._log.warning(f"cancel unexpected return: {status}")
        return (executed, cpt)

    async def handle(self, res: dict, expected_type: str, requests: list[Any]) -> list[bool]:
        status = res.get("status")
        response = res.get("response", {})
        request_type = response.get("type")
        data = response.get("data", {})
        statuses = data.get("statuses", [])

        if not (status == "ok" and request_type and statuses):
            await self._log.critical(f"{res}")
            return []
        check_type = "order" if expected_type == "modify" else expected_type
        if request_type != check_type:
            await self._log.critical(f"{expected_type} request returned {request_type}: {res}")

        if request_type == "order":
            executed, cpt = await self.handle_order(statuses, expected_type)
        elif request_type == "cancel":
            executed, cpt = await self.handle_cancel(statuses)
        else:
            executed, cpt = [], 0
            await self._log.warning(f"unknown method: {request_type} | {res}")
        if len(requests) > cpt:
                await self._log.warning(f"{len(requests) - cpt} {expected_type} not executed")
        return executed

    async def run(self):
        try:
            while self.running:
                action = await self.actions.get()
                await action
                self.last = time.time()
                await asyncio.sleep(self.COOLDOWN)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.error(f"{e}")
        finally: return
            

    async def batch_order(self, orders: list[Order]):
        action = self.exchange.batch_order(orders)
        await self.actions.put(action)
        # la je peux pas return handle(res) comment je fai
