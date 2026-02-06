import time
import asyncio
from typing import Any
from log.logger import Logger
from core.HL.utils.types import Order
from core.HL.exchange import Exchange
from core.HL.hl import HL

class Executor:
    def __init__(self, hl: HL):
        self.COOLDOWN = 10 # wait 10s minimum between each actions
        self.exchange: Exchange = hl.exchange
        self._log = Logger(self, hl.messenger)
        self.last = 0

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

    async def handle(self, res: dict, expected_type: str, actions: list[Any]) -> list[bool]:
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
        if len(actions) > cpt:
                await self._log.warning(f"{len(actions) - cpt} {expected_type} not executed")
        return executed

    async def batch_order(self, orders: list[Order]):
        timestamp = time.time()
        laps = timestamp - self.last
        if laps < self.COOLDOWN:
            await asyncio.sleep(self.COOLDOWN - laps)
        res = await self.exchange.batch_order(orders)
        executed, n = await self.handle(res, "order")
        if len(orders) != len(executed):
