import time
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

    async def handle(self, res, expected_type) -> list[bool]:
        status = res.get("status")
        response = res.get("response", {})
        request_type = response.get("type")
        data = response.get("data", {})
        statuses = data.get("statuses", [])

        if not (status == "ok" and request_type and statuses):
            await self._log.critical(res)
            return []
        check_type = "order" if expected_type == "modify" else expected_type
        if request_type != check_type:
            await self._log.critical(f"{expected_type} request returned {request_type}: {res}")

        executed = []
        cpt = 0
        if request_type == "cancel":
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
        elif request_type == "order":
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
        return executed

    async def batch_order(self, orders: list[Order]):
        res = await self.exchange.batch_order(orders)
        executed, n = await self.handle(res, "order")
        if len(orders) != len(executed):
