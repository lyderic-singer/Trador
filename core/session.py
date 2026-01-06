import aiohttp
from HLB.logs.logger import Logger

class Session:
    def __init__(self, messenger):
        self.session = None
        self._log = Logger(self, messenger)

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            await self._log.debug("Creating new aiohttp session.")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        elif self.session.closed:
            await self._log.debug("Session was closed. Creating a new one.")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        else:
            await self._log.debug("Reusing existing aiohttp session.")
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self._log.debug("Closing aiohttp session.")
            await self.session.close()
        else:
            await self._log.debug("Session already closed or does not exist.")
