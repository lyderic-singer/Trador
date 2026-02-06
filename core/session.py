import aiohttp
from log.logger import Logger

class Session:
    def __init__(self, messenger):
        self.session = None
        self._log = Logger(self, messenger)

    async def start(self):
        try:
            if self.session and not self.session.closed:
                return await self._log.info("already started")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
            await self._log.success("Started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def get(self) -> aiohttp.ClientSession:
        if self.session is None:
            await self._log.debug("New session")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        if self.session.closed:
            await self._log.info("Session closed, creating a new one")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        return self.session

    async def stop(self):
        if self.session and not self.session.closed:
            await self.session.close()
            await self._log.success("Stopped")
        else:
            await self._log.info("Session already closed or does not exist")
