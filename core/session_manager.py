import aiohttp
from logs.message_manager import Logger

class SessionManager:
    def __init__(self, message_manager):
        self.session = None
        self._logging = Logger(self, message_manager)

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            await self._logging.debug("Creating new aiohttp session.")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        elif self.session.closed:
            await self._logging.debug("Session was closed. Creating a new one.")
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        else:
            await self._logging.debug("Reusing existing aiohttp session.")
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self._logging.debug("Closing aiohttp session.")
            await self.session.close()
        else:
            await self._logging.debug("Session already closed or does not exist.")
