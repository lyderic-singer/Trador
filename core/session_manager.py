import aiohttp
from logs.message_manager import MessageManager

class SessionManager:
    def __init__(self, message_manager : MessageManager):
        self.session = None
        self.message_manager = message_manager

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            await self.log.info("")
