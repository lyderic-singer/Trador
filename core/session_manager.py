import aiohttp

class SessionManager:
    def __init__(self):
        self.session = None

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
