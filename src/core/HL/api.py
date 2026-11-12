import json
from json import JSONDecodeError
from log.messenger import Messenger
from core.HL.utils.error import ClientError, ServerError
from core.HL.utils.types import Any
from core.session import Session
from log.logger import Logger

class HLApi:
    def __init__(self, url: str, messenger: Messenger, session : Session):
        self.url = url
        self.session = session
        self._log = Logger(self, messenger)

    async def post(self, path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.url + path
        session = await self.session.get()
        async with session.post(url, json=payload) as response:
            await self.handle(response)
            return await response.json()

    async def handle(self, response):
        status_code = response.status
        if status_code < 400:
            return
        text = await response.text()
        await self._log.warning(f"HTTP {status_code} | {text}")
        if 400 <= status_code < 500:
            try:
                err = json.loads(text)
            except JSONDecodeError:
                raise ClientError(status_code, None, text, None, response.headers)
            if err is None:
                raise ClientError(status_code, None, text, None, response.headers)
            error_data = err.get("data")
            raise ClientError(status_code, err.get("code"), err.get("msg"), response.headers, error_data)
        raise ServerError(status_code, text)
