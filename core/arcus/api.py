import json
from typing import Any
from core.session import Session
from core.arcus.utils.signing import canonical
from log.logger import Logger
from log.messenger import Messenger


class ArcusError(Exception):
    def __init__(self, status: int, body: Any, path: str):
        self.status = status
        self.body = body
        self.path = path
        super().__init__(f"HTTP {status} on {path}: {body}")

    @property
    def retry(self) -> int | None:
        return self.body.get("retryAfterMs") if isinstance(self.body, dict) else None


class ArcusApi:
    def __init__(self, url: str, messenger: Messenger, session: Session):
        self.url = url
        self.session = session
        self._log = Logger("Arcus Api", messenger)

    async def get(self, path: str, params: dict | None = None, headers: dict | None = None) -> Any:
        session = await self.session.get()
        params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in (params or {}).items() if v is not None}
        async with session.get(self.url + path, params=params, headers=headers) as response:
            return await self.handle(response, path)

    async def post(self, path: str, body: Any, headers: dict | None = None, params: dict | None = None) -> Any:
        session = await self.session.get()
        data = canonical(body).encode()
        async with session.post(self.url + path, data=data, params=params,
                                headers={"Content-Type": "application/json", **(headers or {})}) as response:
            return await self.handle(response, path)

    async def handle(self, response, path: str) -> Any:
        text = await response.text()
        try:
            body = json.loads(text) if text else {}
        except json.JSONDecodeError:
            body = text
        if response.status >= 400:
            await self._log.warning(f"HTTP {response.status} {path} | {text[:500]}")
            raise ArcusError(response.status, body, path)
        return body
