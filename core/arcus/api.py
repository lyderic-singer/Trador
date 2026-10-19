import json
import time
from typing import Any
from core.latency import LATENCY
from core.session import Session
from core.arcus.utils.signing import canonical
from log.logger import Logger
from log.messenger import Messenger


class ArcusError(Exception):
    def __init__(self, status: int, body: Any, path: str, after: str | None = None):
        self.after = after
        self.status = status
        self.body = body
        self.path = path
        super().__init__(f"HTTP {status} on {path}: {body}")

    @property
    def retry(self) -> int | None:
        if isinstance(self.body, dict) and self.body.get("retryAfterMs") is not None:
            return int(self.body["retryAfterMs"])
        if self.after is not None:
            return int(float(self.after) * 1000)
        return None


class ArcusApi:
    def __init__(self, url: str, messenger: Messenger, session: Session):
        self.url = url
        self.session = session
        self._log = Logger("Arcus Api", messenger)

    @staticmethod
    def method(path: str) -> str:
        parts = [p for p in path.split("/") if p and p != "v1"]
        return parts[0] if parts else path

    async def get(self, path: str, params: dict | None = None, headers: dict | None = None) -> Any:
        session = await self.session.get()
        params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in (params or {}).items() if v is not None}
        t = time.perf_counter()
        ok, error = False, ""
        try:
            async with session.get(self.url + path, params=params, headers=headers) as response:
                out = await self.handle(response, path)
                ok = True
                return out
        except ArcusError as e:
            error = str(e.status)
            raise
        except Exception as e:
            error = type(e).__name__
            raise
        finally:
            LATENCY.record("arcus", self.method(path), "rest", (time.perf_counter() - t) * 1000, ok, error=error)

    async def post(self, path: str, body: Any, headers: dict | None = None, params: dict | None = None, items: int = 1) -> Any:
        session = await self.session.get()
        data = canonical(body).encode()
        t = time.perf_counter()
        ok, error = False, ""
        try:
            async with session.post(self.url + path, data=data, params=params,
                                    headers={"Content-Type": "application/json", **(headers or {})}) as response:
                out = await self.handle(response, path)
                ok = True
                return out
        except ArcusError as e:
            error = str(e.status)
            raise
        except Exception as e:
            error = type(e).__name__
            raise
        finally:
            LATENCY.record("arcus", self.method(path), "rest", (time.perf_counter() - t) * 1000, ok, items, error)

    async def handle(self, response, path: str) -> Any:
        text = await response.text()
        try:
            body = json.loads(text) if text else {}
        except json.JSONDecodeError:
            body = text
        if response.status >= 400:
            await self._log.warning(f"HTTP {response.status} {path} | {text[:500]}")
            raise ArcusError(response.status, body, path, response.headers.get("Retry-After"))
        return body
