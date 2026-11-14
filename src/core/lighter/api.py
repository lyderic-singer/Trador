import json
import time
from typing import Any
from core.latency import LATENCY
from core.session import Session
from core.lighter.utils.constants import RATE_LIMITED
from log.logger import Logger
from log.messenger import Messenger


class LighterError(Exception):
    """a refusal of the exchange: HTTP status (429 for every rate limit, websocket codes included), Lighter code"""

    def __init__(self, status: int, code: int | None, message: str, path: str):
        self.status = 429 if code in RATE_LIMITED else status
        self.code = code
        self.message = message
        self.path = path
        super().__init__(f"{path}: {message} (code {code}, HTTP {status})")

    @property
    def retry(self) -> int | None:
        """ms to wait before sending again (the Gateway reads it on a 429)"""
        return 1000 if self.status in (429, 405) else None


class LighterApi:
    def __init__(self, url: str, messenger: Messenger, session: Session):
        self.url = url
        self.session = session
        self._log = Logger("Lighter Api", messenger)
        self.token = None      # callable -> auth token, wired by the client

    @staticmethod
    def method(path: str) -> str:
        return path.rsplit("/", 1)[-1]

    def headers(self, auth: bool) -> dict | None:
        if not auth:
            return None
        token = self.token() if self.token else None
        if not token:
            raise LighterError(401, None, "no auth token (read-only client: no API key in the .env)", "auth")
        return {"Authorization": token}

    async def get(self, path: str, params: dict | None = None, auth: bool = False) -> Any:
        session = await self.session.get()
        params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in (params or {}).items() if v is not None}
        t = time.perf_counter()
        ok, error = False, ""
        try:
            async with session.get(self.url + path, params=params, headers=self.headers(auth)) as response:
                out = await self.handle(response, path)
                ok = True
                return out
        except LighterError as e:
            error = str(e.code or e.status)
            raise
        except Exception as e:
            error = type(e).__name__
            raise
        finally:
            LATENCY.record("lighter", self.method(path), "rest", (time.perf_counter() - t) * 1000, ok, error=error)

    async def post(self, path: str, form: dict, auth: bool = False, items: int = 1) -> Any:
        """form-urlencoded body, as every POST of the API"""
        session = await self.session.get()
        headers = {"Content-Type": "application/x-www-form-urlencoded", **(self.headers(auth) or {})}
        t = time.perf_counter()
        ok, error = False, ""
        try:
            async with session.post(self.url + path, data={k: v for k, v in form.items() if v is not None},
                                    headers=headers) as response:
                out = await self.handle(response, path)
                ok = True
                return out
        except LighterError as e:
            error = str(e.code or e.status)
            raise
        except Exception as e:
            error = type(e).__name__
            raise
        finally:
            LATENCY.record("lighter", self.method(path), "rest", (time.perf_counter() - t) * 1000, ok, items, error)

    async def handle(self, response, path: str) -> Any:
        text = await response.text()
        try:
            body = json.loads(text) if text else {}
        except json.JSONDecodeError:
            body = text
        code = body.get("code") if isinstance(body, dict) else None
        if response.status >= 400 or (code is not None and int(code) != 200):
            message = body.get("message") if isinstance(body, dict) else str(body)[:300]
            if response.status in (429, 405) or path.rsplit("/", 1)[-1] not in ("sendTx", "sendTxBatch"):
                await self._log.warning(f"HTTP {response.status} {path} | {text[:500]}", send_tg=False)
            raise LighterError(response.status, int(code) if code is not None else None, message or "", path)
        return body
