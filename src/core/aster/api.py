import json
import time
from typing import Any
from urllib.parse import urlencode
from yarl import URL
from core.latency import LATENCY
from core.session import Session
from core.aster.utils.signing import Signer
from log.logger import Logger
from log.messenger import Messenger


class AsterError(Exception):
    """a refusal of the exchange: HTTP status, Aster code (negative) and message"""

    def __init__(self, status: int, code: int | None, message: str, path: str, after: str | None = None):
        self.status = status
        self.code = code
        self.message = message
        self.path = path
        self.after = after
        super().__init__(f"{path}: {message} (code {code}, HTTP {status})")

    @property
    def retry(self) -> int | None:
        """ms to wait before sending again (429: rate limit, 418: IP banned for having insisted)"""
        if self.after is not None:
            try:
                return int(float(self.after) * 1000)
            except ValueError:
                pass
        return {429: 2000, 418: 120_000}.get(self.status)

    @property
    def unknown(self) -> bool:
        """the request may have been executed (503 and the timeouts of the matching engine)"""
        return self.status >= 500 or self.code in (-1001, -1006, -1007)


class AsterApi:
    def __init__(self, url: str, messenger: Messenger, session: Session, signer: Signer | None = None):
        self.url = url
        self.session = session
        self.signer = signer
        self._log = Logger("Aster Api", messenger)
        self.weight = 0          # request weight used in the current minute (X-MBX-USED-WEIGHT-1M), per IP
        self.orders = 0          # orders counted in the current minute (X-MBX-ORDER-COUNT-1M), per account
        self.quiet: set[int] = set()   # error codes not logged here (the caller reports them in its results)

    @staticmethod
    def method(path: str) -> str:
        return path.rsplit("/", 1)[-1]

    async def request(self, verb: str, path: str, params: dict | None = None, signed: bool = False, items: int = 1) -> Any:
        """GET / DELETE: parameters in the query string; POST / PUT: in a form body. A signed request is sent as the
        exact string that was signed"""
        params = {k: ("true" if v is True else "false" if v is False else v) for k, v in (params or {}).items() if v is not None}
        if signed:
            if self.signer is None:
                raise AsterError(401, None, "read-only client: no Aster API wallet key in the .env", path)
            text = self.signer.sign(params)
        else:
            text = urlencode(params)
        session = await self.session.get()
        url, data = self.url + path, None
        if verb in ("GET", "DELETE"):
            url = URL(f"{url}?{text}" if text else url, encoded=True)
        else:
            data = text.encode()
        name = self.method(path) if verb in ("GET", "POST") else f"{self.method(path)}:{verb.lower()}"
        t = time.perf_counter()
        ok, error = False, ""
        try:
            async with session.request(verb, url, data=data,
                                       headers={"Content-Type": "application/x-www-form-urlencoded"}) as response:
                out = await self.handle(response, path)
                ok = True
                return out
        except AsterError as e:
            error = str(e.code or e.status)
            raise
        except Exception as e:
            error = type(e).__name__
            raise
        finally:
            LATENCY.record("aster", name, "rest", (time.perf_counter() - t) * 1000, ok, items, error)

    async def get(self, path: str, params: dict | None = None, signed: bool = False) -> Any:
        return await self.request("GET", path, params, signed)

    async def handle(self, response, path: str) -> Any:
        text = await response.text()
        for header, field in (("X-MBX-USED-WEIGHT-1M", "weight"), ("X-MBX-ORDER-COUNT-1M", "orders")):
            value = response.headers.get(header)
            if value is not None:
                try:
                    setattr(self, field, int(value))
                except ValueError:
                    pass
        try:
            body = json.loads(text) if text else {}
        except json.JSONDecodeError:
            body = text
        failed = isinstance(body, dict) and isinstance(body.get("code"), int) and body["code"] < 0
        if response.status >= 400 or failed:
            code = body.get("code") if isinstance(body, dict) else None
            message = body.get("msg") if isinstance(body, dict) else str(body)[:300]
            if code not in self.quiet or response.status in (418, 429) or response.status >= 500:
                await self._log.warning(f"HTTP {response.status} {path} | {text[:400]}", send_tg=response.status == 418)
            raise AsterError(response.status, code, message or "", path, response.headers.get("Retry-After"))
        return body
