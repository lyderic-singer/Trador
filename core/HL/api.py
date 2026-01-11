import json
from json import JSONDecodeError
import aiohttp

from .utils.error import ClientError, ServerError
from .utils.types import Any
from ..session import Session
from log.logger import Logger


class API:
    def __init__(self, url: str, messenger, session : Session):
        self.url = url
        self.session = session
        self._log = Logger(self, messenger)

    async def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.url + url_path
        await self._log.debug(f"POST {url} | Payload: {payload}")
        try:
            session = await self.session.get()
            async with session.post(url, json=payload) as response:
                await self._handle_exception(response)
                try:
                    data = await response.json()
                    await self._log.debug(f"Response JSON recue")
                    return data
                except aiohttp.ContentTypeError:
                    text = await response.text()
                    await self._log.warning(f"Invalid JSON response")
                    return {"error": f"Could not parse JSON"}
        except Exception as e:
            await self._log.error(f"POST request failed: {e}")
            raise

    async def _handle_exception(self, response):
        status_code = response.status
        if status_code < 400:
            return
        text = await response.text()
        await self._log.warning(f"HTTP {status_code} | Response text: {text}")
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

    async def stop(self):
        await self._log.info("Closing API session.")
        await self.session.close()
