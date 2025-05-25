import json
import logging
from json import JSONDecodeError
import aiohttp
from .utils.constants import MAINNET_API_URL
from .utils.error import ClientError, ServerError
from .utils.types import Any


class API:
    def __init__(self, base_url=None):
        self.base_url = base_url or MAINNET_API_URL
        self._logger = logging.getLogger(__name__)
        self._session = None

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        return self._session

    async def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.base_url + url_path
        session = await self._get_session()
        async with session.post(url, json=payload) as response:
            await self._handle_exception(response)
            try:
                return await response.json()
            except aiohttp.ContentTypeError:
                text = await response.text()
                return {"error": f"Could not parse JSON: {text}"}

    async def _handle_exception(self, response):
        status_code = response.status
        if status_code < 400:
            return
        text = await response.text()
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

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None
