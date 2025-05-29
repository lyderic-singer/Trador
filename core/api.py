import json
from json import JSONDecodeError
import aiohttp

from .utils.error import ClientError, ServerError
from .utils.types import Any
from .session_manager import SessionManager
from logs.message_manager import Logger


class API:
    def __init__(self, base_url: str, message_manager, session : SessionManager):
        self.base_url = base_url
        self.session = session
        self._logging = Logger(self, message_manager)

    async def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.base_url + url_path
        await self._logging.debug(f"POST {url} | Payload: {payload}")
        try:
            session = await self.session.get_session()
            async with session.post(url, json=payload) as response:
                await self._handle_exception(response)
                try:
                    data = await response.json()
                    await self._logging.debug(f"Response JSON recue")
                    return data
                except aiohttp.ContentTypeError:
                    text = await response.text()
                    await self._logging.warning(f"Invalid JSON response")
                    return {"error": f"Could not parse JSON"}
        except Exception as e:
            await self._logging.error(f"POST request failed: {e}")
            raise

    async def _handle_exception(self, response):
        status_code = response.status
        if status_code < 400:
            return
        text = await response.text()
        await self._logging.warning(f"HTTP {status_code} | Response text: {text}")
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
        await self._logging.info("Closing API session.")
        await self.session.close()
