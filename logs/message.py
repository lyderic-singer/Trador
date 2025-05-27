import time
import asyncio
from datetime import datetime, timezone
from enum import Enum

class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    CRITICAL = "critical"

class Log:
    def __init__(self, message: str, level : LogLevel, send_tg : bool = False, save: bool = False):
        self.timestamp = time.time()
        self.iso_timestamp = datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()
        self.message = message
        self.send_tg = send_tg
        self.level = level
        self.save = save
    
class MessageManager:
    def __init__(self):
        self.messages = asyncio.Queue()

    async def _put( self, message: str, level : LogLevel, send_tg : bool = False, save: bool = False):
        log = Log(message, level, send_tg, save)
        await self.messages.put(log)

    async def info(self, message: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.INFO, send_tg, save)

    async def warning(self, message: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.WARNING, send_tg, save)

    async def error(self, message: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.ERROR, send_tg, save)

    async def critical(self, message: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.CRITICAL, send_tg, save)

    async def debug(self, message: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.DEBUG, send_tg, save)