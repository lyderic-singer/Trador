import asyncio
import inspect
import os
import time
from datetime import datetime, timezone
from enum import Enum

class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    CRITICAL = "critical"
    EXCEPTION = "exception"

class Log:
    def __init__(self, message: str, level : LogLevel, context : str = "", send_tg : bool = False, save: bool = False):
        self.timestamp = time.time()
        self.iso_timestamp = datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()
        self.message = message
        self.send_tg = send_tg
        self.level = level
        self.context = context
        self.save = save
    
class MessageManager:
    def __init__(self):
        self.messages = asyncio.Queue()

    def _get_context(self):
        frame = inspect.stack()[2]
        filename = os.path.basename(frame.filename)
        return filename

    async def _put( self, message: str, level : LogLevel, context : str, send_tg : bool = False, save: bool = False):
        log = Log(message, level, context, send_tg, save)
        await self.messages.put(log)

    async def info(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.INFO, context, send_tg, save)

    async def warning(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.WARNING, context, send_tg, save)

    async def error(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.ERROR, context, send_tg, save)

    async def critical(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.CRITICAL, context, send_tg, save)

    async def debug(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.DEBUG, context, send_tg, save)

    async def exception(self, message: str, context: str, send_tg: bool = False, save: bool = False):
        await self._put(message, LogLevel.EXCEPTION, context, send_tg, save)

class Logger():
    def __init__(self, message_manager : MessageManager):
        self.message_manager = message_manager
        self.filepath = 