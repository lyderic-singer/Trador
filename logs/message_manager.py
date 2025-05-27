import asyncio
import inspect
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from enum import Enum

def get_file(objet):
    return os.path.basename(inspect.getfile(objet.__class__))

class LogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    CRITICAL = "critical"
    EXCEPTION = "exception"
    SUCCESS = "success"

class Message:
    def __init__(self, level: LogLevel, context: str, message: str, save: bool = False, send_tg: bool = False):
        self.timestamp = time.time()
        now = datetime.now(ZoneInfo("Europe/Paris")).replace(microsecond=0)
        offset_hours = int(now.utcoffset().total_seconds() // 3600)
        self.custom_timestamp = now.strftime(f"%Y-%m-%d %H:%M:%S ({f"UTC{offset_hours:+d}"})")
        self.level = level
        self.context = context
        self.message = message
        self.save = save
        self.send_tg = send_tg

class MessageManager:
    def __init__(self):
        self.messages = asyncio.Queue()

    async def _put(self, level: LogLevel, context: str, message: str, save: bool, send_tg: bool):
        log = Message(level, context, message, save, send_tg)
        await self.messages.put(log)

    async def info(self, context: str, message: str, save: bool = False, send_tg: bool = False):
        await self._put(LogLevel.INFO, context, message, save, send_tg)

    async def warning(self, context: str, message: str, save: bool = True, send_tg: bool = True):
        await self._put(LogLevel.WARNING, context, message, save, send_tg)

    async def error(self, context: str, message: str, save: bool = True, send_tg: bool = True):
        await self._put(LogLevel.ERROR, context, message, save, send_tg)

    async def critical(self, context: str, message: str, save: bool = True, send_tg: bool = True):
        await self._put(LogLevel.CRITICAL, context, message, save, send_tg)

    async def debug(self, context: str, message: str, save: bool = False, send_tg: bool = False):
        await self._put(LogLevel.DEBUG, context, message, save, send_tg)

    async def exception(self, context: str, message: str, save: bool = True, send_tg: bool = True):
        await self._put(LogLevel.EXCEPTION, context, message, save, send_tg)

    async def success(self, context: str, message: str, save: bool = True, send_tg: bool = True):
        await self._put(LogLevel.SUCCESS, context, message, save, send_tg)

class Logger():
    def __init__(self, objet, message_manager: MessageManager):
        self.message_manager = message_manager
        self.filepath = get_file(objet)

    async def info(self, message: str, save: bool = False, send_tg: bool = False):
        await self.message_manager.info(self.filepath, message, save, send_tg)

    async def warning(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.warning(self.filepath, message, save, send_tg)

    async def error(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.error(self.filepath, message, save, send_tg)

    async def critical(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.critical(self.filepath, message, save, send_tg)

    async def debug(self, message: str, save: bool = False, send_tg: bool = False):
        await self.message_manager.debug(self.filepath, message, save, send_tg)

    async def exception(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.exception(self.filepath, message, save, send_tg)

    async def success(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.success(self.filepath, message, save, send_tg)

t = time.time()
for i in range(1000000):
    m = Message( LogLevel.DEBUG, "pourtroll", "aller les négros", True , True)

e = time.time()

print(f"temps d'ex : {e -t}")