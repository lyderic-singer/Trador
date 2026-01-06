import asyncio
import inspect
import os
import sys
import time
from enum import Enum

def get_file(objet):
    return os.path.basename(inspect.getfile(objet.__class__))

def _callfunname():
    try:
        return sys._getframe(2).f_code.co_name
    except Exception:
        try:
            return inspect.stack()[2].function
        except Exception:
            return "<unknown>"

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
        self.iso_timestamp = None
        self.level = level
        self.context = context
        self.message = message
        self.save = save
        self.send_tg = send_tg

    def _get_iso(self):
        if self.iso_timestamp is None:
            self.iso_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        return self.iso_timestamp

    def format(self) -> str:
        return f"[{self._get_iso()}] [{self.level.value.upper()}] [{self.context}] {self.message}"

    def format_tg(self) -> str:
        emoji = {
            'debug': '🔧',
            'info': 'ℹ️',
            'warning': '⚠️',
            'error': '❌',
            'critical': '🔥',
            'success': '✅'
        }.get(self.level.value.lower(), '📌')
        return f"{emoji} {self.message}"

class Messenger:
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

    async def success(self, context: str, message: str, save: bool = True, send_tg: bool = False):
        await self._put(LogLevel.SUCCESS, context, message, save, send_tg)

class Logger():
    def __init__(self, objet, message_manager: Messenger):
        self.message_manager = message_manager
        if isinstance(objet, str):
            self.filepath = objet
        else:
            self.filepath = get_file(objet)

    async def info(self, message: str, save: bool = True, send_tg: bool = False):
        await self.message_manager.info(self.filepath, message, save, send_tg)

    async def warning(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.warning(self.filepath + " in " + _callfunname(), message, save, send_tg)

    async def error(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.error(self.filepath + " in " + _callfunname(), message, save, send_tg)

    async def critical(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.critical(self.filepath + " in " + _callfunname(), message, save, send_tg)

    async def debug(self, message: str, save: bool = True, send_tg: bool = False):
        await self.message_manager.debug(self.filepath, message, save, send_tg)

    async def exception(self, message: str, save: bool = True, send_tg: bool = True):
        await self.message_manager.exception(self.filepath + " in " + _callfunname(), message, save, send_tg)

    async def success(self, message: str, save: bool = True, send_tg: bool = False):
        await self.message_manager.success(self.filepath, message, save, send_tg)