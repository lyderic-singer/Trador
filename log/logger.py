import inspect
import os
import sys
import time
from enum import Enum

def get_file(objet):
    return os.path.basename(inspect.getfile(objet.__class__))[:-3]

def get_function():
    try:
        return sys._getframe(3).f_code.co_name
    except Exception:
        try:
            return inspect.stack()[3].function
        except Exception:
            return "<unknown>"

def get_line():
    try:
        return sys._getframe(3).f_lineno
    except Exception:
        try:
            return inspect.stack()[3].lineno
        except Exception:
            return -1

class LogLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"
    CRITICAL = "CRITICAL"
    EXCEPTION = "EXCEPTION"
    SUCCESS = "SUCCESS"
    CUSTOM = "CUSTOM"

class Message:
    def __init__(self, level: LogLevel, context: str, text: str, display: bool, save: bool = False, send_tg: bool = False, styles: str | None = None):
        self.timestamp = time.time()
        self.date = None
        self.level = level
        self.context = context[0].upper() + context[1:]
        self.display = display
        self.text = text
        self.save = save
        self.send_tg = send_tg
        self.styles = styles

    def format_date(self):
        if self.date is None:
            self.date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        return self.date

    def format(self) -> str:
        return f"[{self.format_date()}] [{self.level.value}] [{self.context}] {self.text}"

    def format_tg(self) -> str:
        emoji = {'DEBUG': '🔧', 'INFO': 'ℹ️', 'WARNING': '⚠️', 'ERROR': '❌', 'CRITICAL': '🔥', 'SUCCESS': '✅'}.get(self.level.value, '📌')
        return f"{emoji} [{self.context}] {self.text}"

class Logger():
    def __init__(self, objet, messenger):
        self.messenger = messenger
        if isinstance(objet, str):
            self.filepath = objet
        else:
            self.filepath = get_file(objet)

    def get_context(self):
        return self.filepath + ':' + f"{get_line()}" + " in " + get_function()

    async def custom(self, text: str, level: LogLevel = LogLevel.CUSTOM, context: str | None = None, display: bool = True, save: bool = True, send_tg: bool = False, styles: str = "BRIGHT_YELLOW"):
        message = Message(level, context if context else self.filepath, text, display, save, send_tg, styles)
        await self.messenger.messages.put(message)

    async def info(self, text: str, display: bool = True, save: bool = True, send_tg: bool = False, styles: str = "CYAN"):
        await self.custom(text, LogLevel.INFO, self.filepath, display, save, send_tg, styles)

    async def warning(self, text: str, display: bool = True, save: bool = True, send_tg: bool = True, styles: str = "BRIGHT_ORANGE BOLD"):
        await self.custom(text, LogLevel.WARNING, self.get_context(), display, save, send_tg, styles)

    async def error(self, text: str, display: bool = True, save: bool = True, send_tg: bool = True, styles: str = "BRIGHT_RED BOLD"):
        await self.custom(text, LogLevel.ERROR, self.get_context(), display, save, send_tg, styles)

    async def critical(self, text: str, display: bool = True, save: bool = True, send_tg: bool = True, styles: str = "BRIGHT_RED BOLD REVERSE"):
        await self.custom(text, LogLevel.CRITICAL, self.get_context(), display, save, send_tg, styles)

    async def debug(self, text: str, display: bool = True, save: bool = False, send_tg: bool = False, styles: str = "BRIGHT_BLACK"):
        await self.custom(text, LogLevel.DEBUG, self.get_context(), display, save, send_tg, styles)

    async def exception(self, text: str, display: bool = True, save: bool = True, send_tg: bool = True, styles: str = "MAGENTA BOLD"):
        await self.custom(text, LogLevel.EXCEPTION, self.get_context(), display, save, send_tg, styles)

    async def success(self, text: str, display: bool = True, save: bool = True, send_tg: bool = False, styles: str = "BRIGHT_GREEN BOLD"):
        await self.custom(text, LogLevel.SUCCESS, self.filepath, display, save, send_tg, styles)