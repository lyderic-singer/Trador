import asyncio
import os
import sys
from core.utils import cancel
from log.logger import Logger, Message

COLORS = {
    "RESET": "\033[0m",
    "BOLD": "\033[1m",
    "UNDERLINE": "\033[4m",
    "REVERSE": "\033[7m",
    "BLACK": "\033[30m",
    "RED": "\033[31m",
    "GREEN": "\033[32m",
    "YELLOW": "\033[33m",
    "BLUE": "\033[34m",
    "MAGENTA": "\033[35m",
    "CYAN": "\033[36m",
    "ORANGE": "\033[38;5;208m",
    "WHITE": "\033[37m",
    "BRIGHT_BLACK": "\033[90m",
    "BRIGHT_RED": "\033[91m",
    "BRIGHT_GREEN": "\033[92m",
    "BRIGHT_YELLOW": "\033[93m",
    "BRIGHT_BLUE": "\033[94m",
    "BRIGHT_MAGENTA": "\033[95m",
    "BRIGHT_ORANGE": "\033[38;5;214m",
    "BRIGHT_CYAN": "\033[96m",
    "BRIGHT_WHITE": "\033[97m",
}

def flush(text, styles=None, end='\n'):
    if styles:
        prefix = ''.join(COLORS[s] for s in styles.split() if s in COLORS)
        text = f"{prefix}{text}{COLORS['RESET']}"
    sys.stdout.write(text + end)
    sys.stdout.flush()

class Messenger:
    def __init__(self, path = "logs.txt"):
        self.telegram = None
        self.path = path
        self.messages: asyncio.Queue[Message] = asyncio.Queue()
        self.running = False
        if path:
            os.makedirs(os.path.dirname(os.fspath(path)) or ".", exist_ok=True)
        self.file = open(path, "a", buffering=1, encoding="utf-8") if path else None
        self._log = Logger(self, self)
        self.task = None
    
    async def save(self, message: Message):
        if self.file is None:
            return
        line = message.format() + "\n"
        try:
            await asyncio.to_thread(self.file.write, line)
            await asyncio.to_thread(self.file.flush)
        except Exception as e:
            await self._log.error(f"{e}")

    async def run(self):
        try:
            while self.running:
                message: Message = await self.messages.get()
                if message.display:
                    flush(message.format(), message.styles)
                if message.save and self.file:
                    await self.save(message)
                if message.send_tg and self.telegram:
                    try:
                        await self.telegram.send(message.format_tg())
                    except Exception as e:
                        await self._log.error(f"cannot send message '{message.format_tg()}': {e}")
                self.messages.task_done()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._log.warning(f"messages handling stopped{e}")
        finally:
            self.running = False

    async def start(self, telegram = None):
        if self.running:
            return await self._log.info("already started")
        try:
            self.telegram = telegram
            if self.task is None or self.task.done():
                self.running = True
                self.task = asyncio.create_task(self.run())
                await self._log.success("Started")
            else:
                await self._log.info("Already started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def stop(self):
        try:
            self.running = False
            await cancel(self.task)
            if self.file:
                self.file.close()
                self.file = None
        except Exception as e:
            await self._log.error(f"{e}")
            raise e
