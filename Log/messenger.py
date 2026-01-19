import asyncio
import sys
from logger import Logger, Message

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
    def __init__(self, telegram = None, path = "logs.txt" ):
        self.telegram = telegram
        self.path = path
        self.messages: asyncio.Queue[Message] = asyncio.Queue()
        self.running = False
        self.file = open(path, "a", buffering=1, encoding="utf-8") if path else None
        self._log = Logger(self, self)
        self.task = None
    
    async def save(self, message: Message):
        line = message.format() + "\n"
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.write, line)

    def write(self, line: str):
        if self.file is None:
            return
        try:
            self.file.write(line)
        except Exception as e:
            flush(f"[ERROR][Logger] Failed to write to log file: {e}")

    async def handle(self):
        self.running = True
        try:
            while self.running:
                message: Message = await self.messages.get()
                if message.display != "debug":
                    flush(message.format(), message.styles)
                if message.save and self.file:
                    await self.save(message)
                if message.send_tg and self.telegram:
                    try:
                        await self.telegram.send(message.format_tg())
                    except Exception as e:
                        flush(f"[ERROR][Logger] Telegram error: {e}")
                self.messages.task_done()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            flush(f"erreur mess: {e}")
        finally:
            self.running = False

    def start(self):
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.handle())
            flush
        else:
            flush("Logger already started")

    async def stop(self):
        self.running = False
        await self.messages.join()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        if self.file:
            self.file.close()
            self.file = None
