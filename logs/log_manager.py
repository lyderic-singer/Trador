import asyncio
import sys

def shell(*args, sep=' ', end='\n'):
    msg = sep.join(map(str, args)) + end
    sys.stdout.write(msg)
    sys.stdout.flush()

class LogManager:
    def __init__(self, message_manager, tg_manager = None, filepath = "logs.txt" ):
        self.message_manager = message_manager
        self.filepath = filepath
        self.tg_manager = tg_manager
        self._running = False
        self.file_handle = open(filepath, "a", buffering=1, encoding="utf-8") if filepath else None
        self._task = None
    
    async def _save_to_file(self, message):
        line = message.format() + "\n"
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_file, line)

    def _write_file(self, line: str):
        if self.file_handle is None:
            return
        try:
            self.file_handle.write(line)
        except Exception as e:
            shell(f"[ERROR][Logger] Failed to write to log file: {e}")


    async def _send_telegram(self, message):
        try:
            await self.tg_manager.send(message.format_tg())
        except Exception as e:
            shell(f"[ERROR][Logger] Telegram error: {e}")

    async def _log(self, message):
        shell(message.format())
        if message.save and self.file_handle:
            await self._save_to_file(message)
        if message.send_tg and self.tg_manager:
            await self._send_telegram(message)

    async def _worker(self):
        self._running = True
        try:
            while self._running:
                log = await self.message_manager.messages.get()
                await self._log(log)
                self.message_manager.messages.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker())
        else:
            shell("LogManager already started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
