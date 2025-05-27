import asyncio

class LogManager:
    def __init__(self, message_manager):
        self.message_manager = message_manager
        self._running = False
        self._task = None

    async def _worker(self):
        self._running = True
        try:
            while self._running:
                log = await self.message_manager.messages.get()
                self.process_log(log)
                self.message_manager.messages.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    def process_log(self, log):
        print(f"[{log.custom_timestamp}] [{log.level.upper()}] [{log.context}] {log.message}")

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker())
        else:
            print("LogManager already started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
