import asyncio

class LogManager:
    def __init__(self, message_manager):
        self.message_manager = message_manager
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            log = await self.message_manager.messages.get()
            self.process_log(log)
            self.message_manager.messages.task_done()

    def process_log(self, log):
        print(f"[{log.iso_timestamp}] [{log.level.upper()}] [{log.context}] {log.message}")

    def stop(self):
        self._running = False
