import asyncio
import json
from pathlib import Path
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

class TelegramManager:
    def __init__(self, config_path="config/telegram.json"):
        self.config = self._load_config(config_path)
        self.token = self.config["TG_TOKEN"]
        self.chat_id = self.config["TG_CHAT_ID"]
        self.messages = asyncio.Queue()
        self._running = False
        self._task = None
        self.app = None

    def _load_config(self, path):
        return json.loads(Path(path).read_text())

    async def _worker(self):
        self._running = True
        try:
            while self._running:
                msg = await self.messages.get()
                await self.app.bot.send_message(chat_id=self.chat_id, text=msg)
                self.messages.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    async def pong(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("pong 🏓")

    async def start(self):
        if self.app is None:
            self.app = ApplicationBuilder().token(self.token).build()

            self.app.add_handler(CommandHandler("pong", self.pong))

        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker())

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    async def send(self, msg: str):
        await self.messages.put(msg)
