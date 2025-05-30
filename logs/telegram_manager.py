import asyncio
import json
from pathlib import Path
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from logs.message_manager import Logger


class TelegramManager:
    def __init__(self, orchester, config_path="config/telegram.json"):
        self.config = json.loads(Path(config_path).read_text())
        self.orchester = orchester
        self._logging = Logger(self, orchester.message_manager)
        self.token = self.config["TG_TOKEN"]
        self.chat_id = self.config["TG_CHAT_ID"]
        self.messages = asyncio.Queue()
        self._task = None
        self._app: Application = ApplicationBuilder().token(self.token).build()
        self._app.add_handler(CommandHandler("ping", self._on_ping))

    async def _on_ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("pong 🏓")

    async def _worker(self):
        try:
            while True:
                msg = await self.messages.get()
                try:
                    await self._app.bot.send_message(chat_id=self.chat_id, text=msg)
                except Exception as e:
                    await self._logging.error(f"[Telegram] send failed: {e}")
                self.messages.task_done()
        except asyncio.CancelledError:
            await self._logging.warning("[Telegram] message worker cancelled")

    async def start(self):
        try:
            await self._app.initialize()
            await self._app.start()
            await self._app.updater.start_polling()
            self._task = asyncio.create_task(self._worker())
        except Exception as e:
            await self._logging.error(f"[Telegram] start failed: {e}")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        try:
            await self._app.stop()
            await self._app.shutdown()
        except Exception as e:
            await self._logging.error(f"[Telegram] stop failed: {e}")

    async def send(self, msg: str):
        try:
            await self.messages.put(msg)
        except Exception as e:
            await self._logging.error(f"[Telegram] enqueue failed: {e}")
