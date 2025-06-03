import asyncio
import json
from pathlib import Path
from telegram import Update, InputFile
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from logs.message_manager import Logger


class TelegramManager:
    def __init__(self, orchester, config_path="config/telegram.json"):
        self.config = json.loads(Path(config_path).read_text())
        self.token = self.config["TG_TOKEN"]
        self.chat_id = self.config["TG_CHAT_ID"]
        self.orchester = orchester
        self._logging = Logger(self, orchester.message_manager)
        self.messages = asyncio.Queue()
        self._app: Application = ApplicationBuilder().token(self.token).build()
        self._worker_task = None
        self._polling_task = None
        self._add_handlers()

    def _add_handlers(self):
        self._app.add_handler(CommandHandler("ping", self._on_ping))
        self._app.add_handler(CommandHandler("run", self._on_run))
        self._app.add_handler(CommandHandler("perpinventory", self._on_perp_inventory))
        self._app.add_handler(CommandHandler("spotinventory", self._on_spot_inventory))
        self._app.add_handler(CommandHandler("graph", self._on_graph))

    async def _on_ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._logging.info("ping received")
        if update.message:
            await update.message.reply_text("pong 🏓")

    async def _on_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message:
            await update.message.reply_text("lancement du bot HL demandé")
        try:
            await self.orchester.run()
            if update.message:
                await update.message.reply_text("bot lancé")
        except Exception as e:
            await self._logging.error(f"[Telegram] erreur dans /run: {e}")
            if update.message:
                await update.message.reply_text("Erreur lors du lancement du bot.")

    async def _on_perp_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._logging.info(f"inventaire demandé")
        try:
            data = await self.orchester.core_manager.info.user_state(self.orchester.address)
            value = round(float(data['marginSummary']['accountValue']), 2)
            positions = round(float(data['marginSummary']['totalNtlPos']), 2)
            await update.message.reply_text(f"(perp inventory) valeur totale : {value}, valeur des positions : {positions}")
            await self._logging.success(f" inventaire : {data}")
        except Exception as e:
            await self._logging.error(f"{e}")
    
    async def _on_spot_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._logging.info(f"inventaire demandé")
        try:
            data = await self.orchester.core_manager.info.spot_user_state(self.orchester.address)
            await update.message.reply_text(f"inventaire spot : {data}")
            await self._logging.success(f" inventaire spot : {data}")
        except Exception as e:
            await self._logging.error(f"{e}")

    async def _on_graph(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.orchester.coin_manager.plot()
        with open(f"data/pictures/plot.jpg", "rb") as f:
            await update.message.reply_photo(photo=InputFile(f), caption="Voici le graphique 📈")

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
        except Exception as e:
            await self._logging.error(f"[Telegram] message worker exception: {e}")

    async def _polling(self):
        try:
            await self._app.initialize()
            await self._app.start()
            await self._app.updater.start_polling()
            await self._logging.info("[Telegram] bot démarré")
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self._logging.warning("[Telegram] polling loop cancelled")
        except Exception as e:
            await self._logging.error(f"[Telegram] polling loop exception: {e}")
        finally:
            try:
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
                await self._logging.info("[Telegram] bot arrêté proprement")
            except Exception as e:
                await self._logging.error(f"[Telegram] erreur lors de l'arrêt: {e}")

    async def start(self):
        self._worker_task = asyncio.create_task(self._worker())
        self._polling_task = asyncio.create_task(self._polling())

    async def stop(self):
        tasks = [self._worker_task, self._polling_task]
        for task in tasks:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def send(self, msg: str):
        try:
            await self.messages.put(msg)
        except Exception as e:
            await self._logging.error(f"[Telegram] enqueue failed: {e}")
