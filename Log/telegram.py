import asyncio
import os
from dotenv import load_dotenv
import os
from time import time
from telegram import Update, InputFile
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from log.logger import Logger

load_dotenv()

class Telegram:
    def __init__(self, orchester, config_path="config/telegram.json"):
        self.token = str(os.getenv("TG_TOKEN"))
        self.chat_id = int(os.getenv("TG_CHAT_ID"))
        self.orchester = orchester
        self._log = Logger(self, orchester.messenger)
        self.messages = asyncio.Queue()
        self._app: Application = ApplicationBuilder().token(self.token).build()
        self._worker_task = None
        self._polling_task = None
        self._register()

    def _register(self):
        self._app.add_handler(CommandHandler("ping", self._on_ping))
        self._app.add_handler(CommandHandler("run", self._on_run))
        self._app.add_handler(CommandHandler("perpinventory", self._on_perp_inventory))
        self._app.add_handler(CommandHandler("spotinventory", self._on_spot_inventory))
        self._app.add_handler(CommandHandler("stop", self._on_stop))
        self._app.add_handler(CommandHandler("kill", self._on_kill))
        self._app.add_handler(CommandHandler("graph", self._on_graph))
        self._app.add_handler(CommandHandler("graph", self._on_adress))

    async def _on_ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("ping demandé")
        if update.message:
            last_message_time = self.orchester.core.ws.last_useful_message
            await update.message.reply_text(f"pong 🏓 dernier message utile recu " + (f"il y a {round( time() - last_message_time, 2) }s" if last_message_time else "jamais"))

    async def _on_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("lancement du bot HL demandé")
        try:
            asyncio.create_task( self.orchester.strategy.run())
            if update.message:
                await update.message.reply_text("bot lancé")
        except Exception as e:
            await self._log.error(f"[Telegram] erreur dans /run: {e}")
            if update.message:
                await update.message.reply_text("Erreur lors du lancement du bot.")

    async def _on_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("arret du bot HL demandé")
        try:
            await self.orchester.strategy.stop()
            if update.message:
                await update.message.reply_text("bot arrété")
        except Exception as e:
            await self._log.error(f"{e}")
            if update.message:
                await update.message.reply_text("Erreur lors du lancement du bot.")

    async def _on_perp_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"inventaire perp demandé")
        try:
            data = await self.orchester.core.info.user_state(self.orchester.address)
            value = round(float(data['marginSummary']['accountValue']), 2)
            positions = round(float(data['marginSummary']['totalNtlPos']), 2)
            await update.message.reply_text(f"(perp inventory) valeur totale : {value}, valeur des positions : {positions}")
            await self._log.success(f" inventaire : {data}")
        except Exception as e:
            await self._log.error(f"{e}")
    
    async def _on_spot_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"inventaire spot demandé")
        try:
            data = await self.orchester.core.info.spot_user_state(self.orchester.address)
            await update.message.reply_text(f"inventaire spot : {data}")
            await self._log.success(f" inventaire spot : {data}")
        except Exception as e:
            await self._log.error(f"{e}")

    async def _on_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("arret complet demandé")
        await self.orchester.stop()
        os._exit(0)

    async def _on_adress(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("addresse demandée")
        await update.message.reply_text(f"addresse : {self.orchester.address}")

    async def _on_graph(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("graph demandé")
        await self.orchester.coin.plot()
        with open(f"data/pictures/plot.jpg", "rb") as f:
            await update.message.reply_photo(photo=InputFile(f), caption="Voici le graphique 📈")

    async def _worker(self):
        try:
            while True:
                msg = await self.messages.get()
                try:
                    await self._app.bot.send_message(chat_id=self.chat_id, text=msg)
                except Exception as e:
                    await self._log.error(f"[Telegram] send failed: {e}")
                self.messages.task_done()
        except asyncio.CancelledError:
            await self._log.warning("[Telegram] message worker cancelled")
        except Exception as e:
            await self._log.error(f"[Telegram] message worker exception: {e}")

    async def _polling(self):
        await self._log.info("Starting Telegram")
        while True:
            try:
                await self._app.initialize()
                await self._app.start()
                await self._app.updater.start_polling()
                await self._log.success("polling lancé")
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                await self._log.warning("polling annulé")
                break
            except Exception as e:
                await self._log.error(f"polling crashed : {e}")
                await self._log.warning("tentative de reconnexion dans 5s...")
                try:
                    await self._app.updater.stop()
                    await self._app.stop()
                    await self._app.shutdown()
                except Exception as stop_error:
                    await self._log.warning(f"erreur durant l'arrêt : {stop_error}")
                await asyncio.sleep(5)

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
            await self._log.error(f"[Telegram] enqueue failed: {e}")
