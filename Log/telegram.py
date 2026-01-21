import asyncio
from dotenv import load_dotenv
import os
from core.HL.utils.functions import cancel
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from log.logger import Logger

load_dotenv()

class Telegram:
    def __init__(self, messenger, delay: float = 2):
        self.token = str(os.getenv("TG_TOKEN"))
        self.chat_id = int(os.getenv("TG_CHAT_ID"))
        self.orchester = None
        self._log = Logger(self, messenger)
        self.messages = asyncio.Queue()
        self.application: Application = ApplicationBuilder().token(self.token).build()
        self.task = None
        self.delay = delay
        self.running = False
        self.application.add_handler(CommandHandler("help", self.on_help))
        self.application.add_handler(CommandHandler("ping", self.on_ping))
        self.application.add_handler(CommandHandler("run", self.on_run))
        self.application.add_handler(CommandHandler("perpinventory", self.on_perp_inventory))
        self.application.add_handler(CommandHandler("spotinventory", self.on_spot_inventory))
        self.application.add_handler(CommandHandler("stop", self.on_stop))
        self.application.add_handler(CommandHandler("kill", self.on_kill))
        self.application.add_handler(CommandHandler("address", self.on_adress))

    async def start(self, orchester):
        try:
            self.running = True
            self.orchester = orchester
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            self.task = asyncio.create_task(self.handle())
            await self._log.success("Started")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def stop(self):
        try:
            self.running = False
            await self.send(None)
            await self.task
            await cancel(self.task)
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def handle(self):
        try:
            while True:
                text = await self.messages.get()
                if text is None:
                    self.messages.task_done()
                    break
                try:
                    await self.application.bot.send_message(chat_id=self.chat_id, text=text)
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    await self._log.error(f"send failed: {e}")
                self.messages.task_done()
        except Exception as e:
            await self._log.error(f"{e}")

    async def send(self, msg: str):
        try:
            await self.messages.put(msg)
        except Exception as e:
            await self._log.error(f"adding message failed: {e}")

    async def on_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("help requested")
        tg_commands = []
        for handler in self.application.handlers.get(0, []):
            if isinstance(handler, CommandHandler):
                commands = handler.commands
                if isinstance(commands, str):
                    commands = [commands]
                for cmd in commands:
                    tg_commands.append(f"/{cmd}")
        tg_commands = "Available commands:\n" + "\n".join(tg_commands)
        await self.send(tg_commands)

    async def on_ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("ping received")
        await self.send(f"pong")

    async def on_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("strategy run requested")
        try:
            await self.orchester.strategy.run()
            await self.send("bot started")
        except Exception as e:
            await self._log.error(f"cannot run strategy: {e}")
            await self.send("Erreur lors du lancement du bot.")

    async def on_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("stop strategy requested")
        try:
            await self.orchester.strategy.stop()
            await self.send("strategy stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            await self.send("error stopping strategy")

    async def on_perp_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"perp inventory requested")
        try:
            data = await self.orchester.core.info.user_state(self.orchester.address)
            value = round(float(data['marginSummary']['accountValue']), 2)
            positions = round(float(data['marginSummary']['totalNtlPos']), 2)
            await self.send(f"(perp inventory) total value : {value}, positions value : {positions}")
        except Exception as e:
            await self.send("cannot fetch perp inventory")
    
    async def on_spot_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"spot inventory requested")
        try:
            data = await self.orchester.core.info.spot_user_state(self.orchester.address)
            await self.send(f"spot inventory: {data}")
        except Exception as e:
            await self.send("cannot fetch spot inventory")

    async def on_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("kill requested")
        await self.send("killing bot")
        await self.orchester.stop()
        os._exit(0)

    async def on_adress(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("address requested")
        await self.send(self.orchester.address)
