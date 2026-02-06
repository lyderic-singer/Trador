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
        self.chat_id = int(os.getenv("TG_CHAT_ID") or 0)
        self.orchester = None
        self._log = Logger(self, messenger)
        self.messages = asyncio.Queue()
        self.application: Application = ApplicationBuilder().token(self.token).build()
        self.task = None
        self.delay = delay
        self.running = False
        self.application.add_handler(CommandHandler("help", self.on_help))
        self.application.add_handler(CommandHandler("ping", self.on_ping))
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
            if self.application.updater:
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
            if self.task:
                await self.task
            await cancel(self.task)
            if self.application.updater:
                await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            await self._log.success("Stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            raise

    async def handle(self):
        while True:
            text = await self.messages.get()
            if text is None:
                self.messages.task_done()
                break
            retry = 5
            backoff = 2
            while retry > 0:
                try:
                    await self.application.bot.send_message(chat_id=self.chat_id, text=text)
                    await asyncio.sleep(self.delay)
                    break
                except Exception as e:
                    retry -= 1
                    await self._log.error(f"send failed: {e}, retries left: {retry}")
                    await asyncio.sleep(backoff)
                    backoff *= 2
            else:
                await self._log.error(f"Failed to send message after retries: {text}")
            self.messages.task_done()

    async def send(self, msg: str | None):
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

    async def on_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("stop strategy requested")
        try:
            if self.orchester:
                await self.orchester.bot.stop()
            await self.send("strategy stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            await self.send("error stopping strategy")

    async def on_perp_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"perp inventory requested")
        try:
            if self.orchester:
                data = await self.orchester.hl.info.user_state(self.orchester.address)
                value = round(float(data['marginSummary']['accountValue']), 2)
                positions = round(float(data['marginSummary']['totalNtlPos']), 2)
                await self.send(f"(perp inventory) total value : {value}, positions value : {positions}")
            else:
                raise
        except Exception as e:
            await self.send("cannot fetch perp inventory")
    
    async def on_spot_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"spot inventory requested")
        try:
            if self.orchester:
                data = await self.orchester.hl.info.spot_user_state(self.orchester.address)
                await self.send(f"spot inventory: {data}")
            else:
                raise
        except Exception as e:
            await self.send("cannot fetch spot inventory")

    async def on_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("kill requested")
        if self.orchester:
            await self.send("killing bot")
            await self.orchester.stop()
        os._exit(0)

    async def on_adress(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("address requested")
        if self.orchester:
            await self.send(self.orchester.address)
