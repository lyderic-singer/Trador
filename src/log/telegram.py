import asyncio
from dotenv import load_dotenv
import os
from core.utils import cancel
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, filters
from log.logger import Logger

load_dotenv()

# shown by /help, in this order
COMMANDS = {
    "help": "this help",
    "ping": "check that the bot answers",
    "pf": "portfolio value per exchange, breakdown and main positions",
    "vol": "traded volume 24h / 7d / 30d / total on each exchange (maker share, fills, fees)",
    "stats": "report of the running strategies (those that provide one)",
    "address": "addresses of the accounts in use",
    "perpinventory": "Hyperliquid perp account value (main dex)",
    "spotinventory": "Hyperliquid spot balances",
    "stop": "stop the strategies (the bot and Telegram stay online)",
    "kill": "stop everything and exit",
}
LIMIT = 4000  # Telegram refuses messages over 4096 characters


def chunks(text: str, limit: int = LIMIT) -> list[str]:
    out, current = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if current:
                out.append(current)
                current = ""
            out.append(line[:limit])
            line = line[limit:]
        if len(current) + len(line) + 1 > limit:
            out.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        out.append(current)
    return out or [""]


def short(x: float) -> str:
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1e9:
        return f"{sign}${x / 1e9:.2f}B"
    if x >= 1e6:
        return f"{sign}${x / 1e6:.2f}M"
    if x >= 1e4:
        return f"{sign}${x / 1e3:.1f}k"
    return f"{sign}${x:,.0f}"


def format_volume(volumes) -> str:
    windows = ("24h", "7d", "30d")
    lines = ["📊 Volume (rolling windows)"]
    totals = {w: 0.0 for w in windows}
    total_all, partial = 0.0, False
    for v in volumes:
        lines.append("")
        lines.append(f"🏦 {v.exchange}" + (f" · {v.label}" if v.label else ""))
        if v.error and not v.volume:
            lines.append(f"  ⚠️ {v.error}")
            partial = True
            continue
        for w in windows:
            vol = v.volume.get(w, 0.0)
            totals[w] += vol
            maker = v.maker.get(w, 0.0)
            share = f"maker {maker / vol:.0%} · " if vol else ""
            lines.append(f"  {w:<4} {short(vol):>9}   {share}{v.fills.get(w, 0)} fills · fees {money(v.fees.get(w, 0.0))}")
        if v.total is not None:
            total_all += v.total
            lines.append(f"  total {'≈' if v.estimated else ''}{short(v.total)}" + (" (estimated from the action pool)" if v.estimated else ""))
        else:
            partial = True
            lines.append("  total n/d")
        if v.truncated:
            lines.append("  ⚠️ incomplete window: the exchange does not return every fill (at least this volume)")
        if v.error:
            lines.append(f"  ⚠️ {v.error}")
    if len(volumes) > 1:
        lines.append("")
        lines.append("Σ " + " · ".join(f"{w} {short(totals[w])}" for w in windows) + f" · total {'≥' if partial else ''}{short(total_all)}")
    return "\n".join(lines)


def format_help() -> str:
    return "Commandes :\n" + "\n".join(f"/{name} — {text}" for name, text in COMMANDS.items())


def money(x: float) -> str:
    return f"{'-' if x < 0 else ''}${abs(x):,.2f}"


def format_portfolio(portfolios, positions: int = 8) -> str:
    total = sum(p.equity for p in portfolios)
    lines = [f"💼 Portfolio {money(total)}"]
    for p in portfolios:
        lines.append("")
        lines.append(f"🏦 {p.exchange}: {money(p.equity)}" + (f" ({p.equity / total:.0%})" if total > 0 else ""))
        if p.error:
            lines.append(f"  ⚠️ {p.error}")
        for name, value in sorted(p.details.items(), key=lambda kv: -abs(kv[1])):
            lines.append(f"  · {name}: {money(value)}")
        lines.append(f"  free {money(p.free)} | exposure {money(p.notional)} | upnl {'+' if p.pnl >= 0 else ''}{money(p.pnl)}"
                     f" | {len(p.positions)} positions")
        ranked = sorted(p.positions.values(), key=lambda x: -abs(x.value))
        for pos in ranked[:positions]:
            lines.append(f"    {pos.symbol} {'long' if pos.size > 0 else 'short'} {money(pos.value)} "
                         f"({'+' if pos.pnl >= 0 else ''}{money(pos.pnl)})")
        if len(ranked) > positions:
            lines.append(f"    … {len(ranked) - positions} more")
    return "\n".join(lines)


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
        # commands are obeyed only from TG_CHAT_ID: anyone else who finds the bot gets no answer (TG_CHAT_ID unset = none)
        self.allowed = filters.Chat(chat_id=self.chat_id)
        self.application.add_handler(CommandHandler("help", self.on_help, filters=self.allowed))
        self.application.add_handler(CommandHandler("ping", self.on_ping, filters=self.allowed))
        self.application.add_handler(CommandHandler("perpinventory", self.on_perp_inventory, filters=self.allowed))
        self.application.add_handler(CommandHandler("spotinventory", self.on_spot_inventory, filters=self.allowed))
        self.application.add_handler(CommandHandler("stop", self.on_stop, filters=self.allowed))
        self.application.add_handler(CommandHandler("kill", self.on_kill, filters=self.allowed))
        self.application.add_handler(CommandHandler("address", self.on_adress, filters=self.allowed))
        self.application.add_handler(CommandHandler("pf", self.on_pf, filters=self.allowed))
        self.application.add_handler(CommandHandler("vol", self.on_vol, filters=self.allowed))
        self.application.add_handler(CommandHandler("stats", self.on_stats, filters=self.allowed))

    async def start(self, orchester):
        if self.running:
            return await self._log.info("already started")
        try:
            self.running = True
            self.orchester = orchester
            await self.application.initialize()
            await self.application.start()
            if self.application.updater:
                await self.application.updater.start_polling()
            self.task = asyncio.create_task(self.run())
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

    async def run(self):
        while True:
            text = await self.messages.get()
            if text is None:
                self.messages.task_done()
                break
            for part in chunks(text):
                retry = 5
                backoff = 2
                while retry > 0:
                    try:
                        await self.application.bot.send_message(chat_id=self.chat_id, text=part)
                        await asyncio.sleep(self.delay)
                        break
                    except Exception as e:
                        retry -= 1
                        await self._log.error(f"send failed: {e}, retries left: {retry}", send_tg=False)
                        await asyncio.sleep(backoff)
                        backoff *= 2
                else:
                    await self._log.error(f"Failed to send message after retries: {part[:200]}", send_tg=False)
            self.messages.task_done()

    async def send(self, msg: str | None):
        try:
            await self.messages.put(msg)
        except Exception as e:
            await self._log.error(f"adding message failed: {e}")

    async def on_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("help requested")
        await self.send(format_help())

    async def on_vol(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("volume requested")
        try:
            if not self.orchester:
                raise ValueError("bot not started")
            await self.send(format_volume(await self.orchester.volumes()))
        except Exception as e:
            await self._log.error(f"volume failed: {e}", send_tg=False)
            await self.send(f"cannot read the volume: {e}")

    async def on_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("stats requested")
        try:
            if not self.orchester:
                raise ValueError("bot not started")
            reports = self.orchester.reports()
            await self.send("\n\n".join(reports) if reports else "no running strategy with a report")
        except Exception as e:
            await self._log.error(f"stats failed: {e}", send_tg=False)
            await self.send(f"cannot build the report: {e}")

    async def on_ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("ping received")
        await self.send(f"pong")

    async def on_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("stop strategy requested")
        try:
            if self.orchester:
                await self.orchester.stop_strategies()
            await self.send("strategies stopped")
        except Exception as e:
            await self._log.error(f"{e}")
            await self.send("error stopping strategy")

    async def on_perp_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info(f"perp inventory requested")
        try:
            hl = self.orchester.clients.get("hyperliquid") if self.orchester else None
            if hl:
                data = await hl.info.clearing_house_state(hl.address)
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
            hl = self.orchester.clients.get("hyperliquid") if self.orchester else None
            if hl:
                data = await hl.info.spot_clearing_house_state(hl.address)
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

    async def on_pf(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("portfolio requested")
        try:
            if not self.orchester:
                raise ValueError("bot not started")
            await self.send(format_portfolio(await self.orchester.portfolios()))
        except Exception as e:
            await self._log.error(f"portfolio failed: {e}")
            await self.send(f"cannot read the portfolio: {e}")

    async def on_adress(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._log.info("address requested")
        if self.orchester:
            await self.send("\n".join(f"{name}: {client.address}" for name, client in self.orchester.clients.items()))
