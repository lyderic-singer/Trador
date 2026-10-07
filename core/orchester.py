import asyncio
import time
from typing import Iterable
from adapters.exchange import Exchange
from adapters.models import Portfolio, Volume
from core.session import Session
from core.utils import call
from adapters import ADAPTERS, CLIENTS
from log.logger import Logger
from log.messenger import Messenger
from log.telegram import Telegram


class Orchester:
    def __init__(self, mainnet: bool = False, notify: bool = False, exchanges: Iterable[str] = ("hyperliquid",)):
        self.mainnet = mainnet
        self.messenger = Messenger("log/logs.txt")
        self.telegram = Telegram(self.messenger) if notify else None
        self.session = Session(self.messenger)
        self._log = Logger(self, self.messenger)
        self.clients = {name: CLIENTS[name](mainnet, self.messenger, self.session) for name in exchanges}
        self.adapters: dict[str, Exchange] = {}
        self.strategies: list = []
        self.readers: dict[str, Exchange] = {}
        self.running = False

    @property
    def hl(self):
        return self.clients.get("hyperliquid")

    def exchange(self, name: str, **options) -> Exchange:
        if name not in self.adapters:
            self.adapters[name] = ADAPTERS[name](self.clients[name], **options)
        return self.adapters[name]

    def reader(self, name: str) -> Exchange:
        # an exchange started without strategy has no adapter yet: a bare one (kept for its caches) reads the account
        if name in self.adapters:
            return self.adapters[name]
        if name not in self.readers:
            self.readers[name] = ADAPTERS[name](self.clients[name])
        return self.readers[name]

    async def portfolios(self) -> list[Portfolio]:
        async def read(name: str, client) -> Portfolio:
            adapter = self.reader(name)
            try:
                return await asyncio.wait_for(adapter.portfolio(), timeout=20)
            except Exception as e:
                return Portfolio(name, error=f"{type(e).__name__}: {e}")
        return list(await asyncio.gather(*(read(n, c) for n, c in self.clients.items())))

    async def volumes(self) -> list[Volume]:
        async def read(name: str) -> Volume:
            try:
                volume = await asyncio.wait_for(self.reader(name).volume(), timeout=60)
                return volume or Volume(name, error="volume not available on this exchange")
            except Exception as e:
                return Volume(name, error=f"{type(e).__name__}: {e}")
        return list(await asyncio.gather(*(read(n) for n in self.clients)))

    def reports(self) -> list[str]:
        out = []
        for strategy in self.strategies:
            report = getattr(strategy, "telegram_report", None)
            if report:
                try:
                    out.append(report())
                except Exception as e:
                    out.append(f"{type(strategy).__name__}: report failed ({e})")
        return out

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        t = time.time()
        await self.messenger.start(self.telegram)
        await self._log.info("Starting")
        await self.session.start()
        await asyncio.gather(*(client.start() for client in self.clients.values()))
        if self.telegram:
            await self.telegram.start(self)
        self.running = True
        await self._log.success(f"Started in {(time.time() - t):.2f} s", send_tg=True)

    async def stop_strategies(self):
        for strategy in reversed(self.strategies):
            try:
                await call(strategy.stop)
            except Exception as e:
                await self._log.error(f"cannot stop {strategy}: {e}")
        self.strategies.clear()

    async def stop(self):
        try:
            await self._log.info("Stopping", send_tg=True)
            self.running = False
            await self.stop_strategies()
            for adapter in self.adapters.values():
                await adapter.stop()
            for client in self.clients.values():
                await client.stop()
            await self.session.stop()
            await self._log.success("Stopped", send_tg=True)
            if self.telegram:
                await self.telegram.stop()
            await self.messenger.stop()
        except Exception:
            await self._log.error("Stop failed")

    async def add_strategy(self, strategy):
        if not self.running:
            raise ValueError("must start before adding a strategy")
        await strategy.start()
        self.strategies.append(strategy)
