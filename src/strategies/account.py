import asyncio
import time
from adapters.exchange import Exchange
from adapters.models import AccountState, Position
from log.logger import Logger


class AccountTracker:
    def __init__(self, exchange: Exchange, symbols: list[str]):
        self.exchange = exchange
        self.symbols = symbols
        self._log = Logger(f"{exchange.name} Account", exchange.messenger)
        self.state = AccountState(exchange.name)
        self.received = asyncio.Event()
        self.running = False
        self.updated = 0.0

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self.exchange.subscribe_account(self.symbols, self.on_state)
        await asyncio.wait_for(self.received.wait(), timeout=30)
        self.running = True
        await self._log.success("Started")

    async def stop(self):
        self.running = False
        self.received.clear()
        await self._log.success("Stopped")

    async def on_state(self, state: AccountState):
        self.state = state
        self.updated = time.time()
        self.received.set()

    async def refresh(self) -> AccountState:
        await self.on_state(await self.exchange.account(self.symbols))
        return self.state

    def age(self) -> float:
        return time.time() - self.updated if self.updated else float("inf")

    def position(self, symbol: str) -> Position:
        return self.state.position(symbol)

    def leverage(self, symbol: str) -> float:
        p = self.state.positions.get(symbol)
        return (p.leverage if p and p.leverage else None) or self.state.leverages.get(symbol, 1.0)

    def power(self, symbol: str) -> float:
        return self.state.equity * self.leverage(symbol)

    def __str__(self):
        return f"<{self.exchange.name} equity={self.state.equity} free={self.state.free} positions={self.state.positions}>"
