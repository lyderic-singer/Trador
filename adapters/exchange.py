from abc import ABC, abstractmethod
from typing import Awaitable, Callable
from adapters.models import AccountState, BBO, Cancel, Candle, Instrument, Modify, OpenOrder, Order, Result

BBOListener = Callable[[BBO], Awaitable[None] | None]
AccountListener = Callable[[AccountState], Awaitable[None] | None]


class Exchange(ABC):
    name: str = "exchange"
    messenger = None

    def canonical(self, symbol: str) -> str:
        return self.instrument(symbol).symbol

    def tradable(self, symbol: str) -> bool:
        return True

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    def instrument(self, symbol: str) -> Instrument: ...

    @abstractmethod
    async def subscribe_bbo(self, symbol: str, listener: BBOListener) -> None: ...

    @abstractmethod
    async def candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]: ...

    @abstractmethod
    async def mid(self, symbol: str) -> float | None: ...

    @abstractmethod
    async def subscribe_account(self, symbols: list[str], listener: AccountListener) -> None: ...

    @abstractmethod
    async def account(self, symbols: list[str]) -> AccountState: ...

    @abstractmethod
    async def open_orders(self, symbol: str | None = None) -> list[OpenOrder]: ...

    @abstractmethod
    async def place(self, orders: list[Order]) -> list[Result]: ...

    @abstractmethod
    async def modify(self, modifies: list[Modify]) -> list[Result]: ...

    @abstractmethod
    async def cancel(self, cancels: list[Cancel]) -> list[Result]: ...

    async def cancel_all(self, symbol: str | None = None) -> None:
        orders = await self.open_orders(symbol)
        if orders:
            await self.cancel([Cancel(o.symbol, o.oid) for o in orders])

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        return False
