from abc import ABC, abstractmethod
from typing import Awaitable, Callable
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Instrument, Listing, Modify, OpenOrder, Order, OrderUpdate,
                             Portfolio, Result, Volume)

BBOListener = Callable[[BBO], Awaitable[None] | None]
AccountListener = Callable[[AccountState], Awaitable[None] | None]
FillListener = Callable[[Fill], Awaitable[None] | None]
OrderListener = Callable[[OrderUpdate], Awaitable[None] | None]
PriceListener = Callable[[str, float], Awaitable[None] | None]


class Exchange(ABC):
    name: str = "exchange"
    messenger = None

    def canonical(self, symbol: str) -> str:
        return self.instrument(symbol).symbol

    def tradable(self, symbol: str) -> bool:
        return True

    @property
    def connected(self) -> bool:
        return True

    def budget(self) -> dict[str, int] | None:
        return None

    def listings(self) -> list[Listing]:
        return []

    async def portfolio(self) -> Portfolio:
        return Portfolio.of(await self.account([]))

    async def volume(self) -> Volume | None:
        """volume traded on the account over 24h / 7j / 30j and since inception (/vol), None when unsupported"""
        return None

    async def book(self, symbol: str) -> BBO | None:
        """best bid / ask read by REST, None when the exchange cannot (the websocket book is then trusted)"""
        return None

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
            await self.cancel([Cancel(o.symbol, o.oid, o.client) for o in orders])

    async def purge(self) -> None:
        await self.cancel_all(None)

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        return False

    async def subscribe_fills(self, symbols: list[str], listener: FillListener) -> None:
        raise NotImplementedError(f"{self.name} does not stream fills")

    async def subscribe_orders(self, symbols: list[str], listener: OrderListener) -> bool:
        return False

    async def subscribe_oracle(self, symbol: str, listener: PriceListener) -> bool:
        return False

    async def schedule_cancel(self, seconds: float | None) -> bool:
        return False
