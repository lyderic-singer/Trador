import asyncio
import math
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any
from adapters.exchange import AccountListener, BBOListener, Exchange
from adapters.models import (AccountState, BBO, Cancel, Candle, Instrument, MarginMode, Modify, OpenOrder, Order,
                         Position, Result, Side, TIF)
from core.utils import call
from core.HL.client import HLClient
from core.HL.utils.types import BuilderInfo, Instrument as Asset
from log.logger import Logger

TIFS = {TIF.ALO: "Alo", TIF.IOC: "Ioc", TIF.GTC: "Gtc"}
FROM_TIFS = {"Alo": TIF.ALO, "Ioc": TIF.IOC, "Gtc": TIF.GTC}
TIMEFRAMES = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
              "8h": 28800, "12h": 43200, "1d": 86400}


def wire(x: Decimal) -> str:
    s = format(x.normalize(), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def rounding(side: Side | None, passive: bool) -> str:
    if side is None:
        return "nearest"
    return ("down" if side.is_buy else "up") if passive else ("up" if side.is_buy else "down")


def round_price(asset: Asset, px: float, mode: str) -> Decimal:
    if px <= 0:
        return Decimal(0)
    digits = int(math.floor(math.log10(px))) + 1
    decimals = max(0, min(asset.px_decimals, 5 - digits))
    step = Decimal(1).scaleb(-decimals)
    mode = {"down": ROUND_FLOOR, "up": ROUND_CEILING}.get(mode)
    return Decimal(str(px)).quantize(step, rounding=mode) if mode else Decimal(str(px)).quantize(step)


def round_size(asset: Asset, sz: float) -> Decimal:
    return Decimal(str(sz)).quantize(Decimal(1).scaleb(-asset.sz_decimals), rounding=ROUND_FLOOR)


class Hyperliquid(Exchange):
    name = "hyperliquid"

    def __init__(self, client: HLClient, builder: BuilderInfo | None = None, maker_fee: float = 0.00015,
                 taker_fee: float = 0.00045, fees: dict[str, tuple[float, float]] | None = None, min_notional: float = 10.0):
        self.client = client
        self.messenger = client.messenger
        self.builder = builder
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.fees = fees or {}
        self.min_notional = min_notional
        self._log = Logger("Hyperliquid", client.messenger)
        self.address = client.address
        self.listeners: list[AccountListener] = []
        self.bbo_listeners: dict[str, list[BBOListener]] = {}
        self.subscribed = False
        self.tracked: str | None = None
        self.state = AccountState(self.name)
        self.ready = False

    async def start(self):
        if not self.client.running:
            await self.client.start()

    async def stop(self):
        pass

    def asset(self, symbol: str) -> Asset:
        return self.client.info.instrument(symbol)

    def instrument(self, symbol: str) -> Instrument:
        asset = self.asset(symbol)
        maker, taker = self.fees.get(asset.name, (self.maker_fee, self.taker_fee))
        return Instrument(
            exchange=self.name, symbol=asset.name, maker_fee=maker, taker_fee=taker,
            min_notional=self.min_notional, min_size=10 ** -asset.sz_decimals,
            pricer=lambda px, side, passive: float(round_price(asset, px, rounding(side, passive))),
            sizer=lambda sz: float(round_size(asset, sz)),
            raw=asset)

    async def subscribe_bbo(self, symbol: str, listener: BBOListener):
        asset = self.asset(symbol)
        if asset.name in self.bbo_listeners:
            self.bbo_listeners[asset.name].append(listener)
            return
        self.bbo_listeners[asset.name] = [listener]

        async def on_bbo(msg):
            bid, ask = (msg.get("bbo") or [None, None])[:2]
            bbo = BBO(asset.name, float(bid["px"]) if bid else None, float(ask["px"]) if ask else None,
                      float(bid["sz"]) if bid else None, float(ask["sz"]) if ask else None)
            for listener in self.bbo_listeners[asset.name]:
                await call(listener, bbo)
        await self.client.websocket.bbo(on_bbo, asset.coin)

    async def candles(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        asset = self.asset(symbol)
        now = int(time.time() * 1000)
        span = TIMEFRAMES.get(timeframe, 60) * 1000 * (count + 1)
        raw = await self.client.info.candles_snapshot(asset.coin, timeframe, now - span, now)
        return [Candle(c["t"] / 1000, float(c["o"]), float(c["h"]), float(c["l"]), float(c["c"]), float(c["v"])) for c in raw][-count:]

    async def mid(self, symbol: str) -> float | None:
        asset = self.asset(symbol)
        return await self.client.info.price(asset.coin, asset.dex)

    def dex(self, symbols: list[str]) -> str:
        dexes = {self.asset(s).dex for s in symbols}
        if len(dexes) != 1:
            raise ValueError(f"symbols {symbols} span several HL dexes {dexes}: use one Hyperliquid per dex")
        return dexes.pop()

    def parse_state(self, chs: dict):
        summary = chs["marginSummary"]
        self.state.equity = float(summary["accountValue"])
        self.state.margin = float(summary["totalMarginUsed"])
        self.state.free = float(chs.get("withdrawable") or 0)
        positions: dict[str, Position] = {}
        for row in chs.get("assetPositions", []):
            p = row["position"]
            lev = p.get("leverage") or {}
            name = self.asset(p["coin"]).name
            positions[name] = Position(
                symbol=name, size=float(p["szi"]), entry=float(p["entryPx"]) if p.get("entryPx") else None,
                value=float(p["positionValue"]), leverage=float(lev.get("value", 1)),
                mode=MarginMode(lev["type"]) if lev.get("type") in ("cross", "isolated") else None,
                pnl=float(p["unrealizedPnl"]),
                liquidation=float(p["liquidationPx"]) if p.get("liquidationPx") else None,
                margin=float(p["marginUsed"]))
            self.state.leverages[name] = float(lev.get("value", 1))
        self.state.positions = positions
        self.state.ts = time.time()

    async def push(self):
        if self.ready:
            snapshot = self.state.copy()
            for listener in self.listeners:
                await call(listener, snapshot)

    async def on_state(self, msg):
        if (msg.get("dex") or "") != self.tracked:
            return
        self.parse_state(msg["clearinghouseState"])
        self.ready = True
        await self.push()

    async def on_asset(self, msg):
        name = self.asset(msg["coin"]).name
        leverage = float(msg["leverage"]["value"])
        self.state.leverages[name] = leverage
        if name in self.state.positions:
            self.state.positions[name].leverage = leverage
        await self.push()

    async def subscribe_account(self, symbols: list[str], listener: AccountListener):
        dex = self.dex(symbols)
        if self.subscribed and dex != self.tracked:
            raise ValueError(f"this Hyperliquid already tracks dex '{self.tracked}': create another one for '{dex}'")
        self.listeners.append(listener)
        fresh = [s for s in symbols if self.asset(s).name not in self.state.leverages]
        if not self.subscribed:
            self.subscribed = True
            self.tracked = dex
            await self.client.websocket.clearinghouse_state(self.on_state, self.address, dex)
        await asyncio.gather(*[self.client.websocket.active_asset_data(self.on_asset, self.address, self.asset(s).coin) for s in fresh])
        await self.push()

    async def account(self, symbols: list[str]) -> AccountState:
        self.parse_state(await self.client.info.clearing_house_state(self.address, self.dex(symbols)))
        self.ready = True
        return self.state.copy()

    async def open_orders(self, symbol: str | None = None) -> list[OpenOrder]:
        asset = self.asset(symbol) if symbol else None
        raw = await self.client.info.open_orders(self.address, dex=asset.dex if asset else (self.tracked or ""))
        out = []
        for o in raw:
            if asset and o["coin"] != asset.coin:
                continue
            out.append(OpenOrder(
                symbol=self.asset(o["coin"]).name, oid=str(o["oid"]), side=Side.BUY if o["side"] == "B" else Side.SELL,
                price=float(o["limitPx"]), size=float(o["sz"]), tif=FROM_TIFS.get(o.get("tif") or "", TIF.GTC),
                reduce=bool(o.get("reduceOnly", False)), client=o.get("cloid"), raw=o))
        return out

    def encode(self, symbol: str, side: Side, size: float, price: float, tif: TIF, reduce: bool, client: str | None = None) -> dict | str:
        asset = self.asset(symbol)
        px = round_price(asset, price, rounding(side, tif != TIF.IOC))
        sz = round_size(asset, size)
        if sz <= 0:
            return f"size {size} rounds to 0 ({asset.sz_decimals} decimals)"
        if px <= 0:
            return f"invalid price {price}"
        order: dict[str, Any] = {"a": asset.asset, "b": side.is_buy, "p": wire(px), "s": wire(sz), "r": reduce,
                                 "t": {"limit": {"tif": TIFS[tif]}}}
        if client and client.startswith("0x") and len(client) == 34:
            order["c"] = client
        return order

    @staticmethod
    def parse_statuses(res: Any, size: int) -> list[Result]:
        if not isinstance(res, dict) or res.get("status") != "ok":
            return [Result(False, error=f"{res}", raw=res)] * size
        response = res.get("response") or {}
        statuses = (response.get("data") or {}).get("statuses", []) if isinstance(response, dict) else []
        out = []
        for s in statuses:
            if s == "success":
                out.append(Result(True, status="success", raw=s))
            elif isinstance(s, dict) and "resting" in s:
                out.append(Result(True, str(s["resting"]["oid"]), "resting", raw=s))
            elif isinstance(s, dict) and "filled" in s:
                f = s["filled"]
                out.append(Result(True, str(f.get("oid")), "filled", float(f.get("totalSz", 0)),
                                  float(f["avgPx"]) if f.get("avgPx") else None, raw=s))
            elif isinstance(s, dict) and "error" in s:
                out.append(Result(False, status="error", error=s["error"], raw=s))
            else:
                out.append(Result(False, error=f"unexpected status {s}", raw=s))
        out += [Result(False, error="missing status")] * (size - len(out))
        return out[:size]

    async def send(self, items: list, build, post) -> list[Result]:
        results: list[Result | None] = [None] * len(items)
        wires, indexes = [], []
        for i, item in enumerate(items):
            w = build(item)
            if isinstance(w, str):
                results[i] = Result(False, error=w)
            else:
                wires.append(w)
                indexes.append(i)
        if wires:
            res = await post(wires)
            for i, r in zip(indexes, self.parse_statuses(res, len(wires))):
                results[i] = r
        return [r for r in results if r is not None]

    async def place(self, orders: list[Order]) -> list[Result]:
        builder = dict(self.builder) if self.builder else None
        return await self.send(
            orders,
            lambda o: self.encode(o.symbol, o.side, o.size, o.price, o.tif, o.reduce, o.client),
            lambda w: self.client.exchange.batch_order(w, builder))

    async def modify(self, modifies: list[Modify]) -> list[Result]:
        def build(m: Modify):
            w = self.encode(m.order.symbol, m.side, m.size, m.price, m.tif or m.order.tif, m.order.reduce)
            return w if isinstance(w, str) else {"oid": int(m.order.oid), "order": w}
        return await self.send(modifies, build, self.client.exchange.batch_modify)

    async def cancel(self, cancels: list[Cancel]) -> list[Result]:
        return await self.send(
            cancels,
            lambda c: {"a": self.asset(c.symbol).asset, "o": int(c.oid)},
            lambda w: self.client.exchange.batch_cancel(True, w))
