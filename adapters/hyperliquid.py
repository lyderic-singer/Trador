import asyncio
import math
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any
from adapters.exchange import AccountListener, BBOListener, Exchange, FillListener, OrderListener, PriceListener, PrintListener
from adapters.models import (AccountState, BBO, Cancel, Candle, Fill, Funding, FundingPayment, Instrument, Listing, MarginMode, Modify,
                             OpenOrder, Order, OrderUpdate, Portfolio, Position, Print, Result, Side, TIF, Volume)
from core.utils import call, cancel
from core.HL.client import HLClient
from core.HL.utils.types import BuilderInfo, Instrument as Asset
from log.logger import Logger

TIFS = {TIF.ALO: "Alo", TIF.IOC: "Ioc", TIF.GTC: "Gtc"}
FROM_TIFS = {"Alo": TIF.ALO, "Ioc": TIF.IOC, "Gtc": TIF.GTC}
STABLES = {"USDC", "USDT0", "USDH", "USDE", "USDT"}
TIMEFRAMES = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
              "8h": 28800, "12h": 43200, "1d": 86400}


def wire(x: Decimal) -> str:
    s = format(x.normalize(), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def rounding(side: Side | None, passive: bool) -> str:
    if side is None:
        return "nearest"
    return ("down" if side.is_buy else "up") if passive else ("up" if side.is_buy else "down")


def tick(asset: Asset, px: float) -> Decimal:
    if px <= 0:
        return Decimal(1).scaleb(-asset.px_decimals)
    digits = int(math.floor(math.log10(px))) + 1
    return Decimal(1).scaleb(-max(0, min(asset.px_decimals, 5 - digits)))


def round_price(asset: Asset, px: float, mode: str) -> Decimal:
    if px <= 0:
        return Decimal(0)
    step = tick(asset, px)
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
        self.fill_listeners: list[FillListener] = []
        self.order_listeners: list[OrderListener] = []
        self.oracle_listeners: dict[str, list[PriceListener]] = {}
        self.print_listeners: dict[str, list[PrintListener]] = {}
        self.seen: set[str] = set()
        # address-based action budget (userRateLimit): 1 action per USDC traded, +10,000 at inception
        self.remaining: dict[str, int] = {}
        self.limits: dict[str, Any] = {}
        self.watching: asyncio.Task | None = None
        self.volumes: tuple[float, Volume] | None = None
        self.ctxs: dict[str, tuple[float, dict]] = {}  # name -> (received, activeAssetCtx ctx)
        self.watched: set[str] = set()

    async def start(self):
        if not self.client.running:
            await self.client.start()
        if self.watching is None or self.watching.done():
            self.watching = asyncio.create_task(self.watch_budget())

    async def stop(self):
        await cancel(self.watching)

    def budget(self) -> dict[str, int] | None:
        return dict(self.remaining) if self.remaining else None

    async def rate_limit(self) -> dict:
        res = await self.client.info.post({"type": "userRateLimit", "user": self.address})
        cap, used = int(res.get("nRequestsCap") or 0), int(res.get("nRequestsUsed") or 0)
        # cancels get min(cap + 100000, 2 cap); the counter used is shared
        self.remaining = {"order": cap - used, "cancel": min(cap + 100_000, 2 * cap) - used}
        self.limits = {"cap": cap, "used": used, "volume": float(res.get("cumVlm") or 0),
                       "surplus": int(res.get("nRequestsSurplus") or 0), "ts": time.time()}
        return res

    async def watch_budget(self):
        while True:
            try:
                await self.rate_limit()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._log.debug(f"userRateLimit read failed: {e}")
            low = self.remaining and self.remaining.get("order", 0) < 5_000
            await asyncio.sleep(10 if low else 30)

    def spend(self, pool: str, n: int):
        # one counter for every action: a cancel uses it too, it is only allowed further (up to the cancel cap)
        for name in self.remaining:
            self.remaining[name] -= n

    async def volume(self) -> Volume:
        """24h / 7j / 30j summed from userFillsByTime (only the 10,000 latest fills exist), total from userRateLimit"""
        if self.volumes and time.time() - self.volumes[0] < 120:
            return self.volumes[1]
        info = self.client.info
        now = int(time.time() * 1000)
        fills: dict[str, dict] = {}
        truncated, calls = False, 0
        # a full response (2000 fills) covers [oldest, newest] of what it returned: the rest of the window is asked
        # again on both sides, whatever the order the API uses (boundaries overlap by 1 ms, rows are deduplicated)
        windows = [(now - 30 * 86_400_000 + i * 10 * 86_400_000, now - 20 * 86_400_000 + i * 10 * 86_400_000) for i in range(3)]
        while windows:
            if calls >= 16:
                truncated = True
                break
            start, end = windows.pop()
            calls += 1
            batch = await info.user_fills_by_time(self.address, start, end) or []
            for row in batch:
                fills[f"{row.get('tid')}:{row.get('hash')}:{row.get('oid')}"] = row
            if len(batch) >= 2000:
                times = [int(row.get("time") or 0) for row in batch]
                low, high = min(times), max(times)
                if low > start:
                    windows.append((start, low + 1))
                if high < end - 1:
                    windows.append((high, end))
                if low <= start and high >= end - 1:
                    truncated = True
        rows = [(float(r.get("time") or 0) / 1000, float(r["px"]) * float(r["sz"]), not r.get("crossed", False),
                 float(r.get("fee") or 0) + float(r.get("builderFee") or 0)) for r in fills.values()]
        out = Volume.of(self.name, rows, now / 1000, "tous dex")
        out.truncated = truncated or len(fills) >= 10_000
        try:
            await self.rate_limit()
            out.total = self.limits["volume"]
        except Exception as e:
            out.error = f"userRateLimit: {e}"
        self.volumes = (time.time(), out)
        return out

    def asset(self, symbol: str) -> Asset:
        return self.client.info.instrument(symbol)

    def instrument(self, symbol: str) -> Instrument:
        asset = self.asset(symbol)
        # fees by asset name first ("xyz:NVDA"), then by dex ("xyz"), then the adapter default
        maker, taker = self.fees.get(asset.name) or self.fees.get(asset.dex) or (self.maker_fee, self.taker_fee)
        return Instrument(
            exchange=self.name, symbol=asset.name, maker_fee=maker, taker_fee=taker,
            min_notional=self.min_notional, min_size=10 ** -asset.sz_decimals,
            pricer=lambda px, side, passive: float(round_price(asset, px, rounding(side, passive))),
            sizer=lambda sz: float(round_size(asset, sz)),
            stepper=lambda px: float(tick(asset, px)),
            raw=asset, max_leverage=getattr(asset, "max_leverage", None),
            # HL maintenance margin is half the initial margin at max leverage
            maintenance=0.5 / asset.max_leverage if getattr(asset, "max_leverage", None) else None,
            initial=1 / asset.max_leverage if getattr(asset, "max_leverage", None) else None,
            isolated=bool(getattr(asset, "isolated_only", False)))

    def listings(self) -> list[Listing]:
        out = []
        for asset in self.client.info.instruments_dict.values():
            if asset.type not in ("perp", "hip3"):
                continue
            out.append(Listing(asset.name, asset.name.split(":")[-1], "", asset.dex, not getattr(asset, "delisted", False)))
        return out

    async def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        asset = self.asset(symbol)
        lev = int(min(leverage, asset.max_leverage or leverage))
        cross = cross and not getattr(asset, "isolated_only", False)
        res = await self.client.exchange.update_leverage(asset.asset, lev, cross)
        ok = isinstance(res, dict) and res.get("status") == "ok"
        if not ok and cross:
            # some HIP-3 dexes refuse cross margin without flagging it in their meta
            res = await self.client.exchange.update_leverage(asset.asset, lev, False)
            ok = isinstance(res, dict) and res.get("status") == "ok"
        if ok:
            self.state.leverages[asset.name] = lev
        return ok

    @staticmethod
    def summarize(chs: dict) -> tuple[float, float, float, float, list[dict]]:
        summary = chs.get("marginSummary") or {}
        positions = [row["position"] for row in chs.get("assetPositions", []) if float(row["position"].get("szi") or 0)]
        return (float(summary.get("accountValue") or 0), float(chs.get("withdrawable") or 0),
                sum(float(p.get("unrealizedPnl") or 0) for p in positions),
                sum(abs(float(p.get("positionValue") or 0)) for p in positions), positions)

    async def portfolio(self) -> Portfolio:
        info = self.client.info
        dexes = [""]
        try:
            dexes += [d["name"] for d in await info.perp_dexs() if isinstance(d, dict) and d.get("name")]
        except Exception:
            dexes += sorted({a.dex for a in info.instruments_dict.values() if a.dex})
        gate = asyncio.Semaphore(6)

        async def read(dex: str):
            async with gate:
                return await info.clearing_house_state(self.address, dex)
        states = await asyncio.gather(*(read(d) for d in dexes), return_exceptions=True)
        out = Portfolio(self.name)
        failed = []
        for dex, chs in zip(dexes, states):
            if isinstance(chs, BaseException) or not isinstance(chs, dict):
                failed.append(dex or "perp")
                continue
            value, free, pnl, notional, positions = self.summarize(chs)
            if value == 0 and not positions:
                continue
            out.details[dex or "perp"] = value
            out.equity += value
            out.free += free
            out.pnl += pnl
            out.notional += notional
            for p in positions:
                try:
                    name = self.asset(p["coin"]).name
                except ValueError:
                    name = p["coin"]
                out.positions[name] = Position(name, float(p["szi"]), float(p["entryPx"]) if p.get("entryPx") else None,
                                               abs(float(p.get("positionValue") or 0)), pnl=float(p.get("unrealizedPnl") or 0))
        try:
            spot = await info.spot_clearing_house_state(self.address)
            balances = [b for b in spot.get("balances", []) if float(b.get("total") or 0)]
            mids = await info.all_mids() if any(b["coin"] not in STABLES for b in balances) else {}
            for b in balances:
                total = float(b["total"])
                if b["coin"] in STABLES:
                    value = total
                else:
                    try:
                        price = float(mids[info.instrument(f"{b['coin']}/USDC").coin])
                        value = total * price
                    except Exception:
                        value = float(b.get("entryNtl") or 0)
                if value:
                    out.details[f"spot {b['coin']}"] = value
                    out.equity += value
        except Exception:
            failed.append("spot")
        if failed:
            out.error = f"unreadable: {', '.join(failed)}"
        return out

    @property
    def connected(self) -> bool:
        return self.client.websocket.connected.is_set()

    def parse_fill(self, row: dict) -> Fill:
        fee = float(row["fee"]) + float(row.get("builderFee") or 0) if row.get("fee") not in (None, "") else None
        return Fill(self.asset(row["coin"]).name, Side.BUY if row["side"] == "B" else Side.SELL, float(row["sz"]),
                    float(row["px"]), str(row.get("oid")), row.get("cloid"), str(row.get("tid") or row.get("hash")),
                    not row.get("crossed", False), None, float(row.get("time") or time.time() * 1000) / 1000, fee)

    async def on_fills(self, msg):
        rows = msg.get("fills", []) if isinstance(msg, dict) else []
        snapshot = isinstance(msg, dict) and msg.get("isSnapshot")
        for row in rows:
            fill = self.parse_fill(row)
            if fill.id in self.seen:
                continue
            self.seen.add(fill.id)
            if not snapshot:
                for listener in self.fill_listeners:
                    await call(listener, fill)

    async def subscribe_fills(self, symbols: list[str], listener: FillListener) -> None:
        self.fill_listeners.append(listener)
        if len(self.fill_listeners) == 1:
            await self.client.websocket.user_fills(self.on_fills, self.address)

    @staticmethod
    def status(raw: str) -> str:
        if raw == "open":
            return "open"
        if raw == "filled":
            return "filled"
        return "rejected" if raw.lower().endswith("rejected") else "canceled"

    async def on_orders(self, msg):
        for row in msg if isinstance(msg, list) else []:
            o = row.get("order") or {}
            if "oid" not in o:
                continue
            update = OrderUpdate(self.asset(o["coin"]).name, str(o["oid"]), Side.BUY if o["side"] == "B" else Side.SELL,
                                 float(o["limitPx"]), float(o.get("origSz") or o["sz"]), float(o["sz"]),
                                 self.status(row.get("status", "")), o.get("cloid"),
                                 float(row.get("statusTimestamp") or time.time() * 1000) / 1000, row)
            if update.status == "filled":
                update.remaining = 0.0
            for listener in self.order_listeners:
                await call(listener, update)

    async def subscribe_orders(self, symbols: list[str], listener: OrderListener) -> bool:
        self.order_listeners.append(listener)
        if len(self.order_listeners) == 1:
            await self.client.websocket.order_updates(self.on_orders, self.address)
        return True

    async def watch_ctx(self, asset: Asset):
        # one activeAssetCtx subscription per coin (the websocket keeps the first callback only): oracle and funding share it
        if asset.name in self.watched:
            return
        self.watched.add(asset.name)

        async def on_ctx(msg):
            ctx = msg.get("ctx") or {}
            self.ctxs[asset.name] = (time.time(), ctx)
            price = ctx.get("oraclePx")
            if price:
                for listener in self.oracle_listeners.get(asset.name, []):
                    await call(listener, asset.name, float(price))
        try:
            await self.client.websocket.active_asset_ctx(on_ctx, asset.coin)
        except Exception:
            self.watched.discard(asset.name)
            raise

    async def subscribe_oracle(self, symbol: str, listener: PriceListener) -> bool:
        asset = self.asset(symbol)
        self.oracle_listeners.setdefault(asset.name, []).append(listener)
        await self.watch_ctx(asset)
        return True

    async def fundings(self, symbols: list[str]) -> dict[str, Funding]:
        """ctx.funding = rate accruing for the current hour (paid at the top of the hour), > 0 = longs pay"""
        assets = [self.asset(s) for s in symbols]
        now = time.time()
        top = (now // 3600 + 1) * 3600
        out: dict[str, Funding] = {}
        for dex in sorted({a.dex for a in assets}):
            try:
                res = await self.client.info.post({"type": "metaAndAssetCtxs", "dex": dex})
                meta, ctxs = res[0], res[1]
                for info, ctx in zip(meta.get("universe", []), ctxs):
                    name = info.get("name")
                    if name and ctx.get("funding") not in (None, ""):
                        self.ctxs[name] = (now, ctx)
            except Exception as e:
                await self._log.debug(f"metaAndAssetCtxs {dex or 'main'} failed: {e}")
        for asset in assets:
            received, ctx = self.ctxs.get(asset.name, (0.0, {}))
            if now - received > 600:
                await self.watch_ctx(asset)  # REST did not return it: the websocket ctx fills the cache
                continue
            try:
                rate = float(ctx["funding"])
            except (KeyError, TypeError, ValueError):
                continue
            out[asset.name] = Funding(asset.name, rate, rate, top, False, received)
        return out

    async def funding_history(self, symbol: str, since: float) -> list[tuple[float, float]]:
        asset = self.asset(symbol)
        start, out = int(since * 1000), {}
        for _ in range(20):
            rows = await self.client.info.post({"type": "fundingHistory", "coin": asset.coin, "startTime": start}) or []
            for r in rows:
                try:
                    out[int(r["time"])] = float(r["fundingRate"])
                except (KeyError, TypeError, ValueError):
                    continue
            if len(rows) < 500:
                break
            start = max(int(r.get("time") or 0) for r in rows) + 1
        return [(t / 1000, v) for t, v in sorted(out.items())]

    async def funding_payments(self, since: float) -> list[FundingPayment]:
        start, out, seen = int(since * 1000), [], set()
        for _ in range(20):
            rows = await self.client.info.post({"type": "userFunding", "user": self.address, "startTime": start}) or []
            for r in rows:
                delta = r.get("delta") or {}
                key = (r.get("time"), r.get("hash"), delta.get("coin"))
                if delta.get("type", "funding") != "funding" or key in seen:
                    continue
                seen.add(key)
                try:
                    name = self.asset(delta["coin"]).name
                except (KeyError, ValueError):
                    name = str(delta.get("coin"))
                try:
                    out.append(FundingPayment(name, float(r["time"]) / 1000, float(delta["usdc"]),
                                              float(delta["fundingRate"]) if delta.get("fundingRate") else None))
                except (KeyError, TypeError, ValueError):
                    continue
            if len(rows) < 500:
                break
            start = max(int(r.get("time") or 0) for r in rows) + 1
        return out

    async def add_margin(self, symbol: str, usd: float) -> bool:
        asset = self.asset(symbol)
        if usd <= 0:
            return False
        res = await self.client.exchange.update_isolated_margin(asset.asset, usd)
        self.spend("order", 1)
        return isinstance(res, dict) and res.get("status") == "ok"

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

    async def subscribe_trades(self, symbol: str, listener: PrintListener) -> bool:
        asset = self.asset(symbol)
        if asset.name in self.print_listeners:
            self.print_listeners[asset.name].append(listener)
            return True
        self.print_listeners[asset.name] = [listener]

        async def on_trades(msg):
            now = time.time()
            for row in msg if isinstance(msg, list) else []:
                try:
                    side = Side.BUY if row.get("side") == "B" else Side.SELL if row.get("side") == "A" else None
                    trade = Print(asset.name, float(row["px"]), float(row["sz"]), side,
                                  float(row.get("time") or now * 1000) / 1000, now)
                except (KeyError, TypeError, ValueError):
                    continue
                for listener in self.print_listeners[asset.name]:
                    await call(listener, trade)
        await self.client.websocket.trades(on_trades, asset.coin)
        return True

    async def book(self, symbol: str) -> BBO | None:
        asset = self.asset(symbol)
        res = await self.client.info.l2_book(asset.name)
        levels = res.get("levels") or [[], []]
        bid = levels[0][0] if levels and levels[0] else None
        ask = levels[1][0] if len(levels) > 1 and levels[1] else None
        return BBO(asset.name, float(bid["px"]) if bid else None, float(ask["px"]) if ask else None,
                   float(bid["sz"]) if bid else None, float(ask["sz"]) if ask else None)

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

    async def send(self, items: list, build, post, pool: str = "order") -> list[Result]:
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
            # a batch of n orders costs n actions of the address budget: count them before the next userRateLimit read
            self.spend(pool, len(wires))
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
            lambda w: self.client.exchange.batch_cancel(True, w), "cancel")
