from time import time
import asyncio
from log.logger import Logger
from collections import defaultdict, deque

class MarginSummary:
    def __init__(self):
        # accountValue on HL | $ account value
        self.value: float | None = None
        # totalNtlPos on HL | sum(abs(positionValue))
        self.notional: float | None = None
        # totalRawUsd on HL | sum(margin) - sum(positionValue)
        self.raw: float | None = None
        # totalMarginUsed on HL | sum(marginUsed)
        self.used: float | None = None
        self.running = asyncio.Event()

    def update(self, margin_summary: dict[str, str]):
        self.value = float(margin_summary["accountValue"])
        self.notional = float(margin_summary["totalNtlPos"])
        self.raw = float(margin_summary["totalRawUsd"])
        self.used = float(margin_summary["totalMarginUsed"])
        if not self.running.is_set():
            self.running.set()

class AccountPerpDexHL:
    def __init__(self, dex, instruments, hl):
        if [dex] != list({i.dex for i in instruments}):
            raise ValueError(f"several provided instrument in {[i.name for i in instruments]} doesn't belong to {dex if dex else '""'}")
        self.dex = dex
        self.instruments = instruments
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.running = asyncio.Event()
         # ==== clearinghouseState ====
        self.account = MarginSummary()
        self.cross = MarginSummary()
        self.withdrawable = None
        self.last_clearinghouseState = 0
        # ==== activeAssetData ====
        self.positions = {}
        self.last_activeAssetData = 0
        # ==== orders / fills ====
        self.open_orders = defaultdict(list)   # coin -> [orders]
        self.order_updates = deque(maxlen=500)
        self.user_fills = deque(maxlen=500)

    async def start(self):
        if self.running.is_set():
            return await self._log.info("already started")
        subscriptions = []
        subscriptions.append(self.hl.websocket.clearinghouse_state(self.on_clearinghouseState, self.hl.address, self.dex))
        # subscriptions.append(self.hl.websocket.open_orders(self.on_openOrders, self.hl.address, self.dex))
        subscriptions += [self.hl.websocket.active_asset_data(self.on_activeAssetData, self.hl.address, i.coin) for i in self.instruments]
        # subscriptions.append(self.hl.websocket.order_updates(self.on_orderUpdates, self.hl.address))
        # subscriptions.append(self.hl.websocket.user_fills(self.on_userFills, self.hl.address))
        await asyncio.gather(*subscriptions)
        await self._log.success("Started")
    
    async def stop(self):
        self.running.clear()
        await self._log.success("Stopped")

    async def on_clearinghouseState(self, msg):
        try:
            dex = msg.get("dex")
            if dex != self.dex:
                return await self._log.warning(f"received msg for another dex {msg}")
            clearing_house_state = msg["clearinghouseState"]
            self.account.update(clearing_house_state["marginSummary"])
            self.cross.update(clearing_house_state["crossMarginSummary"])
            self.withdrawable = float(clearing_house_state["withdrawable"])
            self.last_clearinghouseState = time()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_activeAssetData(self, msg):
        try:
            data = msg
            coin = data["coin"]
            self.leverage[coin] = data["leverage"]
            self.availableSzLong[coin] = float(data["maxTradeSzs"][0])
            self.availableSzShort[coin] = float(data["maxTradeSzs"][1])
            self.availableDolLong[coin] = float(data["availableToTrade"][0])
            self.availableDolShort[coin] = float(data["availableToTrade"][1])
            self.last_activeAssetData[coin] = time()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_openOrders(self, msg):
        try:
            orders = msg["orders"]
            self.open_orders.clear()
            for o in orders:
                self.open_orders[o["coin"]].append({
                    "side": o["side"],
                    "px": float(o["limitPx"]),
                    "sz": float(o["sz"]),
                    "oid": o["oid"],
                    "reduceOnly": o["reduceOnly"],
                    "tif": o["tif"],
                    "ts": o["timestamp"],
                })
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_orderUpdates(self, msg):
        try:
            for upd in msg:
                self.order_updates.append({
                    "coin": upd["order"]["coin"],
                    "oid": upd["order"]["oid"],
                    "status": upd["status"],
                    "ts": upd["statusTimestamp"],
                })
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_userFills(self, msg):
        try:
            if msg.get("isSnapshot"):
                return
            for f in msg["fills"]:
                self.user_fills.append({
                    "coin": f["coin"],
                    "px": float(f["px"]),
                    "sz": float(f["sz"]),
                    "side": f["side"],
                    "pnl": float(f["closedPnl"]),
                    "fee": float(f["fee"]),
                    "time": f["time"],
                    "oid": f["oid"],
                })
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    def __str__(self):
        positions_summary = ", ".join(
            f"{coin}: sz={p['szi']} px={p['entryPx']} pnl={p['unrealizedPnl']}"
            for coin, p in self.positions.items()
        ) or "No positions"
        return (
            f"<AccountPerpHL dexs={self.dexs} | "
            f"withdrawable={self.withdrawable} | "
            f"positions=[{positions_summary}] | "
            f"open_orders_count={sum(len(o) for o in self.open_orders.values())}>"
        )
