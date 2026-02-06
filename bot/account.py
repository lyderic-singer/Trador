from time import time
import asyncio
from log.logger import Logger
from collections import defaultdict, deque

class AccountPerpHL:
    def __init__(self, instruments, hl):
        self.instruments = instruments
        self.dexs = list({i.dex for i in instruments})
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.running = asyncio.Event()
         # ==== clearinghouseState ====
        self.margin_summary = None
        self.cross_margin_summary = None
        self.withdrawable = None
        self.positions = {}  # coin -> position dict
        self.last_clearinghouseState = None
        # ==== activeAssetData ====
        self.leverage = {}
        self.availableSzLong = {}
        self.availableSzShort = {}
        self.availableDolLong = {}
        self.availableDolShort = {}
        self.last_activeAssetData = {}
        # ==== orders / fills ====
        self.open_orders = defaultdict(list)   # coin -> [orders]
        self.order_updates = deque(maxlen=500)
        self.user_fills = deque(maxlen=500)

    async def start(self):
        subscriptions = []
        for dex in self.dexs:
            subscriptions.append(self.hl.websocket.clearinghouse_state(self.on_clearinghouseState, self.hl.address, dex))
            subscriptions.append(self.hl.websocket.open_orders(self.on_openOrders, self.hl.address, dex))
        subscriptions += [self.hl.websocket.active_asset_data(self.on_activeAssetData, self.hl.address, instrument.coin) for instrument in self.instruments]
        subscriptions.append(self.hl.websocket.order_updates(self.on_orderUpdates, self.hl.address))
        subscriptions.append(self.hl.websocket.user_fills(self.on_userFills, self.hl.address))
        await asyncio.gather(*subscriptions)
        self.running.set()
        await self._log.success("Started")
    
    async def stop(self):
        self.running.clear()
        await self._log.success("Stopped")

    async def on_clearinghouseState(self, msg):
        try:
            ch = msg["clearinghouseState"]
            self.margin_summary = ch["marginSummary"]
            self.cross_margin_summary = ch["crossMarginSummary"]
            self.withdrawable = float(ch["withdrawable"])
            self.positions.clear()
            for p in ch["assetPositions"]:
                pos = p["position"]
                self.positions[pos["coin"]] = {
                    "szi": float(pos["szi"]),
                    "entryPx": float(pos["entryPx"]),
                    "positionValue": float(pos["positionValue"]),
                    "unrealizedPnl": float(pos["unrealizedPnl"]),
                    "roe": float(pos["returnOnEquity"]),
                    "liqPx": float(pos["liquidationPx"] or 0),
                    "marginUsed": float(pos["marginUsed"]),
                    "leverage": pos["leverage"],
                }
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
