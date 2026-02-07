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

    def update(self, margin_summary: dict[str, str]):
        self.value = float(margin_summary["accountValue"])
        self.notional = float(margin_summary["totalNtlPos"])
        self.raw = float(margin_summary["totalRawUsd"])
        self.used = float(margin_summary["totalMarginUsed"])
    
    def __str__(self):
        return f"value={self.value}, notional={self.notional}, raw={self.raw}, used={self.used}"

class Position:
    def __init__(self, instrument):
        self.instrument = instrument
        self.received = asyncio.Event() # received one activeAssetData
        # ==== clearingHouseState ====
        self.size = None
        self.entry = None
        self.value = None
        self.liquidation = None
        self.margin = None
        self.funding = None # dict {"allTime": "0.0", "sinceOpen": "0.0", "sinceChange": "0.0"}
        # ==== activeAssetData ====
        self.mode =  None # margin mode: "cross" or "isolated"
        self.leverage = None
        self.raw = None # rawUsd on hl | margin - positionValue
        self.longable = None # maxTradeSzs[0] on hl | sz available to long
        self.shortable = None # maxTradeSzs[1] on hl | sz available to short
    
    def __str__(self):
        return (
            f"{self.instrument.coin}: size={self.size}, entry={self.entry}, "
            f"value={self.value}, liquidation={self.liquidation}, margin={self.margin}, "
            f"mode={self.mode}, leverage={self.leverage}, raw={self.raw}, "
            f"longable={self.longable}, shortable={self.shortable}"
        )

    def update_clearingHouseState(self, position):
        if self.instrument.coin != position["coin"]:
            raise ValueError(f"cannot update {self.instrument.coin} with {position["coin"]} data")
        self.size = float(position["szi"])
        self.entry = float(position["entryPx"])
        self.value = float(position["positionValue"])
        self.liquidation  = float(position["liquidationPx"] or 0)
        self.margin = float(position["marginUsed"])
        self.funding = position["cumFunding"]
        
    def update_activeAssetData(self, data):
        if self.instrument.coin != data["coin"]:
            raise ValueError(f"cannot update {self.instrument.coin} with {data["coin"]} data")
        l = data["leverage"]
        self.mode = l["type"]
        self.leverage = l["value"]
        self.raw = float(l["rawUsd"])
        maxTradeSzs = data["maxTradeSzs"]
        self.longable = float(maxTradeSzs[0])
        self.shortable = float(maxTradeSzs[1])
        self.received.set()

class AccountPerpDexHL:
    def __init__(self, dex, instruments, hl):
        if [dex] != list({i.dex for i in instruments}):
            raise ValueError(f"several provided instrument in {[i.name for i in instruments]} doesn't belong to {dex if dex else '""'}")
        self.dex = dex
        self.instruments = instruments
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.running = False
        # ==== clearinghouseState ====
        self.received = asyncio.Event()
        self.account = MarginSummary()
        self.cross = MarginSummary()
        self.withdrawable = None
        self.last_clearinghouseState = 0
        # ==== activeAssetData ====
        self.positions: dict[str, Position] = {i.coin: Position(i) for i in instruments}
        self.last_activeAssetData = 0

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        subscriptions = []
        subscriptions.append(self.hl.websocket.clearinghouse_state(self.on_clearinghouseState, self.hl.address, self.dex))
        subscriptions += [self.hl.websocket.active_asset_data(self.on_activeAssetData, self.hl.address, i.coin) for i in self.instruments]
        await asyncio.gather(*subscriptions)
        await self.received.wait()
        await asyncio.gather(*[p.received.wait() for p in self.positions.values()])
        await self._log.success("Started")
        self.running = True
    
    async def stop(self):
        self.running = False
        self.received.clear()
        for p in self.positions.values():
            p.received.clear()
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
            assetPositions = msg.get("assetPositions", [])
            for assetPosition in assetPositions:
                position_update = assetPosition["position"]
                position = self.positions[position_update["coin"]]
                position.update_clearingHouseState(position_update)
            self.last_clearinghouseState = time()
            self.received.set()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_activeAssetData(self, msg):
        try:
            coin = msg.get("coin")
            if coin not in self.positions:
                return await self._log.warning(f"received activeAssetData for unknown coin {coin}")
            position = self.positions[coin]
            position.update_activeAssetData(msg)
            self.last_activeAssetData = time()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    def __str__(self):
        return (
            f"<AccountPerpDexHL dex={self.dex} | "
            f"accountMargin = {self.account} | "
            f"crossMargin = {self.cross} | " if self.dex == "" else ""
            f"withdrawable={self.withdrawable} | "
            f"positions=[{self.positions}]>"
        )
