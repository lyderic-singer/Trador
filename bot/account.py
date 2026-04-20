from time import time
import asyncio
from core.HL.hl import HL
from core.HL.utils.types import Instrument
from log.logger import Logger

class MarginSummary:
    def __init__(self):
        # accountValue on HL | $ account value
        self.value: float = 0
        # totalNtlPos on HL | sum(abs(positionValue))
        self.notional: float = 0
        # totalRawUsd on HL | sum(margin) - sum(positionValue)
        self.raw: float = 0
        # totalMarginUsed on HL | sum(marginUsed)
        self.used: float = 0

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
        self.received_asset = asyncio.Event() # received one activeAssetData
        self.active = False
        # ==== clearingHouseState ====
        self.size = 0
        self.entry = None
        self.value = 0
        self.liquidation = None
        self.margin = 0
        self.funding = {"allTime": "0.0", "sinceOpen": "0.0", "sinceChange": "0.0"}
        self.pnl = 0
        # ==== activeAssetData ====
        self.mode = None # margin mode: "cross" or "isolated"
        self.leverage = 1
        self.raw = None # rawUsd on hl | margin - positionValue
        self.longable = 0 # maxTradeSzs[0] on hl | $ available to long (poorly calculated from hl data: doesn't take into account current orders while their value is under positionValue)
        self.shortable = 0 # maxTradeSzs[1] on hl | $ available to short (poorly calculated from hl data: doesn't take into account current orders while their value is under positionValue)

    def update_clearingHouseState(self, position):
        if position is None and self.active:
            self.active = False
            self.size = 0
            self.entry = None
            self.value = 0
            self.liquidation = None
            self.margin = 0
            self.funding = {"allTime": "0.0", "sinceOpen": "0.0", "sinceChange": "0.0"}
            self.pnl = 0
        elif position:
            if self.instrument.coin != position["coin"]:
                raise ValueError(f"cannot update {self.instrument.coin} with {position["coin"]} data")
            self.size = float(position["szi"])
            self.entry = float(position["entryPx"])
            self.value = float(position["positionValue"])
            self.liquidation = None if position["liquidationPx"] is None else float(position["liquidationPx"])
            self.margin = float(position["marginUsed"])
            self.funding = position["cumFunding"]
            self.pnl = float(position["unrealizedPnl"])
        
    def update_activeAssetData(self, data):
        if self.instrument.coin != data["coin"]:
            raise ValueError(f"cannot update {self.instrument.coin} with {data["coin"]} data")
        l = data["leverage"]
        self.mode = l["type"]
        self.leverage = l["value"]
        if self.mode == "isolated": # be carefull
            self.raw = float(l["rawUsd"])
        availableToTrade = data["availableToTrade"]
        self.longable = float(availableToTrade[0]) * self.leverage
        self.shortable = float(availableToTrade[1]) * self.leverage
        self.received_asset.set()

    def __str__(self):
        return (
            f"{self.instrument.coin}: size={self.size}, entry={self.entry}, "
            f"value={self.value}, liquidation={self.liquidation}, margin={self.margin}, "
            f"mode={self.mode}, leverage={self.leverage}, raw={self.raw}, "
            f"longable={self.longable}, shortable={self.shortable}"
        )

class AccountPerpDexHL:
    def __init__(self, dex: str, instruments: list[Instrument], hl: HL):
        if [dex] != list({i.dex for i in instruments}):
            raise ValueError(f"several provided instrument in {[i.name for i in instruments]} doesn't belong to {dex if dex else '""'}")
        self.dex = dex
        self.instruments = instruments
        self.hl = hl
        self._log = Logger(self, hl.messenger)
        self.pnl: float = 0
        self.running = False
        # ==== clearinghouseState ====
        self.received_state = asyncio.Event()
        self.account = MarginSummary()
        self.cross = MarginSummary()
        self.withdrawable = None
        self.last_state = 0
        # ==== activeAssetData ====
        self.positions: dict[str, Position] = {i.coin: Position(i) for i in instruments}
        self.last_asset = 0

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        subscriptions = []
        subscriptions.append(self.hl.websocket.clearinghouse_state(self.on_clearinghouseState, self.hl.address, self.dex))
        subscriptions += [self.hl.websocket.active_asset_data(self.on_activeAssetData, self.hl.address, i.coin) for i in self.instruments]
        await asyncio.gather(*subscriptions)
        await self.received_state.wait()
        await asyncio.gather(*[p.received_asset.wait() for p in self.positions.values()])
        await self._log.success("Started")
        self.running = True

    async def stop(self):
        self.running = False
        self.received_state.clear()
        for p in self.positions.values():
            p.received_asset.clear()
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
            to_update = set(self.positions)
            for assetPosition in assetPositions:
                position_update = assetPosition["position"]
                coin = position_update["coin"]
                position = self.positions.get(coin)
                if not position:
                    instrument = self.hl.info.instrument(coin)
                    position = Position(instrument)
                    self.positions[instrument.coin] = position
                position.update_clearingHouseState(position_update)
                to_update.discard(coin)
            for coin in to_update:
                position = self.positions[coin]
                position.update_clearingHouseState(None)
            self.last_state = time()
            self.received_state.set()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    async def on_activeAssetData(self, msg):
        try:
            coin = msg.get("coin")
            if coin not in self.positions:
                return await self._log.warning(f"received activeAssetData for unknown coin {coin}")
            position = self.positions[coin]
            position.update_activeAssetData(msg)
            self.last_asset = time()
        except Exception as e:
            await self._log.error(f"{e}, {msg}")

    def __str__(self):
        return (
            f"<AccountPerpDexHL dex={self.dex} | "
            f"accountMargin = {self.account} | "
            f"crossMargin = {self.cross} | "
            f"withdrawable={self.withdrawable} | "
            f"positions={[f"{p}" for p in self.positions]}>"
        )
