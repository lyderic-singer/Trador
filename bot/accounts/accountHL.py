from time import time
from ...core.HL.utils.functions import cancel

class AccountHL:
    def __init__(self, instrument, hl):
        self.instrument = instrument
        self.hl = hl
        self._log = Logger(f"{self.instrument.coin} Market", hl.messenger)
        self.running = asyncio.Event()
        self.task = None
        # ==== state ====
        # ==== asset ====
        self.position
        # ==== orders ====
        # ==== fills ====

    async def start(self):
        await self.hl.websocket.clearinghouse_state(self.on_clearinghouseState, self.hl.address, self.instrument.dex)
        await self.hl.websocket.active_asset_data(self.on_activeAssetData, self.hl.address, self.instrument.coin)
        await self.hl.websocket.open_orders(self.on_openOrders, self.hl.address, self.instrument.dex)
        await self.hl.websocket.user_fills(self.on_userFills, self.hl.address)
    
