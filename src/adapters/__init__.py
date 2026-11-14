from adapters.arcus import Arcus
from adapters.aster import Aster
from adapters.hyperliquid import Hyperliquid
from adapters.lighter import Lighter
from core.HL.client import HLClient
from core.arcus.client import ArcusClient
from core.aster.client import AsterClient
from core.lighter.client import LighterClient

CLIENTS = {"hyperliquid": HLClient, "arcus": ArcusClient, "lighter": LighterClient, "aster": AsterClient}
ADAPTERS = {"hyperliquid": Hyperliquid, "arcus": Arcus, "lighter": Lighter, "aster": Aster}

__all__ = ["Arcus", "Aster", "Hyperliquid", "Lighter", "CLIENTS", "ADAPTERS"]
