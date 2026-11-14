from adapters.arcus import Arcus
from adapters.hyperliquid import Hyperliquid
from adapters.lighter import Lighter
from core.HL.client import HLClient
from core.arcus.client import ArcusClient
from core.lighter.client import LighterClient

CLIENTS = {"hyperliquid": HLClient, "arcus": ArcusClient, "lighter": LighterClient}
ADAPTERS = {"hyperliquid": Hyperliquid, "arcus": Arcus, "lighter": Lighter}

__all__ = ["Arcus", "Hyperliquid", "Lighter", "CLIENTS", "ADAPTERS"]
