from adapters.arcus import Arcus
from adapters.hyperliquid import Hyperliquid
from core.HL.client import HLClient
from core.arcus.client import ArcusClient

CLIENTS = {"hyperliquid": HLClient, "arcus": ArcusClient}
ADAPTERS = {"hyperliquid": Hyperliquid, "arcus": Arcus}

__all__ = ["Arcus", "Hyperliquid", "CLIENTS", "ADAPTERS"]
