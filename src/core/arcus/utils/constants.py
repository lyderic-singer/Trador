MAINNET_API_URL = "https://api.arcus.xyz"
MAINNET_WS_URL = "wss://api.arcus.xyz/v1/ws"
TESTNET_API_URL = "https://api.testnet.arcus.xyz"
TESTNET_WS_URL = "wss://api.testnet.arcus.xyz/v1/ws"

PAYLOAD_VERSION = 1
OP_PLACE = 1
OP_CANCEL = 2
OP_MODIFY = 3

SIDE = {"BUY": 0, "SELL": 1}
TIF = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}

EXPIRY_DAYS = 40
MAX_BATCH = 100

TIMEFRAMES = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200, "1w": 604800,
}

ACCEPTED = {"ACK", "PENDING", "OPEN", "PARTIALLY_FILLED", "FILLED", "CANCEL_ACKNOWLEDGED", "CANCELED", "APPLIED"}
