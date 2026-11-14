MAINNET_API_URL = "https://mainnet.zklighter.elliot.ai"
MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
TESTNET_API_URL = "https://testnet.zklighter.elliot.ai"
TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"
CHAIN_ID = {True: 304, False: 300}

# transaction types (docs: Data Structures, Constants and Errors)
TX_CREATE_ORDER = 14
TX_CANCEL_ORDER = 15
TX_CANCEL_ALL_ORDERS = 16
TX_MODIFY_ORDER = 17
TX_UPDATE_LEVERAGE = 20
TX_UPDATE_MARGIN = 29
# transactions that use one unit of the volume quota (a cancel is free)
QUOTA = {TX_CREATE_ORDER, TX_CANCEL_ALL_ORDERS, TX_MODIFY_ORDER}

ORDER_LIMIT = 0
TIF_IOC = 0
TIF_GTT = 1
TIF_POST_ONLY = 2
CANCEL_ALL_IMMEDIATE = 0
CANCEL_ALL_SCHEDULED = 1
CANCEL_ALL_ABORT = 2
ALL_MARKETS = 255
MARGIN_CROSS = 0
MARGIN_ISOLATED = 1

MAX_CLIENT_INDEX = 2**48 - 1     # clientOrderIndex is a uint48; exchange order indexes are above 2**48
MAX_PRICE = 2**32 - 1            # the integer price of a transaction is a uint32
EXPIRY_DAYS = 28                 # an order must expire between 5 minutes and 30 days ahead (0 for an IOC)
MAX_BATCH_WS = 50                # jsonapi/sendtxbatch (error 21514 beyond)
MAX_BATCH_REST = 15              # larger REST batches may be refused at the CDN edge (docs: signing transactions)
AUTH_TTL = 7 * 3600              # an auth token lives 8 h at most
AUTH_REFRESH = 6 * 3600
USDC_SCALE = 1_000_000

# error codes (HTTP body / websocket "error")
INVALID_NONCE = (21104, 21105)
RATE_LIMITED = (23000, 23001, 23002, 23003, 30009)

TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "12h": 43200, "1d": 86400}

# order status of the REST / websocket Order object -> standard status
OPEN = ("in-progress", "pending", "open")
# cancelled by the engine when the order came in (never rested): a refusal, not a cancel
REFUSED = ("canceled-post-only", "canceled-margin-not-allowed", "canceled-position-not-allowed", "canceled-invalid-balance")
