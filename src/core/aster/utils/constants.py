MAINNET_API_URL = "https://fapi.asterdex.com"
MAINNET_WS_URL = "wss://fstream.asterdex.com"
TESTNET_API_URL = "https://fapi.asterdex-testnet.com"
TESTNET_WS_URL = "wss://fstream.asterdex-testnet.com"
# chainId of the EIP-712 domain "AsterSignTransaction" (an off-chain signing id, not a deposit chain)
CHAIN_ID = {True: 1666, False: 714}

MAX_BATCH_PLACE = 5       # POST /fapi/v3/batchOrders
MAX_BATCH_MODIFY = 5      # PUT  /fapi/v3/batchOrders
MAX_BATCH_CANCEL = 10     # DELETE /fapi/v3/batchOrders, one symbol per request
LISTEN_KEEPALIVE = 30 * 60   # a listenKey lives 60 minutes after its last keepalive
MAX_STREAMS = 200            # per market data connection
CLIENT_ID = r"^[\.A-Z\:/a-z0-9_-]{1,36}$"

TIMEFRAMES = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
              "6h": 21600, "8h": 28800, "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800}

# commission assets counted 1:1 in dollars
STABLES = {"USDT", "USDC", "USD1", "USDF", "USD", "BUSD", "FDUSD"}
# "the order is not there (any more)": reported so that the order trackers drop it (strategies.orders.GONE)
GONE = (-2011, -2013)
# an answer that says nothing about what happened to the request: never turned into "refused"
UNKNOWN = (-1001, -1006, -1007)
