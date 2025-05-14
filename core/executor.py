from hyperliquid.utils import constants
import example_utils

address, info, exchange = example_utils.setup(base_url=constants.TESTNET_API_URL, skip_ws=True)
meta = info.meta()
sz_decimals = {a["name"]: a["szDecimals"] for a in meta["universe"]}

class Executor:
    def place_limit(self, coin, is_buy, size, price):
        result = exchange.order(
            coin,
            is_buy,
            round(size, sz_decimals[coin]),
            round(price, 6),
            {"limit": {"tif": "Alo"}}
        )
        if result.get("status") != "ok":
            print(f"❌ Order error: {result.get('error', 'Unknown error')}")
        else:
            print(f"✅ {'Buy' if is_buy else 'Sell'} @ {price:.2f}")