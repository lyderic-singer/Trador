"""A Lighter exchange in memory, behind the REAL engine (core/lighter) and adapter: the HTTP layer (LighterApi.get /
post), the websocket and the signer are replaced, everything above them is the production code. The simulator applies
what the docs describe: one nonce per API key (a wrong one is refused without being used), a 200 that only means
"accepted", the outcome pushed afterwards on the account channels, post-only cancelled when it would cross, IOC filled
against the book or cancelled, client order indexes on 48 bits."""
import asyncio
import json
import time
from core.lighter.api import LighterError
from core.lighter.client import LighterClient
from core.lighter.utils.constants import (CANCEL_ALL_IMMEDIATE, TIF_IOC, TIF_POST_ONLY, TX_CANCEL_ALL_ORDERS,
                                          TX_CANCEL_ORDER, TX_CREATE_ORDER, TX_MODIFY_ORDER, TX_UPDATE_LEVERAGE,
                                          TX_UPDATE_MARGIN)
from core.lighter.utils.types import Tx
from core.lighter.websocket import LighterWebSocket
from core.session import Session
from log.messenger import Messenger

ACCOUNT = 4242
MARKETS = [
    {"symbol": "BTC", "market_id": 0, "market_type": "perp", "status": "active", "price_decimals": 1, "size_decimals": 5,
     "min_base_amount": "0.00020", "min_quote_amount": "10.000000", "default_initial_margin_fraction": 500,
     "min_initial_margin_fraction": 200, "maintenance_margin_fraction": 120, "last_trade_price": 100000.0,
     "daily_quote_token_volume": 9e8},
    {"symbol": "SPY", "market_id": 7, "market_type": "perp", "status": "active", "price_decimals": 2, "size_decimals": 2,
     "min_base_amount": "0.02", "min_quote_amount": "10.000000", "default_initial_margin_fraction": 1000,
     "min_initial_margin_fraction": 1000, "maintenance_margin_fraction": 600, "last_trade_price": 700.0,
     "daily_quote_token_volume": 5e6},
    {"symbol": "ETH/USDC", "market_id": 2048, "market_type": "spot", "status": "active", "price_decimals": 2, "size_decimals": 4},
]


class FakeSigner:
    """signs nothing: a transaction is its own description (the simulator reads it back)"""

    def __init__(self):
        self.calls: list[dict] = []
        self.tokens = 0

    def tx(self, kind: int, ref: int | None = None, **fields) -> Tx:
        self.calls.append({"type": kind, **fields})
        return Tx(kind, json.dumps(fields), f"hash-{fields['key']}-{fields['nonce']}", ref)

    def create_order(self, key, nonce, market, client, base, price, is_ask, tif, reduce, expiry):
        return self.tx(TX_CREATE_ORDER, client, key=key, nonce=nonce, market=market, client=client, base=base, price=price,
                       is_ask=bool(is_ask), tif=tif, reduce=bool(reduce), expiry=expiry)

    def modify_order(self, key, nonce, market, index, base, price):
        return self.tx(TX_MODIFY_ORDER, index, key=key, nonce=nonce, market=market, index=index, base=base, price=price)

    def cancel_order(self, key, nonce, market, index):
        return self.tx(TX_CANCEL_ORDER, index, key=key, nonce=nonce, market=market, index=index)

    def cancel_all(self, key, nonce, tif, at_ms):
        return self.tx(TX_CANCEL_ALL_ORDERS, key=key, nonce=nonce, tif=tif, at=at_ms)

    def update_leverage(self, key, nonce, market, fraction, mode):
        return self.tx(TX_UPDATE_LEVERAGE, key=key, nonce=nonce, market=market, fraction=fraction, mode=mode)

    def update_margin(self, key, nonce, market, usdc, direction):
        return self.tx(TX_UPDATE_MARGIN, key=key, nonce=nonce, market=market, usdc=usdc, direction=direction)

    def auth(self, key, ttl):
        self.tokens += 1
        return f"token-{key}-{self.tokens}"

    def check(self):
        return None


class FakeSocket:
    """the surface of LighterWebSocket the engine and the adapter use"""
    parse = staticmethod(LighterWebSocket.parse)

    def __init__(self, sim):
        self.sim = sim
        self.connected = asyncio.Event()
        self.connected.set()
        self.subscriptions: dict[str, object] = {}
        self.callbacks: dict[str, object] = {}
        self.private: dict[str, bool] = {}
        self.connect_listeners: list = []
        self.disconnect_listeners: list = []
        self.token = None
        self.frames: list[tuple[str, dict]] = []
        self.refused: set[str] = set()
        self.refreshed = 0

    async def start(self):
        pass

    async def stop(self):
        pass

    async def subscribe(self, channel, callback, private=False, timeout=15):
        key = channel.replace("/", ":")
        if channel.split("/")[0] in self.refused:
            raise ValueError(f"subscription {channel} refused")
        if key in self.callbacks:
            return
        if private:
            assert self.token and self.token(), "a private channel needs the auth token"
        self.callbacks[key] = callback
        self.subscriptions[key] = callback
        self.private[key] = private
        await self.sim.snapshot(channel)

    async def unsubscribe(self, channel):
        key = channel.replace("/", ":")
        self.callbacks.pop(key, None)
        self.subscriptions.pop(key, None)

    async def refresh(self):
        self.refreshed += 1

    async def push(self, channel: str, msg: dict, kind: str = "update"):
        key = channel.replace("/", ":")
        callback = self.callbacks.get(key)
        if callback is not None:
            await callback({"type": f"{kind}/{channel.split('/')[0]}", "channel": key, **msg})

    async def post(self, kind: str, data: dict):
        if not self.connected.is_set():
            raise ConnectionError("lighter websocket not connected")
        self.frames.append((kind, data))
        future = asyncio.get_running_loop().create_future()
        if kind == "jsonapi/sendtx":
            types, infos = [data["tx_type"]], [json.dumps(data["tx_info"])]
        else:
            types, infos = json.loads(data["tx_types"]), json.loads(data["tx_infos"])
        if self.sim.silent:
            return future                       # the reply never comes
        try:
            body = self.sim.send(types, infos)
            future.set_result({"type": kind, "data": {"id": "1", **body}})
        except LighterError as e:
            future.set_result({"type": "error", "error": {"code": e.code, "message": e.message}})
        return future


class LighterSim:
    def __init__(self, monkeypatch, taker_key: bool = False, tier: str = "premium", account: int | None = ACCOUNT):
        for field, value in (("ACCOUNT", str(account) if account is not None else ""), ("API_KEY", "5"), ("PRIVATE_KEY", "ab" * 40),
                             ("TAKER_API_KEY", "6" if taker_key else ""), ("TAKER_PRIVATE_KEY", "cd" * 40 if taker_key else "")):
            monkeypatch.setenv(f"LIGHTER_TESTNET_{field}", value)
        self.messenger = Messenger(None)
        self.signer = FakeSigner()
        self.client = LighterClient(False, self.messenger, Session(self.messenger), signer=self.signer)
        self.client.api.get = self.get
        self.client.api.post = self.post
        self.ws = FakeSocket(self)
        self.ws.token = self.client.token
        self.client.websocket = self.client.exchange.ws = self.ws
        self.client.running = True
        self.client.tier = tier
        self.tier = tier
        self.quota = 1000
        self.nonces = {5: 10, 6: 100}
        self.orders: dict[int, dict] = {}        # order_index -> row
        self.next_order = 2**48 + 1000
        self.next_trade = 1
        self.book = {0: (99990.0, 100010.0), 7: (699.90, 700.10)}   # market -> (best bid, best ask)
        self.positions: dict[int, dict] = {}
        self.collateral = 10_000.0
        self.trades: list[dict] = []
        self.requests: list[tuple[str, dict]] = []
        self.posts: list[tuple[str, dict]] = []
        self.fail: list[Exception] = []          # raised by the next POSTs, in order
        self.silent = False                      # websocket replies never come
        self.drop = False                        # transactions are accepted (200) but never executed
        self.cancel_at: int | None = None        # scheduled cancel-all (ms)
        self.leverage: dict[int, tuple[int, int]] = {}
        self.margin: list[tuple[int, int]] = []
        self.candles: list[dict] = []
        self.funding = {"funding_rate": "0.0008", "current_funding_rate": "0.0012", "index_price": "700.05", "mark_price": "700.02"}

    async def load(self):
        await self.client.info.load_markets()

    # ---- REST ----

    async def get(self, path, params=None, auth=False):
        params = params or {}
        self.requests.append((path, dict(params)))
        if auth:
            assert self.client.token(), "auth endpoint without token"
        name = path.rsplit("/", 1)[-1]
        if name == "orderBookDetails":
            return {"code": 200, "order_book_details": [m for m in MARKETS if m["market_type"] == "perp"],
                    "spot_order_book_details": [m for m in MARKETS if m["market_type"] == "spot"]}
        if name == "nextNonce":
            return {"code": 200, "nonce": self.nonces[int(params["api_key_index"])]}
        if name == "accountLimits":
            return {"code": 200, "user_tier": self.tier, "volume_quota_remaining": self.quota}
        if name == "account":
            return {"code": 200, "accounts": [self.account()]}
        if name == "accountActiveOrders":
            market = params.get("market_id")
            return {"code": 200, "orders": [dict(o) for o in self.orders.values()
                                            if market in (None, 255) or o["market_index"] == int(market)]}
        if name == "trades":
            since = int(params.get("from") or 0)
            return {"code": 200, "trades": [t for t in reversed(self.trades) if t["timestamp"] > since]}
        if name == "candles":
            return {"code": 200, "r": params["resolution"], "c": self.candles[-int(params["count_back"]):]}
        if name == "orderBookOrders":
            bid, ask = self.book[int(params["market_id"])]
            return {"code": 200, "bids": [{"price": str(bid), "remaining_base_amount": "5"}],
                    "asks": [{"price": str(ask), "remaining_base_amount": "4"}]}
        if name == "fundings":
            return {"code": 200, "fundings": [{"timestamp": 1_700_000_000, "value": "0.1", "rate": "0.0010", "direction": "long"},
                                              {"timestamp": 1_700_003_600, "value": "0.1", "rate": "0.0020", "direction": "short"}]}
        if name == "positionFunding":
            return {"code": 200, "position_fundings": [{"timestamp": 1_700_000_000, "market_id": 7, "change": "-0.42",
                                                        "rate": "0.0010", "position_size": "10", "position_side": "long"}]}
        raise AssertionError(f"unexpected GET {path}")

    async def post(self, path, form, auth=False, items=1):
        self.posts.append((path, dict(form)))
        if self.fail:
            raise self.fail.pop(0)
        if path.endswith("sendTx"):
            return self.send([int(form["tx_type"])], [form["tx_info"]])
        if path.endswith("sendTxBatch"):
            return self.send(json.loads(form["tx_types"]), json.loads(form["tx_infos"]))
        raise AssertionError(f"unexpected POST {path}")

    def account(self) -> dict:
        positions = [dict(p) for p in self.positions.values()]
        pnl = sum(float(p["unrealized_pnl"]) for p in positions)
        used = sum(float(p["position_value"]) * 0.1 for p in positions)
        return {"account_index": ACCOUNT, "collateral": f"{self.collateral:.6f}", "total_asset_value": f"{self.collateral + pnl:.6f}",
                "available_balance": f"{self.collateral + pnl - used:.6f}", "positions": positions}

    # ---- sequencer ----

    def send(self, types: list[int], infos: list[str]) -> dict:
        txs = [(t, json.loads(i)) for t, i in zip(types, infos)]
        key = txs[0][1]["key"]
        for n, (_, tx) in enumerate(txs):          # the API refuses the whole request on a wrong nonce
            if tx["nonce"] != self.nonces[key] + n:
                raise LighterError(400, 21104, "invalid nonce", "sendTx")
        self.nonces[key] += len(txs)
        used = sum(1 for t, _ in txs if t in (TX_CREATE_ORDER, TX_MODIFY_ORDER, TX_CANCEL_ALL_ORDERS))
        self.quota -= used
        if not self.drop:
            for t, tx in txs:
                self.apply(t, tx)
        else:
            self.nonces[key] -= len(txs)           # dropped before execution: the nonces were never used
        return {"code": 200, "tx_hash": [f"hash-{tx['key']}-{tx['nonce']}" for _, tx in txs] if len(txs) > 1
                else f"hash-{key}-{txs[0][1]['nonce']}", "predicted_execution_time_ms": int(time.time() * 1000) + 5,
                "volume_quota_remaining": self.quota}

    def later(self, coroutine):
        asyncio.get_running_loop().create_task(coroutine)

    def scale(self, market: int) -> tuple[float, float]:
        m = next(x for x in MARKETS if x["market_id"] == market)
        return 10 ** -m["price_decimals"], 10 ** -m["size_decimals"]

    def find(self, index: int) -> dict | None:
        return self.orders.get(index) or next((o for o in self.orders.values() if o["client_order_index"] == index), None)

    def row(self, tx: dict, status: str) -> dict:
        tick, step = self.scale(tx["market"])
        self.next_order += 1
        size = tx["base"] * step
        return {"order_index": self.next_order, "client_order_index": tx["client"], "market_index": tx["market"],
                "owner_account_index": ACCOUNT, "initial_base_amount": f"{size:.5f}", "remaining_base_amount": f"{size:.5f}",
                "filled_base_amount": "0", "filled_quote_amount": "0", "price": f"{tx['price'] * tick:.2f}",
                "is_ask": tx["is_ask"], "reduce_only": tx["reduce"], "status": status, "type": "limit",
                "time_in_force": {TIF_IOC: "immediate-or-cancel", TIF_POST_ONLY: "post-only"}.get(tx["tif"], "good-till-time"),
                "order_expiry": tx["expiry"], "timestamp": int(time.time())}

    def apply(self, kind: int, tx: dict):
        if kind == TX_CREATE_ORDER:
            row = self.row(tx, "open")
            bid, ask = self.book[tx["market"]]
            price, crosses = float(row["price"]), False
            crosses = price <= bid if tx["is_ask"] else price >= ask
            if tx["tif"] == TIF_POST_ONLY and crosses:
                row["status"] = "canceled-post-only"
                return self.later(self.push_order(row))
            if tx["tif"] == TIF_IOC:
                if not crosses:
                    row["status"] = "canceled-too-much-slippage"
                    return self.later(self.push_order(row))
                self.orders[row["order_index"]] = row
                return self.later(self.fill(row["order_index"], None, bid if tx["is_ask"] else ask, maker=False))
            self.orders[row["order_index"]] = row
            self.later(self.push_order(row))
        elif kind == TX_CANCEL_ORDER:
            row = self.find(tx["index"])
            if row:                                   # an unknown order is a no-op
                del self.orders[row["order_index"]]
                row["status"] = "canceled"
                self.later(self.push_order(row))
        elif kind == TX_MODIFY_ORDER:
            row = self.find(tx["index"])
            if row:
                tick, step = self.scale(tx["market"])
                row["price"] = f"{tx['price'] * tick:.2f}"
                row["initial_base_amount"] = row["remaining_base_amount"] = f"{tx['base'] * step:.5f}"
                self.later(self.push_order(row))
        elif kind == TX_CANCEL_ALL_ORDERS:
            if tx["tif"] == CANCEL_ALL_IMMEDIATE:
                for row in list(self.orders.values()):
                    del self.orders[row["order_index"]]
                    row["status"] = "canceled"
                    self.later(self.push_order(row))
            else:
                self.cancel_at = tx["at"] or None
        elif kind == TX_UPDATE_LEVERAGE:
            self.leverage[tx["market"]] = (tx["fraction"], tx["mode"])
        elif kind == TX_UPDATE_MARGIN:
            self.margin.append((tx["market"], tx["usdc"]))

    # ---- websocket pushes ----

    async def snapshot(self, channel: str):
        name = channel.split("/")[0]
        if name == "account_all_orders":
            orders: dict[str, list] = {}
            for o in self.orders.values():
                orders.setdefault(str(o["market_index"]), []).append(dict(o))
            await self.ws.push(channel, {"orders": orders}, "subscribed")
        elif name == "account_all_trades":
            await self.ws.push(channel, {"trades": {}, "daily_volume": 1000.5, "weekly_volume": 7000.0,
                                         "monthly_volume": 30000.0, "total_volume": 123456.0}, "subscribed")
        elif name == "account_all_positions":
            await self.ws.push(channel, {"positions": {str(k): dict(v) for k, v in self.positions.items()}}, "subscribed")
        elif name == "user_stats":
            a = self.account()
            await self.ws.push(channel, {"stats": {"collateral": a["collateral"], "portfolio_value": a["total_asset_value"],
                                                   "available_balance": a["available_balance"]}}, "subscribed")
        elif name == "market_stats":
            market = int(channel.split("/")[1])
            await self.ws.push(channel, {"market_stats": {"market_id": market, **self.funding,
                                                          "funding_timestamp": int(time.time() * 1000)}}, "subscribed")
        elif name == "ticker":
            await self.quote(int(channel.split("/")[1]))

    async def quote(self, market: int, bid: float | None = None, ask: float | None = None):
        if bid is not None and ask is not None:
            self.book[market] = (bid, ask)
        bid, ask = self.book[market]
        await self.ws.push(f"ticker/{market}", {"ticker": {"s": "x", "a": {"price": str(ask), "size": "4.00"},
                                                           "b": {"price": str(bid), "size": "5.00"},
                                                           "last_updated_at": int(time.time() * 1e6)}})

    async def push_order(self, row: dict):
        await self.ws.push(f"account_all_orders/{ACCOUNT}", {"orders": {str(row["market_index"]): [dict(row)]}})

    async def fill(self, order_index: int, size: float | None = None, price: float | None = None, maker: bool = True):
        """fills an order of the account against an outside account, pushes the trade, the order and the position"""
        row = self.orders[order_index]
        remaining = float(row["remaining_base_amount"])
        size = remaining if size is None else min(size, remaining)
        price = float(row["price"]) if price is None else price
        filled = float(row["filled_base_amount"]) + size
        row["filled_base_amount"], row["remaining_base_amount"] = f"{filled:.5f}", f"{remaining - size:.5f}"
        row["filled_quote_amount"] = f"{float(row['filled_quote_amount']) + size * price:.6f}"
        done = remaining - size <= 1e-12
        if done or row["time_in_force"] == "immediate-or-cancel":
            row["status"] = "filled" if done else "canceled-not-enough-liquidity"
            del self.orders[order_index]
        ask = row["is_ask"]
        trade = {"trade_id": self.next_trade, "market_id": row["market_index"], "size": f"{size:.5f}", "price": f"{price:.2f}",
                 "usd_amount": f"{size * price:.6f}", "ask_id": order_index if ask else 999, "bid_id": 999 if ask else order_index,
                 "ask_client_id": row["client_order_index"] if ask else 0, "bid_client_id": 0 if ask else row["client_order_index"],
                 "ask_account_id": ACCOUNT if ask else 1, "bid_account_id": 1 if ask else ACCOUNT,
                 "is_maker_ask": ask if maker else not ask, "timestamp": int(time.time() * 1000) + self.next_trade,
                 "maker_fee": 40, "taker_fee": 280}
        self.next_trade += 1
        self.trades.append(trade)
        market = row["market_index"]
        p = self.positions.get(market) or {"market_id": market, "sign": 1, "position": "0", "avg_entry_price": "0",
                                           "position_value": "0", "unrealized_pnl": "0", "initial_margin_fraction": "10.00",
                                           "margin_mode": 0, "allocated_margin": "0", "liquidation_price": "0"}
        signed = float(p["position"]) * p["sign"] + (-size if ask else size)
        p.update(sign=-1 if signed < 0 else 1, position=f"{abs(signed):.5f}", avg_entry_price=f"{price:.2f}",
                 position_value=f"{abs(signed) * price:.6f}")
        self.positions[market] = p
        await self.ws.push(f"account_all_trades/{ACCOUNT}", {"trades": {str(market): [trade]}})
        await self.push_order(row)
        await self.ws.push(f"account_all_positions/{ACCOUNT}", {"positions": {str(market): dict(p)}})
        return trade
