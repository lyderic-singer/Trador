"""An Aster futures exchange in memory, behind the REAL engine (core/aster) and adapter: only the HTTP call
(AsterApi.request) and the two websockets are replaced. It answers like the v3 API: an order object or
{"code", "msg"}, GTX expired when it would cross, IOC filled against the book or expired, batches answered element by
element, the user data stream pushing ORDER_TRADE_UPDATE / ACCOUNT_UPDATE."""
import asyncio
import json
import time
from core.aster.api import AsterError
from core.aster.client import AsterClient
from core.session import Session
from log.messenger import Messenger

KEY = "0x" + "11" * 32
USER = "0x00000000000000000000000000000000000000Aa"


def symbol(name: str, base: str, tick: str, step: str, min_qty: str, quote: str = "USDT", kind: str = "PERPETUAL") -> dict:
    return {"symbol": name, "pair": name, "contractType": kind, "status": "TRADING", "baseAsset": base, "quoteAsset": quote,
            "pricePrecision": 2, "quantityPrecision": 3, "underlyingType": "COIN" if base == "BTC" else "STOCK",
            "filters": [{"filterType": "PRICE_FILTER", "tickSize": tick, "minPrice": tick, "maxPrice": "1000000"},
                        {"filterType": "LOT_SIZE", "stepSize": step, "minQty": min_qty, "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.05", "multiplierDown": "0.95"},
                        {"filterType": "MAX_NUM_ORDERS", "limit": 200}]}


SYMBOLS = [symbol("BTCUSDT", "BTC", "0.1", "0.001", "0.001"), symbol("SPYUSDT", "SPY", "0.01", "0.01", "0.01"),
           symbol("SPYUSD1", "SPY", "0.01", "0.01", "0.01", quote="USD1"),
           symbol("BTCUSDT_261225", "BTC", "0.1", "0.001", "0.001", kind="CURRENT_QUARTER")]


class MarketSocket:
    def __init__(self):
        self.connected = asyncio.Event()
        self.connected.set()
        self.streams: dict[str, object] = {}
        self.disconnect_listeners: list = []
        self.connect_listeners: list = []
        self.refused: set[str] = set()
        self.sim = None

    async def start(self):
        pass

    async def stop(self):
        pass

    async def subscribe(self, stream, callback, timeout=10.0):
        if stream.split("@")[1] in self.refused:
            raise ValueError(f"subscription {stream} refused")
        fresh = stream not in self.streams
        self.streams.setdefault(stream, callback)
        if fresh and stream.endswith("@bookTicker") and self.sim is not None:
            await self.sim.quote(stream.split("@")[0].upper())      # the first quote comes right after the subscription

    async def push(self, stream: str, data: dict):
        await self.streams[stream](data)


class UserSocket:
    def __init__(self):
        self.connected = asyncio.Event()
        self.listeners: list = []
        self.disconnect_listeners: list = []
        self.connect_listeners: list = []
        self.running = False
        self.fail: Exception | None = None

    async def start(self):
        if self.fail:
            raise self.fail
        self.running = True
        self.connected.set()

    async def stop(self):
        self.running = False

    async def push(self, msg: dict):
        for listener in self.listeners:
            await listener(msg)


class AsterSim:
    def __init__(self, monkeypatch, key: bool = True):
        monkeypatch.setenv("ASTER_TESTNET_PRIVATE_KEY", KEY if key else "")
        monkeypatch.setenv("ASTER_TESTNET_ADDRESS", USER)
        for field in ("SIGNER", "SEND_USER", "API_URL", "WS_URL"):
            monkeypatch.delenv(f"ASTER_TESTNET_{field}", raising=False)
        self.messenger = Messenger(None)
        self.client = AsterClient(False, self.messenger, Session(self.messenger))
        self.client.api.request = self.request
        self.client.websocket = self.market = MarketSocket()
        self.market.sim = self
        self.client.user = self.user = UserSocket()
        self.client.running = True
        self.calls: list[tuple[str, str, dict]] = []
        self.orders: dict[int, dict] = {}
        self.next_order = 1000
        self.next_trade = 1
        self.book = {"BTCUSDT": (99990.0, 100010.0), "SPYUSDT": (699.90, 700.10)}
        self.wallet = 10_000.0
        self.positions: dict[str, dict] = {}
        self.leverage = {"BTCUSDT": 20, "SPYUSDT": 10}
        self.margin_type: dict[str, str] = {}
        self.trades: list[dict] = []
        self.countdowns: dict[str, int] = {}
        self.fail: list[Exception] = []
        self.hedge_mode = False
        self.klines: list[list] = []
        self.listen_keys: list[str] = []

    async def load(self):
        await self.client.info.start()

    # ---- HTTP ----

    async def request(self, verb, path, params=None, signed=False, items=1):
        params = {k: v for k, v in (params or {}).items() if v is not None}
        name = path.rsplit("/v3/", 1)[-1]
        self.calls.append((verb, name, dict(params)))
        if signed:
            assert self.client.signer is not None, "signed request without key"
        if self.fail and (verb != "GET" or signed):
            raise self.fail.pop(0)
        handler = getattr(self, f"{verb.lower()}_{name.replace('/', '_')}", None)
        if handler is None:
            raise AssertionError(f"unexpected {verb} {path}")
        return handler(params)

    def get_exchangeInfo(self, p):
        return {"rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
                               {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1200}],
                "symbols": SYMBOLS}

    def get_premiumIndex(self, p):
        rows = [{"symbol": s, "markPrice": str((b + a) / 2), "indexPrice": str((b + a) / 2 + 0.05), "lastFundingRate": "0.00040000",
                 "nextFundingTime": 1_800_000_000_000} for s, (b, a) in self.book.items()]
        return next(r for r in rows if r["symbol"] == p["symbol"]) if "symbol" in p else rows

    def get_fundingInfo(self, p):
        return [{"symbol": "SPYUSDT", "fundingIntervalHours": 4}, {"symbol": "BTCUSDT", "fundingIntervalHours": 8}]

    def get_ticker_24hr(self, p):
        return [{"symbol": "BTCUSDT", "quoteVolume": "5000000.5"}, {"symbol": "SPYUSDT", "quoteVolume": "1000.0"}]

    def get_time(self, p):
        return {"serverTime": int(time.time() * 1000)}

    def get_ticker_bookTicker(self, p):
        bid, ask = self.book[p["symbol"]]
        return {"symbol": p["symbol"], "bidPrice": str(bid), "bidQty": "5", "askPrice": str(ask), "askQty": "4"}

    def get_klines(self, p):
        return self.klines[-int(p["limit"]):]

    def get_fundingRate(self, p):
        return [{"symbol": p["symbol"], "fundingRate": "0.00040000", "fundingTime": 1_700_000_000_000},
                {"symbol": p["symbol"], "fundingRate": "-0.00020000", "fundingTime": 1_700_014_400_000}]

    def get_positionSide_dual(self, p):
        return {"dualSidePosition": self.hedge_mode}

    def get_commissionRate(self, p):
        return {"symbol": p["symbol"], "makerCommissionRate": "0.00005", "takerCommissionRate": "0.0004"}

    def get_leverageBrackets(self, p):
        return [{"symbol": p["symbol"], "brackets": [{"bracket": 2, "initialLeverage": 10, "notionalFloor": 50000, "maintMarginRatio": 0.02},
                                                     {"bracket": 1, "initialLeverage": 25, "notionalFloor": 0, "maintMarginRatio": 0.01}]}]

    def get_account(self, p):
        positions = []
        for s in self.book:
            pos = self.positions.get(s, {"size": 0.0, "entry": 0.0})
            mark = sum(self.book[s]) / 2
            positions.append({"symbol": s, "positionAmt": str(pos["size"]), "entryPrice": str(pos["entry"]), "leverage": str(self.leverage[s]),
                              "unrealizedProfit": str((mark - pos["entry"]) * pos["size"]), "isolated": self.margin_type.get(s) == "ISOLATED",
                              "notional": str(pos["size"] * mark), "initialMargin": str(abs(pos["size"]) * mark / self.leverage[s]),
                              "isolatedWallet": "12.5", "positionSide": "BOTH"})
        pnl = sum(float(x["unrealizedProfit"]) for x in positions)
        used = sum(float(x["initialMargin"]) for x in positions)
        return {"totalWalletBalance": str(self.wallet), "totalUnrealizedProfit": str(pnl), "totalMarginBalance": str(self.wallet + pnl),
                "totalInitialMargin": str(used), "availableBalance": str(self.wallet + pnl - used), "positions": positions}

    def get_positionRisk(self, p):
        return [{"symbol": s, "positionAmt": str(pos["size"]), "markPrice": str(sum(self.book[s]) / 2), "liquidationPrice": "650.5",
                 "marginType": "isolated" if self.margin_type.get(s) == "ISOLATED" else "cross", "positionSide": "BOTH"}
                for s, pos in self.positions.items()]

    def get_openOrders(self, p):
        return [dict(o) for o in self.orders.values() if p.get("symbol") in (None, o["symbol"])]

    def get_userTrades(self, p):
        return [t for t in self.trades if t["symbol"] == p["symbol"] and t["time"] >= int(p.get("startTime") or 0)]

    def get_income(self, p):
        return [{"symbol": "SPYUSDT", "incomeType": "FUNDING_FEE", "income": "-0.42", "asset": "USDT", "time": 1_700_000_000_000, "tranId": 1}]

    def post_listenKey(self, p):
        self.listen_keys.append(f"key{len(self.listen_keys) + 1}")
        return {"listenKey": self.listen_keys[-1]}

    def put_listenKey(self, p):
        return {}

    def post_leverage(self, p):
        self.leverage[p["symbol"]] = int(p["leverage"])
        return {"symbol": p["symbol"], "leverage": int(p["leverage"]), "maxNotionalValue": "1000000"}

    def post_marginType(self, p):
        if self.margin_type.get(p["symbol"], "CROSSED") == p["marginType"]:
            raise AsterError(400, -4046, "No need to change margin type.", "/fapi/v3/marginType")
        self.margin_type[p["symbol"]] = p["marginType"]
        return {"code": 200, "msg": "success"}

    def post_positionMargin(self, p):
        return {"code": 200, "amount": float(p["amount"]), "type": int(p["type"])}

    def post_countdownCancelAll(self, p):
        self.countdowns[p["symbol"]] = int(p["countdownTime"])
        return {"symbol": p["symbol"], "countdownTime": str(p["countdownTime"])}

    # ---- orders ----

    def later(self, coroutine):
        asyncio.get_running_loop().create_task(coroutine)

    def create(self, p: dict) -> dict:
        if p["symbol"] not in self.book:
            return {"code": -1121, "msg": "Invalid symbol."}
        if any(o.get("clientOrderId") == p.get("newClientOrderId") for o in self.orders.values()) and p.get("newClientOrderId"):
            return {"code": -4116, "msg": "ClientOrderId is duplicated."}
        bid, ask = self.book[p["symbol"]]
        price, qty, buy = float(p["price"]), float(p["quantity"]), p["side"] == "BUY"
        if p.get("reduceOnly") == "true" and not self.positions.get(p["symbol"], {}).get("size"):
            return {"code": -2022, "msg": "ReduceOnly Order is rejected."}
        if price * qty > self.wallet * self.leverage[p["symbol"]]:
            return {"code": -2019, "msg": "Margin is insufficient."}
        self.next_order += 1
        o = {"orderId": self.next_order, "symbol": p["symbol"], "status": "NEW", "clientOrderId": p.get("newClientOrderId") or f"auto{self.next_order}",
             "price": p["price"], "avgPrice": "0", "origQty": p["quantity"], "executedQty": "0", "cumQuote": "0",
             "timeInForce": p["timeInForce"], "type": "LIMIT", "reduceOnly": p.get("reduceOnly") == "true", "side": p["side"],
             "positionSide": "BOTH", "updateTime": int(time.time() * 1000)}
        crosses = price >= ask if buy else price <= bid
        if p["timeInForce"] == "GTX" and crosses:
            o["status"] = "EXPIRED"
            self.later(self.push_order(o, "EXPIRED"))
            return o
        if p["timeInForce"] == "IOC":
            if not crosses:
                o["status"] = "EXPIRED"
                self.later(self.push_order(o, "EXPIRED"))
                return o
            self.orders[o["orderId"]] = o
            self.execute(o, qty, ask if buy else bid, maker=False)
            return dict(o)
        self.orders[o["orderId"]] = o
        self.later(self.push_order(o, "NEW"))
        return dict(o)

    def lookup(self, p: dict) -> dict | None:
        if p.get("orderId") is not None:
            return self.orders.get(int(p["orderId"]))
        return next((o for o in self.orders.values() if o["clientOrderId"] == p.get("origClientOrderId")), None)

    def amend(self, p: dict) -> dict:
        o = self.lookup(p)
        if o is None:
            return {"code": -2013, "msg": "Order does not exist."}
        if float(p["quantity"]) <= float(o["executedQty"]):
            return {"code": -5027, "msg": "No need to modify the order."}
        o["price"], o["origQty"] = p["price"], p["quantity"]
        self.later(self.push_order(o, "AMENDMENT"))
        return dict(o)

    def remove(self, p: dict) -> dict:
        o = self.lookup(p)
        if o is None:
            return {"code": -2011, "msg": "Unknown order sent."}
        del self.orders[o["orderId"]]
        o["status"] = "CANCELED"
        self.later(self.push_order(o, "CANCELED"))
        return dict(o)

    @staticmethod
    def single(row: dict, path: str) -> dict:
        if "code" in row and "orderId" not in row:
            raise AsterError(400, row["code"], row["msg"], path)
        return row

    def post_order(self, p):
        return self.single(self.create(p), "/fapi/v3/order")

    def put_order(self, p):
        return self.single(self.amend(p), "/fapi/v3/order")

    def delete_order(self, p):
        return self.single(self.remove(p), "/fapi/v3/order")

    def post_batchOrders(self, p):
        rows = json.loads(p["batchOrders"])
        assert len(rows) <= 5, "more than 5 orders in a batch"
        return [self.create(r) for r in rows]

    def put_batchOrders(self, p):
        rows = json.loads(p["batchOrders"])
        assert len(rows) <= 5, "more than 5 modifies in a batch"
        return [self.amend(r) for r in rows]

    def delete_batchOrders(self, p):
        ids = json.loads(p.get("orderIdList") or "[]")
        clients = json.loads(p.get("origClientOrderIdList") or "[]")
        assert len(ids) + len(clients) <= 10, "more than 10 cancels in a batch"
        return [self.remove({"orderId": i}) for i in ids] + [self.remove({"origClientOrderId": c}) for c in clients]

    def delete_allOpenOrders(self, p):
        for o in [o for o in self.orders.values() if o["symbol"] == p["symbol"]]:
            self.remove({"orderId": o["orderId"]})
        return {"code": 200, "msg": "The operation of cancel all open order is done."}

    # ---- matching and pushes ----

    def execute(self, o: dict, size: float, price: float, maker: bool) -> dict:
        executed = float(o["executedQty"]) + size
        quote = float(o["cumQuote"]) + size * price
        o["executedQty"], o["cumQuote"], o["avgPrice"] = f"{executed:g}", f"{quote:g}", f"{quote / executed:g}"
        done = executed >= float(o["origQty"]) - 1e-12
        o["status"] = "FILLED" if done else ("EXPIRED" if o["timeInForce"] == "IOC" else "PARTIALLY_FILLED")
        if o["status"] in ("FILLED", "EXPIRED"):
            self.orders.pop(o["orderId"], None)
        signed = size if o["side"] == "BUY" else -size
        pos = self.positions.setdefault(o["symbol"], {"size": 0.0, "entry": 0.0})
        pos["size"], pos["entry"] = round(pos["size"] + signed, 8), price
        trade = {"symbol": o["symbol"], "id": self.next_trade, "orderId": o["orderId"], "side": o["side"], "price": str(price),
                 "qty": str(size), "commission": str(size * price * (0.00005 if maker else 0.0004)), "commissionAsset": "USDT",
                 "maker": maker, "time": int(time.time() * 1000) + self.next_trade}
        self.next_trade += 1
        self.trades.append(trade)
        self.later(self.push_order(o, "TRADE", trade))
        self.later(self.user.push({"e": "ACCOUNT_UPDATE", "E": trade["time"], "a": {"m": "ORDER", "B": [], "P": []}}))
        return trade

    async def fill(self, order_id: int, size: float | None = None):
        o = self.orders[order_id]
        size = float(o["origQty"]) - float(o["executedQty"]) if size is None else size
        trade = self.execute(o, size, float(o["price"]), maker=True)
        for _ in range(4):
            await asyncio.sleep(0)
        return trade

    async def push_order(self, o: dict, execution: str, trade: dict | None = None):
        event = {"s": o["symbol"], "c": o["clientOrderId"], "S": o["side"], "o": "LIMIT", "f": o["timeInForce"], "q": o["origQty"],
                 "p": o["price"], "ap": o["avgPrice"], "x": execution, "X": o["status"], "i": o["orderId"], "z": o["executedQty"],
                 "l": trade["qty"] if trade else "0", "L": trade["price"] if trade else "0", "T": int(time.time() * 1000),
                 "t": trade["id"] if trade else 0, "m": bool(trade and trade["maker"]), "R": o["reduceOnly"], "ps": "BOTH"}
        if trade:
            event.update(n=trade["commission"], N=trade["commissionAsset"])
        await self.user.push({"e": "ORDER_TRADE_UPDATE", "E": event["T"], "T": event["T"], "o": event})

    async def quote(self, name: str, bid: float | None = None, ask: float | None = None):
        if bid is not None and ask is not None:
            self.book[name] = (bid, ask)
        bid, ask = self.book[name]
        stream = f"{name.lower()}@bookTicker"
        if stream in self.market.streams:
            await self.market.push(stream, {"e": "bookTicker", "s": name, "b": str(bid), "B": "5", "a": str(ask), "A": "4",
                                            "T": int(time.time() * 1000), "E": int(time.time() * 1000)})
