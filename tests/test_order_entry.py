"""websocket order entry (Arcus `post`, Hyperliquid `post`) against local fake servers: envelope, signatures, reply
parsing, error mapping, strictly increasing ids under concurrency, REST fallback when the socket is down, latency
samples"""
import asyncio
import json
from decimal import Decimal

import websockets
from cryptography.hazmat.primitives.asymmetric import ed25519

from core.arcus.api import ArcusError
from core.arcus.exchange import ArcusExchange
from core.arcus.utils.signing import Signer, canonical, place_payload
from core.arcus.utils.types import ArcusCancel, ArcusOrder, Market
from core.arcus.websocket import ArcusWebSocket
from core.latency import LATENCY, percentile
from log.logger import Logger
from log.messenger import Messenger

KEY = "11" * 32
ADDRESS = "0xabcdef0000000000000000000000000000000001"
MARKET = {"marketId": 23, "marketDisplayName": "SPY-USD", "baseAsset": "SPY", "quoteAsset": "USD", "tickSize": "0.01",
          "stepSize": "0.0001", "minOrderSize": "0.0001", "minOrderNotional": "10", "status": "ONLINE",
          "initialMarginFraction": "0.02"}


def verify(public: str, message: str, signature: str):
    ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(public)).verify(bytes.fromhex(signature), message.encode())


def exchange(ws=None, api=None) -> ArcusExchange:
    ex = ArcusExchange.__new__(ArcusExchange)
    ex._log = Logger("t", Messenger(None))
    ex.signer, ex.address, ex.index = Signer(KEY), ADDRESS, 0
    ex.transport, ex.ws, ex.timeout, ex.clock, ex.shapes = "ws", ws, 2.0, 0, set()
    ex.api = api
    return ex


async def arcus_server(handler):
    seen = []

    async def serve(sock):
        async for raw in sock:
            msg = json.loads(raw)
            seen.append(msg)
            reply = await handler(msg)
            if reply is not None:
                await sock.send(json.dumps(reply))
    server = await websockets.serve(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}", seen


def test_arcus_ws_orders():
    async def go():
        async def handler(msg):
            if msg.get("type") != "post":
                return None
            req = msg["request"]
            items = req["payload"].get("orders") or req["payload"].get("cancels") or []
            if any(str(o.get("price")) == "1.00" or o.get("price") == "1" for o in items):
                return {"method": req["type"], "id": msg["id"], "status": 429,
                        "error": {"type": "RateLimited", "message": "slow down", "retryAfterMs": 1500}}
            return {"method": req["type"], "id": msg["id"], "status": 202,
                    "result": {"responses": [{"orderId": f"o{msg['id']}-{i}", "status": "ACK"} for i in range(len(items))]}}
        server, url, seen = await arcus_server(handler)
        ws = ArcusWebSocket(url, Messenger(None))
        await ws.start()
        rest = []

        class Api:
            async def post(self, path, body, headers, params, items=1):
                rest.append(path)
                return {"responses": [{"orderId": "rest", "status": "ACK"}]}
        ex = exchange(ws, Api())
        m = Market(MARKET)
        res = await ex.batch_place([ArcusOrder(m, True, Decimal("1.3"), Decimal("770.01"), "ALO", client="0x" + "ab" * 16)])
        assert res["responses"][0]["status"] == "ACK" and not rest
        msg = seen[-1]
        req = msg["request"]
        assert msg["type"] == "post" and req["type"] == "batchPlaceOrders" and req["apiKey"] == Signer(KEY).public
        element = req["payload"]["orders"][0]
        ts = int(req["timestamp"])
        assert element["timestamp"] == ts and req["signature"] == element["signature"]
        verify(Signer(KEY).public, canonical(place_payload(ADDRESS, 0, ts, int(element["goodTilTime"]) * 1000, 23, 77001,
                                                           13000, False, 0, 3, "0x" + "ab" * 16)), element["signature"])
        # cancel by client id, before any acknowledgement
        res = await ex.batch_cancel([ArcusCancel(m, client="0x" + "cd" * 16)])
        assert seen[-1]["request"]["type"] == "batchCancelOrders"
        assert seen[-1]["request"]["payload"]["cancels"][0]["kind"] == "clientId"
        # a 429 reply raises like REST, with the retry hint
        try:
            await ex.batch_place([ArcusOrder(m, True, Decimal("20"), Decimal("1.00"), "ALO")])
            raise AssertionError("should raise")
        except ArcusError as e:
            assert e.status == 429 and e.retry == 1500
        # 40 concurrent requests: the server sees strictly increasing ids
        before = len(seen)
        await asyncio.gather(*(ex.batch_place([ArcusOrder(m, i % 2 == 0, Decimal("1"), Decimal("770.01"), "ALO")])
                               for i in range(40)))
        ids = [x["id"] for x in seen[before:]]
        assert len(ids) == 40 and ids == sorted(ids) and len(set(ids)) == 40
        stats = LATENCY.stats("arcus")
        assert ("arcus", "batchPlaceOrders", "ws") in stats and stats[("arcus", "batchPlaceOrders", "ws")]["errors"] >= 1
        # socket down: REST at once (never waits for a reconnection)
        await ws.stop()
        res = await ex.batch_place([ArcusOrder(m, True, Decimal("1"), Decimal("770.01"), "ALO")])
        assert rest == ["/v1/batchPlaceOrders"] and res["responses"][0]["orderId"] == "rest"
        server.close()
        await server.wait_closed()
    asyncio.run(go())


def test_arcus_ws_unsupported_falls_back():
    async def go():
        async def handler(msg):
            if msg.get("type") == "post":
                return {"method": msg["request"]["type"], "id": msg["id"], "status": 501,
                        "error": {"type": "NotImplemented", "message": "no"}}
        server, url, seen = await arcus_server(handler)
        ws = ArcusWebSocket(url, Messenger(None))
        await ws.start()
        ex = exchange(ws, None)
        m = Market(MARKET)
        try:
            await ex.batch_place([ArcusOrder(m, True, Decimal("1"), Decimal("770.01"), "ALO")])
        except ArcusError:
            pass
        assert ex.transport == "rest" and not ex.streaming
        await ws.stop()
        server.close()
        await server.wait_closed()
    asyncio.run(go())


def test_hl_ws_post():
    from eth_account import Account
    from core.HL.exchange import HLExchange
    from core.HL.websocket import HLWebSocket

    async def go():
        seen = []

        async def serve(sock):
            async for raw in sock:
                msg = json.loads(raw)
                seen.append(msg)
                if msg.get("method") == "post":
                    payload = msg["request"]["payload"]
                    if payload["action"]["type"] == "cancel":
                        response = {"type": "error", "payload": "bad cancel"}
                    else:
                        response = {"type": "action", "payload": {"status": "ok", "response": {
                            "type": "order", "data": {"statuses": [{"resting": {"oid": 77}}]}}}}
                    await sock.send(json.dumps({"channel": "post", "data": {"id": msg["id"], "response": response}}))
        server = await websockets.serve(serve, "127.0.0.1", 0)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        ws = HLWebSocket(url, Messenger(None), None)
        await ws.start()
        ex = HLExchange.__new__(HLExchange)
        ex.wallet, ex.vault_address, ex.expires_after, ex.last_nonce = Account.create(), None, None, 0
        ex.url, ex.transport, ex.ws, ex.timeout = "https://api.hyperliquid.xyz", "ws", ws, 2.0
        ex._log = Logger("t", Messenger(None))
        res = await ex.batch_order([{"a": 1, "b": True, "p": "1", "s": "1", "r": False, "t": {"limit": {"tif": "Alo"}}}])
        assert res["status"] == "ok" and res["response"]["data"]["statuses"][0]["resting"]["oid"] == 77
        sent = seen[-1]
        assert sent["method"] == "post" and sent["request"]["type"] == "action"
        assert set(sent["request"]["payload"]) >= {"action", "nonce", "signature", "vaultAddress"}
        res = await ex.batch_cancel(True, [{"a": 1, "o": 77}])
        assert res == {"status": "err", "response": "bad cancel"}
        assert ("hyperliquid", "order", "ws") in LATENCY.stats("hyperliquid")
        await ws.stop()
        server.close()
        await server.wait_closed()
    asyncio.run(go())


def test_percentile():
    assert percentile([1, 2, 3, 4], 0.5) == 2.5 and percentile([], 0.5) is None


def test_reconnects_and_listeners():
    from core.HL.websocket import HLWebSocket

    async def go():
        connections = []

        async def serve(sock):
            connections.append(sock)
            if len(connections) == 1:
                await sock.close()          # the first connection is dropped by the server at once
                return
            async for _ in sock:
                pass
        server = await websockets.serve(serve, "127.0.0.1", 0)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        dropped = []
        arcus = ArcusWebSocket(url, Messenger(None))
        arcus.disconnect_listeners.append(lambda: dropped.append(1))
        await arcus.start()
        hl = HLWebSocket(url, Messenger(None), None)
        await hl.start()
        for _ in range(60):
            await asyncio.sleep(0.05)
            if arcus.connects >= 2 and hl.connects >= 2 and arcus.connected.is_set() and hl.connected.is_set():
                break
        assert dropped and (arcus.connects >= 2 or hl.connects >= 2)
        try:
            await arcus.request("post", {}, timeout=0.2, wait=True)
        except asyncio.TimeoutError:
            pass                                # connected again: the request went out (no reply from this server)
        await arcus.stop()
        await hl.stop()
        try:
            await hl.post({"type": "action", "payload": {}})
            raise AssertionError("socket down: must raise at once")
        except ConnectionError:
            pass
        server.close()
        await server.wait_closed()
    asyncio.run(go())
