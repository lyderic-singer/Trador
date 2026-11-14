import asyncio
import json
import time
from typing import Callable
from core.latency import LATENCY
from core.lighter.api import LighterApi, LighterError
from core.lighter.info import LighterInfo
from core.lighter.utils.constants import INVALID_NONCE, MAX_BATCH_REST, MAX_BATCH_WS
from core.lighter.utils.signing import Signer
from core.lighter.utils.types import Tx
from log.logger import Logger
from log.messenger import Messenger

Build = Callable[[int, int], Tx]   # (api key index, nonce) -> signed transaction


class Lane:
    """one API key = one nonce stream and one queue on the exchange: a transaction never executes before the earlier
    ones of the same key, so a taker order (140-300 ms speed bump) delays the quotes sent after it on the same key.
    Two lanes when the .env gives a second key: "maker" (post-only, modify, cancel) and "taker" (IOC)"""

    def __init__(self, key: int):
        self.key = key
        self.nonce: int | None = None   # next nonce to sign (None: read it from the exchange first)
        self.lock = asyncio.Lock()
        self.dirty = False              # an outcome was unknown or refused: read nextNonce before signing again
        self.inflight = 0               # requests sent whose reply has not come (websocket only)
        self.sent = 0.0                 # time.time() of the last send
        self.checked = 0.0


class LighterExchange:
    """signs and sends transactions. Nonces are kept here: one counter per lane, read once from nextNonce, then
    incremented locally; read again after any failure or unknown outcome (never guessed), and compared with the
    exchange when the lane is idle (a transaction dropped by the sequencer without using its nonce blocks every later
    one of the same key: `reconcile` sees it).

    transport "rest" (default): one HTTPS request per batch, the lane is held until the reply (two requests of one key
    must reach the exchange in nonce order). transport "ws": the batch goes on the open websocket, the lane is held
    only while the frame is written (order is kept by the socket) and replies are awaited concurrently; REST is used
    when the socket is down at send time (never as a retry: the transaction may have arrived)."""

    def __init__(self, messenger: Messenger, api: LighterApi, info: LighterInfo, signer: Signer | None, account: int,
                 keys: dict[str, int]):
        self._log = Logger("Lighter Exchange", messenger)
        self.api = api
        self.info = info
        self.signer = signer
        self.account = account
        self.lanes: dict[int, Lane] = {key: Lane(key) for key in set(keys.values())}
        self.keys = keys                # "maker" / "taker" -> api key index
        self.transport = "rest"
        self.ws = None                  # LighterWebSocket, wired by the client
        self.timeout = 5.0              # s, a websocket reply later than that is an unknown outcome
        self.quota: int | None = None   # volume quota left (from the replies; None until the first one that gives it)
        self.quota_ts = 0.0
        self.bucket: int | None = None  # sendTx requests left in the minute (Premium accounts only)
        self.price_protection: bool | None = None  # None = exchange default (on: a limit price 5 % through the mark is refused)

    @property
    def streaming(self) -> bool:
        ws = getattr(self, "ws", None)
        return getattr(self, "transport", "rest") == "ws" and ws is not None and ws.connected.is_set()

    def lane(self, name: str = "maker") -> Lane:
        return self.lanes[self.keys.get(name, self.keys["maker"])]

    async def sync(self, lane: Lane):
        """nonce of the lane read from the exchange, once every reply still expected has come"""
        waited = time.time()
        while lane.inflight and time.time() - waited < self.timeout + 1:
            await asyncio.sleep(0.005)
        before = lane.nonce
        lane.nonce = await self.info.next_nonce(self.account, lane.key)
        lane.dirty = False
        lane.checked = time.time()
        if before is not None and before != lane.nonce:
            await self._log.info(f"api key {lane.key}: nonce {before} -> {lane.nonce} (read from the exchange)", display=False)

    async def reconcile(self, quiet: float = 2.0):
        """for each lane used since its last check and idle for `quiet` s: the exchange must expect our next nonce.
        Lower means transactions accepted by the API were dropped before execution (and every later one with them)"""
        now = time.time()
        for lane in self.lanes.values():
            if lane.nonce is None or lane.sent <= lane.checked or now - lane.sent < quiet or lane.inflight or lane.lock.locked():
                continue
            async with lane.lock:
                mine = lane.nonce
                try:
                    theirs = await self.info.next_nonce(self.account, lane.key)
                except Exception as e:
                    await self._log.debug(f"nonce check failed: {e}")
                    continue
                lane.checked = time.time()
                if theirs != mine:
                    lane.nonce, lane.dirty = theirs, False
                    await self._log.warning(
                        f"api key {lane.key}: the exchange expects nonce {theirs}, we were at {mine}: "
                        + (f"{mine - theirs} transaction(s) accepted by the API never executed" if theirs < mine
                           else "another process signs with this key (one key per bot)"), send_tg=theirs > mine)

    def account_for(self, body: dict):
        if body.get("volume_quota_remaining") is not None:
            self.quota, self.quota_ts = int(body["volume_quota_remaining"]), time.time()
        if body.get("remaining_send_tx") is not None:
            self.bucket = int(body["remaining_send_tx"])

    async def send(self, builds: list[Build], lane: str = "maker") -> list[dict]:
        """signs and sends; one {"ok", "hash", "tx", "error", "code"} per build, in order. A batch is accepted or
        refused as a whole by the API. Raises (LighterError 429, ConnectionError, TimeoutError) only when no chunk
        could be reported: otherwise the failure is written in the results of its chunk"""
        if not builds:
            return []
        if self.signer is None:
            raise LighterError(401, None, "read-only client: no Lighter API key in the .env", "sendTx")
        ln = self.lane(lane)
        size = MAX_BATCH_WS if self.streaming else MAX_BATCH_REST
        parts = [builds[i:i + size] for i in range(0, len(builds), size)]
        waits: list[asyncio.Future] = []
        for k, part in enumerate(parts):
            try:
                wait = await self.chunk(ln, part)
            except Exception as e:
                if not waits:
                    raise
                # earlier chunks left: this one and the next ones are reported failed (never sent, or outcome unknown)
                waits += [asyncio.ensure_future(self.failed(len(x), e)) for x in parts[k:]]
                break
            waits.append(asyncio.ensure_future(wait))
        results = await asyncio.gather(*waits, return_exceptions=True)
        if len(results) == 1 and isinstance(results[0], BaseException):
            raise results[0]
        out: list[dict] = []
        for part, r in zip(parts, results):
            out += (await self.failed(len(part), r)) if isinstance(r, BaseException) else r
        return out

    async def chunk(self, lane: Lane, builds: list[Build]):
        """sends one batch and returns an awaitable giving its results"""
        n = len(builds)
        async with lane.lock:
            try:
                if lane.nonce is None or lane.dirty:
                    await self.sync(lane)
                assert lane.nonce is not None
                txs = [build(lane.key, lane.nonce + i) for i, build in enumerate(builds)]
            except Exception as e:
                return self.failed(n, e)
            method = "sendTx" if n == 1 else "sendTxBatch"
            t = time.perf_counter()
            if self.streaming:
                try:
                    future = await self.ws.post(*self.frame(txs))
                except ConnectionError:
                    future = None  # the socket dropped before the frame left: nothing was sent, REST takes it
                if future is not None:
                    lane.nonce += n
                    lane.inflight += 1
                    lane.sent = time.time()
                    return self.reply(lane, txs, future, method, t)
            try:
                lane.sent = time.time()
                body = await self.rest(txs)
            except LighterError as e:
                lane.dirty = True
                if e.status == 429:
                    raise
                return self.failed(n, e, txs)
            except Exception:
                lane.dirty = True      # unknown outcome: the nonce is read again, the caller is told it does not know
                raise
            lane.nonce += n
            self.account_for(body)
            return self.done(txs, body)

    async def reply(self, lane: Lane, txs: list[Tx], future: asyncio.Future, method: str, t: float) -> list[dict]:
        n = len(txs)
        try:
            msg = await asyncio.wait_for(future, timeout=self.timeout)
            body = self.ws.parse(msg, method)
        except LighterError as e:
            lane.dirty = True
            LATENCY.record("lighter", method, "ws", (time.perf_counter() - t) * 1000, False, n, str(e.code or e.status))
            if e.status == 429:
                raise
            return await self.failed(n, e, txs)
        except Exception as e:
            lane.dirty = True
            LATENCY.record("lighter", method, "ws", (time.perf_counter() - t) * 1000, False, n, type(e).__name__)
            raise
        finally:
            lane.inflight -= 1
        LATENCY.record("lighter", method, "ws", (time.perf_counter() - t) * 1000, True, n)
        self.account_for(body)
        return await self.done(txs, body)

    @staticmethod
    def frame(txs: list[Tx]) -> tuple[str, dict]:
        if len(txs) == 1:
            return "jsonapi/sendtx", {"tx_type": txs[0].type, "tx_info": json.loads(txs[0].info)}
        return "jsonapi/sendtxbatch", {"tx_types": json.dumps([tx.type for tx in txs]),
                                       "tx_infos": json.dumps([tx.info for tx in txs])}

    async def rest(self, txs: list[Tx]) -> dict:
        if len(txs) == 1:
            form = {"tx_type": txs[0].type, "tx_info": txs[0].info}
            if self.price_protection is not None:
                form["price_protection"] = "true" if self.price_protection else "false"
            return await self.api.post("/api/v1/sendTx", form)
        return await self.api.post("/api/v1/sendTxBatch", {"tx_types": json.dumps([tx.type for tx in txs]),
                                                          "tx_infos": json.dumps([tx.info for tx in txs])}, items=len(txs))

    @staticmethod
    async def done(txs: list[Tx], body: dict) -> list[dict]:
        hashes = body.get("tx_hash")
        hashes = hashes if isinstance(hashes, list) else [hashes] * len(txs)
        return [{"ok": True, "hash": (hashes[i] if i < len(hashes) else None) or tx.hash, "tx": tx, "error": None,
                 "code": 200, "eta": body.get("predicted_execution_time_ms")} for i, tx in enumerate(txs)]

    @staticmethod
    async def failed(n: int, e: Exception, txs: list[Tx] | None = None) -> list[dict]:
        code = getattr(e, "code", None)
        text = getattr(e, "message", None) or str(e) or type(e).__name__
        if code in INVALID_NONCE:
            text = f"invalid nonce ({text})"
        return [{"ok": False, "hash": None, "tx": txs[i] if txs else None, "error": text, "code": code} for i in range(n)]
