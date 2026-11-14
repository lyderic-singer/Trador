import asyncio
import os
import time
from core.config import env
from core.session import Session
from core.lighter.api import LighterApi
from core.lighter.exchange import LighterExchange
from core.lighter.info import LighterInfo
from core.lighter.utils.constants import (AUTH_REFRESH, AUTH_TTL, MAINNET_API_URL, MAINNET_WS_URL, TESTNET_API_URL,
                                          TESTNET_WS_URL)
from core.lighter.utils.signing import SdkSigner, Signer
from core.lighter.websocket import LighterWebSocket
from core.utils import cancel
from log.logger import Logger
from log.messenger import Messenger


def optional(mainnet: bool, field: str) -> str | None:
    return os.getenv(f"LIGHTER_{'MAINNET' if mainnet else 'TESTNET'}_{field}") or None


def credentials(mainnet: bool) -> tuple[int | None, dict[int, str], dict[str, int]]:
    """account index, {api key index: private key}, {"maker" / "taker": api key index}.

    LIGHTER_<NET>_ACCOUNT        account index (the number in the URL of the account page; a sub-account has its own)
    LIGHTER_<NET>_API_KEY        index of the API key (4 to 254; 0-3 belong to the Lighter apps)
    LIGHTER_<NET>_PRIVATE_KEY    private key of that API key (not the Ethereum key)
    LIGHTER_<NET>_TAKER_API_KEY / _TAKER_PRIVATE_KEY   optional second key for IOC orders (see Lane)
    Without key the client is read-only (market data); without account, public data only."""
    account = optional(mainnet, "ACCOUNT")
    key = optional(mainnet, "PRIVATE_KEY")
    if account is None or key is None:
        return (int(account) if account is not None else None), {}, {}
    index = int(env("LIGHTER", mainnet, "API_KEY"))
    keys, lanes = {index: key.strip()}, {"maker": index, "taker": index}
    taker = optional(mainnet, "TAKER_PRIVATE_KEY")
    if taker:
        t = int(env("LIGHTER", mainnet, "TAKER_API_KEY"))
        if t == index:
            raise ValueError("LIGHTER taker key: TAKER_API_KEY must be another index than API_KEY")
        keys[t], lanes["taker"] = taker.strip(), t
    for i in keys:
        if not 4 <= i <= 254:
            raise ValueError(f"LIGHTER api key index {i}: must be between 4 and 254 (0-3 are reserved)")
    return int(account), keys, lanes


class LighterClient:
    def __init__(self, mainnet: bool, messenger: Messenger, session: Session, signer: Signer | None = None):
        self.messenger = messenger
        self.mainnet = mainnet
        self._log = Logger("Lighter Client", messenger)
        self.account, self.private_keys, self.lanes = credentials(mainnet)
        self.readonly = not self.private_keys
        self.address = f"account {self.account}" if self.account is not None else ""
        self.url = optional(mainnet, "API_URL") or (MAINNET_API_URL if mainnet else TESTNET_API_URL)
        self.api = LighterApi(self.url, messenger, session)
        self.info = LighterInfo(messenger, self.api)
        self.signer = signer
        self.exchange = LighterExchange(messenger, self.api, self.info, signer, self.account or 0,
                                        self.lanes or {"maker": 0})
        self.websocket = LighterWebSocket(optional(mainnet, "WS_URL") or (MAINNET_WS_URL if mainnet else TESTNET_WS_URL),
                                          messenger)
        self.exchange.ws = self.websocket
        self.api.token = self.websocket.token = self.token
        self.session = session
        self.running = False
        self.tokens: tuple[str, float] | None = None    # auth token, time.time() it was made
        self.renewed = False                            # a new token replaced one the websocket channels still use
        self.tasks: list[asyncio.Task] = []
        self.tier = ""                                  # "standard", "premium"... (accountLimits)
        self.limits: dict = {}

    def token(self) -> str | None:
        """auth token of the private REST endpoints and websocket channels (made by the API key, 8 h at most)"""
        if self.signer is None:
            return None
        if self.tokens is None or time.time() - self.tokens[1] > AUTH_REFRESH:
            self.renewed = self.tokens is not None
            self.tokens = (self.signer.auth(self.lanes["maker"], AUTH_TTL), time.time())
        return self.tokens[0]

    async def start(self):
        if self.running:
            return await self._log.info("already started")
        await self._log.info("Starting")
        await self.info.start()
        if not self.readonly and self.signer is None:
            self.signer = self.exchange.signer = await SdkSigner.create(self.url, self.account, self.private_keys)
        if self.signer is not None:
            await self.check()
        await self.websocket.start()
        # REST carries the orders (default transport) and every account read: its TLS connections are kept warm
        # (2 connections every 20 s: a Standard account only has 60 requests a minute)
        self.session.warm(self.url + "/api/v1/status", every=20.0, connections=2, venue="lighter")
        self.tasks.append(asyncio.create_task(self.watch()))
        self.running = True
        mode = "read-only" if self.readonly else f"account {self.account}, api key(s) {sorted(self.private_keys)}"
        await self._log.success(f"Started ({len(self.info.by_id)} perps, {mode})")

    async def check(self):
        """the keys of the .env must be the ones registered for the account; the tier decides what the bot can do"""
        error = await asyncio.to_thread(self.signer.check)
        if error:
            raise ValueError(f"Lighter API key refused for account {self.account}: {error} "
                             f"(LIGHTER_{'MAINNET' if self.mainnet else 'TESTNET'}_API_KEY / _PRIVATE_KEY / _ACCOUNT)")
        for lane in self.exchange.lanes.values():
            await self.exchange.sync(lane)
        try:
            self.limits = await self.info.limits(self.account)
        except Exception as e:
            return await self._log.warning(f"cannot read the account limits: {e}", send_tg=False)
        self.tier = str(self.limits.get("user_tier_name") or self.limits.get("user_tier") or "").lower()
        quota = self.limits.get("volume_quota_remaining")
        if quota not in (None, ""):
            self.exchange.quota, self.exchange.quota_ts = int(float(quota)), time.time()
        if "standard" in self.tier:
            await self._log.warning("Lighter account on the STANDARD tier: 60 requests per minute orders included, 300 ms "
                                    "added to every taker order, cancel and modify. Fine for a slow strategy, not for "
                                    "market making: switch to Premium (changeAccountTier) before quoting")
        else:
            await self._log.info(f"tier {self.tier or '?'}, volume quota {self.exchange.quota}")

    async def watch(self):
        """nonces compared with the exchange when idle, auth token renewed before it expires"""
        while True:
            await asyncio.sleep(5)
            try:
                await self.exchange.reconcile()
                if self.signer is not None and self.tokens:
                    self.token()
                    if self.renewed:
                        self.renewed = False
                        await self.websocket.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._log.debug(f"watch failed: {e}")

    async def stop(self):
        for task in self.tasks:
            await cancel(task)
        self.tasks.clear()
        if self.running:
            await self.websocket.stop()
            self.running = False
        close = getattr(self.signer, "close", None)
        if close:
            await close()
        await self._log.success("Stopped")
