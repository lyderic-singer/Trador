# Lighter

Engine `core/lighter` + adapter `adapters/lighter.py`. Official docs: https://docs.lighter.xyz (API: `/api`).
Strategies only see the `Exchange` interface; everything below is handled in the integration.

## Getting started

1. **Signer**: Lighter does not sign with an Ethereum key (Schnorr over ECgFp5, Poseidon2 hash). The only reference
   implementation is the exchange's Go signer, shipped compiled in the official Python SDK. It is **not** in the
   project dependencies (nothing changes for a server that does not trade Lighter):
   ```shell
   uv add --group lighter lighter-sdk     # once, on the dev machine: updates pyproject.toml and uv.lock, to commit
   uv sync --group lighter                # on the server trading Lighter (a bare `uv sync` uninstalls it, `uv run` does not)
   ```
   Only the `sign_*` functions of the SDK are used (`core/lighter/utils/signing.py`, class `SdkSigner`); REST,
   websocket and nonces are the project's own. Linux x86-64 / arm64, macOS and Windows x86-64 are provided by the SDK.
2. **Account**: create an API key on app.lighter.xyz (index **4 to 254**; 0-3 belong to the Lighter apps) and note the
   account index (a sub-account has its own).
3. **`.env`**:
   | Variable | Role |
   |---|---|
   | `LIGHTER_MAINNET_ACCOUNT` | account (or sub-account) index |
   | `LIGHTER_MAINNET_API_KEY` | API key index |
   | `LIGHTER_MAINNET_PRIVATE_KEY` | private key of that API key (not the Ethereum key) |
   | `LIGHTER_MAINNET_TAKER_API_KEY`, `LIGHTER_MAINNET_TAKER_PRIVATE_KEY` | optional: 2nd key for IOCs (see "queues per key") |
   Without a private key the client starts read-only (prices); without an account, public data only.
4. **Check live** before any strategy: `uv run python -m tools.check_exchange lighter SPY --order --ioc` (no position
   taken), then `--ws` before using the websocket transport.
5. **Run**: `STRATEGY=enveloppe_lighter` (see `src/main.py`).

## Differences with Hyperliquid / Arcus, and how the integration handles them

| Topic | Lighter | Handled how |
|---|---|---|
| Order answer | a signed transaction; the answer (code 200) means **accepted**, not executed | resting order: `Result.ok` at once, status `accepted`, the rest from the order stream (like an Arcus ACK). **IOC**: the adapter waits for the real end of the order (at most `ioc_timeout`, 4 s) and returns filled size and average price like Hyperliquid; with no news, `ok` with 0 filled, which a hedging caller treats as "unknown, counted filled until the fills come" |
| Refusal after acceptance | a post-only that would cross, margin...: the order is cancelled on arrival (`canceled-post-only`...); a transaction can also be rejected by the sequencer | `OrderUpdate` `rejected` (reason in `raw["rejectionReason"]`); `account_tx` channel for failed transactions |
| Client id | **48-bit** integer, unique | the low 48 bits of the strategy's hexadecimal id; index → full id table for fills and updates (lost on restart: orders left open come back without a client id) |
| Order id | the order index is assigned later by the sequencer | the id given to strategies is the client index (accepted wherever the API takes an order index); an order placed outside the bot keeps its exchange index |
| Nonce | a counter **per API key**; a transaction refused by the API does not consume it; a gap blocks every following one | local counter per key, read again from `nextNonce` after any failure or unknown outcome (never guessed), compared with the exchange when the key is idle; one bot instance per API key |
| Queues per key | a transaction never executes before the previous ones of the same key; taker orders are delayed (see tiers) | optional "taker" key: IOCs go on their own key and do not delay the quotes |
| Modify | keeps the order; meaning of `base_amount` undocumented for a partially filled order | untouched order: native modify (same id). Partially filled order: cancel + new order in **the same batch** (new id, the old one reported `replaced`) |
| Cancel | free (no quota), always "accepted" even if the order no longer exists | `ok`; the stream tells what happened |
| Sizes and prices | integers: price × 10^`price_decimals`, size × 10^`size_decimals`; prices on 32 bits | rounded by the adapter, refused locally otherwise |
| Minimums | `min_base_amount` **and** `min_quote_amount` (resting orders) | `Instrument.min_size` / `min_notional`, checked before sending; a reduce-only can be smaller |
| Expiry | every resting order expires (5 min to 30 days) | 28 days (Arcus: 40) |
| Price protection | `sendTx` refuses a limit price more than 5 % through the mark; `sendTxBatch` does not check | left as default |
| Funding | paid **every hour**, rate in **percent** in `market_stats` | returned as a fraction per hour (`/100`), next payment = next full hour |
| Closed markets (RWA) | SPY, QQQ, NVDA... trade 24/7; no "closed" flag in the API | `Instrument.closed` and `Funding.closed` always `False`: the out-of-session logic driven by these fields (bands, raised Arcus margin) never triggers; strategy-specific calendars still apply |
| Fees | by account tier, not by market | by default those of the tier read at startup (Standard 0 / 0, Premium 0.4 / 2.8 bp without staked LIT); `LIGHTER = {"maker_fee": .., "taker_fee": ..}` in `main.py` for a reduced tier. The real fee of each fill comes from the trade (rate in millionths of the notional) |
| Account | positions and balances pushed over the websocket; `GET /account` weighs 300 | state kept from the stream; REST at startup, every `poll` s (15) and on demand at most once per `account_interval` (2 s) |
| Unknown leverage | a market never set takes the market's default leverage | reported as is (trackers would assume 1x) |
| Action budget | **volume quota** (Premium / Plus): 1 unit per order placed or modified and per cancel-all, +1 per ~$2 traded, starts at 1,000, cap 5 M, 1 free transaction every 15 s | `budget()` = `{"order": quota, "cancel": ∞}`; `None` on Standard |
| Dead man's switch | **scheduled** cancel-all, whole account; each call costs 1 quota unit | `schedule_cancel(s)` reschedules it, `None` drops it |
| Candles | 1m, 5m, 15m, 30m, 1h, 4h, 12h, 1d | the others (3m, 2h, 8h) are rebuilt from the largest native one that divides them |
| `/vol` volume | the exchange gives day / week / month / total | no maker share, fill count or fees |
| Order transport | REST (`sendTx`, batches of 15) or websocket (`jsonapi/sendtx`, batches of 50) | **REST by default**: the key is held until the answer (two requests of the same key must arrive in nonce order). `"transport": "ws"`: the key is only held while writing the frame, answers awaited in parallel, REST if the socket is down at send time |

## Account tiers: read before quoting

| | Standard | Premium | Plus |
|---|---|---|---|
| Maker / taker fees | 0 / 0 | 0.4 / 2.8 bp | 0.5 / 0.5 bp |
| Maker / cancel / taker delay | 200 / 300 / 300 ms | 0 / 0 / 140-200 ms | 60 / 75 / 100 ms |
| REST limit | **60 requests / minute, orders included** | 24,000 weight / minute (more with staked LIT) | same |
| Order sending | within the 60 req / min | 4,000 requests / minute + volume quota | same |

A **Standard account cannot market make** (60 requests per minute in total): the client says so at startup. Changing
tier requires an account with no position and no order.

**The volume quota starts at 1,000**: with action-reserve thresholds designed for Arcus / HL, a new account is
throttled until the quota grows. Either buy quota (500 USDC per million), let volume build it (+1 per ~$2 traded), or
adapt the thresholds in the strategy parameters.

## What could NOT be verified live

Developed without network access to the exchange or the SDK: everything is tested against a simulator written from
the docs (`tests/lighter_sim.py`). To check with `tools.check_exchange`, then on a small account:

- the SDK calls (`lighter.SignerClient(url=, account_index=, api_private_keys=)`, `sign_create_order(...)` etc. with
  `nonce=` and `api_key_index=`): written from the source of the `elliottech/lighter-python` repository, never run;
- the exact format of the websocket answer to `jsonapi/sendtx` (hence REST by default; the parser accepts the `id`
  returned at the root or in `data`, otherwise arrival order);
- the unit of `initial_margin_fraction` on positions (`"0.1"` and `"10.00"` are both accepted), the meaning of
  `direction` in the funding history (taken as "the side that pays"), the unit of `update_margin` (USDC × 10⁶);
- that a native modify keeps the client index, and that a scheduled cancel-all replaces the previous one;
- the content of `accountLimits` (tier name), of the `account_tx` and `user_stats` channels. For `account_tx`, status
  0 is read as "failed"; to be safe it is only believed if the order never showed up on the order stream within a
  second (an order that exists is never rejected on the strength of a transaction status).
