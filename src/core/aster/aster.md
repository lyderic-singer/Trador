# Aster

Engine `core/aster` + adapter `adapters/aster.py`, futures API **v3** ("Pro API").
Official docs: https://asterdex.github.io/aster-api-website/futures-v3/general-info/
Strategies only see the `Exchange` interface; everything below is handled in the integration.

## Getting started

1. Create an **API wallet** ("agent") in **Pro API** mode on https://www.asterdex.com/en/api-wallet; the main wallet
   must have deposited at least once. Switch the account to **One-way Mode** (the client refuses to start in Hedge
   Mode: strategies hold one net position per market).
2. **`.env`**:
   | Variable | Role |
   |---|---|
   | `ASTER_MAINNET_PRIVATE_KEY` | private key of the API wallet (the agent), never the main wallet's |
   | `ASTER_MAINNET_ADDRESS` | main wallet (display; sent as `user` only if `ASTER_MAINNET_SEND_USER=1`) |
   | `ASTER_MAINNET_SIGNER` | optional: agent address, checked against the key |
   | `ASTER_MAINNET_API_URL`, `ASTER_MAINNET_WS_URL` | optional: other hosts (default `fapi.asterdex.com` / `fstream.asterdex.com`) |
   No new dependency (EIP-712 signing with `eth-account`, already used for Hyperliquid).
3. **Check live** before any strategy: `uv run python -m tools.check_exchange aster SPY --order --ioc --switch` (no
   position taken).
4. **Run**: `STRATEGY=enveloppe_aster` (see `src/main.py`).

## Signing

The request parameters, then `nonce` (microseconds) and `signer` (agent address), are url-encoded in this order; that
string is the `msg` field of an EIP-712 message (domain `AsterSignTransaction`, version 1, chainId 1666 on mainnet /
714 on testnet, type `Message{string msg}`) signed by the agent key. `signature` is appended to the same string, sent
as is (query of a GET / DELETE, body of a POST / PUT). The exchange keeps the last 100 nonces of an agent, refuses a
duplicate and any nonce more than 60 s away from its clock: the clock is compared at startup then every 10 minutes,
and nonces are corrected beyond a 2 s offset.

## Differences with Hyperliquid / Arcus, and how the integration handles them

| Topic | Aster | Handled how |
|---|---|---|
| Sending orders | **HTTPS only** (no orders over the websocket); 5 orders, 5 modifies or 10 cancels (one market) per request | a list is split into batches sent in parallel; a single order goes alone |
| Order answer | synchronous; `newOrderRespType=RESULT` returns the final state of an IOC | `Result` carries filled size and average price, like Hyperliquid |
| Post-only | time in force `GTX`; if it would cross, the order comes back `EXPIRED` | `Result.ok = False` ("post only would cross") and `OrderUpdate` `rejected` |
| Unexecuted IOC | `EXPIRED` with 0 executed | failure "IOC could not match", never an open order |
| Unknown outcome | 503 and timeouts: the order may have gone through | exception raised (never "refused"): a hedging caller counts it filled until the fills come |
| Client id | string of 1 to 36 characters | the strategies' hexadecimal id (34 characters) goes through as is |
| Modify | keeps the id but **always loses queue priority**, even for a size decrease; takes the **total** quantity | the adapter adds the already filled size to the requested remaining size; LIMIT orders only |
| Vanished order | codes -2011 / -2013 | `NOT_FOUND` prefix so that trackers forget it |
| Price limits | `PERCENT_PRICE` filter: buy ≤ mark × up, sell ≥ mark × down | checked before sending (local refusal), without restricting the legal side |
| Minimums | `LOT_SIZE.minQty` and `MIN_NOTIONAL` ($5) | `Instrument.min_size` / `min_notional` |
| Funding | paid every **4 or 8 hours** depending on the market, not every hour | rates returned **per hour** (period rate / hours in the period) to compare venues; the real payment stays per period: a position opened and closed between two payments receives nothing |
| Closed markets | no session flag | `Instrument.closed` / `Funding.closed` always `False` (same remark as Lighter) |
| Fees | per account and per market (`commissionRate`) | read for every followed market and used instead of the configured ones (default 1 / 3.5 bp); a fill paid in an asset other than USDT / USD1 / USDC has `fee = None` |
| Account | `ACCOUNT_UPDATE` only carries what changes | read again over REST after every stream event (grouped over 150 ms) and every `poll` s (10) |
| Private stream | `listenKey` (60 min, extended every 30); connections cut at 24 h | new key on every reconnection; missed fills read again over REST (`userTrades`) on the followed markets |
| Dead man's switch | `countdownCancelAll`, **per market** | `schedule_cancel` arms it on every followed or traded market |
| Action budget | per-minute limits (2,400 weight per IP, 1,200 orders per account), no cumulative reserve | `budget()` = `None`; a 429 is raised with its `Retry-After` (the Gateway pauses sending); 418 = IP banned for insisting |
| Margin | cross by default, isolated per market; multi-asset mode possible | `set_leverage(symbol, x, cross)` sets the margin type then the leverage; equity is `totalMarginBalance`, counted in dollars |
| `/vol` volume | no account volume endpoint (`userTrades` is per market, in 7-day slices) | "volume not available" |
| Markets | perpetuals `BTCUSDT`, `SPYUSDT`... | found by symbol, by base (`SPY`, USDT quote first) or by another venue's name (`SPY-USD`) |

## What could NOT be verified live

Developed without network access to the exchange: everything is tested against a simulator written from the docs
(`tests/aster_sim.py`). To check with `tools.check_exchange`, then on a small account:

- **the signature**: follows the docs example (field order, `msg`, domain) and is verified by recovering the address,
  but never presented to the exchange. The docs list `user` among the parameters without sending it in their example:
  it is not sent by default (`ASTER_MAINNET_SEND_USER=1` to add it if the exchange answers -1022);
- the host (`fapi.asterdex.com`; the docs example uses `fapi3.asterdex.com`) and the websocket URL `…/ws` with live
  subscription;
- that the `RESULT` answer of an IOC is final (otherwise: `ok` with 0 filled, resolved by the fills);
- the exact content of `/account` (presence of every market with its leverage) and of `leverageBrackets`;
- the default fees (1 / 3.5 bp): replaced by the account's as soon as a market is followed.
