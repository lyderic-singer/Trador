# Trador

Asynchronous trading framework in Python for perpetual DEXs: **Hyperliquid**, **Arcus**, **Lighter** and **Aster**
behind one common `Exchange` interface. A strategy written once runs unchanged on every venue.

The low-level engines talk to each exchange in its native format (signing, nonces, websockets, rate limits); the
adapters translate that into standard models (`Order`, `Fill`, `Position`, `AccountState`...); strategies only ever see
the adapters. Around that: a Telegram control bot, a launch journal that diffs parameters between runs, and an offline
test suite that runs the real engines against in-memory exchange simulators.

> **Public version.** The production strategies (market making, funding, arbitrage, HIP-4 outcomes...), their hedge
> engine and their research tools are private and not part of this repository. One complete example strategy,
> **Enveloppe**, is included to show how the framework is used.

## Highlights

- **Four exchange integrations from scratch**: EIP-712 / L1 action signing (Hyperliquid, Aster), Ed25519 API keys
  (Arcus), Schnorr signer over ECgFp5 via the official SDK (Lighter), per-key nonce management, clock-skew correction.
- **Orders over the websocket** (Hyperliquid, Arcus) with transparent fallback to REST when the socket drops, strictly
  increasing request ids, bounded in-flight requests, latency measured per venue / method / transport.
- **Resilient connections**: automatic reconnection with backoff, silent recovery of short drops, alerts only on real
  incidents, re-subscription and REST resync of missed fills.
- **Normalized differences**: asynchronous acknowledgements (Lighter, Arcus) vs synchronous results (Aster), 48-bit
  client ids, modify-vs-replace semantics, hourly vs 4 h / 8 h funding, price bands, minimum sizes / notionals, all
  handled in the adapters so strategies never branch on the venue.
- **Strategy building blocks**: market data (BBO, candles, EMAs), account tracking, a cooldown-aware executor, local
  order tracking and a batched gateway that respects each venue's action budget.
- **Operations**: Telegram commands (`/pf`, `/vol`, `/stop`, `/kill`...) restricted to one chat, a launch journal
  recording the git commit, versions and every parameter, with the diff since the previous launch.
- **Offline test suite in virtual time**: ~11 minutes of real-time `sleep`s and timeouts run in a few seconds;
  Lighter and Aster are tested against simulators written from their docs, plugged under the real engine and adapter.

## Layout

```
src/
  main.py          entry point: picks a STRATEGY key, configures the exchange, starts the strategy
  core/            low-level engines, one package per exchange (HL, arcus, lighter, aster) + orchester, sessions,
                   reconnecting socket, latency, launch journal
  adapters/        Exchange interface, standard models, one adapter per exchange
  strategies/      building blocks (market, account, executor, orders, gateway) and the Enveloppe example
  log/             logger, messenger, Telegram bot
  tools/           live checks of an exchange integration (send real, tiny, never-filled orders)
tests/             offline tests (pytest), fake exchange, multi-asset simulator, Lighter / Aster simulators
documentation/     ARCHITECTURE.md
```

More in [`documentation/ARCHITECTURE.md`](documentation/ARCHITECTURE.md), and per exchange in
[`src/core/lighter/lighter.md`](src/core/lighter/lighter.md) and [`src/core/aster/aster.md`](src/core/aster/aster.md).

## Quick start

[uv](https://docs.astral.sh/uv/) manages Python, the virtual environment and the locked dependencies.

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh     # Windows: powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
git clone <repo-url> Trador && cd Trador
uv sync                                             # creates .venv from uv.lock
uv run pytest                                       # offline tests, a few seconds
cp .env.example .env && chmod 600 .env              # then fill in the keys of the venue you use
uv run src/main.py                                  # or: uv run trador
```

### Configuration (`.env`)

| Variable | Role |
|---|---|
| `STRATEGY` | `enveloppe_hl`, `enveloppe_arcus`, `enveloppe_lighter` or `enveloppe_aster` |
| `HL_MAINNET_PRIVATE_KEY`, `HL_MAINNET_ADDRESS` | Hyperliquid: key that signs (preferably an **API wallet**) and the traded account |
| `ARCUS_MAINNET_PRIVATE_KEY`, `ARCUS_MAINNET_ADDRESS`, `ARCUS_MAINNET_ACCOUNT` | Arcus: Ed25519 API signing key, master wallet, sub-account |
| `LIGHTER_MAINNET_ACCOUNT`, `LIGHTER_MAINNET_API_KEY`, `LIGHTER_MAINNET_PRIVATE_KEY` | Lighter: account index, API key index (4-254) and its private key |
| `ASTER_MAINNET_PRIVATE_KEY`, `ASTER_MAINNET_ADDRESS` | Aster: API wallet ("Pro API" agent) key and main wallet |
| `TG_TOKEN`, `TG_CHAT_ID` | Telegram bot (optional, `NOTIFY = False` in `src/main.py` to disable) |

`*_TESTNET_*` variables are used when `MAINNET = False` in `src/main.py`. Without keys, the Hyperliquid client starts
read-only (market data). Never put a main wallet key on a server: use an API wallet / agent key where the venue has one.

Exchange-specific setup (account mode, API wallets, Lighter signer, Aster one-way mode) is described in each
exchange's `.md`. Before trading on a new venue, check the integration live without taking a position:

```shell
uv run python -m tools.check_exchange lighter SPY --order --ioc
uv run python -m tools.check_exchange aster SPY --order --ioc --switch
```

### Running

- `MAINNET`, `NOTIFY`, fees and builder codes are at the top of `src/main.py`;
- clean stop: `Ctrl+C`, or `/stop` (strategies) / `/kill` (everything) on Telegram;
- everything is written under `data/` at the project root (logs, launch journal), ignored by git.

At every start the bot logs which strategy, code and parameters run, and what changed since the previous launch:

```
▶️ Launch enveloppe_arcus · trador 0.1.0 · git 3f9c2a1b7e@main · mainnet · enveloppe.parameters.Parameters
2 change(s) since the launch of 2026-11-12 16:08:41 +0100:
  SPREAD: 0.005 → 0.004
  exchanges.arcus.maker_fee: 0.0 → 0.0001
```

`data/runs/history.jsonl` keeps every launch, one JSON per line.

## Telegram commands

| Command | Effect |
|---|---|
| `/help` | list of commands |
| `/ping` | check that the bot answers |
| `/pf` | portfolio value per exchange, breakdown and main positions |
| `/vol` | traded volume 24h / 7d / 30d / total per exchange (maker share, fills, fees) |
| `/stats` | report of the running strategies that provide one |
| `/address` | addresses of the accounts in use |
| `/perpinventory`, `/spotinventory` | Hyperliquid perp account / spot balances |
| `/stop` | stop the strategies (the bot and Telegram stay online) |
| `/kill` | stop everything and exit |

Commands only answer the chat `TG_CHAT_ID`.

## Tests

```shell
uv run pytest                          # everything, a few seconds
uv run pytest tests/test_lighter.py -q
TRADOR_REAL_TIME=1 uv run pytest       # on the real clock (~11 minutes)
```

`tests/conftest.py` runs the suite in **virtual time**: when the asyncio loop has nothing left to do and would wait for
its next timer, the wait is skipped and every clock the code can read (`time.time()`, `time.monotonic()`,
`datetime.now()`...) moves forward by the same amount. Sleeps, timeouts and durations behave as before, in the same
order, without waiting. Real time is kept for tests using real local sockets or threads, and for tests marked
`@pytest.mark.realtime`.

> `tools.test_orders` and `tools.check_exchange --order / --ioc` send **real orders** with the keys of the `.env`
> (tiny, far from the market, cancelled right away): they are live checks, not offline tests.

## Writing a strategy

A strategy receives one or more `Exchange` objects and only uses `adapters.models`:

```python
orchester = Orchester(mainnet=True, notify=False, exchanges=("arcus",))
await orchester.start()
arcus = orchester.exchange("arcus", maker_fee=0.0, taker_fee=0.000225)
await orchester.add_strategy(Enveloppe(arcus, "SPY-USD"), "enveloppe_arcus")
```

See [`src/strategies/enveloppe/enveloppe.md`](src/strategies/enveloppe/enveloppe.md) for the example strategy.

## Disclaimer

Educational project. Trading with leverage can lose more than the deposited margin; nothing here is financial advice.
