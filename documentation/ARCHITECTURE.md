# Architecture

```
pyproject.toml uv.lock    dependencies and environment (uv), pytest config
.env.example              template of the .env (keys, STRATEGY, Telegram)
README.md                 overview, setup and operation
documentation/            ARCHITECTURE.md (this file)
src/                      all the code; src/ is on sys.path (editable install by uv)
  main.py                 entry point: STRATEGY, fees / builder per exchange, launch
  core/                   low-level exchange engines (native format of each API)
    HL/                   Hyperliquid: HLClient, HLApi, HLInfo, HLExchange, HLWebSocket, utils/
    arcus/                Arcus:       ArcusClient, ArcusApi, ArcusInfo, ArcusExchange, ArcusWebSocket, utils/
    lighter/              Lighter:     LighterClient, LighterApi, LighterInfo, LighterExchange (nonces, batches),
                          LighterWebSocket, utils/ (signing.py: signer of the official SDK), lighter.md
    aster/                Aster:       AsterClient, AsterApi, AsterInfo, AsterExchange, AsterMarketSocket /
                          AsterUserSocket, utils/ (signing.py: EIP-712), aster.md
    socket.py             Socket: reconnecting websocket, base of the Lighter and Aster sockets
    orchester.py          infrastructure (logs, Telegram, session) + startup of the clients
    latency.py            LATENCY: round trip of every request (venue, method, ws / rest), clock offsets
    run.py                journal of every launch: strategy, versions, parameters, diff (data/runs)
    config.py             ROOT (project root), DATA, LOG_FILE; .env reading
    session.py utils.py
  adapters/               engine <-> standard format
    models.py             Side, TIF, Instrument, Order, Modify, Cancel, OpenOrder, Result, BBO, Candle, Position, AccountState
    exchange.py           the Exchange interface (what a strategy sees)
    hyperliquid.py        Hyperliquid(Exchange) over core/HL
    arcus.py              Arcus(Exchange) over core/arcus
    lighter.py            Lighter(Exchange) over core/lighter
    aster.py              Aster(Exchange) over core/aster
    utils.py              passive / aggressive rounding, candles rebuilt from a native timeframe
    __init__.py           CLIENTS / ADAPTERS registry
  strategies/
    market.py account.py executor.py    common building blocks (MarketData, AccountTracker, Executor)
    orders.py gateway.py  local order tracking (OrderTracker) + batched gateway (Gateway)
    enveloppe/            the example strategy: enveloppe.py, parameters.py, enveloppe.md
    hedging.py            placeholder: the hedge engine of the production strategies is private
  tools/
    check_exchange.py     live check of the Lighter / Aster integration, never takes a position
    test_orders.py        live place / cancel on Arcus and Hyperliquid
  log/                    logger, messenger, Telegram
tests/                    offline tests, FakeExchange, Simulator (multi-asset with matching), conftest.py (virtual
                          time), lighter_sim.py / aster_sim.py (in-memory exchanges under the real engines)
data/logs/logs.txt        bot log (console + file)
data/runs/                one JSON per strategy (last launch) + history.jsonl (every launch)
```

Dependency direction: `strategies → adapters → core`. A strategy never imports `core.HL` or `core.arcus`: it receives
an `Exchange` and only handles `adapters.models`.

**Lighter and Aster** implement the same `Exchange` interface (`adapters/lighter.py`, `adapters/aster.py`). A market
is named as in the strategy parameters (`SPY-USD`): each adapter finds its own by base (`SPY` on Lighter, `SPYUSDT` on
Aster) and `Exchange.canonical` returns the native name. The differences of each exchange (asynchronous answers on
Lighter, 48-bit client ids, nonces per API key, volume quota; HTTPS-only orders, GTX, funding per 4 / 8 h period,
per-market dead man's switch on Aster...) are handled in the adapter and listed
in `src/core/lighter/lighter.md` and `src/core/aster/aster.md`. Live check without a position:
`uv run python -m tools.check_exchange <venue> <market> --order --ioc`.

| | Hyperliquid | Arcus | Lighter | Aster |
|---|---|---|---|---|
| engine | `core/HL` (`HLClient`) | `core/arcus` (`ArcusClient`) | `core/lighter` (`LighterClient`) | `core/aster` (`AsterClient`) |
| adapter | `adapters/hyperliquid.py` | `adapters/arcus.py` | `adapters/lighter.py` | `adapters/aster.py` |
| `.env` prefix | `HL_` | `ARCUS_` | `LIGHTER_` | `ASTER_` |

## Optional capabilities of an Exchange

On top of the base (BBO, candles, account, orders), an adapter can expose:
`subscribe_fills`, `subscribe_orders`, `subscribe_oracle`, `schedule_cancel` (dead man's switch), `purge` (atomic
cancel of the whole account), `connected`, `budget()` (remaining action pools), `listings()` (listed markets, for
automatic discovery), `fundings()` / `funding_history()` / `funding_payments()` (current and predicted hourly funding,
history, account payments), `add_margin()` (margin added to an isolated position), `subscribe_trades()` (public
trades), `portfolio()` (account value, used by `/pf`), `volume()` (24h / 7d / 30d / total volume, used by `/vol`),
`book()` (best price over REST), `set_leverage()`, and, on `Instrument`, `tick()`, `closed`, `lower` / `upper`
(out-of-session price bands), `max_leverage`, `maintenance` (maintenance margin fraction), `isolated` (isolated-only
market). Arcus and Hyperliquid implement everything (`schedule_cancel` on HL = `scheduleCancel`, which cancels ALL the
orders of the address). The HL `budget()` comes from `userRateLimit` (read every 30 s, every batch sent is counted
between two reads). On `Fill`, `fee` gives the real fee when the exchange returns it. Market making needs at least
`subscribe_fills`.

Hyperliquid fees: `Hyperliquid(fees={...})` accepts an asset name (`"mkts:US500"`) or a whole dex (`"xyz"`).

HIP-4 outcomes (optional, Hyperliquid only): `outcomes()` (reads `outcomeMeta` and registers the
`#<10 x outcome + side>` symbols), `outcome_balances()` (spot USDC and shares), `merge_outcome()`, `reserve_actions()`
(`reserveRequestWeight`), `mark()`, `subscribe_depth()` (l2Book). A fill or an order on an unknown `#enc` coin
registers it on the fly. HL nonces are strictly increasing: two actions signed in the same millisecond are never
rejected.

## Sending orders: websocket first

Both the Hyperliquid and Arcus engines send orders on the already open websocket (no HTTPS connection, no TLS
handshake, no per-request headers):

- **Arcus**: `ArcusExchange.batch` sends `{"type": "post", "id": n, "request": {"type": "batchPlaceOrders", "payload",
  "apiKey", "timestamp", "signature"}}`; every item stays signed, the `{"status": 202, "result"}` answer is mapped back
  to the REST format. A 4xx / 5xx answer raises `ArcusError` as in REST (429 included, with `retryAfterMs`). Ids are
  strictly increasing (lock on send), at most 45 requests in flight (server limit 50). REST is used when the socket is
  down at send time (never as a retry: the order may have arrived), when the server answers 404 / 405 / 501, or when
  an answer cannot be matched to its orders. `transport = "rest"` forces REST. `scheduleCancel`, `cancelAllOrders`,
  `setLeverage` stay on REST (connections kept warm: `GET /health` on 3 connections every 15 s). The clock is compared
  with `GET /v1/time` at startup then every 10 min (corrected beyond a 2 s offset, ±30 s being rejected).
- **Hyperliquid**: `HLExchange.post` sends `{"method": "post", "id": n, "request": {"type": "action", "payload"}}`, the
  `{"channel": "post", "data": {"id", "response"}}` answer is returned in the HTTP format; HTTP if the socket is down.
  Without an HL key in the `.env`, the client starts read-only (market data).
- Websocket compression is disabled (less CPU per frame), reconnection after 1 s then exponential up to 10 s. A drop
  repaired by the first reconnection attempt is not reported (one line in logs.txt only); the error (and Telegram)
  only comes if that attempt fails too, or if connections keep dying within 30 s, and the end of a reported incident
  is announced ("reconnected after N s").
- Every request is measured in `core.latency.LATENCY` (method, transport, ms, errors); `Exchange.transport` tells which
  way orders go, `Exchange.on_disconnect()` warns when the socket drops.

## Telegram

`/pf`: portfolio value, breakdown per exchange (Hyperliquid dexs and spot balances, Arcus sub-account) and main
positions. `/vol`: volume traded over 24h, 7d, 30d and since inception, per exchange (maker share, number of fills,
fees). Hyperliquid: fills from `userFillsByTime` (only the latest 10,000 exist), total = `cumVlm` of `userRateLimit`.
Arcus: fills from `/v1/fills` on the sub-account, total estimated from the action pool. `/stats`: report of the
strategies that provide one (`telegram_report()`). `Orchester.portfolios()` and `Orchester.volumes()` read every
started exchange, even without a strategy. Messages over 4,000 characters are split. Commands only answer `TG_CHAT_ID`
(`filters.Chat` on every `CommandHandler`).

## Adding an exchange

1. `core/<name>/`: the engine, with a `<Name>Client(mainnet, messenger, session)` that reads its credentials through
   `core.config.env("<NAME>", mainnet, "<FIELD>")`.
2. `adapters/<name>.py`: a class inheriting from `adapters.exchange.Exchange`.
3. Register it in `adapters/__init__.py` (`CLIENTS` and `ADAPTERS`).

Every strategy then runs on it, and `Orchester(exchanges=("<name>", ...))` knows how to start it.

## Arcus: points of attention

- Orders are asynchronous (`ACK`): the account state is read again over REST on every `positions` / `orders` /
  `account` websocket event, plus polling.
- Position sizes and entry prices come back as engine integers; the adapter recomputes them from the notional, the
  mark and the PnL.
- `modify` must send back the immutable fields of the order (side, tif, reduceOnly, goodTilTime): the adapter reads
  them from `OpenOrder.raw`.
- Perp fees are not published in the docs: set them in `Arcus(maker_fee=…, taker_fee=…)` (see `GET /v1/feetiers`).

## Hyperliquid: `availableToTrade` with open orders

`availableToTrade` (in `activeAssetData`) does not account for resting orders on the opposite side of a position as
long as their total stays under the position size: with a 70 $ long, a resting 50 $ ask still reports 170 $ available
to sell instead of 120 $, and the order is then refused with "insufficient margin to place order". Do not size an
order from `availableToTrade` when orders are open: recompute the free margin from the account state and the locally
tracked orders.

## Launch journal

`Orchester.add_strategy(strategy, label)` writes before every start (`core/run.py`): the strategy (`STRATEGY` key,
class, `VERSION` / `BUILD` of the module), the code (version in `pyproject.toml`, git commit, branch, uncommitted
files), every field of the parameters dataclass and the exchange options (fees, builder). It compares with the previous
launch of the same strategy (`data/runs/<strategy>.json`) and lists every changed parameter (`old → new`), in the
console, `data/logs/logs.txt` and Telegram. Complete lines in logs.txt: `RUN parameters {...}`, `RUN context {...}`;
history: `data/runs/history.jsonl`. Nothing in the journal can stop a strategy from starting.

## Tests

```
uv run pytest -q                     # pyproject.toml: testpaths = tests, pythonpath = src
TRADOR_REAL_TIME=1 uv run pytest -q  # real clock (~11 min) instead of the virtual time of tests/conftest.py
```

`tests/conftest.py` runs the suite in virtual time: the wait of an idle asyncio loop is skipped and every readable
clock (`time.time`, `time.monotonic`, `datetime.now`...) moves forward by the same amount. Lighter and Aster are tested
against in-memory exchanges written from their docs (`tests/lighter_sim.py`, `tests/aster_sim.py`), plugged under the
real engine and the real adapter (only the HTTP call, the websocket and, for Lighter, the signer are replaced).
