# Enveloppe

Example strategy of the public version of Trador: it shows how a strategy uses the framework, and runs unchanged on
every venue.

## Idea

"Envelope" market making around an exponential moving average (EMA) of the mid. A grid of post-only (ALO) limit orders
is placed on both sides of the EMA, then re-centred when the EMA moves. The bet: the price oscillates around its
average, the far orders get filled on the excesses and the position comes back to zero when the price returns to the
average.

The strategy only knows an `Exchange`:

```python
Enveloppe(orchester.exchange("hyperliquid", **HL), "mkts:US500")
Enveloppe(orchester.exchange("arcus", **ARCUS), "SPY-USD", Parameters(COOLDOWN=1.0))
Enveloppe(orchester.exchange("lighter"), "SPY-USD", Parameters(COOLDOWN=1.0))
```

## The grid

- Centre: `EMA(close)` over `WINDOW` candles of `TIMEFRAME`, extended in real time by the mid.
- First level at `OFFSET × step` from the EMA, then `LEVELS` levels per side.
- Total width of one side ≈ `SPREAD × EMA`.
- `ALPHA` spaces the levels geometrically: `ALPHA = 1` regular grid, `ALPHA > 1` levels further and further apart.

## Sizes

- Capacity of one side = `CAP × (equity × leverage ∓ position)`, converted into units at the mid. The grid never uses
  more than `CAP` of the buying power, position included.
- Geometric distribution: the last (furthest) level is `SKEW` times bigger than the first. More size where mean
  reversion is most likely.
- An order under `MIN_VALUE` dollars (or under the exchange minimum) is not sent.

## The cycle (every `POLL_DELAY` seconds)

1. Read open orders, position and market.
2. If an order was just placed → **modify** to realign sizes and prices.
3. Otherwise, if levels are missing on the active side → **place** the missing levels (within the remaining budget).
4. Otherwise, if the EMA moved by less than `THRESHOLD` since the last modify → **skip**.
5. Otherwise → **modify** the whole grid around the new EMA.

Modified orders never cross the book: a bid stays under `mid × (1 − maker fee)`, an ask above `mid × (1 + maker fee)`.

## `INV` mode

With `INV = True`, only one side is worked at a time:
- mid above the EMA → manage the **bids** (wait for the price to come down),
- mid below the EMA → manage the **asks**.

This avoids accumulating in the direction of the short-term trend.

## Parameters (`parameters.py`)

| Parameter | Default | Role |
|---|---|---|
| `MAKER_FEES` | `None` | maker fee used by the price guard; `None` = the exchange's value |
| `SPREAD` | `0.005` | width of one side of the grid (0.5 % of the EMA) |
| `OFFSET` | `0.0001` | offset of the first level from the EMA, as a fraction of the step |
| `ALPHA` | `1` | geometric spacing factor of the levels |
| `SKEW` | `2` | size of the last level / size of the first |
| `LEVELS` | `24` | number of orders per side |
| `CAP` | `0.85` | maximum share of the buying power used |
| `MIN_VALUE` | `10.5` | minimum notional of an order in $ |
| `POLL_DELAY` | `10.1` | seconds between two cycles |
| `THRESHOLD` | `0.0001` | relative EMA move that triggers a re-centre |
| `TIMEFRAME` | `"5m"` | candle unit for the EMA |
| `WINDOW` | `5` | EMA period |
| `INV` | `True` | one active side only, against the short-term trend |
| `COOLDOWN` | `10.0` | minimum seconds between two requests to the exchange (HL rate limit; can be lower on Arcus) |
| `CLEANUP` | `False` | cancel the instrument's orders when the strategy stops |

## Risks

- **Strong trend**: the grid accumulates against the move up to `CAP`. There is no stop-loss.
- **Gaps / market open** (equities, indices): several levels can fill at once.
- **Arcus out of session**: price bands and a higher initial margin while the underlying market is closed.
- **Latency**: a 10 s cycle leaves orders stale during fast moves.
