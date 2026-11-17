import asyncio
import os
import sys
import traceback
from core.config import ROOT
from core.orchester import Orchester
from core.HL.utils.builders import *
from strategies.enveloppe import Enveloppe, Parameters as EnveloppeParameters

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

MAINNET = True
NOTIFY = True

# This public version ships a single example strategy, Enveloppe (see src/strategies/enveloppe/enveloppe.md): a grid of
# post-only orders around an EMA of the mid. It only talks to the `Exchange` interface, so the same class runs on every
# venue below. The production strategies are kept private (see src/strategies/hedging.py).
# STRATEGY=<key> in the .env overrides the value below (one repo, several servers)
STRATEGY = os.getenv("STRATEGY") or "enveloppe_hl"

# Hyperliquid: fees by asset ("mkts:US500") or by whole dex ("xyz"); an optional builder code (fee 0 = nothing taken,
# but the main wallet must have approved the builder once, see README)
HL = {"builder": {"b": ENTROPY, "f": 0}, "fees": {"mkts:US500": (0.00003, 0.00009)}}
ARCUS = {"maker_fee": 0.0, "taker_fee": 0.000225, "refresh": 15.0}
# Lighter / Aster: left empty, Lighter takes the fees of the account tier and Aster reads the account's commission
# rates; e.g. LIGHTER = {"maker_fee": .., "taker_fee": .., "transport": "ws"}
LIGHTER: dict = {}
ASTER: dict = {}

# STRATEGY key -> (exchange to start, its adapter options, market, strategy parameters)
SETUPS = {
    "enveloppe_hl": ("hyperliquid", HL, "mkts:US500", EnveloppeParameters()),
    "enveloppe_arcus": ("arcus", ARCUS, "SPY-USD", EnveloppeParameters(COOLDOWN=1.0)),
    "enveloppe_lighter": ("lighter", LIGHTER, "SPY-USD", EnveloppeParameters(COOLDOWN=1.0)),
    "enveloppe_aster": ("aster", ASTER, "SPY-USD", EnveloppeParameters(COOLDOWN=1.0)),
}
EXCHANGES = {key: (venue,) for key, (venue, *_) in SETUPS.items()}


def build(orchester: Orchester, key: str) -> Enveloppe:
    """the strategy of a STRATEGY key, on its exchange. Markets are named as on Arcus ("SPY-USD"): the Lighter and Aster
    adapters find their own market of the same base ("SPY", "SPYUSDT")"""
    venue, options, market, parameters = SETUPS[key]
    return Enveloppe(orchester.exchange(venue, **options), market, parameters)


async def main():
    orchester = None
    # data/ (journals, state files, logs) always at the project root, wherever the bot is launched from
    if (ROOT / "pyproject.toml").exists():
        os.chdir(ROOT)
    if STRATEGY not in SETUPS:
        raise SystemExit(f"STRATEGY={STRATEGY!r} unknown, expected one of: {', '.join(SETUPS)}")
    try:
        orchester = Orchester(MAINNET, NOTIFY, EXCHANGES[STRATEGY])
        await orchester.start()
        await orchester.add_strategy(build(orchester, STRATEGY), STRATEGY)
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception:
        traceback.print_exc()
    finally:
        if orchester:
            await orchester.stop()


def run():
    """entry point: `uv run src/main.py` or `uv run trador`"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
