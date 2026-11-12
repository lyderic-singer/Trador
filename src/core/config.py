import os
from pathlib import Path
from dotenv import load_dotenv

# src/core/config.py -> the project root (where .env, pyproject.toml and data/ live)
ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
LOG_FILE = DATA / "logs" / "logs.txt"

load_dotenv(ROOT / ".env") or load_dotenv()


def env(exchange: str, mainnet: bool, field: str, default: str | None = None) -> str:
    name = f"{exchange}_{'MAINNET' if mainnet else 'TESTNET'}_{field}"
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"missing {name} in .env")
    return value
