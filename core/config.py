import os
from dotenv import load_dotenv

load_dotenv()


def env(exchange: str, mainnet: bool, field: str, default: str | None = None) -> str:
    name = f"{exchange}_{'MAINNET' if mainnet else 'TESTNET'}_{field}"
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"missing {name} in .env")
    return value
