"""Journal of every launch: which strategy, which code, which parameters, and what changed since the last launch.

On each `Orchester.add_strategy`, `describe()` builds a record:

    strategy   the STRATEGY key of main.py ("enveloppe_hl", ...), the class, its VERSION / BUILD if the module has one
    code       project version (pyproject.toml), git commit / branch / uncommitted changes, python
    parameters every field of the strategy's Parameters dataclass (`strategy.base` when a strategy keeps the parameters it was
               built with apart from the active set, `strategy.p` otherwise), nested dataclasses and lists included
    exchanges  the options given to `orchester.exchange(...)` (fees, builder code)

`changes()` compares it with the previous launch of the same strategy (data/runs/<strategy>.json), parameter by
parameter (dotted paths: `UNIVERSE[2].symbol`, `WEEKEND.H_BP`...). `save()` keeps the record as the new reference and
appends it to data/runs/history.jsonl (one JSON per line: `jq`, pandas.read_json(lines=True)).

Nothing here may stop a strategy from starting: the caller catches every error.
"""
import dataclasses
import enum
import json
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from core.config import DATA, ROOT

RUNS = DATA / "runs"
SHOWN = 40          # changes listed in the console / Telegram (all of them are in logs.txt and history.jsonl)
WIDTH = 120         # a value longer than this is cut in the human readable lines


def project_version(root: Path = ROOT) -> str:
    try:
        import tomllib
        with open(root / "pyproject.toml", "rb") as f:
            return str(tomllib.load(f)["project"]["version"])
    except Exception:
        try:
            from importlib.metadata import version
            return version("trador")
        except Exception:
            return "unknown"


def git(root: Path = ROOT) -> dict:
    """commit, branch, date of the commit and whether files differ from it ({} without git or outside a repository)"""
    def run(*args: str) -> str:
        out = subprocess.run(("git", *args), cwd=root, capture_output=True, text=True, timeout=3)
        if out.returncode != 0:
            raise RuntimeError(out.stderr.strip())
        return out.stdout.strip()
    try:
        info = {"commit": run("rev-parse", "--short=10", "HEAD"),
                "branch": run("rev-parse", "--abbrev-ref", "HEAD"),
                "date": run("log", "-1", "--format=%cI"),
                "subject": run("log", "-1", "--format=%s")[:100]}
        dirty = [line[3:] for line in run("status", "--porcelain", "--untracked-files=no").splitlines() if line]
        info["dirty"] = bool(dirty)
        if dirty:
            info["modified"] = dirty[:20]
        return info
    except Exception:
        return {}


def jsonable(x: Any, depth: int = 0) -> Any:
    """a plain JSON value for anything a Parameters dataclass may hold"""
    if depth > 12:
        return repr(x)
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if x == x and x not in (float("inf"), float("-inf")) else str(x)
    if isinstance(x, enum.Enum):
        return jsonable(x.value, depth + 1)
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        return {f.name: jsonable(getattr(x, f.name, None), depth + 1) for f in dataclasses.fields(x)}
    if isinstance(x, dict):
        return {str(k): jsonable(v, depth + 1) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [jsonable(v, depth + 1) for v in x]
    if isinstance(x, (set, frozenset)):
        return sorted((jsonable(v, depth + 1) for v in x), key=repr)
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, type) or callable(x):
        return f"{getattr(x, '__module__', '?')}.{getattr(x, '__qualname__', repr(x))}"
    return repr(x)


def parameters_of(strategy) -> Any:
    """the parameters the strategy was BUILT with: a strategy that derives an active set from them keeps them
    in `base`, every other strategy in `p`"""
    for name in ("base", "p", "parameters", "params"):
        value = getattr(strategy, name, None)
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return value
    return None


def strategy_version(strategy) -> dict:
    module = sys.modules.get(type(strategy).__module__)
    out = {}
    for name in ("VERSION", "BUILD"):
        value = getattr(strategy, name, None) or getattr(module, name, None)
        if isinstance(value, str):
            out[name.lower()] = value
    return out


def describe(strategy, label: str | None = None, mainnet: bool | None = None, exchanges: dict | None = None,
             extra: dict | None = None) -> dict:
    params = parameters_of(strategy)
    record = {
        "ts": time.time(),
        "date": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "strategy": label or type(strategy).__name__.lower(),
        "class": f"{type(strategy).__module__}.{type(strategy).__qualname__}",
        "strategy_version": strategy_version(strategy),
        "version": project_version(),
        "git": git(),
        "python": platform.python_version(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "mainnet": mainnet,
        "parameters_class": f"{type(params).__module__}.{type(params).__qualname__}" if params is not None else None,
        "parameters": jsonable(params) if params is not None else {},
        "exchanges": jsonable(exchanges or {}),
    }
    tag = os.getenv("MM_TAG")
    if tag:
        record["mm_tag"] = tag
    if extra:
        record.update(jsonable(extra))
    return record


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """{"A": {"B": [1, {"C": 2}]}} -> {"A.B[0]": 1, "A.B[1].C": 2}; an empty dict / list stays a leaf"""
    out: dict[str, Any] = {}
    if isinstance(value, dict) and value:
        for k, v in value.items():
            out.update(flatten(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(value, list) and value:
        for i, v in enumerate(value):
            out.update(flatten(v, f"{prefix}[{i}]"))
    else:
        out[prefix] = value
    return out


def short(value: Any, width: int = WIDTH) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= width else text[:width - 1] + "…"


def changes(previous: dict | None, current: dict) -> list[str]:
    """human readable differences between two records (code, mode, preset, parameters, exchange options)"""
    if not previous:
        return []
    out = []
    def code(r: dict) -> str:
        g = r.get("git") or {}
        sv = r.get("strategy_version") or {}
        return " ".join(x for x in (r.get("version"), sv.get("version"), sv.get("build"), g.get("commit"),
                                    "+modifs" if g.get("dirty") else "") if x)
    if code(previous) != code(current):
        out.append(f"code: {code(previous) or '?'} → {code(current) or '?'}")
    for key in ("mainnet", "parameters_class", "class", "host", "mm_tag"):
        if previous.get(key) != current.get(key):
            out.append(f"{key}: {short(previous.get(key))} → {short(current.get(key))}")
    for section in ("parameters", "exchanges"):
        old, new = flatten(previous.get(section) or {}), flatten(current.get(section) or {})
        head = "" if section == "parameters" else "exchanges."
        for path in sorted(set(old) | set(new)):
            if path not in new:
                out.append(f"- {head}{path} (was {short(old[path])})")
            elif path not in old:
                out.append(f"+ {head}{path} = {short(new[path])}")
            elif old[path] != new[path]:
                out.append(f"{head}{path}: {short(old[path])} → {short(new[path])}")
    return out


def headline(record: dict) -> str:
    g = record.get("git") or {}
    sv = record.get("strategy_version") or {}
    parts = [f"▶️ Launch {record['strategy']}"]
    if sv.get("version"):
        parts.append(sv["version"])
    parts.append(f"trador {record.get('version', '?')}")
    if g:
        parts.append(f"git {g.get('commit')}@{g.get('branch')}" + (" (uncommitted changes)" if g.get("dirty") else ""))
    else:
        parts.append("git ?")
    if record.get("mainnet") is not None:
        parts.append("mainnet" if record["mainnet"] else "TESTNET")
    if record.get("parameters_class"):
        parts.append(record["parameters_class"].removeprefix("strategies."))
    parts.append(record.get("host", ""))
    return " · ".join(p for p in parts if p)


def path_for(label: str, root: Path = RUNS) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in label) or "strategy"
    return root / f"{safe}.json"


def load(label: str, root: Path = RUNS) -> dict | None:
    try:
        with open(path_for(label, root), encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def save(record: dict, diff: list[str], root: Path = RUNS):
    root.mkdir(parents=True, exist_ok=True)
    target = path_for(record["strategy"], root)
    tmp = target.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=1)
    os.replace(tmp, target)
    with open(root / "history.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(dict(record, changes=diff), ensure_ascii=False) + "\n")


async def log_run(logger, strategy, label: str | None = None, mainnet: bool | None = None,
                  exchanges: dict | None = None, root: Path = RUNS, notify: bool = True) -> dict:
    """writes the record in logs.txt (complete), the console and Telegram (headline + changes), data/runs"""
    record = describe(strategy, label, mainnet, exchanges)
    previous = load(record["strategy"], root)
    diff = changes(previous, record)
    head = headline(record)
    if previous is None:
        summary = "first recorded run of this strategy (reference for the next launches)"
    elif diff:
        since = previous.get("date", "?")
        summary = f"{len(diff)} change(s) since the launch of {since}:\n" + "\n".join(
            f"  {line}" for line in diff[:SHOWN])
        if len(diff) > SHOWN:
            summary += f"\n  … {len(diff) - SHOWN} more (data/runs/history.jsonl)"
    else:
        summary = f"no code or parameter change since the launch of {previous.get('date', '?')}"
    await logger.info(f"{head}\n{summary}", send_tg=notify)
    # everything, one line each, grep-friendly: "RUN parameters {...}"
    await logger.info("RUN parameters " + json.dumps(record["parameters"], ensure_ascii=False, sort_keys=True),
                      display=False)
    await logger.info("RUN context " + json.dumps({k: v for k, v in record.items() if k != "parameters"},
                                                  ensure_ascii=False, sort_keys=True), display=False)
    if len(diff) > SHOWN:
        await logger.info("RUN changes " + json.dumps(diff, ensure_ascii=False), display=False)
    save(record, diff, root)
    return record
