import asyncio
import dataclasses
import json
from dataclasses import dataclass, field

from core import run


@dataclass
class Leg:
    symbol: str = "SPY"
    size: float = 100.0


@dataclass
class Params:
    SPREAD: float = 2.0
    LEVELS: int = 3
    PAIRS: list = field(default_factory=lambda: [Leg(), Leg("QQQ", 50.0)])
    HOURS: tuple = (7, 15)


class Strategy:
    VERSION = "test v1"

    def __init__(self, p: Params):
        self.p = p


class Log:
    def __init__(self):
        self.lines = []

    async def info(self, text, display=True, save=True, send_tg=False, **_):
        self.lines.append((text, display, send_tg))


def test_describe_and_flatten():
    record = run.describe(Strategy(Params()), "demo", True, {"arcus": {"maker_fee": 0.0}})
    assert record["strategy"] == "demo" and record["strategy_version"] == {"version": "test v1"}
    assert record["parameters"]["PAIRS"][1] == {"symbol": "QQQ", "size": 50.0}
    flat = run.flatten(record["parameters"])
    assert flat["PAIRS[1].symbol"] == "QQQ" and flat["HOURS[0]"] == 7
    json.dumps(record)


def test_parameters_of_prefers_the_base_dataclass():
    s = Strategy(Params())
    s.base = Params(SPREAD=9.0)          # base = what it was built with, p = the active set
    assert run.parameters_of(s).SPREAD == 9.0
    s.base = (0.0, 0.0)                  # volume: base is a tuple, the parameters are in p
    assert run.parameters_of(s).SPREAD == 2.0


def test_changes_between_two_launches(tmp_path):
    log = Log()
    asyncio.run(run.log_run(log, Strategy(Params()), "demo", True, {"arcus": {"maker_fee": 0.0}}, root=tmp_path))
    assert "first recorded run" in log.lines[0][0]
    changed = dataclasses.replace(Params(), SPREAD=2.5, PAIRS=[Leg()])
    asyncio.run(run.log_run(log, Strategy(changed), "demo", True, {"arcus": {"maker_fee": 0.0001}}, root=tmp_path))
    text = [t for t, display, _ in log.lines if display][-1]
    assert "SPREAD: 2.0 → 2.5" in text
    assert '- PAIRS[1].symbol (was "QQQ")' in text
    assert "exchanges.arcus.maker_fee: 0.0 → 0.0001" in text
    asyncio.run(run.log_run(log, Strategy(changed), "demo", True, {"arcus": {"maker_fee": 0.0001}}, root=tmp_path))
    assert "no code or parameter change" in [t for t, display, _ in log.lines if display][-1]
    history = [json.loads(line) for line in (tmp_path / "history.jsonl").read_text().splitlines()]
    assert len(history) == 3 and history[1]["changes"] and not history[2]["changes"]
    assert json.loads((tmp_path / "demo.json").read_text())["parameters"]["SPREAD"] == 2.5
