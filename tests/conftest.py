"""Virtual time for the test suite.

The tests drive real strategies with `asyncio.run(...)` and hundreds of `await asyncio.sleep(...)`: run on the wall
clock the suite spends ~11 minutes doing nothing but waiting. Here the clock is the one of the tests: whenever the event
loop has nothing left to run and would block until its next timer, that wait is skipped and the clock jumps forward by
exactly that amount. Every clock a test or the code can read follows: `time.time()`, `time.monotonic()` (the clock of
asyncio), their `_ns` versions, `time.gmtime()` / `localtime()` / `strftime()` without argument and `datetime.now()`.
So a `sleep(2)`, a `wait_for(..., 5)` or a `time.time() - t0 > 30` behave exactly as before, in the same order, only
without the waiting; `time.perf_counter()` (latency measures) stays real. Nothing in src/ or in the tests is changed.

Real time is kept whenever something real is in flight, so nothing can be skipped over:
- the test belongs to a module listed in REALTIME below (test_order_entry.py: real local websocket servers), or
  carries `@pytest.mark.realtime`;
- a socket is registered on the loop;
- a thread started by `run_in_executor` / `asyncio.to_thread` has not finished;
- the loop has no timer at all (it waits for an external event).

`TRADOR_REAL_TIME=1 uv run pytest` runs the suite on the wall clock, as before.
"""
import asyncio
import datetime as _datetime
import os
import selectors
import time
import pytest

# test modules that talk to real local sockets: they always run on the wall clock
REALTIME = {"test_order_entry"}
REAL = os.getenv("TRADOR_REAL_TIME", "").strip().lower() in ("1", "true", "yes")


class Clock:
    offset = 0.0        # seconds skipped so far
    busy = 0            # executor jobs in flight
    frozen = False      # the running test asked for the wall clock
    skipped = 0         # waits skipped (for the report at the end)


_time, _monotonic, _time_ns, _monotonic_ns = time.time, time.monotonic, time.time_ns, time.monotonic_ns


def _now() -> float:
    return _time() + Clock.offset


def _mono() -> float:
    return _monotonic() + Clock.offset


def _now_ns() -> int:
    return _time_ns() + int(Clock.offset * 1e9)


def _mono_ns() -> int:
    return _monotonic_ns() + int(Clock.offset * 1e9)


_gmtime, _localtime, _strftime, _ctime = time.gmtime, time.localtime, time.strftime, time.ctime


def _gm(secs=None):
    return _gmtime(_now() if secs is None else secs)


def _local(secs=None):
    return _localtime(_now() if secs is None else secs)


def _format(fmt, t=None):
    return _strftime(fmt, _localtime(_now()) if t is None else t)


def _c(secs=None):
    return _ctime(_now() if secs is None else secs)


class _Instance(type):
    """instances of the real datetime (built by C code or before the patch) still pass isinstance(x, datetime)"""

    def __instancecheck__(cls, obj):
        return isinstance(obj, _RealDatetime)

    def __subclasscheck__(cls, sub):
        return issubclass(sub, _RealDatetime)


_RealDatetime = _datetime.datetime


class VirtualDatetime(_RealDatetime, metaclass=_Instance):
    @classmethod
    def now(cls, tz=None):
        return cls.fromtimestamp(_now(), tz)

    @classmethod
    def utcnow(cls):
        return cls.fromtimestamp(_now(), _datetime.timezone.utc).replace(tzinfo=None)

    @classmethod
    def today(cls):
        return cls.fromtimestamp(_now())


class WarpSelector(selectors.DefaultSelector):
    """`select(timeout)` is where an idle event loop sleeps until its next timer: skip that sleep"""

    def select(self, timeout=None):
        events = super().select(0)
        if events or (timeout is not None and timeout <= 0):
            return events
        # only the loop's own wake-up pipe is registered when no real I/O is pending
        if timeout is None or Clock.frozen or Clock.busy > 0 or len(self.get_map()) > 1:
            return super().select(timeout)
        Clock.offset += timeout
        Clock.skipped += 1
        return events


class WarpLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__(WarpSelector())

    def run_in_executor(self, executor, func, *args):
        Clock.busy += 1
        future = super().run_in_executor(executor, func, *args)

        def done(_):
            Clock.busy -= 1
        future.add_done_callback(done)
        return future


if not REAL:
    time.time, time.monotonic, time.time_ns, time.monotonic_ns = _now, _mono, _now_ns, _mono_ns
    time.gmtime, time.localtime, time.strftime, time.ctime = _gm, _local, _format, _c
    # conftest.py is imported before any test module or src package: their `from datetime import datetime` gets this one
    _datetime.datetime = VirtualDatetime
    # asyncio.run() -> Runner -> events.new_event_loop(): every loop of the suite is a WarpLoop
    asyncio.events.new_event_loop = asyncio.new_event_loop = WarpLoop


def pytest_configure(config):
    config.addinivalue_line("markers", "realtime: run this test on the wall clock (no skipped waits)")


@pytest.fixture(autouse=True)
def _clock(request):
    Clock.frozen = request.module.__name__.rsplit(".", 1)[-1] in REALTIME or request.node.get_closest_marker("realtime") is not None
    yield
    Clock.frozen = False


def pytest_report_header(config):
    if REAL:
        return "clock: real time (TRADOR_REAL_TIME=1)"
    return "clock: virtual time, idle waits are skipped (tests/conftest.py; TRADOR_REAL_TIME=1 for the wall clock)"


def pytest_terminal_summary(terminalreporter):
    if not REAL and Clock.skipped:
        terminalreporter.write_line(f"virtual time: {Clock.offset:.0f} s of idle waiting skipped ({Clock.skipped} waits)")
