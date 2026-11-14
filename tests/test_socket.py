import asyncio
import core.socket as module
from core.socket import Socket
from log.messenger import Messenger


class Connection:
    """a websocket that delivers its frames, then closes (or raises)"""

    def __init__(self, frames, error=None, hold=False):
        self.frames, self.error, self.hold = list(frames), error, hold
        self.sent, self.closed = [], asyncio.Event()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return self.frames.pop(0)
        if self.hold:
            await self.closed.wait()
        if self.error:
            raise self.error
        raise StopAsyncIteration

    async def send(self, text):
        self.sent.append(text)

    async def close(self):
        self.closed.set()


class Echo(Socket):
    def __init__(self):
        super().__init__("wss://first", Messenger(None), "Echo WebSocket")
        self.got, self.opens, self.closes, self.urls = [], 0, 0, []

    async def endpoint(self):
        self.urls.append(f"wss://host/{len(self.urls) + 1}")
        return self.urls[-1]

    async def opened(self):
        self.opens += 1
        await self.ws.send("subscribe")

    def closed(self):
        self.closes += 1

    async def handle(self, raw):
        self.got.append(raw)


def drain(messenger):
    out = []
    while not messenger.messages.empty():
        m = messenger.messages.get_nowait()
        out.append((m.level.value, m.text))
    return out


def test_reconnects_resubscribes_and_tells_the_listeners(monkeypatch):
    async def go():
        connections = [Connection(["a", "b"]), OSError("refused"), Connection(["c"], hold=True)]
        asked = []

        def connect(url, **options):
            asked.append((url, options))
            c = connections.pop(0)
            if isinstance(c, Exception):
                raise c
            return c
        monkeypatch.setattr(module.websockets, "connect", connect, raising=False)
        ws = Echo()
        down, up = [], []
        ws.disconnect_listeners.append(lambda: down.append(1))
        ws.connect_listeners.append(lambda: up.append(1))
        await ws.start()
        await asyncio.sleep(5)
        # first connection: frames handled, then the server closes; the retry is refused; the next one works
        assert ws.got == ["a", "b", "c"] and ws.opens == 2 and ws.connects == 2 and ws.connected.is_set()
        assert [u for u, _ in asked] == ["wss://host/1", "wss://host/2", "wss://host/3"]    # the URL is asked each time
        assert asked[0][1]["compression"] is None and asked[0][1]["ping_interval"] == 20.0
        assert down == [1] and up == [1]                  # told once of the cut, once of the RE-connection
        levels = [level for level, _ in drain(ws._log.messenger)]
        assert "ERROR" in levels and levels[-1] == "SUCCESS"   # the failed retry made it an incident, its end is announced
        await ws.send("x")
        await ws.stop()
        assert not ws.connected.is_set() and ws.task is None
        try:
            await ws.send("y")
            raise AssertionError("send on a stopped socket")
        except ConnectionError:
            pass
    asyncio.run(go())


def test_start_fails_when_nothing_answers(monkeypatch):
    async def go():
        def connect(url, **options):
            raise OSError("unreachable")
        monkeypatch.setattr(module.websockets, "connect", connect, raising=False)
        ws = Echo()
        try:
            await ws.start(timeout=3)
            raise AssertionError("started without connection")
        except ConnectionError as e:
            assert "Echo WebSocket" in str(e)
        assert not ws.running and ws.task is None
    asyncio.run(go())


def test_lighter_socket_reports_like_the_others():
    from core.lighter.websocket import LighterWebSocket
    from tests.test_ws_reporting import scenario
    scenario(LighterWebSocket("wss://x", Messenger(None)))
