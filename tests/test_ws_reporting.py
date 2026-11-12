"""a websocket cut that the first retry repairs is not reported; a failed retry or flapping connections are"""
import asyncio
import time

from core.arcus.websocket import ArcusWebSocket
from core.HL.websocket import HLWebSocket
from log.messenger import Messenger


def drain(messenger) -> list:
    out = []
    while not messenger.messages.empty():
        m = messenger.messages.get_nowait()
        out.append((m.level.value, m.text, m.display, m.send_tg))
    return out


def errors(messages) -> list:
    return [m for m in messages if m[0] == "ERROR"]


def scenario(ws):
    async def go():
        m = ws._log.messenger
        ws.running = True
        ws.connects = 1
        await ws.report_connected()
        assert drain(m)[0][1] == "connected"
        # cut, the first retry works: nothing visible, one line in logs.txt
        await ws.report_disconnected(ConnectionError("1006"))
        ws.connects = 2
        await ws.report_connected()
        out = drain(m)
        assert not errors(out) and len(out) == 1 and out[0][2] is False and not out[0][3]
        # cut, the retry fails: one error, then the end of the incident is reported (Telegram)
        await ws.report_disconnected(ConnectionError("1006"))
        await ws.report_disconnected(OSError("refused"))
        await ws.report_disconnected(OSError("refused"))
        out = drain(m)
        assert len(errors(out)) == 1 and "reconnection failed" in errors(out)[0][1]
        ws.connects = 3
        await ws.report_connected()
        out = drain(m)
        assert out[0][0] == "SUCCESS" and out[0][3] and "reconnected" in out[0][1]
        assert ws.down_at == 0.0 and not ws.down_logged
        # stopping: never reported
        ws.running = False
        await ws.report_disconnected(ConnectionError("bye"))
        assert not drain(m)
    asyncio.run(go())


def test_hl_quiet_when_the_first_retry_works():
    scenario(HLWebSocket("wss://x", Messenger(None), None))


def test_arcus_quiet_when_the_first_retry_works():
    scenario(ArcusWebSocket("wss://x", Messenger(None)))


def test_flapping_connections_are_still_reported():
    async def go():
        hl = HLWebSocket("wss://x", Messenger(None), None)
        hl.running, hl.connects, hl.flaps = True, 5, 1      # reconnected, then died within flap_s
        await hl.report_disconnected(ConnectionError("1006"))
        assert errors(drain(hl._log.messenger))
        ar = ArcusWebSocket("wss://x", Messenger(None))
        ar.running, ar.connects, ar.since = True, 5, time.time() - 2
        ar.connected.set()                                   # the connection lived 2 s
        await ar.report_disconnected(ConnectionError("1006"))
        assert ar.flaps == 1 and errors(drain(ar._log.messenger))
        ar.connected.set()
        ar.since = time.time() - 3600                        # a connection that lived an hour: quiet again
        await ar.report_disconnected(ConnectionError("1006"))
        assert ar.flaps == 0
    asyncio.run(go())
