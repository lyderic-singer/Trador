"""
Live check of the Lighter or Aster integration with the keys of the .env, never taking a position.

    uv run python -m tools.check_exchange lighter                 # read only: markets, prices, account
    uv run python -m tools.check_exchange lighter SPY --order     # + a post-only order far from the market: place, modify, cancel
    uv run python -m tools.check_exchange lighter SPY --order --ws    # same, orders over the websocket (Lighter only)
    uv run python -m tools.check_exchange aster SPY --order --ioc --switch
    uv run python -m tools.check_exchange lighter BTC --testnet --order

What each option checks (everything the offline tests cannot prove):

  (no option)    keys accepted, account tier, clock, tick / step / minimums / fees of the market, best price from the
                 websocket, REST book, candles, funding, equity / free margin / positions / leverages, open orders.
  --order        a post-only buy 3 % under the bid at the minimum size (never filled): acknowledgement, the order on
                 the private stream with the right id and client id, present in open_orders, modify (same id
                 expected), cancel, gone. Timing of every step.
  --ioc          an IOC buy 3 % under the bid (cannot match): the result must be "IOC could not match"; shows the real
                 delay of a taker order (Lighter speed bump).
  --switch       arms then disarms the dead man's switch (schedule_cancel). On Lighter each call costs 1 quota unit.
  --ws           Lighter: websocket transport for orders (jsonapi/sendtx), to validate before using it in production.

The script stops at the first failing step with the details; nothing is left in the book (cancel of the test orders at
the end with --order).
"""
import argparse
import asyncio
import secrets
import sys
import time

from adapters import ADAPTERS
from adapters.models import Cancel, Modify, Order, Side, TIF
from core.orchester import Orchester

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

AWAY = 0.03


def say(ok: bool | None, text: str):
    print(f"{'✅' if ok else '❌' if ok is False else '  '} {text}", flush=True)


class Failed(Exception):
    pass


def need(condition, text: str):
    say(bool(condition), text)
    if not condition:
        raise Failed(text)


async def until(predicate, timeout: float = 8.0) -> float | None:
    """seconds until predicate() is true, None on timeout"""
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        if predicate():
            return time.perf_counter() - start
        await asyncio.sleep(0.02)
    return None


async def read_only(ex, symbol: str):
    client = ex.client
    inst = ex.instrument(symbol)
    name = inst.symbol
    say(None, f"market {name}: tick {inst.tick(1)}, min size {inst.min_size}, min notional ${inst.min_notional}, fees "
              f"maker / taker {inst.maker_fee * 1e4:g} / {inst.taker_fee * 1e4:g} bp, max leverage {inst.max_leverage}")
    if ex.name == "lighter":
        say(None, f"account tier: {client.tier or '?'}; volume quota: {client.exchange.quota}; "
                  f"API key(s) {sorted(client.private_keys)}; nonces {[l.nonce for l in client.exchange.lanes.values()]}")
    bbos = []
    await ex.subscribe_bbo(name, bbos.append)
    took = await until(lambda: bbos and bbos[-1].bid and bbos[-1].ask)
    need(took is not None, f"best price from the websocket: {bbos[-1].bid if bbos else None} / {bbos[-1].ask if bbos else None}"
         + (f" in {took * 1000:.0f} ms, age {(time.time() - bbos[-1].ts) * 1000:.0f} ms" if took is not None else " (nothing received)"))
    book = await ex.book(name)
    need(book and book.bid and book.ask and book.bid < book.ask, f"REST book: {book.bid} / {book.ask}")
    need(abs(book.mid / bbos[-1].mid - 1) < 0.01, "REST book and websocket agree (< 1 %)")
    candles = await ex.candles(name, "1m", 5)
    need(len(candles) >= 3 and candles[-1].time > time.time() - 600 and candles[0].time < candles[-1].time,
         f"1m candles: {len(candles)}, last {time.strftime('%H:%M:%S', time.localtime(candles[-1].time)) if candles else '-'} "
         f"close {candles[-1].close if candles else '-'}")
    need(abs(candles[-1].close / book.mid - 1) < 0.02, "last candle close to the book (< 2 %)")
    funding = (await ex.fundings([name])).get(name)
    need(funding is not None, f"funding: {funding.rate * 100:.5f} %/h paid, {funding.predicted * 100:.5f} %/h predicted, next at "
         f"{time.strftime('%H:%M', time.localtime(funding.next_at)) if funding and funding.next_at else '?'}" if funding else "funding: nothing")
    if funding and funding.rate is not None and abs(funding.rate) > 0.001:
        say(False, "hourly funding > 0.1 %/h: check the unit (see lighter.md / aster.md)")
    say(None, f"mark {ex.mark(name)}")
    if getattr(client, "readonly", False):
        say(None, "no key in the .env: read only, end of the checks")
        return None
    state = await ex.account([name])
    need(state.equity >= 0, f"account: equity ${state.equity:.2f}, free ${state.free:.2f}, margin ${state.margin:.2f}, "
         f"leverage {name} {state.leverages.get(name)}, positions "
         f"{ {s: (p.size, p.entry) for s, p in state.positions.items()} or 'none'}")
    orders = await ex.open_orders(None)
    say(True, f"open orders: {len(orders)}" + (f" ({', '.join(f'{o.symbol} {o.side.value} {o.size}@{o.price}' for o in orders[:5])})" if orders else ""))
    budget = ex.budget()
    say(None, f"action budget: {budget}")
    return book


async def order_cycle(ex, symbol: str, book):
    inst = ex.instrument(symbol)
    name = inst.symbol
    updates, fills = [], []
    need(await ex.subscribe_orders([name], updates.append), "private order stream subscribed")
    await ex.subscribe_fills([name], fills.append)
    price = inst.round_price(book.bid * (1 - AWAY), Side.BUY, passive=True)
    size = inst.round_size(max(inst.min_notional * 1.3 / price, inst.min_size))
    while size * price < inst.min_notional * 1.05:
        size = inst.round_size(size + inst.min_size)
    client = "0x" + secrets.token_hex(16)
    say(None, f"post-only buy {size} {name} @ {price} (~${size * price:.2f}, {AWAY:.0%} under the bid), client id {client}")
    t = time.perf_counter()
    result = (await ex.place([Order(name, Side.BUY, size, price, TIF.ALO, False, client)]))[0]
    need(result.ok, f"place: {result.status} oid={result.oid} in {(time.perf_counter() - t) * 1000:.0f} ms {result.error or ''}")
    try:
        took = await until(lambda: any(u.client == client and u.status == "open" for u in updates))
        seen = next((u for u in updates if u.client == client), None)
        need(took is not None, f"order seen on the stream in {took * 1000:.0f} ms" if took is not None else
             f"order never seen \"open\" on the stream (received: {[(u.oid, u.status, u.client) for u in updates][-3:]})")
        need(seen.oid == result.oid, f"same id in the response and on the stream ({result.oid} / {seen.oid})")
        need((seen.price, seen.size) == (price, size), f"price and size on the stream = those sent ({seen.price}, {seen.size})")
        opened = [o for o in await ex.open_orders(name) if o.oid == result.oid]
        need(opened and opened[0].client == client and opened[0].tif == TIF.ALO,
             f"open_orders returns it with its client id and time in force ({opened[0].client if opened else None})")
        target = inst.round_price(price - 5 * inst.tick(price), Side.BUY, passive=True)
        t = time.perf_counter()
        moved = (await ex.modify([Modify(opened[0], size, target, TIF.ALO)]))[0]
        need(moved.ok, f"modify -> {target}: oid={moved.oid} in {(time.perf_counter() - t) * 1000:.0f} ms {moved.error or ''}")
        say(moved.oid == result.oid or None, f"modify {'keeps' if moved.oid == result.oid else 'changes'} the order id")
        took = await until(lambda: any(u.oid == moved.oid and u.status == "open" and u.price == target for u in updates))
        need(took is not None, f"new price seen on the stream in {(took or 0) * 1000:.0f} ms")
        t = time.perf_counter()
        gone = (await ex.cancel([Cancel(name, moved.oid, client)]))[0]
        need(gone.ok, f"cancel in {(time.perf_counter() - t) * 1000:.0f} ms {gone.error or ''}")
        took = await until(lambda: any(u.oid == moved.oid and u.closed for u in updates))
        need(took is not None, f"cancel seen on the stream in {(took or 0) * 1000:.0f} ms (status "
             f"{next((u.status for u in reversed(updates) if u.oid == moved.oid), None)})")
        await asyncio.sleep(0.5)
        need(not [o for o in await ex.open_orders(name) if o.client == client], "nothing left in open_orders")
        need(not fills, "no fill")
    finally:
        try:
            left = [o for o in await ex.open_orders(name) if o.client == client or o.oid == result.oid]
            if left:
                await ex.cancel([Cancel(o.symbol, o.oid, o.client) for o in left])
                say(None, f"cleanup: {len(left)} test order(s) cancelled")
        except Exception as e:
            say(False, f"cleanup failed, check the book by hand: {e}")


async def ioc_check(ex, symbol: str, book):
    inst = ex.instrument(symbol)
    price = inst.round_price(book.bid * (1 - AWAY), Side.BUY, passive=True)
    size = inst.round_size(max(inst.min_notional * 1.3 / price, inst.min_size))
    t = time.perf_counter()
    result = (await ex.place([Order(inst.symbol, Side.BUY, size, price, TIF.IOC, False, "0x" + secrets.token_hex(16))]))[0]
    took = (time.perf_counter() - t) * 1000
    need(not result.ok and not result.filled and "could not match" in (result.error or ""),
         f"unmatchable IOC -> \"{result.error or result.status}\" in {took:.0f} ms (filled {result.filled})")


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("venue", choices=sorted(set(ADAPTERS) - {"hyperliquid", "arcus"}))
    ap.add_argument("symbol", nargs="?")
    ap.add_argument("--testnet", action="store_true")
    ap.add_argument("--order", action="store_true")
    ap.add_argument("--ioc", action="store_true")
    ap.add_argument("--switch", action="store_true")
    ap.add_argument("--ws", action="store_true")
    args = ap.parse_args()
    symbol = args.symbol or "BTC"
    orchester = Orchester(not args.testnet, False, (args.venue,))
    code = 0
    try:
        await orchester.start()
        options = {"transport": "ws"} if args.ws and args.venue == "lighter" else {}
        ex = orchester.exchange(args.venue, **options)
        await ex.start()
        say(True, f"{args.venue} started ({'testnet' if args.testnet else 'mainnet'}), {len(ex.listings())} markets, "
                  f"order transport: {ex.transport}")
        book = await read_only(ex, symbol)
        if book is not None:
            if args.order:
                await order_cycle(ex, symbol, book)
            if args.ioc:
                await ioc_check(ex, symbol, book)
            if args.switch:
                ex.symbols.add(ex.instrument(symbol).symbol) if hasattr(ex, "symbols") else None
                need(await ex.schedule_cancel(60), "dead man's switch armed at 60 s")
                need(await ex.schedule_cancel(None), "dead man's switch disarmed")
        say(True, "done")
    except Failed:
        code = 1
        say(False, "stopped: fix this point before running a strategy")
    except Exception as e:
        code = 1
        say(False, f"{type(e).__name__}: {e}")
    finally:
        await orchester.stop()
    raise SystemExit(code)


if __name__ == "__main__":
    asyncio.run(main())
