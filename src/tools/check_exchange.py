"""
Vérifie en réel l'intégration Lighter ou Aster avec les clés du .env, sans jamais prendre de position.

    uv run python -m tools.check_exchange lighter                 # lecture seule : marchés, prix, compte
    uv run python -m tools.check_exchange lighter SPY --order     # + un ordre post-only loin du marché : place, modify, cancel
    uv run python -m tools.check_exchange lighter SPY --order --ws    # idem, ordres par le websocket (Lighter seulement)
    uv run python -m tools.check_exchange aster SPY --order --ioc --switch
    uv run python -m tools.check_exchange lighter BTC --testnet --order

Ce que chaque option contrôle (tout ce que les tests hors ligne ne peuvent pas prouver) :

  (sans option)  clés acceptées, palier du compte, horloge, tick / pas / minimums / frais du marché, meilleur prix par le
                 websocket, carnet REST, bougies, funding, équité / marge libre / positions / leviers, ordres ouverts.
  --order        un achat post-only 3 % sous le bid à la taille minimale (jamais rempli) : accusé de réception, arrivée
                 de l'ordre sur le flux privé avec le bon id et le bon client id, présence dans open_orders, modify
                 (même id attendu), annulation, disparition. Temps de chaque étape.
  --ioc          un achat IOC 3 % sous le bid (ne peut pas matcher) : le résultat doit être « IOC could not match », et
                 montre le délai réel d'un ordre taker (speed bump Lighter).
  --switch       arme puis désarme le dead man's switch (schedule_cancel). Sur Lighter chaque appel coûte 1 unité de quota.
  --ws           Lighter : transport websocket pour les ordres (jsonapi/sendtx), à valider avant de le mettre en production.

Le script s'arrête à la première étape en échec avec le détail ; rien n'est laissé dans le carnet (cancel_all du marché
à la fin si --order).
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
    say(None, f"marché {name} : tick {inst.tick(1)}, taille min {inst.min_size}, notionnel min {inst.min_notional} $, frais "
              f"maker / taker {inst.maker_fee * 1e4:g} / {inst.taker_fee * 1e4:g} pb, levier max {inst.max_leverage}")
    if ex.name == "lighter":
        say(None, f"palier du compte : {client.tier or '?'} ; quota de volume : {client.exchange.quota} ; "
                  f"clé(s) API {sorted(client.private_keys)} ; nonces {[l.nonce for l in client.exchange.lanes.values()]}")
    bbos = []
    await ex.subscribe_bbo(name, bbos.append)
    took = await until(lambda: bbos and bbos[-1].bid and bbos[-1].ask)
    need(took is not None, f"meilleur prix par le websocket : {bbos[-1].bid if bbos else None} / {bbos[-1].ask if bbos else None}"
         + (f" en {took * 1000:.0f} ms, âge {(time.time() - bbos[-1].ts) * 1000:.0f} ms" if took is not None else " (rien reçu)"))
    book = await ex.book(name)
    need(book and book.bid and book.ask and book.bid < book.ask, f"carnet REST : {book.bid} / {book.ask}")
    need(abs(book.mid / bbos[-1].mid - 1) < 0.01, "carnet REST et websocket cohérents (< 1 %)")
    candles = await ex.candles(name, "1m", 5)
    need(len(candles) >= 3 and candles[-1].time > time.time() - 600 and candles[0].time < candles[-1].time,
         f"bougies 1m : {len(candles)}, dernière {time.strftime('%H:%M:%S', time.localtime(candles[-1].time)) if candles else '-'} "
         f"close {candles[-1].close if candles else '-'}")
    need(abs(candles[-1].close / book.mid - 1) < 0.02, "dernière bougie proche du carnet (< 2 %)")
    funding = (await ex.fundings([name])).get(name)
    need(funding is not None, f"funding : {funding.rate * 100:.5f} %/h payé, {funding.predicted * 100:.5f} %/h prévu, prochain à "
         f"{time.strftime('%H:%M', time.localtime(funding.next_at)) if funding and funding.next_at else '?'}" if funding else "funding : rien")
    if funding and funding.rate is not None and abs(funding.rate) > 0.001:
        say(False, "funding horaire > 0,1 %/h : unité à vérifier (voir lighter.md / aster.md)")
    say(None, f"mark {ex.mark(name)}")
    if getattr(client, "readonly", False):
        say(None, "pas de clé dans le .env : lecture seule, fin des contrôles")
        return None
    state = await ex.account([name])
    need(state.equity >= 0, f"compte : équité {state.equity:.2f} $, libre {state.free:.2f} $, marge {state.margin:.2f} $, "
         f"levier {name} {state.leverages.get(name)}, positions "
         f"{ {s: (p.size, p.entry) for s, p in state.positions.items()} or 'aucune'}")
    orders = await ex.open_orders(None)
    say(True, f"ordres ouverts : {len(orders)}" + (f" ({', '.join(f'{o.symbol} {o.side.value} {o.size}@{o.price}' for o in orders[:5])})" if orders else ""))
    budget = ex.budget()
    say(None, f"budget d'actions : {budget}")
    return book


async def order_cycle(ex, symbol: str, book):
    inst = ex.instrument(symbol)
    name = inst.symbol
    updates, fills = [], []
    need(await ex.subscribe_orders([name], updates.append), "flux privé des ordres souscrit")
    await ex.subscribe_fills([name], fills.append)
    price = inst.round_price(book.bid * (1 - AWAY), Side.BUY, passive=True)
    size = inst.round_size(max(inst.min_notional * 1.3 / price, inst.min_size))
    while size * price < inst.min_notional * 1.05:
        size = inst.round_size(size + inst.min_size)
    client = "0x" + secrets.token_hex(16)
    say(None, f"achat post-only {size} {name} @ {price} (~{size * price:.2f} $, {AWAY:.0%} sous le bid), client id {client}")
    t = time.perf_counter()
    result = (await ex.place([Order(name, Side.BUY, size, price, TIF.ALO, False, client)]))[0]
    need(result.ok, f"place : {result.status} oid={result.oid} en {(time.perf_counter() - t) * 1000:.0f} ms {result.error or ''}")
    try:
        took = await until(lambda: any(u.client == client and u.status == "open" for u in updates))
        seen = next((u for u in updates if u.client == client), None)
        need(took is not None, f"ordre vu sur le flux en {took * 1000:.0f} ms" if took is not None else
             f"ordre jamais vu « open » sur le flux (reçu : {[(u.oid, u.status, u.client) for u in updates][-3:]})")
        need(seen.oid == result.oid, f"même id dans la réponse et sur le flux ({result.oid} / {seen.oid})")
        need((seen.price, seen.size) == (price, size), f"prix et taille du flux = ceux envoyés ({seen.price}, {seen.size})")
        opened = [o for o in await ex.open_orders(name) if o.oid == result.oid]
        need(opened and opened[0].client == client and opened[0].tif == TIF.ALO,
             f"open_orders le rend avec son client id et son time in force ({opened[0].client if opened else None})")
        target = inst.round_price(price - 5 * inst.tick(price), Side.BUY, passive=True)
        t = time.perf_counter()
        moved = (await ex.modify([Modify(opened[0], size, target, TIF.ALO)]))[0]
        need(moved.ok, f"modify -> {target} : oid={moved.oid} en {(time.perf_counter() - t) * 1000:.0f} ms {moved.error or ''}")
        say(moved.oid == result.oid or None, f"le modify {'garde' if moved.oid == result.oid else 'change'} l'id de l'ordre")
        took = await until(lambda: any(u.oid == moved.oid and u.status == "open" and u.price == target for u in updates))
        need(took is not None, f"nouveau prix vu sur le flux en {(took or 0) * 1000:.0f} ms")
        t = time.perf_counter()
        gone = (await ex.cancel([Cancel(name, moved.oid, client)]))[0]
        need(gone.ok, f"cancel en {(time.perf_counter() - t) * 1000:.0f} ms {gone.error or ''}")
        took = await until(lambda: any(u.oid == moved.oid and u.closed for u in updates))
        need(took is not None, f"annulation vue sur le flux en {(took or 0) * 1000:.0f} ms (statut "
             f"{next((u.status for u in reversed(updates) if u.oid == moved.oid), None)})")
        await asyncio.sleep(0.5)
        need(not [o for o in await ex.open_orders(name) if o.client == client], "plus rien dans open_orders")
        need(not fills, "aucun fill")
    finally:
        try:
            left = [o for o in await ex.open_orders(name) if o.client == client or o.oid == result.oid]
            if left:
                await ex.cancel([Cancel(o.symbol, o.oid, o.client) for o in left])
                say(None, f"nettoyage : {len(left)} ordre(s) de test annulé(s)")
        except Exception as e:
            say(False, f"nettoyage impossible, vérifier le carnet à la main : {e}")


async def ioc_check(ex, symbol: str, book):
    inst = ex.instrument(symbol)
    price = inst.round_price(book.bid * (1 - AWAY), Side.BUY, passive=True)
    size = inst.round_size(max(inst.min_notional * 1.3 / price, inst.min_size))
    t = time.perf_counter()
    result = (await ex.place([Order(inst.symbol, Side.BUY, size, price, TIF.IOC, False, "0x" + secrets.token_hex(16))]))[0]
    took = (time.perf_counter() - t) * 1000
    need(not result.ok and not result.filled and "could not match" in (result.error or ""),
         f"IOC non matchable -> « {result.error or result.status} » en {took:.0f} ms (rempli {result.filled})")


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
        say(True, f"{args.venue} démarré ({'testnet' if args.testnet else 'mainnet'}), {len(ex.listings())} marchés, "
                  f"transport des ordres : {ex.transport}")
        book = await read_only(ex, symbol)
        if book is not None:
            if args.order:
                await order_cycle(ex, symbol, book)
            if args.ioc:
                await ioc_check(ex, symbol, book)
            if args.switch:
                ex.symbols.add(ex.instrument(symbol).symbol) if hasattr(ex, "symbols") else None
                need(await ex.schedule_cancel(60), "dead man's switch armé à 60 s")
                need(await ex.schedule_cancel(None), "dead man's switch désarmé")
        say(True, "terminé")
    except Failed:
        code = 1
        say(False, "arrêt : corriger ce point (ou me renvoyer cette sortie) avant de lancer une stratégie")
    except Exception as e:
        code = 1
        say(False, f"{type(e).__name__}: {e}")
    finally:
        await orchester.stop()
    raise SystemExit(code)


if __name__ == "__main__":
    asyncio.run(main())
