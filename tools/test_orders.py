"""
Teste l'envoi d'ordres sur Arcus et Hyperliquid avec les clés du .env et les réglages de main.py (fundings).

    python -m tools.test_venues

Pour chaque venue : un ordre ALO (post-only) 3 % sous le bid, qui ne peut pas se remplir, puis son annulation.
Côté HL, il affiche aussi l'adresse qui signe (HL_MAINNET_PRIVATE_KEY) et vérifie qu'elle est bien autorisée à
trader pour HL_MAINNET_ADDRESS (sinon : "User or API Wallet ... does not exist").
"""
import asyncio
import secrets
import sys

from adapters.models import Cancel, Order, Side, TIF
from core.orchester import Orchester
from main import ARCUS, HL_FUNDINGS, MAINNET

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

TESTS = [("arcus", "QQQ-USD", ARCUS), ("hyperliquid", "xyz:XYZ100", HL_FUNDINGS)]
AWAY = 0.03  # 3 % sous le bid


async def check_hl_wallet(client):
    signer, user = client.account.address, client.address
    print(f"hyperliquid  signe avec {signer}, compte {user}")
    if client.readonly:
        print("hyperliquid  ❌ pas de clé HL dans le .env : lecture seule")
        return
    if signer.lower() == user.lower():
        print("hyperliquid  clé = wallet principal")
        return
    role = await client.info.post({"type": "userRole", "user": signer})
    agents = await client.info.post({"type": "extraAgents", "user": user}) or []
    approved = [a for a in agents if a.get("address", "").lower() == signer.lower()]
    print(f"hyperliquid  rôle de la clé : {role}")
    print(f"hyperliquid  agent approuvé pour le compte : {approved or '❌ NON'}")


async def test(orchester: Orchester, venue: str, symbol: str, options: dict):
    ex = orchester.exchange(venue, **options)
    await ex.start()
    inst = ex.instrument(symbol)
    bbo = await ex.book(symbol)
    price = inst.round_price(bbo.bid * (1 - AWAY), Side.BUY, passive=True)
    if not inst.bounded(price) and inst.lower:
        price = inst.round_price(inst.lower, Side.BUY, passive=True)
    size = inst.round_size(max(inst.min_notional, 10.0) * 1.2 / price)
    order = Order(symbol, Side.BUY, size, price, TIF.ALO, False, "0x" + secrets.token_hex(16))

    print(f"{venue:12} carnet {bbo.bid} / {bbo.ask} -> achat {size} @ {price} (~{size * price:.2f} $)")
    result = (await ex.place([order]))[0]
    print(f"{venue:12} {'✅' if result.ok else '❌'} place : {result.status or ''} oid={result.oid} {result.error or ''}")
    if not result.ok:
        return

    await asyncio.sleep(2)
    oid = result.oid or next((o.oid for o in await ex.open_orders(symbol) if o.client == order.client), None)
    if not oid:
        print(f"{venue:12} ⚠️ ordre introuvable pour l'annuler : vérifie à la main")
        return
    cancelled = (await ex.cancel([Cancel(symbol, oid, order.client)]))[0]
    print(f"{venue:12} {'✅' if cancelled.ok else '❌'} cancel : {cancelled.status or ''} {cancelled.error or ''}")


async def main():
    orchester = Orchester(MAINNET, False, [venue for venue, _, _ in TESTS])
    try:
        await orchester.start()
        await check_hl_wallet(orchester.hl)
        for venue, symbol, options in TESTS:
            try:
                await test(orchester, venue, symbol, options)
            except Exception as e:
                print(f"{venue:12} ❌ {type(e).__name__}: {e}")
    finally:
        await orchester.stop()


if __name__ == "__main__":
    asyncio.run(main())
