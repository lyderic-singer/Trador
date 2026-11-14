# Lighter

Moteur `core/lighter` + adapter `adapters/lighter.py`. Doc officielle : https://docs.lighter.xyz (API : `/api`).
Les stratégies ne voient que l'interface `Exchange` ; tout ce qui suit est géré dans l'intégration.

## Mise en route

1. **Signer** : Lighter ne signe pas avec une clé Ethereum (Schnorr sur ECgFp5, hash Poseidon2). La seule implémentation
   de référence est le signer Go de l'exchange, livré compilé dans le SDK Python officiel. Il n'est **pas** dans les
   dépendances du projet (rien ne change pour un serveur qui ne trade pas Lighter) :
   ```shell
   uv add --group lighter lighter-sdk     # une fois, sur la machine de dev : met à jour pyproject.toml et uv.lock, à commit
   uv sync --group lighter                # sur le serveur qui trade Lighter (un `uv sync` nu le désinstalle, `uv run` non)
   ```
   Seules les fonctions `sign_*` du SDK sont utilisées (`core/lighter/utils/signing.py`, classe `SdkSigner`) ; REST,
   websocket et nonces sont ceux du projet. Linux x86-64 / arm64, macOS et Windows x86-64 sont fournis par le SDK.
2. **Compte** : créer une clé API sur app.lighter.xyz (index **4 à 254** ; 0-3 sont ceux des applis Lighter) et noter
   l'index du compte (un sous-compte a le sien).
3. **`.env`** :
   | Variable | Rôle |
   |---|---|
   | `LIGHTER_MAINNET_ACCOUNT` | index du compte (ou du sous-compte) |
   | `LIGHTER_MAINNET_API_KEY` | index de la clé API |
   | `LIGHTER_MAINNET_PRIVATE_KEY` | clé privée de cette clé API (pas la clé Ethereum) |
   | `LIGHTER_MAINNET_TAKER_API_KEY`, `LIGHTER_MAINNET_TAKER_PRIVATE_KEY` | optionnel : 2ᵉ clé pour les IOC (voir « files par clé ») |
   Sans clé privée le client démarre en lecture seule (prix) ; sans compte, données publiques seulement.
4. **Vérifier en réel** avant toute stratégie : `uv run python -m tools.check_exchange lighter SPY --order --ioc`
   (aucune position prise), puis `--ws` avant d'utiliser le transport websocket.
5. **Lancer** : `STRATEGY=<stratégie>_lighter` (voir `src/main.py`), Lighter y remplace Arcus.

## Différences avec Hyperliquid / Arcus, et ce qu'en fait l'intégration

| Sujet | Lighter | Géré comment |
|---|---|---|
| Réponse à un ordre | une transaction signée ; la réponse (code 200) veut dire **acceptée**, pas exécutée | ordre au repos : `Result.ok` tout de suite, statut `accepted`, la suite par le flux d'ordres (comme un ACK Arcus). **IOC** : l'adapter attend la fin réelle de l'ordre (au plus `ioc_timeout`, 4 s) et rend taille remplie et prix moyen comme Hyperliquid ; sans nouvelle, `ok` avec 0 rempli, que la couverture traite déjà comme « inconnu, compté rempli jusqu'aux fills » |
| Refus après acceptation | post-only qui croiserait, marge… : l'ordre est annulé à l'arrivée (`canceled-post-only`…) ; une transaction peut aussi être rejetée par le séquenceur | `OrderUpdate` `rejected` (raison dans `raw["rejectionReason"]`) ; canal `account_tx` pour les transactions échouées |
| Client id | entier **48 bits**, unique | les 48 bits de poids faible de l'id hexadécimal de la stratégie ; table index → id complet pour les fills et mises à jour (perdue au redémarrage : les ordres restés ouverts reviennent sans client id) |
| Id d'ordre | l'index d'ordre est attribué plus tard par le séquenceur | l'id donné aux stratégies est l'index client (accepté partout où l'API prend un index d'ordre) ; un ordre posé hors du bot garde son index d'exchange |
| Nonce | un compteur **par clé API** ; une transaction refusée par l'API ne le consomme pas ; un trou bloque toutes les suivantes | compteur local par clé, relu sur `nextNonce` après tout échec ou issue inconnue (jamais deviné), comparé à l'exchange quand la clé est au repos ; une seule instance du bot par clé API |
| Files par clé | une transaction ne s'exécute jamais avant les précédentes de la même clé ; les ordres taker sont retardés (voir paliers) | clé « taker » optionnelle : les IOC partent sur leur propre clé et ne retardent pas les cotations |
| Modify | garde l'ordre ; sens de `base_amount` non documenté pour un ordre partiellement rempli | ordre intact : modify natif (même id). Ordre partiellement rempli : cancel + nouvel ordre dans **le même lot** (nouvel id, l'ancien est signalé `replaced`) |
| Cancel | gratuit (pas de quota), toujours « accepté » même si l'ordre n'existe plus | `ok` ; c'est le flux qui dit ce qui s'est passé |
| Tailles et prix | entiers : prix × 10^`price_decimals`, taille × 10^`size_decimals` ; prix sur 32 bits | arrondis par l'adapter, refus local sinon |
| Minimums | `min_base_amount` **et** `min_quote_amount` (ordres au repos) | `Instrument.min_size` / `min_notional`, contrôlés avant envoi ; un reduce-only peut être plus petit |
| Expiration | tout ordre au repos expire (5 min à 30 jours) | 28 jours (Arcus : 40) |
| Protection de prix | `sendTx` refuse un prix limite à plus de 5 % au travers du mark ; `sendTxBatch` ne fait pas ce contrôle | laissé par défaut |
| Funding | payé **chaque heure**, taux en **pourcent** dans `market_stats` | rendu en fraction par heure (`/100`), prochain paiement = prochaine heure pile |
| Marchés fermés (RWA) | SPY, QQQ, NVDA… cotent 24 h/24 ; pas d'indicateur « fermé » dans l'API | `Instrument.closed` et `Funding.closed` toujours `False` : la logique « hors séance » pilotée par ces champs (bornes, marge relevée d'Arcus) ne se déclenche pas ; les calendriers propres aux stratégies continuent de s'appliquer |
| Frais | selon le palier du compte, pas par marché | par défaut ceux du palier lu au démarrage (Standard 0 / 0, Premium 0,4 / 2,8 pb sans LIT staké) ; `LIGHTER = {"maker_fee": .., "taker_fee": ..}` dans `main.py` pour un palier réduit. Le frais réel de chaque fill vient du trade (taux en millionièmes du notionnel) |
| Compte | positions et soldes poussés par websocket ; `GET /account` pèse 300 | état tenu par le flux ; REST au démarrage, toutes les `poll` s (15) et à la demande au plus une fois par `account_interval` (2 s) |
| Levier inconnu | un marché jamais réglé prend le levier par défaut du marché | rapporté tel quel (les trackers supposeraient 1x) |
| Budget d'actions | **quota de volume** (Premium / Plus) : 1 unité par ordre placé ou modifié et par cancel-all, +1 par ~2 $ tradés, départ 1 000, plafond 5 M, 1 transaction gratuite toutes les 15 s | `budget()` = `{"order": quota, "cancel": ∞}` ; `None` en Standard |
| Dead man's switch | cancel-all **programmé**, tout le compte ; chaque appel coûte 1 unité de quota | `schedule_cancel(s)` le reprogramme, `None` l'abandonne |
| Bougies | 1m, 5m, 15m, 30m, 1h, 4h, 12h, 1d | les autres (3m, 2h, 8h) sont reconstruites depuis la plus grande native qui les divise |
| Volume `/vol` | l'exchange donne jour / semaine / mois / total | pas de part maker, ni nombre de fills, ni frais |
| Transport des ordres | REST (`sendTx`, lots de 15) ou websocket (`jsonapi/sendtx`, lots de 50) | **REST par défaut** : la clé est tenue jusqu'à la réponse (deux requêtes d'une même clé doivent arriver dans l'ordre des nonces). `"transport": "ws"` : la clé n'est tenue que le temps d'écrire la trame, réponses attendues en parallèle, REST si le socket est coupé à l'envoi |

## Paliers de compte : à lire avant de coter

| | Standard | Premium | Plus |
|---|---|---|---|
| Frais maker / taker | 0 / 0 | 0,4 / 2,8 pb | 0,5 / 0,5 pb |
| Délai maker / cancel / taker | 200 / 300 / 300 ms | 0 / 0 / 140-200 ms | 60 / 75 / 100 ms |
| Limite REST | **60 requêtes / minute, ordres compris** | 24 000 de poids / minute (plus avec du LIT staké) | idem |
| Envoi d'ordres | dans les 60 req / min | 4 000 requêtes / minute + quota de volume | idem |

Un compte **Standard ne peut pas faire de market making** (60 requêtes par minute tout compris) : le client le dit au
démarrage. Changer de palier demande un compte sans position ni ordre.

**Le quota de volume démarre à 1 000** : avec les seuils par défaut des stratégies (réserve de 5 000 actions pensée pour
Arcus / HL), un compte neuf est bridé tant que le quota n'a pas monté. Soit acheter du quota (500 USDC le million),
soit laisser le volume le construire (+1 par ~2 $ tradés), soit adapter les seuils dans les paramètres de la stratégie.

## Ce qui n'a PAS pu être vérifié en réel

Développé sans accès réseau à l'exchange ni au SDK : tout est testé contre un simulateur écrit depuis la doc
(`tests/lighter_sim.py`). À contrôler avec `tools.check_exchange` puis sur un petit compte :

- l'appel du SDK (`lighter.SignerClient(url=, account_index=, api_private_keys=)`, `sign_create_order(...)` etc. avec
  `nonce=` et `api_key_index=`) : écrit d'après le code source du dépôt `elliottech/lighter-python`, jamais exécuté ;
- le format exact de la réponse websocket à `jsonapi/sendtx` (d'où REST par défaut ; le parseur accepte l'`id` renvoyé
  à la racine ou dans `data`, sinon l'ordre d'arrivée) ;
- l'unité de `initial_margin_fraction` des positions (`"0.1"` et `"10.00"` sont tous deux acceptés), le sens de
  `direction` dans l'historique de funding (pris comme « le côté qui paie »), l'unité de `update_margin` (USDC × 10⁶) ;
- qu'un modify natif garde bien l'index client, et qu'un cancel-all programmé remplace le précédent ;
- le contenu de `accountLimits` (nom du palier), des canaux `account_tx` et `user_stats`. Pour `account_tx`, le statut
  0 est lu comme « échoué » ; par prudence il n'est cru que si l'ordre n'est jamais apparu sur le flux d'ordres dans la
  seconde (un ordre qui existe n'est jamais rejeté sur la foi d'un statut de transaction).
