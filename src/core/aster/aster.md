# Aster

Moteur `core/aster` + adapter `adapters/aster.py`, API futures **v3** (« Pro API »).
Doc officielle : https://asterdex.github.io/aster-api-website/futures-v3/general-info/
Les stratégies ne voient que l'interface `Exchange` ; tout ce qui suit est géré dans l'intégration.

## Mise en route

1. Créer un **API wallet** (« agent ») en mode **Pro API** sur https://www.asterdex.com/en/api-wallet ; le wallet
   principal doit avoir déposé au moins une fois. Passer le compte en **One-way Mode** (le client refuse de démarrer en
   Hedge Mode : les stratégies tiennent une position nette par marché).
2. **`.env`** :
   | Variable | Rôle |
   |---|---|
   | `ASTER_MAINNET_PRIVATE_KEY` | clé privée de l'API wallet (l'agent), jamais celle du wallet principal |
   | `ASTER_MAINNET_ADDRESS` | wallet principal (affichage ; envoyé comme `user` seulement si `ASTER_MAINNET_SEND_USER=1`) |
   | `ASTER_MAINNET_SIGNER` | optionnel : adresse de l'agent, contrôlée contre la clé |
   | `ASTER_MAINNET_API_URL`, `ASTER_MAINNET_WS_URL` | optionnel : autres hôtes (défaut `fapi.asterdex.com` / `fstream.asterdex.com`) |
   Aucune dépendance nouvelle (signature EIP-712 avec `eth-account`, déjà utilisé pour Hyperliquid).
3. **Vérifier en réel** avant toute stratégie : `uv run python -m tools.check_exchange aster SPY --order --ioc --switch`
   (aucune position prise).
4. **Lancer** : `STRATEGY=<stratégie>_aster` (voir `src/main.py`), Aster y remplace Arcus.

## Signature

Les paramètres de la requête, puis `nonce` (microsecondes) et `signer` (adresse de l'agent), sont url-encodés dans cet
ordre ; cette chaîne est le champ `msg` d'un message EIP-712 (domaine `AsterSignTransaction`, version 1, chainId 1666
en mainnet / 714 en testnet, type `Message{string msg}`) signé par la clé de l'agent. `signature` est ajoutée à la même
chaîne, envoyée telle quelle (query d'un GET / DELETE, corps d'un POST / PUT). L'exchange garde les 100 derniers
nonces d'un agent, refuse un doublon et tout nonce à plus de 60 s de son horloge : l'horloge est comparée au démarrage
puis toutes les 10 minutes, et les nonces corrigés au-delà de 2 s d'écart.

## Différences avec Hyperliquid / Arcus, et ce qu'en fait l'intégration

| Sujet | Aster | Géré comment |
|---|---|---|
| Envoi d'ordres | **HTTPS seulement** (pas d'ordres par websocket) ; 5 ordres, 5 modifies ou 10 cancels (un seul marché) par requête | une liste est coupée en lots envoyés en parallèle ; un ordre seul part seul |
| Réponse à un ordre | synchrone ; `newOrderRespType=RESULT` rend l'état final d'un IOC | `Result` porte taille remplie et prix moyen, comme Hyperliquid |
| Post-only | time in force `GTX` ; s'il croiserait, l'ordre revient `EXPIRED` | `Result.ok = False` (« post only would cross ») et `OrderUpdate` `rejected` |
| IOC non exécuté | `EXPIRED` avec 0 exécuté | échec « IOC could not match », jamais un ordre ouvert |
| Issue inconnue | 503 et timeouts : l'ordre a pu passer | exception levée (jamais « refusé ») : la couverture le compte rempli jusqu'aux fills |
| Client id | chaîne de 1 à 36 caractères | l'id hexadécimal des stratégies (34 caractères) passe tel quel |
| Modify | garde l'id mais **perd toujours la priorité de file**, même pour une baisse de taille ; prend la quantité **totale** | l'adapter ajoute le déjà-rempli à la taille restante demandée ; ordres LIMIT seulement |
| Ordre disparu | codes -2011 / -2013 | préfixe `NOT_FOUND` pour que les trackers l'oublient |
| Limites de prix | filtre `PERCENT_PRICE` : achat ≤ mark × up, vente ≥ mark × down | contrôlé avant envoi (refus local), sans brider le côté légal |
| Minimums | `LOT_SIZE.minQty` et `MIN_NOTIONAL` (5 $) | `Instrument.min_size` / `min_notional` |
| Funding | payé toutes les **4 ou 8 heures** selon le marché, pas chaque heure | taux rendus **par heure** (taux de la période / heures de la période) pour comparer les venues ; le paiement réel reste par période : une position ouverte et refermée entre deux paiements ne touche rien |
| Marchés fermés | pas d'indicateur de séance | `Instrument.closed` / `Funding.closed` toujours `False` (même remarque que Lighter) |
| Frais | par compte et par marché (`commissionRate`) | lus pour chaque marché suivi et utilisés à la place de ceux configurés (défaut 1 / 3,5 pb) ; un fill payé dans un autre actif que USDT / USD1 / USDC a `fee = None` |
| Compte | `ACCOUNT_UPDATE` ne porte que ce qui change | relu en REST après chaque évènement du flux (groupés sur 150 ms) et toutes les `poll` s (10) |
| Flux privé | `listenKey` (60 min, prolongée toutes les 30) ; connexions coupées à 24 h | nouvelle clé à chaque reconnexion ; fills manqués relus en REST (`userTrades`) sur les marchés suivis |
| Dead man's switch | `countdownCancelAll`, **par marché** | `schedule_cancel` l'arme sur chaque marché suivi ou tradé |
| Budget d'actions | limites par minute (2 400 de poids par IP, 1 200 ordres par compte), pas de réserve cumulée | `budget()` = `None` ; un 429 est levé avec son `Retry-After` (le Gateway suspend les envois) ; 418 = IP bannie pour avoir insisté |
| Marge | cross par défaut, isolée par marché ; mode multi-actifs possible | `set_leverage(symbol, x, cross)` règle le type de marge puis le levier ; l'équité est `totalMarginBalance`, comptée en dollars |
| Volume `/vol` | pas d'endpoint de volume du compte (`userTrades` est par marché, par tranches de 7 jours) | « volume not available » |
| Marchés | perpétuels `BTCUSDT`, `SPYUSDT`… | trouvés par symbole, par base (`SPY`, quote USDT en premier) ou par le nom d'une autre venue (`SPY-USD`) |

## Ce qui n'a PAS pu être vérifié en réel

Développé sans accès réseau à l'exchange : tout est testé contre un simulateur écrit depuis la doc
(`tests/aster_sim.py`). À contrôler avec `tools.check_exchange` puis sur un petit compte :

- **la signature** : conforme à l'exemple de la doc (ordre des champs, `msg`, domaine) et vérifiée par recouvrement de
  l'adresse, mais jamais présentée à l'exchange. La doc liste `user` parmi les paramètres sans l'envoyer dans son
  exemple : il n'est pas envoyé par défaut (`ASTER_MAINNET_SEND_USER=1` pour l'ajouter si l'exchange répond -1022) ;
- l'hôte (`fapi.asterdex.com` ; l'exemple de la doc utilise `fapi3.asterdex.com`) et l'URL websocket `…/ws` avec
  souscription à chaud ;
- que la réponse `RESULT` d'un IOC est bien finale (sinon : `ok` avec 0 rempli, résolu par les fills) ;
- le contenu exact de `/account` (présence de tous les marchés avec leur levier) et de `leverageBrackets` ;
- les frais par défaut (1 / 3,5 pb) : remplacés par ceux du compte dès qu'un marché est suivi.
