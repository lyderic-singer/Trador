# Enveloppe

## Idée

Market making « en enveloppe » autour d'une moyenne mobile exponentielle (EMA) du mid.
On pose une grille d'ordres limites post-only (ALO) de part et d'autre de l'EMA, puis on la recentre quand l'EMA bouge.
Le pari : le prix oscille autour de sa moyenne, les ordres éloignés se font remplir dans les excès et la position
revient vers zéro quand le prix revient à la moyenne.

La stratégie ne connaît qu'un `Exchange` : elle tourne telle quelle sur Hyperliquid ou Arcus.

```python
Enveloppe(orchester.exchange("hyperliquid", **HL), "mkts:US500")
Enveloppe(orchester.exchange("arcus", **ARCUS), "SPY-USD", Parameters(COOLDOWN=1.0))
```

## La grille

- Centre : `EMA(close)` calculée sur `WINDOW` bougies de `TIMEFRAME`, prolongée en temps réel par le mid.
- Premier niveau à `OFFSET × pas` de l'EMA, puis `LEVELS` niveaux par côté.
- Largeur totale d'un côté ≈ `SPREAD × EMA`.
- `ALPHA` espace les niveaux de façon géométrique : `ALPHA = 1` grille régulière, `ALPHA > 1` niveaux de plus en plus espacés.

## Les tailles

- Capacité d'un côté = `CAP × (equity × levier ∓ position)`, convertie en unités au mid.
  On ne peut jamais dépasser `CAP` du pouvoir d'achat, position comprise.
- Répartition géométrique : le dernier niveau (le plus éloigné) est `SKEW` fois plus gros que le premier.
  On charge davantage là où le retour à la moyenne est le plus probable.
- Un ordre sous `MIN_VALUE` dollars (ou sous le minimum de l'exchange) n'est pas envoyé.

## Le cycle (toutes les `POLL_DELAY` secondes)

1. Lecture des ordres ouverts, de la position et du marché.
2. Si un placement vient d'avoir lieu → on **modifie** pour réaligner tailles et prix.
3. Sinon, s'il manque des niveaux du côté actif → on **place** les niveaux manquants (en complétant le budget restant).
4. Sinon, si l'EMA a bougé de moins de `THRESHOLD` depuis la dernière modification → on **passe**.
5. Sinon → on **modifie** toute la grille autour de la nouvelle EMA.

Les ordres modifiés ne traversent jamais le carnet : un bid reste sous `mid × (1 − frais maker)`, un ask au-dessus de `mid × (1 + frais maker)`.

## Mode `INV`

Avec `INV = True`, on ne travaille qu'un seul côté à la fois :
- mid au-dessus de l'EMA → on gère les **bids** (on attend que le prix redescende),
- mid sous l'EMA → on gère les **asks**.

On évite ainsi d'accumuler dans le sens de la tendance courte.

## Paramètres (`parameters.py`)

| Paramètre | Défaut | Rôle |
|---|---|---|
| `MAKER_FEES` | `None` | frais maker utilisés pour le garde-fou de prix ; `None` = valeur de l'exchange |
| `SPREAD` | `0.005` | largeur d'un côté de la grille (0,5 % de l'EMA) |
| `OFFSET` | `0.0001` | décalage du premier niveau par rapport à l'EMA, en fraction du pas |
| `ALPHA` | `1` | facteur géométrique d'espacement des niveaux |
| `SKEW` | `2` | taille du dernier niveau / taille du premier |
| `LEVELS` | `24` | nombre d'ordres par côté |
| `CAP` | `0.85` | part maximale du pouvoir d'achat utilisée |
| `MIN_VALUE` | `10.5` | notionnel minimal d'un ordre en $ |
| `POLL_DELAY` | `10.1` | secondes entre deux cycles |
| `THRESHOLD` | `0.0001` | variation relative d'EMA qui déclenche un recentrage |
| `TIMEFRAME` | `"5m"` | unité des bougies pour l'EMA |
| `WINDOW` | `5` | période de l'EMA |
| `INV` | `True` | un seul côté actif, contre la tendance courte |
| `COOLDOWN` | `10.0` | secondes minimum entre deux requêtes à l'exchange (rate limit HL ; peut baisser sur Arcus) |
| `CLEANUP` | `False` | annule les ordres de l'instrument à l'arrêt de la stratégie |

## Risques

- **Tendance forte** : la grille accumule contre le mouvement jusqu'à `CAP`. Aucun stop-loss n'est intégré.
- **Gap / ouverture de marché** (actions, indices) : plusieurs niveaux peuvent être remplis d'un coup.
- **Arcus hors séance** : bandes de prix et marge initiale plus élevée quand le sous-jacent est fermé.
- **Latence** : un cycle de 10 s laisse les ordres décalés pendant les mouvements rapides.
