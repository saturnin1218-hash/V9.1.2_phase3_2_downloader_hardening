# Colonnes de sortie, sidecars et manifests

Ce document décrit de manière opérationnelle ce que produit le pipeline natif V9.1.2, comment lire les colonnes, et quels artefacts JSON vérifier après un run.

## 1. Vue d’ensemble des datasets natifs

Selon le mode choisi, le pipeline peut écrire :

- `native_raw_trades.csv` : trades bruts téléchargés / fournis en entrée
- `native_funding.csv` : funding Binance Futures
- `native_metrics.csv` : métriques open interest / sidecars dérivés
- `native_liquidations.csv` : liquidations brutes
- `native_merged.csv` : features + sidecars déjà alignés temporellement
- `native_features.csv` : dataset feature engineering principal
- `native_export.csv` ou `.parquet` : export final prêt à consommer

## 2. Colonnes du dataset feature principal

### 2.1 Colonnes de marché agrégées

- `timestamp` : fin de barre UTC.
- `open` : premier prix observé dans la fenêtre.
- `high` : plus haut de la fenêtre.
- `low` : plus bas de la fenêtre.
- `close` : dernier prix de la fenêtre.

### 2.2 Colonnes d’activité

- `trade_count` : nombre de trades dans la barre.
- `quantity_sum` : somme des quantités échangées.
- `notional_sum` : somme des notionnels `price * quantity`.

### 2.3 Déséquilibres acheteurs / vendeurs

- `buy_notional` : notionnel des trades côté acheteur agressif.
- `sell_notional` : notionnel des trades côté vendeur agressif.
- `signed_notional_imbalance` : `(buy_notional - sell_notional) / notional_sum`.
- `large_buy_notional` : notionnel acheteur des gros trades.
- `large_sell_notional` : notionnel vendeur des gros trades.
- `large_trade_count` : nombre de trades marqués comme gros.
- `large_trade_share` : part des gros trades dans la barre.
- `large_signed_imbalance` : déséquilibre signé sur les gros trades uniquement.

### 2.4 Features rolling

- `amihud_24` : approximation d’illiquidité moyenne sur 24 fenêtres.
- `vpin_50` : proxy VPIN sur 50 fenêtres.
- `notional_z_96` : z-score rolling du notionnel sur 96 fenêtres.
- `return_1` : rendement simple du close par rapport à la barre précédente.
- `close_z_96` : z-score rolling du close sur 96 fenêtres.

### 2.5 Label supervisé

- `label_tb` : triple barrier strict en logique first-touch.

Interprétation standard :

- `1.0` : take profit touché avant stop loss
- `-1.0` : stop loss touché avant take profit
- `0.0` : aucune barrière touchée sur l’horizon, ou égalité intra-barre selon la politique choisie

## 3. Colonnes sidecars enrichies après merge

### 3.1 Funding

- `funding_rate` : taux de funding associé au dernier point disponible avant la barre.
- `mark_price` : mark price associée au funding.

### 3.2 Metrics / Open Interest

Selon les données réellement disponibles :

- `sum_open_interest`
- `sum_open_interest_value`
- `cmc_circulating_supply`
- `count_toptrader_long_short_ratio`

### 3.3 Liquidations agrégées

Les liquidations brutes sont resamplées à la fréquence cible, puis mergées en backward asof :

- `liquidation_events` : nombre d’événements de liquidation sur la barre.
- `liquidation_notional_sum` : notionnel total liquidé.
- `liquidation_qty_sum` : quantité totale liquidée.

## 4. Exemple de colonnes observées sur un run feature-only complet

Exemple typique :

```text
timestamp
open
high
low
close
trade_count
quantity_sum
notional_sum
buy_notional
sell_notional
large_buy_notional
large_sell_notional
large_trade_count
large_trade_share
signed_notional_imbalance
large_signed_imbalance
amihud_24
vpin_50
notional_z_96
return_1
close_z_96
label_tb
funding_rate
mark_price
sum_open_interest
sum_open_interest_value
cmc_circulating_supply
count_toptrader_long_short_ratio
liquidation_events
liquidation_notional_sum
liquidation_qty_sum
```

## 5. Exemples d’artefacts JSON à vérifier

### 5.1 `run_manifest.json`

À vérifier après chaque run :

- `run_id`
- `version`
- `native_mode`
- `execution_mode`
- `started_at`, `ended_at`
- `quality_status`
- `stages`
- `config`

### 5.2 `artifacts_manifest.json`

Permet de retrouver :

- tous les fichiers produits
- leur taille
- leur rôle logique dans le pipeline

### 5.3 `contract_manifest.json`

À utiliser pour confirmer que le dataset final respecte les colonnes minimales attendues.

### 5.4 `quality_manifest.json`

Contient les quality gates, par exemple :

- nombre minimal de lignes
- ratio maximal de NaN
- diversité minimale des labels
- régularité / gaps temporels si activés

### 5.5 `execution_metrics_manifest.json`

Permet de suivre les durées des stages et d’identifier rapidement les goulots d’étranglement.

### 5.6 `stage_lineage_manifest.json`

Expose la provenance des entrées / sorties entre stages. Très utile pour l’audit et le debug.

## 6. Recommandations de lecture opérationnelle

Pour valider un run sans ambiguïté :

1. vérifier `contract_manifest.json`
2. vérifier `quality_manifest.json`
3. ouvrir `native_features.csv`
4. confirmer la présence des sidecars attendus
5. vérifier `execution_metrics_manifest.json` pour repérer les étapes lentes

## 7. Phase 2 performance

La phase 2 introduit un backend d’accélération optionnel :

- `run.native_acceleration_backend: auto`
- `run.native_acceleration_backend: pandas`
- `run.native_acceleration_backend: polars`

Ce backend cible d’abord :

- les merges `asof` des sidecars
- le resample des liquidations

L’objectif est une migration progressive vers Polars sur les traitements volumineux, sans casser la compatibilité Pandas actuelle.

## 8. Phase 3 — accélération microstructure

La phase 3 ajoute une accélération optionnelle sur la production des colonnes microstructure.

### Colonnes directement concernées

- `price_open`, `price_high`, `price_low`, `price_close`
- `trade_count`
- `quantity_sum`
- `notional_sum`
- `buy_notional`, `sell_notional`
- `large_buy_notional`, `large_sell_notional`
- `large_trade_count`
- `large_trade_share`
- `signed_notional_imbalance`
- `large_signed_imbalance`

### Colonnes finalisées après agrégation

- `amihud_24`
- `vpin_50`
- `notional_z_96`
- puis plus loin dans le pipeline : `return_1`, `close_z_96`, `label_tb`

### Compatibilité fonctionnelle

L’objectif n’est pas de changer la définition métier des colonnes, mais de déplacer les opérations lourdes d’agrégation vers un backend plus rapide quand il est disponible.

En cas d’absence de Polars, ou si `run.native_acceleration_backend: pandas`, le pipeline garde le comportement Pandas historique.
