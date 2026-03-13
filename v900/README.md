# Binance Quant Builder V9.1.2

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![Packaging](https://img.shields.io/badge/packaging-pyproject%20%2B%20uv-green)
![Tests](https://img.shields.io/badge/tests-pytest-success)
![Lint](https://img.shields.io/badge/lint-ruff-orange)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-black)

Pipeline Python pour construire des datasets quantitatifs à partir de données Binance, avec deux modes complémentaires :

- **bridge legacy** pour conserver la compatibilité avec le moteur historique
- **pipeline natif pré-production** pour un flux plus traçable, plus testable et plus industrialisable

Cette branche V9.1.2 cible surtout la **fiabilité pré-prod** : configuration structurée, manifests, quality gates, logs structurés, contrôles de schéma, bridge legacy progressif et base CI reproductible.

---

## Sommaire

- [Pourquoi ce projet](#pourquoi-ce-projet)
- [Fonctionnalités principales](#fonctionnalités-principales)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Commandes CLI](#commandes-cli)
- [Configuration](#configuration)
- [Sorties produites](#sorties-produites)
- [Features et colonnes exportées](#features-et-colonnes-exportées)
- [Développement local](#développement-local)
- [CI GitHub Actions](#ci-github-actions)
- [Roadmap court / moyen terme](#roadmap-court--moyen-terme)

---

## Pourquoi ce projet

`binance-quant-builder` sert à produire des datasets propres et réutilisables pour :

- recherche quantitative
- feature engineering
- labeling supervisé
- contrôle qualité des données
- entraînement de modèles ML
- préparation de pipelines de backtest
- industrialisation progressive d’un stack quant crypto

Le projet vise à éviter les faiblesses classiques des pipelines artisanaux :

- merges fragiles
- timestamps incohérents
- sidecars partiellement couverts sans visibilité
- quality gates trop tardives
- exports peu traçables
- exécutions difficiles à reproduire

---

## Fonctionnalités principales

### Pipeline natif

- chargement de configuration YAML / TOML / JSON
- validation structurée avant exécution
- mode `plan`, `download-only`, `feature-only`, `export-only`, `full`
- production d’artefacts reproductibles et de manifests JSON
- logs structurés JSONL par run
- quality gates sur les datasets exportés
- contrats de schéma sur les étapes clés

### Données et enrichissements

- téléchargement des aggTrades Binance
- sidecars optionnels : funding, open interest / metrics, liquidations
- merge `asof` orienté passé pour limiter le lookahead
- features de microstructure agrégées par fréquence
- labels triple barrier
- rapport de couverture NaN

### Exploitation / pré-prod

- bridge legacy pour conserver la compatibilité v7.x/v8.x
- rate limiter avec backends `thread_local`, `sqlite`, `redis`, `noop`
- build packaging via `pyproject.toml`
- dépendances gelées via `uv.lock`
- CI GitHub Actions : lint + tests + build + quality smoke check

---

## Architecture

### Package principal

- `binance_quant_builder/cli.py` : CLI Typer
- `binance_quant_builder/config.py` : chargement, dump et validation de config
- `binance_quant_builder/processor.py` : orchestration principale
- `binance_quant_builder/api.py` : exécution des modes natifs
- `binance_quant_builder/downloader.py` : récupération des sources Binance
- `binance_quant_builder/merge.py` : alignement et enrichissement
- `binance_quant_builder/exporter.py` : export CSV / Parquet
- `binance_quant_builder/manifests.py` : manifests JSON
- `binance_quant_builder/schema_contracts.py` : contrats métiers / schéma
- `binance_quant_builder/quality_gates.py` : contrôles qualité
- `binance_quant_builder/observability.py` : logs structurés
- `binance_quant_builder/rate_limiter.py` : limitation des appels

### Sous-modules métier

- `binance_quant_builder/features/` : features de microstructure
- `binance_quant_builder/labels/` : labels, dont triple barrier
- `binance_quant_builder/report/` : rapports qualité et NaN
- `binance_quant_builder/utils/` : temps, I/O, numérique, logging
- `binance_quant_builder/legacy/` : bridge de compatibilité historique

### Documentation complémentaire

- `docs/operations.md` : notes d’exploitation
- `docs/output_columns.md` : description des colonnes et artefacts produits

---

## Installation

### Pré-requis

- Python **3.11+**
- recommandé : environnement virtuel dédié
- optionnel : `uv` pour un environnement verrouillé et reproductible

### Option A — installation simple avec pip

```bash
python -m venv .venv
source .venv/bin/activate
# Windows PowerShell : .venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

### Option B — installation dev avec pip

```bash
python -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .
```

### Option C — installation reproductible avec uv

```bash
uv venv
source .venv/bin/activate
uv sync --extra yaml --extra parquet --extra dev
```

### Extras disponibles

- `yaml` : support YAML via `PyYAML`
- `parquet` : support Parquet via `pyarrow`
- `redis` : backend Redis pour le rate limiting
- `dev` : `pytest`, `pytest-cov`, `ruff`, `build`

### Fichiers de dépendances fournis

- `requirements.txt` : dépendances runtime exportées
- `requirements-dev.txt` : dépendances de développement
- `uv.lock` : lockfile reproductible pour `uv`

Pour régénérer les fichiers lock/export :

```bash
uv lock
uv export --no-hashes --format requirements-txt -o requirements.txt
uv export --no-hashes --format requirements-txt --all-extras -o requirements-dev.txt
```

---

## Quickstart

### 1. Vérifier la configuration exemple

```bash
binance-quant-builder validate-config --config config.example.yaml
```

Sortie attendue typique :

```json
{
  "status": "ok",
  "errors": [],
  "config": {
    "run": {
      "symbol": "BTCUSDT",
      "market": "futures_um",
      "freq": "1h",
      "native_mode": "full"
    }
  }
}
```

### 2. Afficher la configuration résolue

```bash
binance-quant-builder show-config --config config.example.yaml
```

### 3. Produire un plan d’exécution natif

```bash
binance-quant-builder native-run --config config.example.yaml
```

Si `native_mode: plan`, le projet écrit un manifeste décrivant :

- les chemins de sortie
- le backend de rate limiting
- les sidecars activés
- les contrôles pré-prod attendus

### 4. Lancer un run complet

```bash
binance-quant-builder run --config config.example.yaml
```

Sorties typiques dans `./output/` :

- `native_raw_trades.csv`
- `native_funding.csv`
- `native_metrics.csv`
- `native_merged.csv`
- `native_features.csv`
- `native_export.csv`
- `run_manifest.json`
- `artifacts_manifest.json`
- `contract_manifest.json`
- `quality_manifest.json`
- `execution_metrics.json`
- `stage_lineage_manifest.json`
- `run.jsonl`

---

## Commandes CLI

Le binaire exposé est :

```bash
binance-quant-builder
```

### Valider une configuration

```bash
binance-quant-builder validate-config --config config.example.yaml
```

### Afficher la config résolue

```bash
binance-quant-builder show-config --config config.example.yaml
```

### Lancer le pipeline principal

```bash
binance-quant-builder run --config config.example.yaml
```

### Forcer le mode natif défini dans la config

```bash
binance-quant-builder native-run --config config.example.yaml
```

---

## Configuration

Le fichier `config.example.yaml` couvre les sections principales.

### Section `run`

Paramètres de run et de sortie :

- `symbol` : paire, ex. `BTCUSDT`
- `market` : `spot` ou `futures_um`
- `freq` : fréquence cible, ex. `1h`
- `start` / `end` : fenêtre temporelle
- `out_dir` : dossier de sortie
- `use_legacy_pipeline` : active le moteur historique
- `native_mode` : `plan`, `download-only`, `feature-only`, `export-only`, `full`
- `native_export_format` : `csv` ou `parquet`
- `native_include_funding`, `native_include_metrics`, `native_include_liquidations` : sidecars optionnels
- `native_merge_tolerance` : tolérance du merge `asof`

### Section `parallel`

- `enabled`
- `backend`
- `workers`

### Section `rate_limit`

- `backend` : `sqlite`, `shared_memory`, `redis`, `noop`
- `max_per_min`
- `sqlite_timeout`
- `sqlite_path`
- `redis_url`

### Section `features`

- `large_trade_quantile`
- `rolling_window`
- `triple_barrier_horizon`
- `triple_barrier_pt`
- `triple_barrier_sl`
- `nan_report`
- options de normalisation ML

### Section `quality`

- `max_na_ratio_critical`
- `min_rows`
- `min_label_classes`
- `allow_degraded_export`

### Exemple de configuration minimaliste orientée feature-only

```yaml
run:
  symbol: BTCUSDT
  market: futures_um
  freq: 1h
  start: 2024-01-01
  end: 2024-01-03
  out_dir: ./output
  use_legacy_pipeline: false
  native_mode: feature-only
  native_input_path: ./output/native_raw_trades.csv

features:
  large_trade_quantile: 0.95
  rolling_window: 1000
  triple_barrier_horizon: 24
  triple_barrier_pt: 0.02
  triple_barrier_sl: 0.02
  nan_report: true
```

### Exemple orienté bridge legacy

```yaml
run:
  symbol: BTCUSDT
  market: futures_um
  freq: 1h
  start: 2024-01-01
  end: 2024-01-10
  out_dir: ./output
  use_legacy_pipeline: true

rate_limit:
  backend: sqlite
  max_per_min: 60
  sqlite_path: ./output/api_rate_limit.sqlite3
```

---

## Sorties produites

### Artefacts de données

- `native_raw_trades.csv` : trades bruts téléchargés ou injectés
- `native_funding.csv` : funding rates
- `native_metrics.csv` : open interest / métriques futures
- `native_liquidations.csv` : liquidations si activées
- `native_merged.csv` : dataset enrichi après sidecars
- `native_features.csv` : dataset features + labels
- `native_export.csv|parquet` : export final

### Artefacts de contrôle et d’observabilité

- `run_manifest.json` : synthèse globale du run
- `artifacts_manifest.json` : liste et métadonnées des artefacts
- `contract_manifest.json` : validation de contrat de schéma
- `quality_manifest.json` : quality gates
- `execution_metrics.json` : métriques d’exécution par stage
- `stage_lineage_manifest.json` : provenance / chaînage des étapes
- `run.jsonl` : logs structurés JSONL

---

## Features et colonnes exportées

Le pipeline natif `feature-only` produit notamment :

### Colonnes OHLC / activité

- `timestamp`
- `open`, `high`, `low`, `close`
- `trade_count`
- `quantity_sum`
- `notional_sum`

### Colonnes d’imbalance et gros trades

- `buy_notional`
- `sell_notional`
- `large_buy_notional`
- `large_sell_notional`
- `large_trade_count`
- `large_trade_share`
- `signed_notional_imbalance`
- `large_signed_imbalance`

### Colonnes dérivées / rolling

- `amihud_24`
- `vpin_50`
- `notional_z_96`
- `return_1`
- `close_z_96`

### Labeling

- `label_tb` : label triple barrier

### Sidecars fusionnés si disponibles

- `funding_rate`
- `mark_price`
- `sum_open_interest`
- `sum_open_interest_value`
- `cmc_circulating_supply`
- `count_toptrader_long_short_ratio`
- `liquidation_events`
- `liquidation_notional_sum`
- `liquidation_qty_sum`

Voir aussi le détail dans [`docs/output_columns.md`](docs/output_columns.md).

---

## Développement local

### Lancer les tests

```bash
pytest -q
```

### Couverture

```bash
pytest -q --cov=binance_quant_builder --cov-report=term-missing
```

### Lint

```bash
ruff check .
```

### Build package

```bash
python -m build
```

### Quality smoke check local

```bash
python scripts/quality_check.py
```

Ce script vérifie au minimum :

- la validité de `config.example.yaml`
- un mini run natif local sans réseau
- la présence des manifests critiques
- la cohérence d’un export feature-only de démonstration

---

## CI GitHub Actions

Le workflow `.github/workflows/ci.yml` exécute :

1. installation via `uv`
2. lint `ruff`
3. tests `pytest`
4. build packaging
5. `quality_check.py`

Objectif : garantir qu’une PR ne casse pas :

- la CLI
- la validation de config
- les tests unitaires et d’intégration de base
- le packaging
- le smoke check qualité

---

## Roadmap court / moyen terme

### Court terme

- enrichir encore les tests d’intégration de bout en bout
- durcir progressivement les quality gates
- rendre les diagnostics de merge plus observables
- fiabiliser encore le bridge legacy avant décommissionnement

### Moyen terme

- migration des gros traitements `merge` / `resample` vers **Polars** pour viser un gain x3 à x5 sur les datasets 1min multi-années
- complétion des tests end-to-end sur petits symboles représentatifs
- documentation plus exhaustive des colonnes produites et des exemples de datasets
- réduction progressive du périmètre `legacy/` jusqu’à suppression du bridge

---

## État actuel

Le dépôt est désormais prêt pour un usage **pré-production encadré**, mais pas encore pour une promesse de production finale “industry-grade” sans chantier supplémentaire sur :

- performance très gros volumes
- qualité des labels avancés
- montée en charge API / weights Binance
- décommissionnement progressif du legacy

C’est une base solide pour industrialiser proprement, avec un niveau de traçabilité déjà nettement supérieur à un script monolithique standard.


## Backend d’accélération (Phase 2)

La V9.1.2 phase 2 introduit un backend d’accélération **optionnel** pour les opérations les plus coûteuses du pipeline natif :

- `merge_asof` des sidecars (`funding`, `metrics`, `liquidations`)
- resample des liquidations avant merge

Configuration dans `run.native_acceleration_backend` :

- `auto` : utilise **Polars** si installé, sinon fallback **Pandas**
- `polars` : tente Polars explicitement, avec fallback silencieux si indisponible
- `pandas` : désactive toute accélération optionnelle

Exemple :

```yaml
run:
  symbol: BTCUSDT
  native_mode: feature-only
  native_input_path: ./output/native_raw_trades.csv
  native_acceleration_backend: auto
```

### Pourquoi cette approche

Le projet reste compatible avec l’environnement actuel tout en ouvrant une trajectoire de performance :

- pas de réécriture complète imposée
- fallback automatique si `polars` n’est pas installé
- surface de risque limitée aux étapes ciblées
- possibilité de bench comparer `pandas` vs `polars` sur les mêmes datasets

### Gains visés

Sur des datasets 1 minute multi-années, les gains dépendent surtout :

- du volume de sidecars fusionnés
- du nombre de timestamps
- du coût I/O CSV/Parquet
- de la mémoire disponible

En pratique, cette phase 2 prépare une migration progressive vers des exécutions plus rapides sur les merges / resamples, sans casser la logique métier existante.

## Exemple end-to-end minimal

### Config feature-only locale

```yaml
run:
  symbol: BTCUSDT
  market: futures_um
  freq: 1h
  out_dir: ./output
  use_legacy_pipeline: false
  native_mode: feature-only
  native_input_path: ./output/native_raw_trades.csv
  native_include_funding: true
  native_include_metrics: true
  native_include_liquidations: true
  native_acceleration_backend: auto

features:
  triple_barrier_horizon: 24
  triple_barrier_pt: 0.02
  triple_barrier_sl: 0.02
  nan_report: true
```

### Commande

```bash
binance-quant-builder native-run --config config.example.yaml
```

### Artefacts attendus

- `native_features.csv`
- `native_merged.csv`
- `run_manifest.json`
- `artifacts_manifest.json`
- `contract_manifest.json`
- `quality_manifest.json`
- `execution_metrics_manifest.json`
- `stage_lineage_manifest.json`
- éventuellement le rapport NaN

### Colonnes typiques attendues dans le dataset final

- `timestamp`, `open`, `high`, `low`, `close`
- `trade_count`, `quantity_sum`, `notional_sum`
- `signed_notional_imbalance`, `large_trade_share`
- `amihud_24`, `vpin_50`, `notional_z_96`
- `return_1`, `close_z_96`, `label_tb`
- `funding_rate`, `mark_price`
- `sum_open_interest`, `sum_open_interest_value`
- `liquidation_events`, `liquidation_notional_sum`, `liquidation_qty_sum`

## Phase 3

La phase 3 étend l’accélération optionnelle vers la **construction des features microstructure**.

### Ce qui est accéléré

Quand `run.native_acceleration_backend` vaut `auto` ou `polars` et que Polars est installé :

- agrégation temporelle des trades en barres (`open/high/low/close`)
- agrégations de volumes et notionnels
- comptage des gros trades et déséquilibres signés
- conservation d’un fallback Pandas strictement compatible

Les calculs roulants de fin de pipeline (`amihud_24`, `vpin_50`, `notional_z_96`) restent volontairement finalisés en Pandas pour préserver la sémantique actuelle et limiter les régressions.

### Benchmark local

Un script dédié est fourni :

```bash
python scripts/benchmark_phase3.py ./output/native_raw_trades.csv --freq 1h --repeat 5
```

Sortie typique :

```json
{
  "rows": 250000,
  "freq": "1h",
  "polars_available": true,
  "pandas": {"repeat": 5.0, "avg_seconds": 1.82, "best_seconds": 1.74},
  "polars": {"repeat": 5.0, "avg_seconds": 0.71, "best_seconds": 0.63},
  "speedup_best_vs_pandas": 2.76
}
```

Le gain réel dépend :

- de la taille du dataset brut
- de la fréquence cible
- du coût d’I/O CSV/Parquet
- du nombre de sidecars fusionnés
- de la mémoire disponible

### Validation phase 3

La suite de tests couvre désormais :

- la cohérence Pandas vs backend accéléré sur les features microstructure
- le benchmark helper
- un scénario end-to-end avec sidecars funding / metrics / liquidations et backend `polars`


## Phase 3.2 — Downloader hardening

La phase 3.2 durcit le téléchargement `aggTrades` :

- pagination bornée par fenêtres temporelles compatibles futures
- poursuite contrôlée via `fromId` dans les fenêtres denses
- diagnostics JSON par fenêtre et manifeste de téléchargement
- propagation des request weights `aggTrades` vers le rate limiter

Le manifeste `native_download_trades_manifest.json` aide à auditer :

- le nombre de fenêtres parcourues
- le nombre de requêtes par fenêtre
- le mode de pagination réellement utilisé
- les warnings de densité ou d'anomalie
