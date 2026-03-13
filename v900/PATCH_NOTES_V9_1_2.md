# V9.1.2 — Patch notes

## Correctifs principaux

### 1) Parsing robuste des timestamps externes
- Correction du bug silencieux sur les `timestamp` numériques en entrée (`ms`, `us`, `s`) dans le flux `feature-only`.
- `ensure_utc_timestamp()` détecte désormais correctement :
  - séries déjà typées datetime
  - séries numériques natives
  - colonnes texte numérisées majoritairement compatibles timestamps Unix
- Évite les conversions erronées vers `NaT` ou vers 1970 quand l'entrée est en millisecondes.

### 2) Backend de rate limit clarifié
- Ajout de l’alias explicite `thread_local`.
- `shared_memory` reste accepté pour compatibilité mais est désormais résolu vers `thread_local`.
- Réduction du risque de confusion conceptuelle sur un backend supposé inter-processus alors qu’il ne l’est pas.

### 3) Signal de complétude sidecars
- Ajout d’un résumé de couverture temporelle pour les sidecars :
  - `requested_start`
  - `requested_end`
  - `observed_start`
  - `observed_end`
  - `coverage_ratio`
  - `warning`
- Les sorties `download-only` et `feature-only` exposent maintenant ces informations.
- `feature-only` remonte `coverage_warnings` quand un sidecar couvre partiellement la fenêtre demandée.

### 4) Cohérence de version
- Passage des versions internes à `9.1.2`.

## Fichiers modifiés
- `binance_quant_builder/utils/time.py`
- `binance_quant_builder/rate_limiter.py`
- `binance_quant_builder/validate.py`
- `binance_quant_builder/api.py`
- `binance_quant_builder/downloader.py`
- `binance_quant_builder/processor.py`
- `binance_quant_builder/__init__.py`
- `pyproject.toml`

## Tests ajoutés
- parsing des timestamps numériques en millisecondes
- alias `thread_local` / compatibilité `shared_memory`
- flux `feature-only` sur input externe à timestamp numérique
- warning de couverture partielle sidecar

## Résultat
- Suite de tests relancée : **47 / 47 OK**
