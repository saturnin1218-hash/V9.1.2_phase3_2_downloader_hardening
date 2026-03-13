# PATCH NOTES — V9.1.2 Phase 2

## Ajouts principaux

- Backend d’accélération optionnel `run.native_acceleration_backend`
  - `auto`
  - `pandas`
  - `polars`
- Nouveau module `binance_quant_builder/accelerated.py`
- Merge `asof` sidecars compatible Polars avec fallback Pandas
- Resample des liquidations compatible Polars avec fallback Pandas
- Documentation enrichie (`README.md`, `docs/output_columns.md`)
- Nouvelles suites de tests :
  - `tests/test_accelerated.py`
  - `tests/test_e2e_phase2.py`
- CI renforcée :
  - `uv sync --locked`
  - `uv lock --check`
  - extra `polars` dans la matrice CI

## Compatibilité

- Comportement par défaut conservé via fallback Pandas
- Aucune rupture obligatoire pour les environnements sans Polars

## Validation locale

- `pytest -q` ✅
- `uv run python -m build` ✅
- `uv run python scripts/quality_check.py` ✅
