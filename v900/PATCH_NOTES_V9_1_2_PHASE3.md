# PATCH NOTES — V9.1.2 Phase 3

## Ajouts principaux

- accélération optionnelle de la construction des features microstructure via Polars
- fallback Pandas conservé
- helper de benchmark `scripts/benchmark_phase3.py`
- nouveaux tests de cohérence Pandas vs accéléré
- nouveau test end-to-end multi-sidecars avec backend `polars`
- documentation README et `docs/output_columns.md` enrichie

## Portée

Cette phase 3 cible surtout les workloads 1min / multi-années où l’agrégation des trades devient dominante en temps CPU.

## Limites assumées

- les calculs roulants finaux restent stabilisés en Pandas pour préserver la sémantique existante
- ce n’est pas encore une migration totale de tout le pipeline natif vers Polars
