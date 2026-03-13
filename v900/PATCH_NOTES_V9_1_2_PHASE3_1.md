# PATCH NOTES — V9.1.2 Phase 3.1

## Exactitude / robustesse

- microstructure pandas alignée sur la sémantique **fin de barre** avec `resample(..., label="right", closed="right")`
- parité Pandas/Polars couverte par test dédié sur les timestamps de barre et agrégats principaux
- `quality_check.py` renforcé : validation CLI réelle (`validate-config`, `show-config`) et lecture CSV/Parquet selon suffixe
- `benchmark_phase3.py` enrichi avec métadonnées d'environnement, taille mémoire et note explicite sur le scope hybride pandas→polars
- downloader `aggTrades` sécurisé par pagination **time-boxed** compatible avec une fenêtre futures inférieure à 1h, avec garde-fou de stagnation du curseur

## Notes

- Le downloader `aggTrades` reste volontairement sur une pagination temporelle explicite ; l'objectif de cette phase est de réduire le risque de trous silencieux et de rester compatible avec les contraintes de fenêtre de l'endpoint futures.
