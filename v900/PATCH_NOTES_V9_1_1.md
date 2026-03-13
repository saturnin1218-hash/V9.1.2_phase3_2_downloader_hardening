# V9.1.1 corrigé et optimisé

## Correctifs majeurs
- correction de `_to_ms()` pour les timestamps timezone-aware (`astimezone(UTC)` au lieu de `replace(tzinfo=UTC)`)
- pagination ajoutée pour `fundingRate`, `openInterestHist` et `allForceOrders`
- correction du bug de chemin retourné en `feature-only` quand le dataset est vide
- correction de `run_native_full()` pour réinjecter explicitement le fichier téléchargé avant le stage feature
- validation réelle des dates `run.start` / `run.end` avec `pandas.Timestamp`
- backend `shared_memory` rendu réellement limitant via un rate limiter thread-safe en mémoire
- User-Agent aligné sur la version 9.1.1

## Durcissements data
- correction du mapping sémantique Open Interest : `CMCCirculatingSupply` est désormais exposé en `cmc_circulating_supply`
- conservation optionnelle de `count_toptrader_long_short_ratio` uniquement si réellement fourni par la payload
- déduplication temporelle sur les sidecars paginés

## Robustesse pipeline
- lecture protégée contre `EmptyDataError`
- exécution native tolérante aux datasets vides avec manifests explicites `PASS_WITH_WARNINGS`

## Tests ajoutés
- test timezone-aware pour `_to_ms()`
- test de pagination funding
- test du backend `shared_memory`
- test de validation de datetime invalide
- test du chemin réel exporté quand `native_features_filename` n'a pas de suffixe

## Résultat
- suite de tests locale : **43/43 OK**
