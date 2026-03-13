# PATCH NOTES — V9.1.2 Phase 3.2 downloader-hardening

## Objectif

Durcir le downloader natif Binance, surtout sur `aggTrades`, pour réduire les risques silencieux de trous, pagination bloquée et sous-estimation du request weight.

## Changements principaux

### Downloader `aggTrades`
- pagination hybride plus sûre :
  - amorçage par fenêtre temporelle bornée
  - poursuite optionnelle par `fromId` quand la fenêtre est dense et dépasse `limit`
- conservation d'un découpage futures `< 1h`
- ajout de garde-fous sur la stagnation du curseur `fromId`
- ajout d'un payload de diagnostics détaillé par fenêtre :
  - nombre de requêtes
  - mode de pagination
  - timestamps min/max observés
  - warnings de densité

### Rate limiter
- support natif de `acquire(weight=...)`
- propagation des poids `aggTrades` par marché :
  - spot : 4
  - futures UM : 20

### API native
- `download-only` écrit désormais un manifeste dédié :
  - `native_download_trades_manifest.json`
- ce manifeste embarque les diagnostics du downloader trades

### Tests
- ajout d'un test de pagination dense via `fromId`
- ajout d'un test de sérialisation des diagnostics
- ajout d'un smoke test sur le paramètre `weight` du rate limiter thread-local

## Impact attendu
- meilleure robustesse sur longues périodes et fenêtres denses
- observabilité accrue du downloader
- limitation plus réaliste vis-à-vis des poids Binance

## Point honnête
- la stratégie est volontairement progressive : elle n'est pas encore une réécriture complète multi-endpoints du downloader
- les diagnostics détectent mieux les situations suspectes, mais ne "prouvent" pas l'absence absolue de trous de marché
