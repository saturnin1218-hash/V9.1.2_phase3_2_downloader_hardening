# Opérations V9.0.0-preprod

## Artefacts de run
Chaque exécution native écrit :
- `run.jsonl`
- `run_manifest.json`
- `artifacts_manifest.json`
- `contract_manifest.json` si un dataset est contrôlé
- `quality_manifest.json` si des quality gates sont évalués

## Relance rapide
- `native_mode: download-only` pour reconstruire les entrées.
- `native_mode: feature-only` pour recalculer features + labels depuis un input local.
- `native_mode: export-only` pour réexporter un dataset existant.
- `native_mode: full` pour la chaîne complète.

## Interprétation des quality gates
- `PASS` : dataset publiable
- `PASS_WITH_WARNINGS` : dataset exploitable mais à surveiller
- `FAIL` : export bloqué sauf si `quality.allow_degraded_export: true`

## Incident courant
### Redis indisponible
Basculer temporairement sur `rate_limit.backend: sqlite`.

### Trop de NA
Vérifier `quality_manifest.json`, puis les sidecars funding/metrics/liquidations et le ratio de merge.

### Contrat cassé
Consulter `contract_manifest.json` pour voir les colonnes manquantes, timestamps invalides ou valeurs négatives.
