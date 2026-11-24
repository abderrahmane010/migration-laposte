# Plan de réconciliation — DEMO_SIMPLE

Généré par dku2dbx 0.0.1. Configuration : `config/reconcile.yaml`.

Le notebook `reconcile.py` compare chaque table produite par le flux migré avec la sortie Dataiku du même dataset, en quatre couches : schéma, nombre de lignes (au total et par partition), agrégats par colonne, puis lignes. Il écrit un résultat par table dans `<catalog>.<schema>.dku2dbx_reconciliation` (Delta) et échoue si une table est en ÉCART.

Paramètres : `catalog`, `schema` (tables Databricks), `ref_catalog`, `ref_schema` (sorties Dataiku dans Unity Catalog), `results_table`.

- Tables comparées : **3**
- Tables exclues (`skip`) : **0**

| Table | Clé | Tolérance abs. | Tolérance rel. | Partitions | Colonnes ignorées | Référence Dataiku |
|---|---|---|---|---|---|---|
| orders_enriched | aucune (EXCEPT ALL) | 0.0 | 0.0 | — | — | `<ref_catalog>.<ref_schema>.orders_enriched` |
| customer_stats | customer_id | 0.0 | 0.001 | — | — | `<ref_catalog>.<ref_schema>.customer_stats` |
| orders_ranked | aucune (EXCEPT ALL) | 0.0 | 0.0 | — | — | `<ref_catalog>.<ref_schema>.orders_ranked` |
