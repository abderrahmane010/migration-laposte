# Migration des datasets — DEMO_COMPLEXE

Généré par dku2dbx 0.0.1. Configuration : `config/data.local.yaml`.

Rien n'est copié par défaut : chaque dataset reçoit une décision explicite. `REFUSÉ` signifie qu'aucune copie n'est générée tant que la raison n'est pas levée (en général : compléter `config/data.local.yaml`).

## Synthèse

- COPIE : **3**
- RECALCUL : **20**
- REFUSÉ : **4**

## Décision par dataset

| Dataset | Rôle | Décision | Federation | Table Unity Catalog | Raison |
|---|---|---|---|---|---|
| anomalies_clean | produit | RECALCUL | — | anomalies_clean | recalculé par le job à partir des sources |
| anomaly_report | produit | RECALCUL | — | anomaly_report | recalculé par le job à partir des sources |
| b2b_clean | produit | RECALCUL | — | b2b_clean | recalculé par le job à partir des sources |
| b2b_scores | produit | RECALCUL | — | b2b_scores | recalculé par le job à partir des sources |
| customer_scores | produit | RECALCUL | — | customer_scores | recalculé par le job à partir des sources |
| customers_b2b | source | COPIE | COMPATIBLE | customers_b2b | copié une fois depuis Lakehouse Federation |
| daily_depot_kpis | produit | RECALCUL | — | daily_depot_kpis | recalculé par le job à partir des sources |
| delivered_enriched | produit | RECALCUL | — | delivered_enriched | recalculé par le job à partir des sources |
| delivered_routes | produit | RECALCUL | — | delivered_routes | recalculé par le job à partir des sources |
| depot_trend | produit | RECALCUL | — | depot_trend | recalculé par le job à partir des sources |
| depot_trend_sorted | produit | RECALCUL | — | depot_trend_sorted | recalculé par le job à partir des sources |
| depots | source | COPIE | COMPATIBLE | depots | copié une fois depuis Lakehouse Federation |
| depots_clean | produit | RECALCUL | — | depots_clean | recalculé par le job à partir des sources |
| depots_geo | produit | RECALCUL | — | depots_geo | recalculé par le job à partir des sources |
| depots_unique | produit | RECALCUL | — | depots_unique | recalculé par le job à partir des sources |
| events_anomalies | produit | REFUSÉ | NON DISPONIBLE | events_anomalies | historique à copier (sortie en mode append de stack_failed : l'historique ne se recalcule pas) ; connexion pg_managed déclarée indisponible pour Lakehouse Federation |
| events_clean | produit | RECALCUL | — | events_clean | recalculé par le job à partir des sources |
| events_delivered | produit | RECALCUL | — | events_delivered | recalculé par le job à partir des sources |
| events_failed | produit | RECALCUL | — | events_failed | recalculé par le job à partir des sources |
| events_other | produit | RECALCUL | — | events_other | recalculé par le job à partir des sources |
| events_sync | produit | REFUSÉ | NON DISPONIBLE | events_sync | historique à copier (partitions existantes jamais recalculées par sync_events) ; connexion pg_managed déclarée indisponible pour Lakehouse Federation |
| parcels_events | source | REFUSÉ | — | parcels_events | le chemin Dataiku absent |
| route_kpis | produit | RECALCUL | — | route_kpis | recalculé par le job à partir des sources |
| routes_ref | source | COPIE | COMPATIBLE | routes_ref | copié une fois depuis Lakehouse Federation |
| status_by_depot | produit | RECALCUL | — | status_by_depot | recalculé par le job à partir des sources |
| top_late_depots | produit | RECALCUL | — | top_late_depots | recalculé par le job à partir des sources |
| DEMO_SIMPLE.customer_stats | partagé | REFUSÉ | — | customer_stats | dataset partagé par un autre projet : à migrer avec lui, puis à brancher (shared_projects) |

## Lakehouse Federation

Aucune connexion n'est supposée compatible : seul `COMPATIBLE` (type pris en charge **et** connexion déclarée `federation: available` avec son `foreign_catalog`) permet de lire ou de copier par Federation.

- COMPATIBLE : 3 dataset(s)
- NON DISPONIBLE : 2 dataset(s)

## Noms

Chaque table porte le nom de son dataset Dataiku ; les identifiants originaux (projet, dataset, recette, connexion, table ou chemin, hash) sont enregistrés dans ses propriétés `dku2dbx.dataiku.*` et dans `donnees.json`.

## Copies

Notebooks dans `notebooks/`, paramètres `catalog`, `schema` et `mode`. Par défaut (`mode=skip`), une table déjà chargée n'est pas réécrite et seules les partitions absentes sont ajoutées : la copie se relance sans doublon. `mode=replace` remplace toute la table, sur demande explicite seulement.

Après chaque copie, le notebook valide le schéma, les types, les partitions, le nombre de lignes et les métadonnées, et échoue s'il reste un problème.

- `notebooks/copie_customers_b2b.py` : customers_b2b
- `notebooks/copie_depots.py` : depots
- `notebooks/copie_routes_ref.py` : routes_ref

## Permissions

Non vérifiées (pas d'accès Databricks : option `--check-uc`). Chaque copie échoue clairement si la source n'est pas lisible ou le schéma cible pas inscriptible.
