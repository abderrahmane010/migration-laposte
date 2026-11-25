# Simplification du Flow — DEMO_COMPLEXE

Généré par dku2dbx 0.0.1.

## Synthèse

- Recettes avant : **20**
- Étapes après : **18**
- Tables matérialisées : 20
- Tables supprimées (devenues des CTE) : 2

## Étapes

Chaque étape porte le nom de sa recette principale (la dernière exécutée).

| Étape | Recettes (ordre d'exécution) | Tables écrites | Tables en CTE |
|---|---|---|---|
| distinct_depots | distinct_depots | depots_unique | — |
| prepare_depots | prepare_depots | depots_clean | — |
| plugin_geocode | plugin_geocode | depots_geo | — |
| sync_events | sync_events | events_sync | — |
| prepare_events | prepare_events | events_clean | — |
| pivot_status | pivot_status | status_by_depot | — |
| split_status | split_status | events_delivered, events_failed, events_other | — |
| join_delivered_depots | join_delivered_depots | delivered_enriched | — |
| group_daily_depot | group_daily_depot | daily_depot_kpis | — |
| group_routes | join_routes, group_routes | route_kpis | delivered_routes |
| python_scoring | python_scoring | customer_scores | — |
| join_b2b_scores | prepare_b2b, join_b2b_scores | b2b_scores | b2b_clean |
| stack_failed | stack_failed | events_anomalies | — |
| prepare_anomalies | prepare_anomalies | anomalies_clean | — |
| sql_anomaly_report | sql_anomaly_report | anomaly_report | — |
| window_depot_trend | window_depot_trend | depot_trend | — |
| sort_depot_trend | sort_depot_trend | depot_trend_sorted | — |
| topn_late_depots | topn_late_depots | top_late_depots | — |

## Tables conservées et justification

| Table | Raisons |
|---|---|
| anomalies_clean | entrée d'une recette LLM_REVIEW 'sql_anomaly_report' |
| anomaly_report | sortie finale (aucune recette ne la lit); sortie d'une recette LLM_REVIEW 'sql_anomaly_report' |
| b2b_scores | sortie finale (aucune recette ne la lit); exposée à d'autres projets; lue ou construite par un scénario |
| customer_scores | sortie d'une recette LLM_REVIEW 'python_scoring' |
| daily_depot_kpis | partitionnée; porte des checks; lue ou construite par un scénario |
| delivered_enriched | lue par 3 recettes; entrée d'une recette LLM_REVIEW 'python_scoring' |
| depot_trend | lue par 2 recettes |
| depot_trend_sorted | sortie finale (aucune recette ne la lit) |
| depots_clean | lue par 3 recettes; sortie d'une recette MANUAL 'prepare_depots'; entrée d'une recette MANUAL 'plugin_geocode'; entrée d'une recette LLM_REVIEW 'sql_anomaly_report' |
| depots_geo | sortie finale (aucune recette ne la lit); sortie d'une recette MANUAL 'plugin_geocode' |
| depots_unique | entrée d'une recette MANUAL 'prepare_depots' |
| events_anomalies | écrite en mode append |
| events_clean | lue par 2 recettes; partitionnée |
| events_delivered | partitionnée |
| events_failed | partitionnée |
| events_other | partitionnée |
| events_sync | partitionnée |
| route_kpis | sortie finale (aucune recette ne la lit) |
| status_by_depot | sortie finale (aucune recette ne la lit) |
| top_late_depots | sortie finale (aucune recette ne la lit); lue ou construite par un scénario |

## Tables supprimées

- b2b_clean
- delivered_routes
