# Simplification du Flow — DEMO_COMPLEXE

Généré par dku2dbx 0.0.1.

## Synthèse

- Recettes avant : **20**
- Étapes après : **18**
- Tables matérialisées : 20
- Tables supprimées (devenues des CTE) : 2

## Étapes

| Étape | Recettes (ordre d'exécution) | Tables écrites | Tables en CTE |
|---|---|---|---|
| step_01 | distinct_depots | depots_unique | — |
| step_02 | prepare_depots | depots_clean | — |
| step_03 | plugin_geocode | depots_geo | — |
| step_04 | sync_events | events_sync | — |
| step_05 | prepare_events | events_clean | — |
| step_06 | pivot_status | status_by_depot | — |
| step_07 | split_status | events_delivered, events_failed, events_other | — |
| step_08 | join_delivered_depots | delivered_enriched | — |
| step_09 | group_daily_depot | daily_depot_kpis | — |
| step_10 | join_routes, group_routes | route_kpis | delivered_routes |
| step_11 | python_scoring | customer_scores | — |
| step_12 | prepare_b2b, join_b2b_scores | b2b_scores | b2b_clean |
| step_13 | stack_failed | events_anomalies | — |
| step_14 | prepare_anomalies | anomalies_clean | — |
| step_15 | sql_anomaly_report | anomaly_report | — |
| step_16 | window_depot_trend | depot_trend | — |
| step_17 | sort_depot_trend | depot_trend_sorted | — |
| step_18 | topn_late_depots | top_late_depots | — |

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
