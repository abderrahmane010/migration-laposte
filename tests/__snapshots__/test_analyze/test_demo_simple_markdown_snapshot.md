# Analyse du projet DEMO_SIMPLE

Généré par dku2dbx 0.0.1. Valeurs à calibrer sur le pilote.

Nom : Démo simple (synthétique)

Source : `DEMO_SIMPLE.zip`

## Synthèse

- **Niveau de complexité : LOW**
- **Effort estimé : 1.25 jours** (coefficients de `scoring.yaml`)
- Vague de migration : 1
- Recettes : 5, dont AUTO 5
- Anomalies du lecteur : 0

## Conditions de score atteintes

(aucun)

## Mesures

| Mesure | Valeur |
|---|---|
| code_share | 0 |
| formula_prepares | 0 |
| partitioned | 0 |
| plugins | 0 |
| python_scenarios | 0 |
| recipes | 5 |
| saved_models | 0 |

## Inventaire

| Objet | Nombre |
|---|---|
| code_recipes | 0 |
| datasets | 8 |
| issues | 0 |
| ml_recipes | 0 |
| partitioned_datasets | 0 |
| plugin_recipes | 0 |
| prepare_recipes | 1 |
| python_scenarios | 0 |
| recipes | 5 |
| saved_models | 0 |
| scenarios | 0 |
| zones | 0 |

### Recettes par type

| Type | Nombre |
|---|---|
| grouping | 1 |
| join | 1 |
| shaker | 1 |
| vstack | 1 |
| window | 1 |

### Recettes par catégorie

| Type | Nombre |
|---|---|
| VISUAL | 5 |

### Datasets par type

| Type | Nombre |
|---|---|
| PostgreSQL | 8 |

### Processeurs Prepare actifs

| Type | Nombre |
|---|---|
| CreateColumnWithGREL | 1 |
| FillEmptyWithValue | 1 |
| FilterOnValue | 1 |

### Plugins utilisés

(aucun)

### Steps Prepare désactivées (ignorées, jamais traduites)

- prepare_orders: ColumnRenamer

## Classement des recettes

### AUTO : traducteur déterministe

| Recette | Type | Phase prévue | Traducteur disponible | Motif |
|---|---|---|---|---|
| group_by_customer | grouping | 3 | oui | — |
| join_orders_customers | join | 3 | oui | — |
| prepare_orders | shaker | 4 | non | — |
| stack_orders | vstack | 3 | oui | — |
| window_orders | window | 3 | oui | — |

### LLM_REVIEW : résidu traité par le LLM, relu

(aucun)

### MANUAL : hors périmètre, migration à la main

(aucun)

## Dépendances avec d'autres projets

| Producteur | Consommateur | Dataset | Vu par |
|---|---|---|---|
| DEMO_SIMPLE | DEMO_COMPLEXE | customer_stats | exposed |

## Anomalies du lecteur

(aucun)
