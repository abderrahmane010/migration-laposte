# Hypothèses sur le format d'export Dataiku

Le format interne des exports DSS n'est pas documenté officiellement. Chaque hypothèse ci-dessous est :

- marquée `# ASSUMPTION (H-xx)` dans le code ;
- reproduite dans au moins une fixture (`tests/fixtures/make_fixture.py`) ;
- **à revérifier sur un vrai export** La Poste (phase 1).

Statuts : `SUPPOSÉE` (non vérifiée), `CONFIRMÉE`, `INFIRMÉE` (avec la correction apportée).

| ID | Hypothèse | Source | Fixture | Statut |
|---|---|---|---|---|
| H-01 | Version DSS 15, champ `dssVersion` au format `15.x.y` | CLAUDE.md | les deux | SUPPOSÉE |
| H-02 | `export-manifest.json` à la racine du ZIP, avec les clés `dssVersion`, `exportedWithPlugins` (liste `{pluginId, pluginVersion}`) et `foreignObjects` (liste `{projectKey, type, id}`). Le lecteur accepte aussi un unique dossier racine (`<dossier>/export-manifest.json`) | CLAUDE.md (clés), forme des éléments supposée | les deux ; `test_manifest_under_a_single_top_folder` | SUPPOSÉE |
| H-03 | `project_config/datasets/<nom>.json` contient `type`, `managed`, `params.{connection, catalog, schema, table, mode}`, `partitioning.dimensions`, `schema.columns[{name, type}]` et `checks` | CLAUDE.md | les deux | SUPPOSÉE |
| H-04 | Une dimension de partition temporelle a la forme `{"name", "type": "time", "params": {"period": "DAY"}}` | supposée | `DEMO_COMPLEXE/parcels_events` | SUPPOSÉE |
| H-05 | La dépendance de partition « même jour » s'écrit `deps: [{"func": "equals", "params": {}}]` dans `inputs.main.items[]` | supposée | `DEMO_COMPLEXE/sync_events` | SUPPOSÉE |
| H-06 | `project_config/recipes/<nom>.json` contient `type`, `inputs.main.items[].{ref, deps}`, `outputs.main.items[].{ref, appendMode}`, `params.engineParams` et `neverRecomputeExistingPartitions` | CLAUDE.md | les deux | SUPPOSÉE |
| H-07 | Une step Prepare a la forme `{"metaType": "PROCESSOR", "type", "params", "disabled"}` | CLAUDE.md | les deux | CONFIRMÉE (API) pour la liste `steps` du payload `.shaker` (4 recettes réelles ; les autres clés du payload ne concernent que l'affichage) |
| H-08 | Une step groupée a `metaType: "GROUP"`, un `name` et des `steps` imbriquées | supposée (CLAUDE.md dit seulement « autre `metaType` ») | `DEMO_COMPLEXE/prepare_events` | SUPPOSÉE |
| H-09 | Le payload d'une recette visuelle est `recipes/<nom>.<type>` : `.shaker`, `.join`, `.grouping`, `.window`, `.vstack`, `.split`, `.distinct`, `.pivot`, `.topn`, `.sort`, `.sync` | CLAUDE.md pour les principaux ; `.topn`, `.sort` et `.sync` supposés | les deux | SUPPOSÉE |
| H-09a | Payload Join : `virtualInputs[].preFilter`, `joins[].{table1, table2, type, conditionsMode, on[].{column1, column2, type, caseInsensitive, normalizeText}}`, `selectedColumns`, `postFilter` | supposée | les deux | CORRIGÉE (format réel, API) : `selectedColumns` est une liste `{name, table, type}` ; options en plus sur les conditions (`strict`, `maxDistance`…) et les entrées (`outputColumnsSelectionMode`) |
| H-09b | Payload Group : `keys[].column`, `values[]` avec un booléen par agrégat (`sum`, `avg`…), `globalCount`, `preFilter`, `postFilter` | supposée | les deux | CONFIRMÉE (API) pour `keys[].column` et `values[]` ; drapeaux en plus : `sum2`, `stddev`, `firstLastNotNull`, `concat`, `concatDistinct`, `type` |
| H-09c | Payload Window : `windows[].{partitioningColumns, orders[], enableLimits}` et `values[]` (avec `lag`/`lagValues`) | supposée | les deux | CORRIGÉE (API) : `rank`, `rowNumber`, `denseRank`, `cumeDist`, `ntile` à la racine ; bornes `windowLimitMode`, `precedingRows`… ; `lead`/`lagDiff`/`leadDiff` |
| H-09d | Payload Stack : `mode: "UNION"`, `virtualInputs`, `addOriginColumn`, `originColumnName` | supposée | les deux | CONFIRMÉE (API) ; en plus `selectedColumns` (liste de noms) et `virtualInputs[].originLabel` |
| H-09e | Payload Split : `mode: "VALUES"`, `column`, `values[].{value, outputIndex}`, `defaultOutputIndex` | supposée | `DEMO_COMPLEXE/split_status` | CORRIGÉE (API) : `valueSplits[].{value, caseSensitive, outputIndex}` (pas `values`) ; une liste par mode (`filterSplits`, `rangeSplits`, `randomSplits`, `centileSplits`) |
| H-09f | Payload Top N : `topCount`, `bottomCount`, `orders[]`, `keys[]` | supposée | `DEMO_COMPLEXE/topn_late_depots` | CORRIGÉE (API) : `firstRows` / `lastRows` (pas `topCount`), `retrievedColumns`, drapeaux `rank`, `rowNumber`, `denseRank`, `duplicateCount` |
| H-09g | Payload Pivot : `keyColumns`, `valuesMode` (`EXPLICIT`), `explicitValues`, `rowIdentifiers`, `aggregations` | supposée | `DEMO_COMPLEXE/pivot_status` | CORRIGÉE (API) : liste `pivots[]` (`keyColumns`, `explicitValues`, `valueLimit` — seul `TOP_N` observé —, `valueColumns[]`), `explicitIdentifiers`, `identifierColumnsSelection` |
| H-10 | Le script d'une recette de code est un fichier texte à côté du JSON : `recipes/<nom>.py` (Python) et `recipes/<nom>.sql` (SQL, type `sql_query`) | supposée (CLAUDE.md : « emplacement à confirmer ») | `DEMO_COMPLEXE/python_scoring`, `sql_anomaly_report` | SUPPOSÉE |
| H-11 | `params.json` → `exposedObjects.objects[].{type, localName, rules[].targetProject}` ; un consommateur référence `PROJET.dataset` dans ses entrées | CLAUDE.md et ROADMAP (phase 2) | `DEMO_SIMPLE` → `DEMO_COMPLEXE` | SUPPOSÉE |
| H-12 | Zones de Flow dans `project_config/zones/<id>.json` (`id`, `name`, `color`), avec une clé `zone` sur les datasets et les recettes | supposée | `DEMO_COMPLEXE` | SUPPOSÉE |
| H-13 | Variables de projet dans `project_config/variables.json` (`standard`, `local`) | supposée | les deux | SUPPOSÉE |
| H-14 | Une recette de plugin a le type `CustomCode_<pluginId>-<recipeId>` et sa configuration dans `recipes/<nom>.json.payload` | supposée | `DEMO_COMPLEXE/plugin_geocode` | SUPPOSÉE |
| H-15 | Scénarios dans `project_config/scenarios/<id>.json`, `type: "step_based"`, avec des steps `build_flowitem` (`params.builds[].{type, itemId, partitionsSpec}`) | supposée | `DEMO_COMPLEXE/DAILY_BUILD` | SUPPOSÉE |
| H-15a | Triggers de scénario : `temporal` (`frequency`, `hour`, `minute`, `timezone`) et `ds_modified` (`projectKey`, `datasetName`) | supposée | `DEMO_COMPLEXE/DAILY_BUILD` | SUPPOSÉE |
| H-16 | Un check de dataset a la forme `{"type": "recordCountInRange", "minimum", "maximum", "id"}` | supposée | `DEMO_COMPLEXE/daily_depot_kpis` | SUPPOSÉE |
| H-17 | `${NODE_DC}` est une variable d'instance, absente de l'export : elle doit être signalée comme non résolue | CLAUDE.md | `DEMO_COMPLEXE/parcels_events` | SUPPOSÉE |

### Hypothèses ajoutées par le lecteur (phase 1)

| ID | Hypothèse | Source | Fixture / test | Statut |
|---|---|---|---|---|
| H-06 (complément) | `inputs` et `outputs` sont des dictionnaires `{"<rôle>": {"items": [...]}}`. `main` est le rôle usuel ; les autres rôles sont lus et conservés dans l'IR (`role`) | supposée | les deux | SUPPOSÉE |
| H-18 | Une variable s'écrit `${nom}`, où `nom` contient des lettres, des chiffres, `_` et `.`. Une variable non résolue reste telle quelle dans le texte et est listée | supposée | `tests/test_variables.py` | SUPPOSÉE |
| H-19 | Une variable scalaire est substituée par son texte (booléens en `true` / `false`). Une variable objet ou liste n'est jamais substituée : elle est signalée comme non résolue | supposée | `test_scalar_values_only` | SUPPOSÉE |
| H-20 | Liste des identifiants de types de recettes (visuelles, code, ML, préfixe `CustomCode_`). Un type absent de la liste n'est pas une erreur : il est conservé avec `status = UNKNOWN` | supposée | `test_unknown_types_are_kept_and_reported` | SUPPOSÉE |
| H-21 | Liste des identifiants de types de datasets (`PostgreSQL`, `HDFS`, `Snowflake`…) | supposée | idem | SUPPOSÉE |
| H-22 | Le moteur choisi est lu dans `params.engineParams.engineType` de la recette | supposée, **peu sûre** : le moteur réel pourrait n'être visible que via l'API (`get_selected_engine_details()`) | les deux | SUPPOSÉE |
| H-23 | Le nom d'une recette ne contient pas de `.`. Son payload est l'unique fichier `recipes/<nom>.*` autre que `<nom>.json` ; en cas de pluralité, l'extension égale au type est préférée, sinon une anomalie est levée | supposée | `test_missing_reference_and_payload` | SUPPOSÉE |
| H-24 | Priorité des variables, de la plus faible à la plus forte : variables d'instance fournies à l'appel, puis `standard`, puis `local`, puis `projectKey` (intégrée). Les variables d'instance (`${NODE_DC}`…) ne figurent pas dans l'export | supposée | `test_variable_priority`, `test_instance_variables_resolve_the_rest` | SUPPOSÉE |

### Hypothèses ajoutées après la lecture d'un projet réel via l'API (correctif de la phase 1)

Le projet a été lu via l'API par le pont `dku2dbx_bridge` (dépôt `agentic_projet`), faute de droit d'export. Seuls des compteurs anonymisés ont été partagés.

| ID | Hypothèse | Source | Fixture / test | Statut |
|---|---|---|---|---|
| H-25 | Une recette `sync` peut n'avoir aucun payload, ou un payload vide : toute sa configuration est dans sa définition. Pour ce type, l'absence de payload ne lève pas `MISSING_PAYLOAD`, alors qu'elle le lève toujours pour les autres recettes visuelles et de code | constatée via l'API : les 3 recettes `sync` d'un projet réel ont un payload vide. À revérifier sur un vrai export ZIP | `test_sync_recipe_payload_is_optional`, `test_other_visual_recipe_still_needs_a_payload` | CONSTATÉE (API) |
| H-26 | Les modèles enregistrés sont dans `project_config/saved_models/<id>.json` (`id`, `name`, `savedModelType`, `miniTask.{taskType, predictionType}`, `zone`). Une recette les référence par leur `id`, comme un dataset : en sortie pour l'entraînement, en entrée (rôle `model`) pour le scoring. Ils deviennent des nœuds `saved_model:<id>` du Flow | supposée. Constat sur un projet réel : sans cette modélisation, les entrées des recettes `prediction_scoring` étaient des références manquantes (2 cas) | `test_saved_model_is_read_and_linked_in_the_flow`, `test_undeclared_saved_model_is_still_a_missing_reference` | CONFIRMÉE (API) : 2 modèles lus sur le projet réel, plus aucune référence manquante. Le chemin dans un vrai export ZIP reste à vérifier |

### Hypothèses ajoutées par l'analyse (phase 2)

| ID | Hypothèse | Source | Fixture / test | Statut |
|---|---|---|---|---|
| H-27 | Processeurs Prepare qui exigent un référentiel ou un calcul hors SQL (classés `MANUAL`, ROADMAP phase 4 tâche 4) : `ZipCodeGeocoder`, `GeoIPProcessor`, `UserAgentClassifier`. Liste à compléter avec le catalogue | supposée (identifiants DSS non vérifiés) | `DEMO_COMPLEXE/prepare_depots`, `test_demo_complexe_figures` | SUPPOSÉE |
| H-28 | Processeurs Prepare porteurs d'une formule Dataiku : `CreateColumnWithGREL`, `FilterOnFormula` (mesure `formula_prepares`) | supposée | `test_formula_rich_prepare_and_disabled_group` | SUPPOSÉE |
| H-29 | Un scénario Python a le type `custom_python` ; un scénario step-based qui exécute du Python a une step de type `custom_python` | supposée | `test_python_scenarios_are_counted` | CONFIRMÉE (API) : sur un projet réel, 2 scénarios `custom_python` et 3 step-based avec une step `custom_python`, sur 13 |
| H-09h | Forme d'un filtre (pré-filtre, post-filtre, Sample/Filter, split) : `{enabled, distinct, uiData: {mode, conditions[]: {col, operator, string, num, input}}}`, plus `$status` (interface seulement). Les valeurs de `mode` et `operator` ne sont pas encore connues : un filtre actif lève `UnsupportedConstruct` tant qu'elles ne sont pas relevées | constatée (API, structure anonymisée) | `no_filter`, `formula_filter` (`make_fixture.py`) | CONSTATÉE (API) ; vocabulaire relevé, voir H-35 |

### Hypothèses ajoutées par les traducteurs (phase 3)

| ID | Hypothèse | Source | Fixture / test | Statut |
|---|---|---|---|---|
| H-30 | Une recette Sync copie, dans l'ordre du schéma de sortie, les colonnes de l'entrée que la sortie garde ; seul `schemaMode: STRICT_SYNC` (observé) est traduit, les autres modes lèvent `UnsupportedConstruct` | supposée ; `STRICT_SYNC` constaté via l'API (3 recettes) | `test_sync_keeps_output_columns_in_output_order`, `test_sync_unknown_schema_mode` | SUPPOSÉE |
| H-31 | Sample/Filter : le payload est le filtre ; `params.selection` porte `samplingMethod`, `maxRecords`, `partitionSelectionMethod`, `ordering` et un second filtre. Seul `HEAD_SEQUENTIAL` est traduit (`LIMIT`) | constatée (API, structure anonymisée : `HEAD_SEQUENTIAL`, `RANDOM_FIXED_NB`) | `test_head_sampling_is_a_limit_with_a_warning`, `test_sampling_never_guesses` | CONSTATÉE (API) |
| H-32 | Types de colonnes Dataiku → Spark : `string`, `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, et `date` → `TIMESTAMP` (une « date » Dataiku porte l'heure). Tout autre type lève `UnsupportedConstruct` quand un `CAST` est nécessaire | supposée | `test_stack_unknown_type_for_a_missing_column` | SUPPOSÉE |
| H-33 | Distinct : `keys[].column` (observé) ; sans clé, toutes les colonnes. La sortie ne garde que les clés (plus la colonne de comptage si `globalCount`) ; le schéma de sortie déclaré est vérifié, un écart lève `UnsupportedConstruct` | supposée ; `keys`, `globalCount` constatés (API) | `test_distinct_*` (`tests/test_translate_simple.py`) | SUPPOSÉE |
| H-34 | Sort : `orders[].{column, desc}` (observé) ; les NULL sont placés **en dernier** dans les deux sens (rendu explicite, car Spark les met en premier en tri croissant) | supposée, **à vérifier sur Dataiku** | `test_sort_runs_on_spark_and_warns` | SUPPOSÉE |
| H-35 | Une condition de filtre teste la colonne `input` avec `operator`, contre `string` ou `num`. Vocabulaire traduit (relevé sur un projet réel) : mode `&&` (ET) ; `== [string]` (égalité exacte, sensible à la casse), `>  [number]` (deux espaces, tel quel), `not empty` (ni NULL ni chaîne vide). Tout autre mode ou opérateur, `distinct: true` et le mode formule (`CUSTOM`, phase 4) lèvent `UnsupportedConstruct` | constatée (API) pour le vocabulaire ; supposée pour le rôle de `input` (et non `col`) et pour la sensibilité à la casse | `tests/test_translate_filters.py` | CONSTATÉE (API) / à confirmer |
| H-36 | Sort : une option de numérotation (`rowNumber`, `rank`, `denseRank`) ajoute une colonne, numérotée dans l'ordre du tri ; son nom est la seule colonne de sortie absente de l'entrée. Plusieurs options à la fois ne sont pas traduites | supposée ; `rowNumber` actif constaté (API) | `test_sort_row_number_runs_on_spark`, `test_sort_never_guesses` | SUPPOSÉE |
| H-37 | Join : `virtualInputs[i].index` est l'entrée lue comme table `i` ; `joins[]` s'enchaînent dans l'ordre (`table1` déjà jointe, `table2` nouvelle) ; condition `EQ` entre `column1` et `column2` `{table, name}` ; `selectedColumns[]` donne les colonnes de sortie dans l'ordre. Types `INNER` (observé), `LEFT`, `RIGHT`, `FULL` (supposés). Les clés se comparent comme en SQL : NULL ne rejoint jamais NULL. Refusés : `caseInsensitive`, `normalizeText`, `strict`, `rightLimit`, colonnes calculées, auto-cast, sélection automatique | constatée (API) pour la forme ; supposée pour les types autres qu'`INNER` et pour les NULL | `tests/test_translate_join.py` | CONSTATÉE (API) / à confirmer |
| H-38 | Group : `keys[].column` et `values[].{column, drapeaux}` (observés) ; `globalCount` ajoute une colonne `count` (`COUNT(*)`) ; une colonne d'agrégat s'appelle `<colonne>_<suffixe>` (`count`, `sum`, `avg`, `min`, `max`) et chaque colonne de sortie déclarée est rattachée par son nom à un seul agrégat, puis convertie au type déclaré. `stddev` (échantillon ou population ?), `first`/`last` (dépendent de l'ordre), `countDistinct`, `concat`… sont refusés tant qu'ils ne sont pas vérifiés | supposée pour les suffixes ; drapeaux constatés (API : `avg`, `min`, `stddev`, comptage actifs) | `tests/test_translate_group.py` | SUPPOSÉE |
| H-39 | Window : une seule entrée `windows[]` (partition, ordre, limites) et `values[]` (drapeaux d'agrégat, `lag`/`lead` avec décalages séparés par des virgules) ; options de numérotation à la racine. Colonnes calculées nommées `<col>_<agrégat>`, `<col>_lag<N>`, `<col>_lead<N>`, `rank`, `rowNumber`, `denseRank`, rattachées par nom au schéma déclaré | supposée pour les noms ; format constaté (API) | `tests/test_translate_window.py` | SUPPOSÉE |
| H-40 | Cadre des agrégats de fenêtre : sans limites, toute la partition ; avec limites (`ROWS`), `N PRECEDING` si `limitPreceding`, `M FOLLOWING` si `limitFollowing`, `UNBOUNDED` du côté non limité. Le cadre utilisé est écrit dans l'en-tête de chaque fichier généré. `RANGE` est refusé | supposée, **à vérifier sur Dataiku** (cas réel : `limitPreceding` actif) | `test_sum_over_the_whole_partition`, `test_limited_frame_moving_sum`, `test_unlimited_following_side` | SUPPOSÉE |
