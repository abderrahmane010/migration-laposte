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
| H-07 | Une step Prepare a la forme `{"metaType": "PROCESSOR", "type", "params", "disabled"}` | CLAUDE.md | les deux | SUPPOSÉE |
| H-08 | Une step groupée a `metaType: "GROUP"`, un `name` et des `steps` imbriquées | supposée (CLAUDE.md dit seulement « autre `metaType` ») | `DEMO_COMPLEXE/prepare_events` | SUPPOSÉE |
| H-09 | Le payload d'une recette visuelle est `recipes/<nom>.<type>` : `.shaker`, `.join`, `.grouping`, `.window`, `.vstack`, `.split`, `.distinct`, `.pivot`, `.topn`, `.sort`, `.sync` | CLAUDE.md pour les principaux ; `.topn`, `.sort` et `.sync` supposés | les deux | SUPPOSÉE |
| H-09a | Payload Join : `virtualInputs[].preFilter`, `joins[].{table1, table2, type, conditionsMode, on[].{column1, column2, type, caseInsensitive, normalizeText}}`, `selectedColumns`, `postFilter` | supposée | les deux | SUPPOSÉE |
| H-09b | Payload Group : `keys[].column`, `values[]` avec un booléen par agrégat (`sum`, `avg`…), `globalCount`, `preFilter`, `postFilter` | supposée | les deux | SUPPOSÉE |
| H-09c | Payload Window : `windows[].{partitioningColumns, orders[], enableLimits}` et `values[]` (avec `lag`/`lagValues`) | supposée | les deux | SUPPOSÉE |
| H-09d | Payload Stack : `mode: "UNION"`, `virtualInputs`, `addOriginColumn`, `originColumnName` | supposée | les deux | SUPPOSÉE |
| H-09e | Payload Split : `mode: "VALUES"`, `column`, `values[].{value, outputIndex}`, `defaultOutputIndex` | supposée | `DEMO_COMPLEXE/split_status` | SUPPOSÉE |
| H-09f | Payload Top N : `topCount`, `bottomCount`, `orders[]`, `keys[]` | supposée | `DEMO_COMPLEXE/topn_late_depots` | SUPPOSÉE |
| H-09g | Payload Pivot : `keyColumns`, `valuesMode` (`EXPLICIT`), `explicitValues`, `rowIdentifiers`, `aggregations` | supposée | `DEMO_COMPLEXE/pivot_status` | SUPPOSÉE |
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
