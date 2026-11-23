# Pièges sémantiques Dataiku → Databricks

Repris de la section « Pièges sémantiques » de `ROADMAP.md`. Chaque piège doit avoir **au moins un test** et une règle dans le traducteur concerné. La colonne « Test » sera remplie au fil des phases.

| Piège | Dataiku | Databricks / Spark | Règle | Phase | Test |
|---|---|---|---|---|---|
| Chaîne vide et NULL | `isBlank`, `FillEmptyWithValue` et `coalesce` traitent `''` comme vide | `coalesce` ne remplace que NULL | Générer `coalesce(nullif(trim(col), ''), valeur)` ou l'équivalent | 4 | à faire |
| Arrondi des négatifs | Documenté : `round(-3.5) = -3` | `round` en HALF_UP, probablement `-4` | Tester, puis générer une expression qui reproduit Dataiku | 4 | à faire |
| Concaténation avec `+` | Concatène des chaînes (moteur DSS) | Addition numérique | Traduire en `concat()` selon le type | 4 | à faire |
| Dates et fuseau | Dates locales selon la configuration | Références de job en UTC | Paramètre de date explicite, documenter le décalage avec Paris | 6 | à faire |
| Patterns de date | — | `MM` = mois, `mm` = minutes | Table de conversion des formats testée | 4 | à faire |
| Fuzzy join | Damerau-Levenshtein (transposition = 1) | `levenshtein` simple (transposition = 2) | Classer `LLM_REVIEW` ou `MANUAL`, recalibrer les seuils | 3 | à faire |
| Top N à égalité | Ordre non déterministe | Idem | Ajouter une colonne de départage | 3 | à faire |
| Distances géographiques | — | `st_distance` est planaire (degrés) | Utiliser `st_distancesphere` pour des mètres | 4 | à faire |
| Typage | « Meanings » et inférence Dataiku | Types Spark stricts | Utiliser le schéma déclaré dans l'export, avec un `cast` explicite | 3–4 | à faire |
| Steps Prepare groupées | `metaType` différent de `PROCESSOR` | — | Lever une erreur, ne jamais filtrer en silence | 4 | à faire. La lecture conserve déjà le groupe et ses steps imbriquées (`test_prepare_steps_including_disabled_and_grouped`) ; l'erreur sera levée par le traducteur (phase 4) |
| Variables dans les noms de tables | `${projectKey}`, `${NODE_DC}` | — | Résoudre, sinon signaler | 1 | **fait** : `test_variables_resolved_in_table_names`, `test_unresolved_variable_is_listed` (`tests/test_reader.py`) et `tests/test_variables.py` |
| Échantillon « premières lignes » | `HEAD_SEQUENTIAL` : les N premières lignes dans l'ordre de stockage | `LIMIT N` sans ordre : lignes quelconques | Traduire en `LIMIT N` **avec un avertissement** dans l'en-tête et le rapport ; l'échantillonnage aléatoire lève `UnsupportedConstruct` | 3 | **fait** : `test_head_sampling_is_a_limit_with_a_warning`, `test_sampling_never_guesses` (`tests/test_translate_sync.py`) |
| Tri et NULL | Position des NULL non documentée ici | `ASC` : NULL en premier par défaut ; `DESC` : en dernier | Rendre l'ordre explicite (`NULLS LAST`, H-34), à vérifier sur Dataiku | 3 | **fait** : `test_sort_runs_on_spark_and_warns` |
| Ordre d'une table | Une recette Sort écrit des lignes triées | Une table Delta ne garantit aucun ordre de lecture | Traduire en `ORDER BY` avec un avertissement dans l'en-tête | 3 | **fait** : `test_sort_runs_on_spark_and_warns` |
| Jointure et NULL | Comportement sur les clés NULL non vérifié | `NULL = NULL` n'est jamais vrai : aucune correspondance | Comparaison SQL standard (H-37), à vérifier sur Dataiku ; sinon générer `<=>` | 3 | **fait** : `test_join_runs_on_spark` (clé NULL non appariée) |
