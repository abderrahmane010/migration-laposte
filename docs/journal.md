# Journal du projet dku2dbx

Une entrée par phase de `ROADMAP.md`, rédigée à la fin de la phase, avant la validation humaine.

---

## Phase 0 — Socle du dépôt (2026-11-21)

**Statut :** terminée côté développement, **en attente de validation humaine**.

### Décisions prises avec le responsable

| Sujet | Décision |
|---|---|
| Installation de uv | Installeur officiel (`~/.local/bin/uv`, version 0.13.9) |
| Version de Python | 3.12 géré par uv (`.python-version`) ; `requires-python >= 3.11` ; CI en 3.11 et 3.12 |
| Plateforme de CI | GitHub Actions (`.github/workflows/ci.yml`) |
| Git | `git init` local sur la branche `main` ; commits Conventional Commits ; aucun remote |

### Réalisé

- Arborescence de `CLAUDE.md` : les packages `src/dku2dbx/*` sont vides, `config/` contient des squelettes, et `templates/`, `exports/` et `out/` existent.
- `pyproject.toml` avec les dépendances de `CLAUDE.md` et `uv.lock`. Configuration de `ruff` (lint et format), `mypy --strict` sur `src/` et `pytest`. Le marqueur `spark` est déclaré.
- CLI `dku2dbx` (typer) avec 6 sous-commandes vides. Chacune quitte avec le **code 2** et le message « pas encore implémentée (phase N) », pour qu'une commande vide ne passe jamais pour un succès.
- `tests/fixtures/make_fixture.py` génère deux exports ZIP synthétiques et reproductibles octet pour octet : entrées triées, date fixe, permissions fixes, aucune compression, JSON canonique.
  - `DEMO_SIMPLE` : 5 recettes (Prepare, Stack, Join, Group, Window), 8 datasets, aucune partition, une step Prepare désactivée. Expose `customer_stats`.
  - `DEMO_COMPLEXE` : 20 recettes de 14 types et 26 datasets, dont `parcels_events` partitionné par jour avec `${NODE_DC}` dans le nom de table. Il contient aussi :
    - une recette Python (idiomes `dataiku.*`) et une recette SQL ;
    - un plugin (`CustomCode_geo-toolkit-geocode`) ;
    - un scénario step-based `DAILY_BUILD` (trigger temporel et trigger dataset) ;
    - une step groupée (`metaType: GROUP`) ;
    - un processeur de référentiel (`ZipCodeGeocoder`) ;
    - une sortie en append et un check de dataset ;
    - 2 zones de Flow et la lecture du dataset partagé `DEMO_SIMPLE.customer_stats`.
- Les 25 hypothèses de format (H-01 à H-17, sous-hypothèses comprises) sont marquées `# ASSUMPTION` dans le générateur et listées dans `docs/hypotheses.md`.
- `docs/pieges_semantiques.md` reprend le tableau de la roadmap, avec une colonne de suivi des tests.
- La CI GitHub Actions lance `ruff check`, `ruff format --check`, `mypy`, la vérification que les fixtures sont à jour, `pytest` (avec Java 17 pour PySpark) et `dku2dbx --help`.
- `pre-commit` : hooks d'hygiène (espaces en fin de ligne, YAML et TOML valides, gros fichiers, clés privées), plus `ruff` et `mypy` lancés via `uv run`.

### Résultats

- `uv run pytest` : 24 tests passés et 1 sauté. Le test sauté vérifie PySpark local et ne peut pas tourner sans Java ici. Résultat identique sous Python 3.11.
- `ruff check`, `ruff format --check` et `mypy --strict` : propres.
- `uv run dku2dbx --help` liste les 6 commandes.

### Points ouverts

- ~~**Java absent du poste**~~ : résolu le 2026-11-22. Java a été installé et le test PySpark local passe.
- ~~**CI non encore exécutée**~~ : résolu le 2026-11-22. Le dépôt a été poussé sur GitHub et la CI est verte.
- Toutes les hypothèses de format sont `SUPPOSÉE` tant qu'aucun vrai export n'est disponible.

---

## Phase 1 — Lecteur d'export → modèle interne (2026-11-22)

**Statut :** terminée côté développement, **en attente de validation humaine**.

Phase 0 validée par le responsable le 2026-11-22 (CI verte sur GitHub).

### Réalisé

- **Modèle interne** (`model/ir.py`, pydantic v2, `extra="forbid"`) :
  - les objets : `Project`, `Zone`, `Dataset`, `Recipe` (avec `RecipeInput` et `RecipeOutput`), `PrepareStep` (récursif pour les groupes), `Scenario` (triggers et steps), `PluginUsage`, `ExposedObject` et `ForeignObject` ;
  - l'enrobage `IrDocument` et son en-tête de traçabilité `IrMeta` : générateur, version, fichier source et SHA-256 du ZIP ;
  - un `status` (`KNOWN` ou `UNKNOWN`) sur chaque objet et une `category` sur chaque recette (`VISUAL`, `CODE`, `PLUGIN`, `ML` ou `UNKNOWN`) ;
  - la définition brute (`raw`), conservée pour tout objet de type inconnu ;
  - le SHA-256 du JSON source sur chaque dataset, recette et scénario, pour les en-têtes de la phase 6.
- **Lecteur ZIP** (`reader/zip_reader.py`) :
  - il lit le ZIP en mémoire, sans jamais l'extraire, et refuse les entrées de plus de 64 Mo ;
  - il interprète le manifest, `params.json`, `variables.json`, les datasets, les recettes et leurs payloads (JSON pour les recettes visuelles et les plugins, texte pour le code), les scénarios et les zones ;
  - il ne plante jamais sur un type inconnu : l'objet est conservé, et `Project.issues` reçoit une anomalie. Les types d'anomalie sont `UNKNOWN_*`, `MISSING_REFERENCE`, `MISSING_PAYLOAD`, `MULTIPLE_PAYLOADS`, `INVALID_PAYLOAD`, `UNRESOLVED_VARIABLE`, `UNPARSED_ENTRY` et `FLOW_CYCLE` ;
  - toute entrée du ZIP non interprétée est listée dans `unparsed_entries`. C'est ce qui révélera les écarts de format sur les vrais exports ;
  - seul un export illisible lève `ExportReadError` : ZIP corrompu, manifest absent ou JSON invalide.
- **Variables** (`reader/variables.py`) : `${projectKey}`, les variables de projet et des variables d'instance optionnelles sont résolues. Le reste est listé dans `unresolved_variables`.
- **Graphe du Flow** (`model/flow.py`, networkx) :
  - les nœuds `dataset:<nom>` et `recipe:<nom>` ont un type : dataset, dataset partagé d'un autre projet ou référence manquante ;
  - l'ordre topologique est lexicographique, donc déterministe ;
  - les cycles sont détectés. Le graphe est stocké dans l'IR sous une forme sérialisable.
- **Lecteur API** (`reader/api_reader.py`) : l'interface seule, conforme au protocole `ProjectReader`. Elle lève `NotImplementedError` en attendant un accès à l'API.
- **CLI** : `dku2dbx analyze <export.zip> --dump-ir [--out out/]` écrit `out/<projet>/ir.json` et affiche un résumé des anomalies. Sans `--dump-ir`, la commande renvoie « prévue en phase 2 ».
- **Hypothèses** : H-18 à H-24 ont été ajoutées et H-02 et H-06 complétées (`docs/hypotheses.md`). Toutes sont marquées `# ASSUMPTION` dans le code.

### Résultats

- `uv run pytest` : **66 tests passés**, dont 2 snapshots d'IR (`tests/__snapshots__/test_reader/`).
- Critères de fin de phase :
  - les deux fixtures sont lues sans erreur ;
  - l'IR sérialisée est stable : elle est figée par snapshot et identique octet pour octet d'une exécution à l'autre ;
  - le graphe de `DEMO_SIMPLE` est vérifié : nœuds, arêtes et ordre topologique ;
  - un type inconnu (recette, dataset, scénario, `metaType` de step, fichier du ZIP) est conservé et signalé.
- `ruff check`, `ruff format --check` et `mypy --strict` (avec le plugin pydantic) : propres.

### Points ouverts

- **Aucun vrai export dans `exports/`** : toutes les hypothèses de format restent `SUPPOSÉE`. Le premier vrai export permettra de les confirmer ou de les corriger : `uv run dku2dbx analyze exports/<x>.zip --dump-ir`, puis examen de `unparsed_entries` et de `issues`.
- **H-22 (moteur d'exécution)** est l'hypothèse la plus fragile : le moteur réellement utilisé n'est peut-être visible que via l'API.
- **Variables d'instance** (`${NODE_DC}`) : le lecteur les accepte en paramètre (`instance_variables`), mais la CLI n'a pas encore d'option pour les passer. À décider en phase 2 ou en phase 7.

### Correctif après lecture d'un projet réel (2026-11-23)

Sans droit d'export, un projet réel a été lu via l'API par le pont `dku2dbx_bridge` (dépôt `agentic_projet`). Il a recomposé un export virtuel, puis le lecteur de cette phase l'a lu tel quel. Seuls des compteurs anonymisés ont été partagés. Résultat : 55 datasets, 45 recettes de 22 types, 13 scénarios et 8 zones, sans aucun type inconnu. Deux écarts venaient du lecteur et sont corrigés :

- **Recettes `sync` sans payload** (H-25) : elles levaient `MISSING_PAYLOAD` à tort (3 cas). L'absence de payload, ou un payload vide, est maintenant admise pour ce seul type.
- **Modèles enregistrés** (H-26) : les entrées des recettes `prediction_scoring` étaient des références manquantes (2 cas). L'IR a un nouvel objet `SavedModel` (`Project.saved_models`), lu dans `saved_models/<id>.json`, et le Flow un nœud `SAVED_MODEL`. Un modèle référencé mais absent de l'export reste une `MISSING_REFERENCE`.

Les snapshots d'IR ne changent que par le champ vide `saved_models`. Résultats : `uv run pytest`, **72 tests passés** ; `ruff` et `mypy --strict` propres.

Vérifié sur le projet réel après correction : 2 modèles enregistrés lus, et **aucune anomalie** `dku2dbx`. Seule reste l'erreur API attendue sur les paramètres du projet (droit d'administration).

Il reste à faire : les dossiers gérés (managed folders) ne sont pas encore modélisés. Ils produiraient le même type de référence manquante, mais aucun n'a été rencontré.

Phase 1 validée par le responsable le 2026-11-23, après vérification sur un projet réel lu via l'API.

## Phase 2 — Analyse, inventaire, score et vagues (2026-11-23)

### Décisions prises avec le responsable

- **Classe cible + état** : chaque recette reçoit la classe que la ROADMAP prévoit pour son type, plus un indicateur « traducteur disponible » (faux partout aujourd'hui). L'effort se calcule sur la classe cible. Les phases 3 et 4 ajouteront leurs types à `AVAILABLE_TRANSLATORS` (`analyze/classify.py`) : le classement se met à jour seul.
- **Recettes de code SQL** (`sql_query`, `sql_script`, `hive`, `impala`, `spark_sql_query`) : `LLM_REVIEW`, faute de traducteur prévu dans la ROADMAP. Une traduction déterministe par `sqlglot` pourra les faire passer en `AUTO` plus tard.
- **Seuils et effort** : valeurs de départ proposées dans `config/scoring.yaml`, toutes « à calibrer sur le pilote ».

### Classes fixées par la ROADMAP ou par moi, à relire

| Classe | Types | Origine |
|---|---|---|
| `AUTO`, phase 3 | `sync`, `sampling` (Sample/Filter), `distinct`, `vstack`, `join`, `grouping`, `window`, `split`, `sort`, `topn`, `pivot` | ROADMAP phase 3 |
| `AUTO`, phase 4 | `shaker` (Prepare) | ROADMAP phase 4 |
| `LLM_REVIEW`, phase 8 | `python`, `pyspark` | ROADMAP phase 8 |
| `LLM_REVIEW`, phase 8 | SQL de code (ci-dessus) | décision du 2026-11-23 |
| `LLM_REVIEW`, phase 8 | `fuzzyjoin`, `upsert` | **mon choix** : pas de traducteur prévu ; pour le fuzzy join, `pieges_semantiques.md` propose `LLM_REVIEW` ou `MANUAL` |
| `MANUAL` | `r`, `sparkr`, `spark_scala`, `shell`, recettes ML, plugins | ROADMAP (hors périmètre v1) |
| `MANUAL` | `geojoin`, `download`, `export`, `merge_folder`, type inconnu | **mon choix** : aucun traducteur prévu |

Une recette qui serait `AUTO` passe en `MANUAL`, avec son motif, si son payload est inexploitable, si un Prepare contient un processeur à référentiel (H-27) ou une step de `metaType` inconnu.

### Réalisé

- `analyze/model.py` : résultats sérialisables (classement, conditions atteintes, liens entre projets, catalogue, vagues).
- `analyze/classify.py` : classe cible, phase prévue, traducteur disponible et motif de chaque recette.
- `analyze/inventory.py` : comptages, mesures du score, processeurs actifs, steps désactivées (une step d'un groupe désactivé l'est aussi), liens avec d'autres projets vus depuis l'export (entrées `PROJET.dataset`, `foreignObjects`, `exposedObjects`).
- `analyze/scoring.py` : lecture et validation de `scoring.yaml` (un niveau, une mesure ou une classe inconnus sont des erreurs) ; niveau = le plus élevé dont une condition est atteinte ; effort = jours par classe + forfait par scénario. La grille est livrée avec le paquet, pour fonctionner hors du dépôt (pont `agentic_projet`).
- `analyze/portfolio.py` : catalogue des processeurs (trié par occurrences) et vagues (composantes fortement connexes regroupées, producteurs avant consommateurs ; liens hors portefeuille listés à part).
- `analyze/report.py` : `analyse.md`/`.json`, `portefeuille.csv`, `vagues.md`, `catalogue_processeurs.md`/`.csv`, reproductibles octet pour octet.
- CLI : `dku2dbx analyze <zip|dossier> [--out] [--dump-ir] [--scoring]`. Un export illisible n'arrête pas le portefeuille (code retour 1 à la fin) ; un projet présent deux fois est une erreur.
- Dépendance ajoutée : `pyyaml`.

### Résultats

- `uv run pytest` : **99 tests passés**, dont 1 snapshot de `analyse.md` de `DEMO_SIMPLE`.
- Critères de fin de phase :
  - `dku2dbx analyze tests/fixtures/exports --out out/` produit tous les fichiers ;
  - les chiffres de `DEMO_SIMPLE` sont vérifiés par test (`test_demo_simple_figures`) : 8 datasets, 5 recettes `AUTO`, niveau LOW, 1,25 jour ;
  - `DEMO_COMPLEXE` : 16 `AUTO`, 2 `LLM_REVIEW`, 2 `MANUAL`, niveau HIGH (1 plugin), 10,5 jours ; vague 2, après `DEMO_SIMPLE` ;
  - le catalogue des processeurs est généré.
- `ruff check`, `ruff format --check` et `mypy --strict` : propres.

### Vérification sur un projet réel (lu via l'API, compteurs anonymisés seulement)

- 45 recettes : 22 `AUTO`, 16 `LLM_REVIEW`, 7 `MANUAL` ; effort 42 jours ; recalculés à la main depuis les types de recettes.
- Niveau VERY_HIGH, dû à une seule condition : 5 scénarios Python (seuil 3). Les autres conditions atteintes sont de niveau HIGH (part de code 0,4, 24 datasets partitionnés, 2 modèles). H-29 confirmée : 2 scénarios `custom_python` et 3 step-based avec une step `custom_python`.
- Catalogue : 3 steps pour 4 recettes Prepare (2 recettes sans step). Le payload a bien une liste `steps` (H-07 confirmée).
- Types de steps de scénario rencontrés, utiles pour la phase 6 : `build_flowitem`, `custom_python`, `run_scenario`, `clear_items`, `sync_hive`.

### Points ouverts

- **Webapps** : critère de la ROADMAP non mesuré, le lecteur ne les lit pas encore.
- **Objets partagés** : sur le projet réel, la clé d'API ne peut pas lire `params.json` ; les `exposedObjects` manquent. Les vagues ne reposent alors que sur le côté consommateur (entrées `PROJET.dataset`).
- **Calibrage** : seuils et jours d'effort à ajuster sur le pilote ; listes H-27 et H-28 à compléter avec le catalogue réel.
- **Variables d'instance** (`${NODE_DC}`) : toujours pas d'option CLI ; reporté en phase 7.

Phase 2 validée par le responsable le 2026-11-23, grille de score conservée telle quelle.

## Phase 3 — Traducteurs des recettes visuelles relationnelles (démarrée le 2026-11-23)

### Décisions prises avec le responsable

- **Format réel d'abord** : avant d'écrire les traducteurs, la structure anonymisée des payloads visuels du projet réel est relevée (`python -m dku2dbx_bridge.structure`, dépôt `agentic_projet` : chemins de clés, types et codes en majuscules uniquement).
- **Noms de tables** : le SQL généré référence les datasets par leur nom Dataiku ; les phases 6 et 7 les remplaceront par `catalog.schema.table` de façon déterministe (`sqlglot`).

### Avancement

- **Format réel relevé** (structure anonymisée) : les fixtures suivent maintenant le vrai format ; H-09a à H-09g corrigées ou confirmées, H-09h (forme des filtres) ajoutée. La commande a été durcie après une fuite d'une valeur saisie (`FAILED`) : une valeur texte n'est montrée que sous une clé de réglage Dataiku.
- **Socle de traduction** : `UnsupportedConstruct` (jamais de devinette), modèle `Translation`, SQL construit avec `sqlglot` (identifiants toujours entre backticks, SQL relu par `sqlglot`), en-tête de traçabilité (projet, recette, version, SHA-256 de la définition et du payload, avertissements), registre `TRANSLATORS` lu par la classification de la phase 2.
- **Commande** `dku2dbx translate <export.zip> [--out]` : un `.sql` par sortie dans `out/<projet>/sql/`, et `translate_report.json` (traduites, non prises en charge avec motif, sans traducteur).
- **Sync/Filter** : Sync (`STRICT_SYNC`) et Sample/Filter (`HEAD_SEQUENTIAL` → `LIMIT`, avec avertissement). Un filtre actif lève `UnsupportedConstruct` tant que le vocabulaire des opérateurs n'est pas relevé. Les dépendances de partition sont signalées (phase 6).
- **Distinct, Stack, Sort** : Distinct sur clés (avec comptage) ou sur toutes les colonnes ; Stack en mode `UNION` (colonnes absentes d'une entrée en `CAST(NULL AS type)`, colonne d'origine) ; Sort en `ORDER BY` avec NULL en dernier (H-34) et un avertissement sur l'ordre des tables Delta. Chaque traducteur vérifie que le SQL produit exactement le schéma de sortie déclaré. La fixture `distinct_depots` dédoublonnait sur une clé en gardant toutes les colonnes : incohérent, elle dédoublonne maintenant sur toutes les colonnes.
- **Filtres** (deuxième relevé de structure) : le vocabulaire observé est traduit (`&&`, `== [string]`, `>  [number]`, `not empty`) dans les pré-filtres, post-filtres (sous-requête) et Sample/Filter ; tout le reste est refusé avec son motif. Les valeurs saisies passent par `sqlglot` (échappement testé sur PySpark). Sort traduit aussi `rowNumber`, `rank` et `denseRank` (H-36).
- **Join** : jointures enchaînées `INNER`/`LEFT`/`RIGHT`/`FULL` sur conditions d'égalité, colonnes qualifiées par table (`t0`, `t1`…), pré-filtre d'une entrée en sous-requête, post-filtre ; schéma de sortie vérifié. Les 4 jointures des fixtures sont traduites.
- **Group** : clés, comptage global, agrégats `count`/`sum`/`avg`/`min`/`max` rattachés par nom aux colonnes déclarées et convertis à leur type (H-38), pré- et post-filtres. `stddev` et les agrégats dépendant de l'ordre sont refusés.
- **Window** : agrégats (ceux de Group), `lag`/`lead` multiples, `rank`/`rowNumber`/`denseRank`, cadre `ROWS` toujours explicite et rappelé dans l'en-tête (H-40). La fixture Window de `DEMO_COMPLEXE` triait sur `day`, une dimension de partition et non une colonne : elle trie maintenant sur le retard moyen (Sort et Top N en aval ajustés).
- **Split** (mode `VALUES`) : un `SELECT` par sortie (fichiers `<recette>__<sortie>.sql`), sortie par défaut avec les NULL et les valeurs sans règle, comparaison insensible à la casse, pré-filtre appliqué à toutes les sorties ; testé : chaque ligne tombe dans exactement une sortie. Les colonnes non texte et les autres modes sont refusés.
- **Top N** : N premières et/ou M dernières lignes par groupe (`ROW_NUMBER`), pré-filtre avant le classement, départage déterministe des égalités par les autres colonnes avec avertissement (piège « Top N à égalité » couvert).
- **Pivot** (valeurs explicites) : comptage et agrégats par valeur (`CASE WHEN`), identifiants en `GROUP BY` ; le pivot dynamique (`TOP_N`) est signalé (H-43).

### Résultats

- `uv run pytest` : **240 tests passés**, dont des exécutions sur PySpark local pour chaque traducteur et 14 snapshots de SQL.
- Critères de fin de phase :
  - les 11 types de la phase 3 sont couverts (les 10 de la ROADMAP, Sync et Sample/Filter comptés à part), chacun avec snapshot du SQL, exécution PySpark sur données synthétiques et cas d'erreur `UnsupportedConstruct` ;
  - toutes les recettes relationnelles de `DEMO_SIMPLE` et `DEMO_COMPLEXE` sont traduites, sans refus (`test_phase_3_end_criterion_every_relational_recipe_is_translated`) ;
  - `dku2dbx translate` écrit un fichier `.sql` par sortie dans `out/`, de façon reproductible.
- `ruff check`, `ruff format --check` et `mypy --strict` : propres.
- Pièges sémantiques couverts par un test : échantillon « premières lignes », tri et NULL, ordre d'une table, Top N à égalité, jointure et NULL, écart-type (refusé), cadre de fenêtre.

### Points ouverts

- **Sur le projet réel, encore refusés (voulu)** : les 2 Group et l'éventuel Window avec `stddev` ; le Split au pré-filtre `distinct` ; le Pivot dynamique (`TOP_N`) ; le Sample/Filter aléatoire (`RANDOM_FIXED_NB`). Chacun apparaît dans `translate_report.json` avec son motif.
- **Hypothèses à confirmer côté Dataiku** : cadre de fenêtre (H-40, le cas réel utilise `limitPreceding`), place des NULL au tri (H-34), clés NULL en jointure (H-37), noms des colonnes calculées (H-38, H-39, H-43), rôle de `input` dans un filtre et sensibilité à la casse (H-35).
- **Fixtures corrigées en cours de phase** : `distinct_depots` (dédoublonnage incohérent) et `window_depot_trend` (tri sur une dimension de partition) ; les traducteurs ont détecté ces incohérences grâce au contrôle du schéma de sortie.
- **Dépendances de partition** : signalées dans l'en-tête, appliquées par le job en phase 6.

Phase 3 validée par le responsable le 2026-11-23, après traduction du projet réel (14 recettes visuelles sur 18 traduites, 4 refusées avec leur motif).

## Phase 4 — Recette Prepare et langage de formules (démarrée le 2026-11-23)

- **Format réel d'abord**, comme en phase 3 : la commande de structure anonymisée du pont couvre maintenant les étapes Prepare (chemins de paramètres par type de processeur) et un recensement des formules (noms de fonctions Dataiku, opérateurs, nombre de littéraux ; jamais le texte d'une formule).
- **Langage de formules** (`translate/prepare/formula.py`) : grammaire `lark` (H-44), expressions typées (texte, nombre, booléen, date) pour choisir entre addition et concaténation, table explicite de 20 fonctions, variables de projet résolues, colonnes vérifiées à chaque étape. Pièges couverts sur PySpark : arrondi des négatifs, `+` entre chaînes, chaîne vide, division par zéro. Le test Spark de la phase 0 utilise maintenant la session partagée (il l'arrêtait pour les tests suivants).
- **Recette Prepare** (`translate/prepare/recipe.py`, `processors.py`) : une CTE par étape, schéma suivi d'étape en étape (une formule voit les colonnes créées avant elle), étapes et groupes désactivés ignorés et listés dans l'en-tête, groupes traités explicitement, autre `metaType` refusé. Registre de 7 processeurs dans l'ordre du catalogue (H-47) ; les processeurs à référentiel (H-27) sont refusés avec la cible proposée (UDF Unity Catalog ou opérateur personnalisé). Le registre des traducteurs a été déplacé dans `translate/registry.py` (import circulaire). Fixture corrigée : `prepare_events` retirait une colonne absente de son entrée.

### Résultats (en attente du format réel des processeurs)

- `uv run pytest` : **322 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase :
  - processeurs couvrant au moins 80 % des occurrences du catalogue : **atteint sur les fixtures** (12 sur 13, seul `ZipCodeGeocoder`, à référentiel, reste) et sur le projet réel (3 étapes sur 3 : `CreateColumnWithGREL`, `ColumnsSelector`) ; `test_phase_4_end_criterion_catalogue_coverage` ;
  - chaque recette Prepare des fixtures est traduite (`DEMO_SIMPLE` : 5 recettes sur 5) ou listée avec l'étape non couverte (`prepare_depots` : `ZipCodeGeocoder`, MANUAL) ;
  - `docs/pieges_semantiques.md` à jour : arrondi des négatifs, `+` entre chaînes, chaîne vide, division par zéro, steps groupées.

### Points ouverts

- **Format réel des paramètres des processeurs (H-47)** : non relevé. Le relevé du projet de diagnostic (3 étapes) le confirmera en partie ; un projet riche en Prepare est nécessaire pour un catalogue représentatif.
- **Fonctions de formule** : 20 fonctions traduites ; le recensement anonyme du projet réel dira lesquelles ajouter (dates, conversions…).
- **Dates** (ajouté ensuite) : table de conversion des formats `SimpleDateFormat` → Spark (piège « Patterns de date », un bug de comptage des lettres trouvé par les tests) et processeur `DateParser` (fuseau déclaré → UTC, H-48), testés sur PySpark en hiver et en été.
- **NULL dans `+`** (H-46) et `substring` (H-45) à vérifier sur Dataiku.

### Vérification sur le projet réel (relevé anonymisé)

- Format des étapes : `CreateColumnWithGREL {column, expression}` et `ColumnsSelector {appliesTo: COLUMNS, columns, keep}` conformes à H-47 ; les clés `alwaysShowComment` et `preview` ne servent qu'à l'affichage.
- Formules : `if`, `!=`, `*`, nombres et texte, tous traduits.
- `dku2dbx translate` : **18 recettes traduites** (14 visuelles, 4 Prepare sur 4), 4 refusées (les mêmes qu'en phase 3), 23 sans traducteur (code, ML).
- Reste supposé : le format des 6 autres processeurs (aucune occurrence dans ce projet) ; à confirmer sur un projet riche en Prepare au pilote.

Phase 4 validée par le responsable le 2026-11-23 (format des processeurs du projet réel confirmé ; 6 processeurs restent supposés, à vérifier au pilote).

## Phase 5 — Simplification du Flow (démarrée le 2026-11-23)

- Règles de conservation de la ROADMAP, plus une règle dans le même esprit : une recette refusée par son traducteur (`UnsupportedConstruct`) est traitée comme une recette `MANUAL` (ses entrées et sorties restent matérialisées), car elle ne peut pas être fondue en CTE.

### Réalisé

- `simplify/flow.py` : raisons de conservation de chaque table produite (règles de la ROADMAP et recette non traduisible), fusion par union-find des producteurs de tables non conservées avec leur unique lecteur, étapes dans l'ordre topologique ; un Flow avec cycle est refusé. `simplify/report.py` : `simplify.md` et `simplify.json`, reproductibles.
- CLI : `dku2dbx simplify <export.zip> [--out] [--no-fusion]`.

### Résultats

- `uv run pytest` : **357 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase :
  - chaque règle de conservation est vérifiée sur un graphe construit à la main (`test_each_keep_rule`, 8 règles, plus la sortie d'une recette non `AUTO`) ;
  - la sortie de `DEMO_COMPLEXE` est vérifiée par snapshot (20 recettes → 18 étapes, 2 tables supprimées) ;
  - `--no-fusion` redonne une étape par recette sur les deux fixtures.
- `DEMO_SIMPLE` : 5 recettes → 3 étapes (Prepare, Stack et Join fusionnés ; `orders_enriched`, lue deux fois, reste).

### Points ouverts

- `DEMO_COMPLEXE` fusionne peu (presque toutes ses tables sont partitionnées, lues plusieurs fois ou touchent une recette non `AUTO`) : le gain réel se mesurera sur les vrais projets.
- Les étapes sont prêtes pour la génération (phase 6) : une étape écrit ses tables matérialisées, ses tables supprimées y deviennent des CTE.

Phase 5 validée par le responsable le 2026-11-23. Décision : une table **construite** par un scénario (step « build ») reste matérialisée, comme une table lue par un scénario (extension de la règle de la ROADMAP, H-49). Projet réel : 45 recettes → 44 étapes (projet de diagnostic, sans longues chaînes ; 32 tables retenues par un scénario).

### Décisions à prendre en fin de phase 9 (notées le 2026-11-23, rien n'est fait)

- Réunir le pont `dku2dbx_bridge` (dépôt `agentic_projet`) dans `dku2dbx`, comme implémentation de `DataikuApiReader` (interface de la phase 1), avec une commande `dku2dbx export-api`.
- Ajouter une commande `dku2dbx migrate <projet>` qui enchaîne les étapes et regroupe les sorties dans un dossier unique par projet, avec un `RAPPORT.md` de synthèse. Hors ROADMAP actuelle : à y inscrire (phase 9 ou pilote) si le responsable le décide.

## Phase 6 — Générateurs de code et bundle (démarrée le 2026-11-23)

### Décisions prises avec le responsable

- **Cibles** : A (job de notebooks SQL) d'abord, puis B (Lakeflow pipeline), dans la même phase.
- **Validation** : locale, contre le schéma JSON officiel des bundles extrait de la CLI Databricks 1.24.0 (version de plus de deux semaines, sommes de contrôle vérifiées ; `databricks bundle schema`, sans connexion à un workspace). Schéma versionné compressé dans `tests/schemas/`. Deux adaptations documentées : les motifs `\p{…}` (inconnus du `re` de Python) sont écartés, et les `oneOf` du schéma, dont les branches se chevauchent, sont lus comme `anyOf` ; un test vérifie que le schéma adapté rejette toujours les erreurs.
- **Catalogue et schéma** : variables du bundle, valeurs fictives explicites (`[CATALOG_DEV]`…), aucun nom réel.

### Avancement (cible A)

- `generate/bundle.py` : un notebook SQL par étape de la phase 5 ; les tables non matérialisées deviennent des CTE (y compris les `WITH` imbriqués des recettes Prepare, validés sur PySpark) ; `CREATE OR REPLACE TABLE … AS`, ou `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` en mode append ; `USE CATALOG` / `USE SCHEMA` sur les paramètres du job (H-50) ; en-tête de traçabilité (recettes, SHA-256, avertissements). Une étape non traduite (code, ML, refus, type de colonne inconnu) devient un notebook qui échoue explicitement (`raise_error`) : le job reste complet.
- Job : une tâche de préparation du schéma, puis une tâche par étape, `depends_on` tiré du Flow. `databricks.yml` : targets `dev` / `prod`, variables, permissions. YAML écrit par des templates `jinja2` (`templates/`, livrés dans le paquet), chaque valeur passée par `tojson`.
- CLI : `dku2dbx generate <export.zip> [--target job] [--out] [--no-fusion]` (`--target pipeline` : pas encore).
- Tests : schéma officiel, cellules SQL relues par `sqlglot`, reproductibilité, notebooks qui échouent, mode append, et **exécution de bout en bout de `DEMO_SIMPLE` sur PySpark** dans l'ordre du job, résultats calculés à la main.
- Fixture : le type `timestamp` (inventé) remplacé par le type Dataiku `date`.
- Bug trouvé en cours de route : `sqlglot` range la clause `WITH` sous la clé `with_` ; les CTE disparaissaient en silence, l'exécution de bout en bout l'aurait détecté.

- **Scénarios** (H-51) : un job par scénario step-based, avec les seules étapes qu'il construit et leur amont, planifié (`schedule`, fuseau Dataiku) ou déclenché par une table ; le flux complet reste un job non planifié. Scénarios Python et steps inconnues : MANUAL, listés.
- **Variables** : chaque variable scalaire devient une variable du bundle et un paramètre de job `dku_<nom>` ; une formule qui l'utilise lit `:dku_<nom>`, typé par un `CAST` (la valeur Dataiku est le défaut, modifiable sans régénérer).
- **Checks** (H-52) : `recordCountInRange` → `assert_true` après l'écriture ; les autres types sont listés.
- **`GENERATION.md`** dans le bundle : étapes à migrer à la main, scénarios (planification, éléments non traduits), variables, checks non traduits.

- **Partitions** (H-53) : colonne `DATE` de partition, `CREATE TABLE IF NOT EXISTS` puis `INSERT INTO … REPLACE WHERE`, entrées « même jour » lues sur la seule partition construite ; paramètres `date`, `date_offset`, `timezone` (le job de scénario reprend le fuseau et le décalage `PREVIOUS_DAY`) ; partitionnements et dépendances non traduits → étape à migrer à la main. Testé sur PySpark avec des paramètres nommés.

- **Cible B** (H-54) : pipeline Lakeflow serverless de vues matérialisées (un fichier SQL par étape), variables lues dans la `configuration` du pipeline ; étapes partitionnées, en append, non traduites et leur aval en tâches SQL après le pipeline ; checks des vues dans une tâche dédiée. `DEMO_SIMPLE` tient entièrement dans le pipeline.

### Résultats

- `uv run pytest` : **394 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase :
  - le bundle de `DEMO_SIMPLE` (et de `DEMO_COMPLEXE`) passe la validation de schéma locale, pour les deux cibles (`databricks.yml`, jobs, pipeline) ;
  - le SQL généré parse avec `sqlglot` ;
  - un test de bout en bout exécute les étapes de `DEMO_SIMPLE` sur PySpark local, pour la cible A (dans l'ordre du job) et pour la cible B (dans l'ordre du pipeline), résultats calculés à la main.
- Pièges couverts : dates et fuseau (date de partition calculée dans le fuseau du job), ordre de lecture, partitions.

### Points ouverts

- **Rien n'a encore tourné sur un workspace Databricks** : `databricks bundle validate` et un déploiement réel restent à faire dès qu'un bac à sable est disponible (H-50, H-54).
- Le projet réel : périodes et dépendances de ses 24 partitions à relever (structure anonymisée) ; scénarios à `run_scenario`, `clear_items`, `sync_hive` encore MANUAL.
- Sources partitionnées : elles devront avoir la colonne de date une fois branchées (phase 7).
