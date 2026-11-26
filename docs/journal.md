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
- Projet réel (relevé anonyme) : 21 partitions `time`/`DAY` et 20 dépendances `equals` traduites ; **`all_available` ajoutée** (5 dépendances : l'entrée est lue entière). Restent refusées : **3 partitions discrètes `value`** (quelle valeur un job doit-il construire ? à décider au pilote).

Phase 6 validée par le responsable le 2026-11-24. Projet réel : 44 étapes, dont 28 à migrer à la main (23 sans traducteur, 4 refus connus, 1 partition par valeur), 16 générées en SQL Databricks.

## Phase 7 — Branchement des sources (2026-11-24)

### Décisions prises avec le responsable

- **Mapping hors Git** : `config/connections.yaml` (versionné) ne contient que des exemples fictifs ; les vraies valeurs vont dans `config/connections.local.yaml` (ignoré par Git), lu en priorité. Une valeur fictive (`[…]`) n'est jamais utilisée.
- **Source non branchée** : une tâche d'ingestion de substitution, qui échoue explicitement, précède les étapes qui la lisent ; le reste du bundle est généré.
- **Vérification Unity Catalog** : prévue (`--check-uc`, SDK Databricks en dépendance optionnelle, testée avec un faux client), non activée faute de workspace.

### Réalisé

- `connect/resolve.py` : chargement et validation du mapping (connexions, `shared_projects`, `instance_variables`), résolution des sources (H-55) ; une variable Dataiku ne bloque que si le modèle de nom l'utilise. Les variables d'instance (point ouvert depuis la phase 1) se renseignent maintenant dans le mapping local.
- `connect/report.py` : `a_brancher.md` (raisons, tables branchées, sources partitionnées, graphe Mermaid avec les sources à brancher en pointillés) et `connect.json`.
- `connect/uc_check.py` : existence des tables et colonnes attendues (faux client en test).
- `generate` : sources branchées qualifiées en `catalog.schema.table` (cibles A et B) ; tâches `ingest_NN` qui échouent pour les sources non branchées, dont dépendent les étapes et le pipeline qui les lisent ; section « Sources » dans `GENERATION.md`.
- CLI : `dku2dbx connect <export.zip> [--connections] [--out] [--check-uc --profile]` ; `generate --connections`.

### Résultats

- `uv run pytest` : **405 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase : les tests couvrent la résolution, les variables non résolues (puis résolues par variable d'instance) et une connexion inconnue ; le rapport « à brancher » est produit pour les fixtures.

### Points ouverts

- Vérification Unity Catalog et ingestion réelle des sources : à faire avec un workspace (le chantier d'ingestion HDFS → Databricks reste hors de `dku2dbx`).
- Les sources partitionnées devront avoir leur colonne de date (H-53).
- Projet réel : 2 sources, toutes deux à brancher. `join_dataset` est un dataset Inline, à recréer en table de référence. `out_s0` est un fichier CSV dans HDFS (connexion `ti-dataiku`), qui n'est synchronisé avec aucun metastore : il faut le copier dans un Volume, le charger en table, puis déclarer son catalogue et son schéma dans le mapping local. Ces noms seront choisis avec l'équipe plateforme.
- Amélioration possible (non faite) : reconnaître les datasets fichiers HDFS pour afficher une raison plus précise.

Phase 7 validée par le responsable le 2026-11-24 (406 tests passés).

## Phase 8 — Résidu assisté par LLM (2026-11-24)

### Décisions prises avec le responsable

- **Python reste en pandas** : seuls les appels `dataiku` sont réécrits. La logique métier n'est pas convertie en PySpark, ce qui ne serait pas déterministe.
- **Une réécriture complète reste « à relire »** : en-tête `TO REVIEW`, distinct de `REVIEW_REQUIRED`, qui est réservé au LLM. Elle n'est jamais AUTO.
- **Recettes SQL en déterministe** : transpilation sqlglot depuis le dialecte de la connexion ; ce qui ne passe pas va au résidu LLM.
- **LLM** : client de l'endpoint Model Serving plus faux client dans les tests. Désactivé tant que `config/llm.yaml` est vide. Pas d'export Lakebridge Switch.

### Réalisé

- `translate/code/sql_recipe.py` (H-56) : une requête par recette ; les variables deviennent des valeurs ou des paramètres de job, et les tables physiques des entrées deviennent des datasets. Sont refusés : table inconnue, fonction inconnue de sqlglot, plusieurs instructions, non-SELECT, colonnes différentes du schéma Dataiku, variable dans une chaîne plus longue. Les divisions entières sont signalées (`pieges_semantiques.md`).
- `translate/code/python_recipe.py` (H-57) : réécriture sur l'arbre syntaxique, en recollant le texte source (commentaires et mise en forme conservés). Idiomes : `get_dataframe`, `write_with_schema`, `write_dataframe`, `get_custom_variables`, `default_project_key`, `Folder(...).get_path()`. Tout autre usage de `dataiku` (dont `api_client()`), un code Python 2, des partitions ou des arguments refusent la recette entière : jamais de réécriture partielle.
- Bundle : les recettes Python deviennent des notebooks `src/<étape>.py`, en tâches du job (jamais dans le pipeline de la cible B), avec leurs checks après l'écriture ; section « Code réécrit à relire » dans `GENERATION.md`.
- `translate/llm/` : `config/llm.yaml` validé (sans token), prompts versionnés `python_v1` et `sql_v1`, entrées JSON reproductibles, appel via le SDK Databricks (H-58), extraction d'un seul bloc de code, contrôles statiques (`ast`, `ruff`, `sqlglot`). La sortie porte `REVIEW_REQUIRED` avec l'endpoint et le SHA-256 du prompt. `RESIDU.md` et `residue.json` listent chaque fichier à relire, y compris les réécritures déterministes.
- CLI : `dku2dbx residue <export.zip> [--llm-config] [--connections] [--out]` ; `translate` écrit aussi `python/*.py`.
- Dépendances de développement : `pandas` (<3, comme les runtimes Databricks) et `pyarrow`, pour les tests d'exécution des recettes Python.

### Résultats

- `uv run pytest` : **475 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase :
  - la recette Python de `DEMO_COMPLEXE` est réécrite par les règles déterministes, avec un snapshot, et exécutée sur PySpark local avec des résultats calculés à la main ;
  - le flux LLM fonctionne avec un faux client : réponses valides, invalides, inexploitables et erreurs d'appel ;
  - le rapport liste chaque fichier à relire.
- `DEMO_COMPLEXE` : la recette SQL et la recette Python sont traitées par les règles ; il ne reste plus que 2 étapes à migrer à la main (Prepare avec géocodage, plugin).

### Points ouverts

- **Aucun endpoint validé** : le flux LLM n'a tourné qu'avec le faux client (H-58 à vérifier).
- Projet réel : nombre de recettes Python et SQL réécrites, et raisons des refus, à relever (relevé anonyme).
- **Endpoint interne compatible OpenAI (décision du 2026-11-24)** : la règle 5 de `CLAUDE.md` autorise désormais, en plus de Databricks Model Serving, un endpoint interne La Poste compatible OpenAI (vLLM, Qwen2.5-7B pour commencer, modèle configurable). Client en bibliothèque standard : HTTPS obligatoire, TLS toujours vérifié (certificat de l'autorité interne), sans proxy par défaut, clé lue dans une variable d'environnement. Configuration réelle dans `config/llm.local.yaml`, ignoré par Git, ou dans le `.env` (`VLLM_BASE_URL`, `VLLM_MODEL`, `SSL_CA_BUNDLE`) (H-59, à vérifier).
- Les 3 recettes `pyspark` d'AEL lisent une connexion Dataiku par l'API d'administration (`client.get_connection`) : aucune règle déterministe possible, le LLM ne peut que proposer un remplacement (secrets, connexion Unity Catalog) à relire.
- **Premier vrai export ZIP (2026-11-24)** : projet de tutoriel public d'une instance d'essai Dataiku Cloud (DSS 15.2.2), exporté par l'API. Le lecteur l'ouvre sans erreur ; 16 entrées non utiles à la migration (analyses ML, dashboards, explorations, timeline) sont listées. Refus propres : processeur Prepare `ColumnReorder` et opérateur de date `<  [date]` d'une recette Sampling (à ajouter). L'export inclut par défaut les fichiers chargés (`exportUploads`, un CSV ici) : à décocher pour les exports La Poste.
- **Premier appel LLM réel** sur ce projet public (aucune donnée La Poste) : `gpt-4o-mini` par l'API compatible OpenAI (H-59 vérifiée sur ce format). Sortie `REVIEW_REQUIRED`, contrôles `ast` et `ruff` passés, plus aucun `dataiku`. La relecture a trouvé un défaut du prompt `python_v1` : il imposait `spark.createDataFrame(df)` même pour un DataFrame Spark (recette `pyspark`), ce qui plante. Corrigé par `python_v2`, qui sépare pandas et Spark et retire `SparkContext`/`SQLContext` ; `python_v1` reste dans le dépôt pour la traçabilité.
- **Second projet réel (AEL, relevé anonyme du 2026-11-24)**, lu via le pont : 9 datasets HDFS, 5 recettes (1 `sync`, 3 `pyspark`, 1 plugin non déclaré dans l'export), 0 scénario, 0 erreur API. Classes : AUTO 1, LLM_REVIEW 3, MANUAL 1. Le `sync` est traduit, aucune construction n'est refusée. Phase 7 : 4 sources sur 3 connexions, toutes branchées avec un mapping fictif (`connections.local.yaml`). Bundle de 5 étapes, dont 4 à migrer à la main. Résidu : 3 entrées prêtes, LLM désactivé. Aucune recette `python` : la réécriture déterministe n'a pas pu être testée sur ce projet.
- Les recettes `pyspark`, `sql_script`, `hive`, `impala` et `spark_sql_query` n'ont pas de règles déterministes : elles vont au résidu LLM.

Phase 8 validée par le responsable le 2026-11-24 (486 tests passés). Décisions en cours de phase : endpoint interne compatible OpenAI autorisé (règle 5), prompt `python_v2`. Restent ouverts : appel du vLLM interne (service arrêté, erreur 503), endpoint Databricks Model Serving, réécriture déterministe des recettes `pyspark`, exécution des notebooks sur un workspace. Suite du travail sur un poste personnel avec une instance d'essai Dataiku Cloud (projets fictifs ou publics uniquement).

## Phase 9 — Réconciliation (2026-11-24)

### Décisions prises avec le responsable

- Clés métier, tolérances, colonnes ignorées et exclusions déclarées par dataset dans `config/reconcile.local.yaml` (ignoré par Git), sinon `config/reconcile.yaml`.
- Comparaison en Spark seul (pas de `datacompy`).
- Validation sur PySpark local ; le test sur un vrai workspace viendra avec le pilote.

### Réalisé

- `reconcile/compare.py` : les quatre couches de la ROADMAP. Schéma (colonnes absentes ou en trop, types, nullabilité ; un élargissement sans perte comme `int -> bigint` est toléré), comptes au total et par colonne de partition (par défaut les dimensions de partition du dataset), agrégats par colonne (NULL, distincts, min, max, plus la somme des numériques), lignes (jointure complète sur la clé : clés manquantes de chaque côté, clés en double, valeurs différentes avec un exemple ; sans clé, `EXCEPT ALL` dans les deux sens). Les tolérances absolue et relative ne s'appliquent qu'aux valeurs numériques ; les comptes, NULL et distincts restent exacts. Le module n'importe que pyspark : il est copié dans le notebook.
- `reconcile/notebook.py` : plan (tables produites par le flux simplifié, phase 5), notebook Databricks autonome (paramètres `catalog`, `schema`, `ref_catalog`, `ref_schema`, `results_table`), résultat par table ajouté à une table Delta, échec du notebook si une table est en `ÉCART`, table absente comptée comme `ÉCART` au lieu de faire planter l'exécution. En-tête de traçabilité : projet, version, SHA-256 de la configuration et du JSON de chaque dataset (H-60).
- `RECONCILIATION.md` (plan) et `ECARTS_ACCEPTES.md` (registre des écarts acceptés : corriger, ou reproduire l'ancien comportement), jamais réécrit une fois créé.
- CLI : `dku2dbx reconcile <export.zip> [--config] [--out]` ; c'était la dernière commande encore vide.

### Résultats

- `uv run pytest` : **505 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin de phase : chaque couche est testée avec des DataFrames identiques, légèrement différents (dans et hors tolérance) et très différents ; le notebook généré s'exécute sur PySpark local (table identique, tolérée, en écart, absente).
- Vrai export (projet de tutoriel Dataiku Cloud) : 3 tables à comparer, plan et notebook générés.

### Points ouverts

- **Aucun workspace** avec à la fois les sorties Dataiku et Databricks : le notebook n'a tourné qu'en local (H-60 à vérifier).
- Le notebook n'est pas encore une tâche du bundle : il s'importe à part (à décider : job de réconciliation dans `resources/`).
- Les tables très grandes : la comparaison ligne à ligne fait une jointure complète ; un échantillonnage ou une comparaison par hash de partition pourra être ajouté si le pilote le demande.

Phase 9 validée par le responsable le 2026-11-24 (505 tests passés). Suite prévue par la ROADMAP : le pilote (2 ou 3 projets réels de bout en bout, en parallèle avec Dataiku, jusqu'à une réconciliation signée par le métier), puis la phase 10 (interface, optionnelle).

## Phase 10 — Interface : Databricks App (2026-11-24)

### Décisions prises avec le responsable

- **Phase 10 avant le pilote**, par décision du responsable (la ROADMAP la place après la validation du pilote).
- Streamlit ; construite et testée en local d'abord (aucun workspace avec les Apps activées pour l'instant).
- L'étape « Migrer » propose le bundle en **téléchargement** ; le déploiement reste un geste humain après relecture (l'app n'a pas besoin du droit de créer des jobs).

### Réalisé

- `src/dku2dbx/webapp/session.py` : le parcours sans Streamlit, une fonction par étape, qui appelle le code de la CLI (aucune logique propre à l'app). Contrôle de l'export déposé (ZIP, 200 Mo au plus), dossier de travail privé par dépôt, ZIP reproductibles (entrées triées, dates fixes), formulaire de branchement converti en `ConnectionsConfig`, formulaire de réconciliation converti en `ReconcileConfig`. Copie de l'export dans un Volume si `DKU2DBX_EXPORTS_VOLUME` est renseigné ; endpoint LLM par `DKU2DBX_LLM_ENDPOINT` (H-61).
- `app/app.py` : six pages (déposer, analyser, simplifier, brancher, migrer, réconcilier), avec métriques, rapports Markdown, éditeurs de tableau et boutons de téléchargement.
- `app/app.yaml`, `app/requirements.txt` (dku2dbx à une révision fixe du dépôt) et `app/databricks.yml` (bundle de l'app, cible `dev`).
- `connect.resolve.sources` rendue publique : l'app et le branchement partagent la définition d'une source.
- Dépendance optionnelle `app` (streamlit) ; streamlit en dépendance de développement pour les tests.

### Résultats

- `uv run pytest` : **514 tests passés** ; `ruff` et `mypy --strict` propres.
- Critère de fin « le parcours complet fonctionne sur `DEMO_SIMPLE` » : vérifié en local, par le module et par les pages elles-mêmes (`streamlit.testing` AppTest : dépôt, analyse, simplification, branchement, génération, réconciliation, export illisible). L'app démarre avec `streamlit run`.
- Critère « l'app est déployée en `dev` via le bundle du projet » : **non atteint**, faute de workspace avec les Databricks Apps activées (H-61 à vérifier).

### Points ouverts

- Déployer en `dev` dès qu'un workspace est disponible (`databricks bundle deploy` dans `app/`), et vérifier l'identité utilisée (service principal de l'app ou identité de l'utilisateur).
- Créer le Volume à accès restreint pour les exports.

## Phase 11 — Migration des datasets et noms Dataiku (2026-11-25)

### Décisions prises avec le responsable

- Ajout des phases 11 (datasets), 12 (lecteur par API) et 13 (agent MCP, après le pilote) à la ROADMAP, dans cet ordre ; exclusions de la V1 inchangées.
- **Option B** pour les étapes : la fusion est conservée, chaque étape porte le nom de sa recette principale.
- Exigences : classification explicite de Lakehouse Federation (rien n'est supposé compatible), identifiants Dataiku d'origine toujours gardés dans les métadonnées, copies idempotentes, correspondance étape → recettes conservée, validation des schémas, types, partitions et permissions.

### Réalisé

- `simplify` : une étape prend le nom de sa dernière recette ; repli sur `step_NN`, avec la raison dans le rapport, si ce nom n'est pas une clé de tâche valide, est réservé par le bundle, ou entre en collision à la casse près (H-62). L'en-tête de chaque notebook liste toujours toutes les recettes de l'étape.
- Lecteur : `format_type` et `format_params` des datasets de fichiers (H-63, constatés sur l'export d'essai).
- `src/dku2dbx/data/` :
  - `config.py` : `config/data.yaml` (modèle fictif) et `config/data.local.yaml` (ignoré par Git) ; une valeur fictive `[...]` n'est jamais utilisée.
  - `plan.py` : une décision par dataset (`BRANCHÉ`, `RÉFÉRENCE`, `COPIE`, `RECALCUL`, `HISTORIQUE`, `REFUSÉ`) et un statut Federation (`COMPATIBLE`, `À CONFIRMER`, `NON DISPONIBLE`, `INCOMPATIBLE`), chacun avec sa raison (H-65 à H-67).
  - `naming.py` : règles de noms Unity Catalog (H-64), collisions à la casse près, propriétés `dku2dbx.dataiku.*` (projet, dataset, type, connexion, schéma, table ou chemin, recette productrice, hash du JSON). Un nom inutilisable est refusé avec une proposition, jamais renommé en silence.
  - `load.py` : moteur de copie autonome recopié dans chaque notebook ; modes `skip` (défaut : rien n'est réécrit, seules les partitions absentes sont ajoutées, comparaison null-safe) et `replace` (explicite) ; aucune perte de valeur silencieuse à la conversion (`try_cast` compté, CSV en `FAILFAST`) ; *column mapping* Delta pour les noms de colonnes à espace (H-69) ; validation schéma, types, partitions, lignes et métadonnées.
  - `notebook.py` : notebooks de copie, `donnees.md`, `donnees.json`.
  - `permissions.py` : vérification optionnelle des droits par le SDK (`--check-uc`, H-68).
- CLI : `dku2dbx data` ; `dku2dbx generate --data` remplace la tâche d'ingestion qui échoue d'une source non branchée par sa copie quand la décision est `COPIE`.
- Bundle : chaque table écrite (job SQL, vues du pipeline, notebooks Python) reçoit les propriétés `dku2dbx.dataiku.*`.

### Résultats

- `uv run pytest` : **554 tests passés** ; `ruff` et `mypy --strict` propres.
- Critères de fin :
  - `DEMO_SIMPLE`, `DEMO_COMPLEXE` et l'export du tutoriel Dataiku Cloud reçoivent chacun une décision et un statut Federation explicites par dataset. Sur le tutoriel, avec une configuration de test : 1 `COPIE` (fichiers déposés) et 3 `RECALCUL`.
  - Les copies s'exécutent deux fois sur PySpark local sans doublon : table fédérée, partitions, fichiers CSV.
  - La validation détecte une colonne absente ou en trop, un type faux, une partition manquante ou vide, un écart de lignes et des métadonnées absentes.
  - Les étapes portent le nom de leur recette principale.
- Le vrai export a révélé un piège : la colonne `Adj Close`, refusée par Delta sans *column mapping*. Il est traité pour les copies.

### Points ouverts

- **Aucun workspace** : Federation, propriétés Delta, *column mapping* et droits n'ont été vérifiés qu'en local ou avec un faux client (H-64, H-65, H-68, H-69).
- Les noms de colonnes refusés par Delta restent à traiter dans les tables du bundle (phase 6).
- Les copies `HISTORIQUE` (datasets produits) sont générées dans `donnees/notebooks/`, à lancer une fois à la main avant le premier run du job : elles ne sont pas encore des tâches du bundle.
- Les sources fichiers ne peuvent pas encore être référencées sur place (table externe) : elles sont copiées.
- L'application (phase 10) n'expose pas encore l'étape « données ».

Phase 11 validée par le responsable le 2026-11-25 (554 tests passés). Limitations conservées explicitement (reprises dans la ROADMAP) :
- tests réels de Lakehouse Federation et de Delta à faire sur un véritable workspace Databricks ;
- noms de colonnes incompatibles avec Delta : à étendre aux tables générées par le bundle ;
- copies d'historique : à intégrer aux tâches du bundle ;
- lecture des fichiers sur place : à développer.

## Phase 12 — Lecteur par l'API Dataiku (2026-11-25)

### Décisions prises avec le responsable

- Objectif prioritaire : un lecteur API fiable qui récupère les mêmes informations que l'export ZIP et produit la même représentation interne.
- Aucune régression du lecteur ZIP ; les deux sources restent disponibles et utilisent le même moteur.
- La comparaison API / ZIP vérifie l'intégralité des métadonnées et des configurations extraites.

### Réalisé

- `reader/entries.py` : l'interface commune des entrées d'un export. Le constructeur du modèle interne du lecteur ZIP la lit, quelle que soit la source (`build_project`). Le code de lecture du ZIP n'a pas changé.
- `reader/api_reader.py` : client **en lecture seule** (GET uniquement, TLS vérifié, sans proxy, clé dans l'environnement, jamais affichée). Il reconstruit en mémoire les entrées d'un export DSS 15 (manifeste, `params.json`, variables, datasets, recettes et payloads, scénarios, zones, modèles), retire les clés que seule l'API ajoute (H-71), puis passe ces entrées au même constructeur.
- `reader/compare.py` : comparaison champ par champ des deux modèles internes. Les listes d'objets sont alignées par nom ou identifiant. Chaque différence est expliquée (provenance) ou non.
- CLI : `--api <clé>` sur `analyze` (répétable), `simplify`, `translate`, `connect`, `data`, `residue`, `generate` et `reconcile` ; nouvelles commandes `dku2dbx list` et `dku2dbx compare <clé> --zip <export>`.
- **Correctif du lecteur ZIP** trouvé en comparant avec l'API : un export DSS 15 écrit `generatedWithDSSVersion`, `originalProjectKey`, des plugins `{pluginId, version}` et `localvariables.json` (H-02 et H-13 corrigées). Les deux formes sont lues : le modèle interne des fixtures est identique octet pour octet.

### Résultats

- `uv run pytest` : **572 tests passés** ; `ruff` et `mypy --strict` propres.
- Instance d'essai Dataiku Cloud (DSS 15.2.2, projet du tutoriel) : `dku2dbx compare` donne **0 différence non expliquée**. Les 8 différences expliquées sont 6 hash de provenance et les entrées d'export non interprétées (et leur avertissement).
- Toutes les commandes produisent les mêmes fichiers par l'API et par le ZIP, aux hash de provenance près. Seule exception : l'analyse, où l'avertissement sur les entrées d'export non interprétées n'existe que pour le ZIP.
- Non-régression : le modèle interne de `DEMO_SIMPLE` et `DEMO_COMPLEXE` lu par le ZIP est identique à celui d'avant la phase.
- Fausse API construite à partir des fixtures : même modèle interne que le ZIP, y compris scénarios, zones, plugins, objets étrangers, partitions et steps Prepare.

### Points ouverts

- Le projet d'essai n'a ni scénario, ni zone, ni modèle enregistré, ni objet partagé, ni variable : ces lectures ne sont vérifiées qu'avec la fausse API (H-72, H-73). À comparer sur un projet qui en a, ou sur un projet La Poste (`dku2dbx compare`, sans aucun envoi externe).
- Les hash `source_sha256` diffèrent entre les deux sources (mise en page du JSON de l'export) : les en-têtes des fichiers générés indiquent donc la source réellement lue.
- L'application (phase 10) ne lit pas encore par l'API.

### Validation (2026-11-26)

- Phase 12 validée par le responsable, y compris le correctif de lecture des exports DSS 15. Les 4 commits sont poussés sur la branche `phase-12-api-reader`.
- H-72 et H-73 restent **ouvertes**. Condition pour déclarer la couverture API complète : un test réel, sur un projet contenant au minimum une zone, un scénario et une variable, dont les lectures API et ZIP sont comparées avec `dku2dbx compare`.

### Premier test réel de la lecture par l'API (2026-11-26)

- Projet La Poste réel (instance interne DSS 15), comparaison `dku2dbx compare` : 18 différences, dont **4 non expliquées**, toutes sur 2 recettes. **Aucune sur les zones, les scénarios, les variables ou les objets partagés.**
- Trois hypothèses corrigées (relevé anonyme, aucun contenu du projet dans Git) :
  - H-70 : un export écrit le fichier de payload **même vide** (vu pour une recette Sync et une recette de plugin) ; le lecteur API l'écrit dès que l'API renvoie un payload.
  - H-14 : le payload d'une recette de plugin s'appelle `<nom>.<type>`, et non `<nom>.json.payload` (supposition issue de la fixture synthétique, corrigée).
  - H-71 : `containerSelection = {containerMode: INHERIT}`, valeur par défaut que le moteur ne lit pas, est écrit ou omis irrégulièrement par les deux sources ; le constructeur commun le retire pour les deux (option A validée par le responsable), toute autre valeur est conservée.
- Non-régression : le modèle interne lu par le ZIP est identique pour `DEMO_SIMPLE` et l'export du tutoriel ; pour `DEMO_COMPLEXE`, seul change le nom du payload de la recette de plugin (fixture corrigée).
- À faire : relancer la comparaison sur le projet réel après mise à jour ; H-72 et H-73 restent ouvertes tant que la présence d'une zone, d'un scénario et d'une variable dans ce projet n'est pas confirmée.

### Objets partagés : correction de H-72 (2026-11-26)

- Second test réel, sur un projet de test de l'instance interne : un dataset partagé par un autre projet, visible dans le Flow mais lu par aucune recette, figure dans les `foreignObjects` de l'export ; le lecteur API, qui les déduisait des recettes, ne le trouvait pas (1 différence non expliquée).
- Correction validée par le responsable (option A) : le lecteur API lit les `exposedObjects` des réglages des autres projets de l'instance (`targetProject` égal à la clé, `appearOnFlow = true`) et garde en complément les datasets lus par les recettes. Le lecteur ZIP n'est pas modifié.
- Un autre projet illisible (HTTP 403 ou 404) n'arrête pas la lecture : il est compté dans un avertissement `FOREIGN_OBJECTS_INCOMPLETE`, et `dku2dbx compare` déclare alors la comparaison **non concluante** (rapport et message, code de sortie 1), même sans différence non expliquée.
- Coût : une requête par projet de l'instance pour chaque lecture par l'API.
- H-73 : zones constatées sur le projet réel. Scénarios et variables restent à vérifier lors de la comparaison finale (projet de test complété par le responsable).
