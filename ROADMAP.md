# ROADMAP — `dku2dbx`, usine de migration Dataiku → Databricks

Ce plan découpe la construction en 14 phases (0 à 13). Les phases 11 à 13 ont été ajoutées le 2026-11-25. Chacune a un objectif, des tâches, des livrables et des critères de fin (« terminé quand… »). Claude Code doit les réaliser **dans l'ordre, une à la fois**, et s'arrêter à la fin de chaque phase pour validation humaine (voir `CLAUDE.md`).

Vue d'ensemble :

| Phase | Nom | Étape du parcours | Résultat |
|---|---|---|---|
| 0 | Socle du dépôt | — | Projet Python outillé, CI, fixtures |
| 1 | Lecteur d'export | Analyser | Export ZIP → modèle interne |
| 2 | Analyse et inventaire | Analyser | Rapport par projet et portefeuille, score, vagues |
| 3 | Recettes visuelles relationnelles | Migrer | Join, Group, Window… → Spark SQL testé |
| 4 | Recette Prepare et formules | Migrer | Registre de processeurs + traducteur de formules |
| 5 | Simplification du Flow | Simplifier | Chaînes de recettes fusionnées en étapes |
| 6 | Générateurs et bundle | Migrer | Notebooks SQL, pipeline, job, bundle déployable |
| 7 | Branchement des sources | Brancher | Sources Dataiku → tables Unity Catalog |
| 8 | Résidu assisté par LLM | Migrer | Recettes Python et cas non couverts, à relire |
| 9 | Réconciliation | Migrer | Preuve d'équivalence des sorties |
| 10 | Interface (optionnelle) | Tout | Databricks App au-dessus de la CLI |
| 11 | Migration des datasets | Brancher | Décision par dataset, copies idempotentes, noms Dataiku conservés |
| 12 | Lecteur par API Dataiku | Analyser | Projet lu sans export ZIP, identique à la lecture du ZIP |
| 13 | Pilotage par agent (MCP) | Tout | Agent qui appelle les outils déterministes, après le pilote |

**Hors périmètre de la v1 (inventoriés mais non convertis) :** modèles ML (Visual ML, saved models), API node, webapps, dashboards, plugins, recettes R/Shell/Scala, objets GenAI (LLM Mesh, Knowledge Banks, agents), Dataiku Govern. Ces objets sont comptés et classés `MANUAL` dans le rapport de la phase 2.

---

## Phase 0 — Socle du dépôt

**Objectif :** un projet Python propre, testable et outillé, avant toute logique métier.

Tâches :
1. Créer l'arborescence décrite dans `CLAUDE.md`, le `pyproject.toml` (dépendances listées dans `CLAUDE.md`), le `.gitignore` (`exports/`, `out/`, `.env`, `*.zip` hors `tests/fixtures/`) et la configuration `ruff`, `mypy` et `pre-commit`.
2. Créer une CLI `dku2dbx` (typer) avec des sous-commandes vides : `analyze`, `simplify`, `translate`, `generate`, `connect`, `reconcile`.
3. Écrire un **générateur de fixtures** (`tests/fixtures/make_fixture.py`) qui fabrique un faux export ZIP Dataiku conforme au format décrit dans `CLAUDE.md`. Il doit contenir 2 projets synthétiques :
   - `DEMO_SIMPLE` : 5 recettes (Prepare, Join, Group, Window, Stack), sans partition ;
   - `DEMO_COMPLEXE` : environ 20 recettes, dont un dataset partitionné par jour, une recette Python, une recette SQL, un scénario step-based et une recette de plugin.
4. Mettre en place la CI (GitLab CI ou GitHub Actions selon le dépôt) : `ruff`, `mypy` et `pytest`.
5. Créer `docs/journal.md`, `docs/hypotheses.md` et `docs/pieges_semantiques.md` (avec le contenu de la section « Pièges sémantiques » en fin de document).

Terminé quand : `uv run pytest` passe, `uv run dku2dbx --help` liste les commandes, la CI est verte et les deux fixtures ZIP sont générées.

---

## Phase 1 — Lecteur d'export → modèle interne

**Objectif :** transformer un export ZIP Dataiku en un modèle interne (IR) typé, indépendant du format Dataiku.

Tâches :
1. Définir le modèle pydantic dans `model/` : `Project`, `Zone`, `Dataset` (connexion, table résolue, partitionnement, schéma, checks), `Recipe` (type, moteur, entrées, sorties, mode append, payload brut et parsé), `PrepareStep`, `Scenario` (steps, triggers), `PluginUsage`, `ExposedObject`.
2. Écrire `reader/zip_reader.py` :
   - lire `export-manifest.json`, `params.json`, `datasets/*.json`, `recipes/*.json` et les payloads (`.shaker`, `.grouping`, `.join`…) ;
   - résoudre les variables (`${projectKey}`, `${NODE_DC}`, variables de projet) dans les noms de tables, et lister celles qui restent non résolues ;
   - construire le graphe du Flow (dataset → recette → dataset) avec `networkx` ;
   - conserver tout type inconnu tel quel, avec `status = UNKNOWN`, sans planter.
3. Prévoir un second lecteur, `reader/api_reader.py`, sur la même interface. Il utilisera l'API `dataikuapi` (`list_projects`, `get_flow().get_graph()`, `get_settings()`, `get_payload()`, `get_json_payload()`, `get_status().get_selected_engine_details()`). En phase 1, seule l'interface est définie ; l'implémentation viendra quand un accès API sera disponible.
4. Ajouter la commande `dku2dbx analyze <export.zip> --dump-ir`, qui écrit l'IR en JSON.

Terminé quand : les deux fixtures sont lues sans erreur, l'IR sérialisée est stable (snapshot), un test vérifie le graphe de `DEMO_SIMPLE` (nœuds, arêtes, ordre topologique) et un test vérifie qu'un type inconnu est conservé et signalé.

Si un vrai export La Poste est disponible dans `exports/`, le lire aussi. Chaque écart avec le format supposé est noté dans `docs/hypotheses.md` et une fixture est ajoutée pour le reproduire.

---

## Phase 2 — Analyse, inventaire, score et vagues

**Objectif :** produire, pour chaque projet et pour tout le portefeuille, la vue « Analyser » : ce qui se migre automatiquement, ce qui passe par le LLM, ce qui est manuel, et l'effort.

Tâches :
1. Compter les recettes par type, les datasets, les zones, les partitions, les scénarios, les plugins et les recettes de code.
2. **Catalogue des processeurs Prepare.** Scanner tous les `.shaker` de tous les exports et compter chaque `type` de processeur. Ce catalogue fixe l'ordre de développement de la phase 4 : on traite d'abord les processeurs les plus fréquents.
3. Classer chaque recette :
   - `AUTO` : traducteur déterministe disponible ;
   - `LLM_REVIEW` : le résidu passera par le LLM avec relecture ;
   - `MANUAL` : objets hors périmètre, plugins, R, ML.

   La classification se met à jour automatiquement à mesure que les traducteurs arrivent.
4. Calculer un score de complexité configurable (`config/scoring.yaml`), inspiré de la grille Lakebridge : 4 niveaux, de LOW à VERY HIGH, et un niveau est atteint dès qu'une seule de ses conditions est remplie. Critères : nombre de recettes, part de code, Prepare riches en formules, partitions, scénarios Python, plugins, webapps, modèles.
5. Estimer l'effort en jours, avec des coefficients par classe dans `scoring.yaml`, marqués « à calibrer sur le pilote ».
6. Construire le graphe entre projets (`exposedObjects` côté producteur, entrées préfixées `PROJET.dataset` côté consommateur) et proposer des **vagues** : composantes fortement connexes regroupées, producteurs avant consommateurs.
7. Produire les sorties :
   - `out/<projet>/analyse.md` et `analyse.json` ;
   - `out/portefeuille.csv` (une ligne par projet) et `out/vagues.md`.

Terminé quand : `dku2dbx analyze exports/ --out out/` produit tous ces fichiers pour les fixtures, les chiffres de `DEMO_SIMPLE` sont vérifiés par test et le catalogue des processeurs est généré.

---

## Phase 3 — Traducteurs des recettes visuelles relationnelles

**Objectif :** convertir de façon déterministe les recettes visuelles « SQL-friendly » en Spark SQL (dialecte Databricks).

Ordre : Sync/Filter → Distinct → Stack (`.vstack`) → Join → Group → Window → Split → Sort → Top N → Pivot.

Pour chaque recette :
1. Lire le payload parsé de l'IR et générer une requête `SELECT` (ou une CTE) formatée avec `sqlglot` (`dialect="databricks"`).
2. Gérer les `preFilter` et `postFilter`, les colonnes sélectionnées et les renommages.
3. Points d'attention :
   - **Join** : types de jointure, conditions multiples, options `caseInsensitive` et `normalizeText`, égalité des NULL ;
   - **Top N** : ajouter une colonne de départage pour rendre le résultat déterministe, et le signaler dans le rapport ;
   - **Pivot** : valeurs explicites ou dynamiques. Les valeurs dynamiques sont signalées, car elles exigent soit une liste figée, soit du code ;
   - **Split** : une requête par sortie, plus la sortie par défaut.
4. Tests pour chaque traducteur :
   - snapshot du SQL ;
   - exécution sur PySpark local avec un DataFrame synthétique, et comparaison avec un résultat attendu calculé à la main ;
   - cas d'erreur : une option non gérée lève `UnsupportedConstruct`.

Terminé quand : les 10 types sont couverts avec leurs tests, toutes les recettes relationnelles de `DEMO_SIMPLE` et `DEMO_COMPLEXE` sont traduites et la commande `dku2dbx translate` écrit un fichier `.sql` par recette dans `out/`.

---

## Phase 4 — Recette Prepare et langage de formules

**Objectif :** traduire les recettes Prepare, qui représentent souvent la moitié des recettes d'un projet (42 sur 86 dans un projet La Poste observé).

Tâches :
1. **Registre des processeurs** (`translate/prepare/registry.py`) : un traducteur par `type` de processeur, qui produit une expression Spark SQL ou une étape CTE. Traiter les processeurs dans l'ordre du catalogue de la phase 2. Les plus probables :
   - renommer, supprimer ou garder des colonnes ;
   - filtrer des lignes ;
   - `CreateColumnWithGREL` (formule) ;
   - `FillEmptyWithValue` ;
   - parser une date, découper une colonne, rechercher et remplacer, extraire par regex ;
   - changer la casse, normaliser ;
   - arrondir, convertir un type, concaténer des colonnes ;
   - fold/unfold.
2. **Traducteur de formules** (`translate/prepare/formula.py`) : une grammaire `lark` du langage de formules Dataiku (opérateurs, `if()`, fonctions de chaîne, de date et mathématiques, références de colonnes, variables `${...}`), puis la génération en Spark SQL. Une fonction inconnue lève `UnsupportedConstruct` ; ne jamais deviner.
3. Les steps `disabled: true` sont ignorées et listées dans le rapport. Un `metaType` autre que `PROCESSOR` (groupe de steps) lève une erreur tant qu'il n'est pas géré explicitement.
4. Les processeurs qui exigent un référentiel (GeoIP, User-Agent, jours fériés, enrichissement département/code postal) ou un calcul hors SQL sont classés `MANUAL`. Ils sont listés avec une proposition de cible : une UDF Unity Catalog ou un custom operator Lakeflow Designer.
5. Tests : un test par processeur et par fonction de formule. Les **pièges sémantiques** de la section finale ont chacun leur test dédié.

Terminé quand : les processeurs qui représentent au moins 80 % des occurrences du catalogue sont traduits et testés, chaque recette Prepare des fixtures est soit traduite, soit explicitement listée avec ses étapes non couvertes, et `docs/pieges_semantiques.md` est à jour.

---

## Phase 5 — Simplification du Flow

**Objectif :** reproduire l'étape « Simplifier ». On fusionne les chaînes de recettes pour ne matérialiser que les tables utiles (exemple observé : 86 recettes deviennent 25 étapes et 61 tables intermédiaires disparaissent).

Règles de fusion. **Une table reste matérialisée** si l'une de ces conditions est vraie :
- c'est une sortie finale ou un objet exposé à un autre projet ;
- elle est lue par plusieurs recettes ;
- elle est partitionnée ou écrite en mode append ;
- elle porte des checks ou des règles qualité, ou elle est lue par un scénario ;
- elle est l'entrée ou la sortie d'une recette `LLM_REVIEW` ou `MANUAL`.

Les autres tables deviennent des CTE ou des vues temporaires dans l'étape qui les consomme.

Tâches :
1. Implémenter l'algorithme sur le graphe de l'IR. Il produit des « étapes », chacune composée d'une séquence de recettes.
2. Ajouter une option `--no-fusion` (1 recette = 1 table), indispensable pour la réconciliation fine et le débogage.
3. Ajouter au rapport : nombre de recettes avant, nombre d'étapes après, tables supprimées et justification de chaque table conservée.

Terminé quand : les tests vérifient chaque règle de conservation sur des graphes construits à la main, la sortie de `DEMO_COMPLEXE` est vérifiée par snapshot et `--no-fusion` redonne une étape par recette.

---

## Phase 6 — Générateurs de code et bundle

**Objectif :** produire un projet Databricks déployable par projet Dataiku.

Deux cibles, à choisir par projet :
- **A. Job de notebooks SQL** : une tâche de préparation du schéma, puis un notebook SQL par étape, orchestrés dans un Lakeflow Job.
- **B. Lakeflow pipeline** : une table ou materialized view par étape, en SQL ou en Python (`from pyspark import pipelines as dp`). Les tables partitionnées ou en append passent par une tâche SQL séparée.

(Une cible C, Lakeflow Designer / « Visual Data Prep », est à étudier plus tard : le format de ses fichiers n'est pas documenté publiquement.)

Tâches :
1. Templates `jinja2` pour les notebooks SQL, les fichiers de pipeline, la définition du job et `databricks.yml` :
   - targets `dev` et `prod` ;
   - variables `catalog` et `schema` ;
   - permissions.
2. **Partitions** : une partition Dataiku devient une colonne de date et une écriture `INSERT INTO … REPLACE WHERE <colonne> = :date`. Le job reçoit un paramètre de date. Avec les références dynamiques (`{{job.start_time.iso_date}}`), **attention à l'UTC** : documenter le décalage avec l'heure de Paris. Les dépendances « Time range » deviennent des prédicats de plage ; « Latest available » est signalé pour revue.
3. **Scénarios step-based** : les steps « build » deviennent des tâches du job, les déclenchements temporels un `schedule`, et les déclenchements « dataset modifié » un trigger *table update*. Les scénarios Python et les triggers SQL ou Python personnalisés sont classés `MANUAL`.
4. **Variables de projet** → variables de bundle et paramètres de job. **Checks et règles qualité simples** → expectations de pipeline ou contrôles SQL ; les autres sont listés.
5. En-tête de traçabilité dans chaque fichier généré (voir `CLAUDE.md`).
6. Commande `dku2dbx generate <projet> --target job|pipeline --out out/<projet>/bundle/`.

Terminé quand : le bundle généré pour `DEMO_SIMPLE` passe `databricks bundle validate` (lancé en CI si un profil Databricks est disponible, sinon par une validation de schéma locale), le SQL généré parse avec `sqlglot` et un test de bout en bout exécute les étapes de `DEMO_SIMPLE` sur PySpark local.

---

## Phase 7 — Branchement des sources

**Objectif :** reproduire l'étape « Brancher » : chaque dataset source Dataiku doit pointer vers une table Unity Catalog.

Tâches :
1. `config/connections.yaml` associe chaque connexion Dataiku à un `catalog.schema` UC, avec des règles de renommage.
2. Résoudre automatiquement les noms de tables. Les sources non résolues apparaissent dans un rapport « à brancher » et sont marquées en pointillé dans le graphe.
3. Optionnel, si un accès Databricks est configuré : vérifier l'existence des tables via le SDK (`databricks-sdk`, `WorkspaceClient().tables.get`) et comparer leurs colonnes au schéma déclaré par Dataiku.
4. Les sources non branchées génèrent une tâche d'ingestion de substitution, signalée dans le rapport.

Terminé quand : les tests couvrent la résolution, les variables non résolues et une connexion inconnue ; le rapport « à brancher » est produit pour les fixtures.

---

## Phase 8 — Résidu assisté par LLM

**Objectif :** traiter ce que les traducteurs déterministes ne couvrent pas, avec relecture humaine obligatoire.

Prérequis bloquant : un endpoint Model Serving Databricks **validé par la sécurité** est renseigné dans `config/llm.yaml`. Sans lui, la phase se limite à préparer les fichiers d'entrée.

Tâches :
1. **Recettes Python** : d'abord une réécriture déterministe des idiomes, puis le LLM pour le reste.

   | Idiome Dataiku | Cible Databricks |
   |---|---|
   | `dataiku.Dataset("x").get_dataframe()` | `spark.table("catalog.schema.x")` (ou `.toPandas()` si nécessaire) |
   | `dataiku.Dataset("y").write_with_schema(df)` | `df.write.mode("overwrite").saveAsTable("catalog.schema.y")` |
   | `dataiku.Folder("f")` | chemin `/Volumes/catalog/schema/volume/` |
   | `dataiku.get_custom_variables()` | paramètres de job ou widgets |
   | `dataiku.api_client()` | `MANUAL` (cas par cas) |

2. Prompts versionnés dans `src/dku2dbx/translate/llm/prompts/`. Chaque entrée envoyée au LLM est un JSON enrichi : définition de la recette, payload, schémas d'entrée et tables UC résolues.
3. Chaque sortie LLM porte l'en-tête `REVIEW_REQUIRED`. Elle est vérifiée statiquement (`ruff`, parse `sqlglot`) et n'est jamais classée `AUTO`.
4. Option alternative : exporter ces entrées au format attendu par **Lakebridge Switch** (prompt YAML personnalisé) au lieu d'appeler le LLM directement.

Terminé quand : la recette Python de `DEMO_COMPLEXE` est réécrite par les règles déterministes et testée, le flux LLM fonctionne avec un endpoint de test (ou un faux client dans les tests) et le rapport liste chaque fichier à relire.

---

## Phase 9 — Réconciliation

**Objectif :** prouver que les sorties Databricks sont identiques à celles de Dataiku, ou expliquer chaque écart.

Hypothèse de travail : les sorties Dataiku sont accessibles dans Unity Catalog (re-plateformage de Dataiku sur Databricks, ou copie des tables de sortie).

Tâches :
1. Générer, par projet, un notebook ou job de réconciliation qui compare chaque table de sortie en couches :
   1. schéma (colonnes, types, nullabilité) ;
   2. nombre de lignes, au total et par partition ;
   3. agrégats métier par colonne (somme, min, max, distincts), avec tolérances configurables ;
   4. comparaison ligne à ligne sur clé métier (`EXCEPT ALL` dans les deux sens, ou hash par ligne), et `datacompy` en option ;
2. Produire un rapport de réconciliation (Markdown et table Delta) avec statut par table : `IDENTIQUE`, `ÉCART TOLÉRÉ` ou `ÉCART`.
3. Fournir un gabarit de **registre des écarts acceptés** (décision métier : corriger, ou reproduire l'ancien comportement).

Terminé quand : les tests couvrent chaque couche avec des DataFrames construits pour être identiques, légèrement différents (dans et hors tolérance) ou complètement différents, et le notebook généré s'exécute sur PySpark local.

---

## Phase 10 — Interface (optionnelle, après validation du pilote)

**Objectif :** offrir aux équipes une interface simple au-dessus de la CLI, sous forme de **Databricks App** (Streamlit ou Dash).

Parcours : déposer un export ZIP → Analyser → Simplifier → Brancher → Migrer (générer, télécharger ou déployer le bundle) → Réconcilier. L'app s'authentifie avec son service principal ou avec l'identité de l'utilisateur. Les exports sont stockés dans un Volume Unity Catalog à accès restreint.

Terminé quand : l'app est déployée en `dev` via le bundle du projet et le parcours complet fonctionne sur `DEMO_SIMPLE`.

---

## Phase 11 — Migration des datasets et noms Dataiku

**Objectif :** chaque dataset Dataiku a sa table Unity Catalog, sous le nom Dataiku, avec ses données quand il faut les déplacer. Ajoutée le 2026-11-25.

Principe : ne jamais copier par défaut. Pour chaque dataset, l'outil décide de façon déterministe s'il faut le **référencer**, le **copier une fois**, le **recalculer** ou le **refuser**, et dit pourquoi.

Tâches :
1. **Décision par dataset** (`dku2dbx data <export>`), à partir de `config/data.yaml` (exemples fictifs) ou `config/data.local.yaml` (ignoré par Git) :

   | Cas | Décision |
   |---|---|
   | Source déjà branchée (phase 7) | `BRANCHÉ` : rien à copier, la table est vérifiée |
   | Source dans une base compatible et déclarée pour Lakehouse Federation | `RÉFÉRENCE` (lecture directe, branchement proposé), ou `COPIE` si la connexion le demande |
   | Source fichier (Filesystem, HDFS, S3, Azure, GCS) dont l'emplacement est déclaré | `COPIE` depuis l'emplacement (Volume ou external location) |
   | Fichier déposé dans Dataiku (`UploadedFiles`) | `COPIE` depuis le Volume où l'équipe dépose les fichiers |
   | Dataset produit par une recette | `RECALCUL` par le job, sauf historique non recalculable |
   | Partitions déjà construites et jamais recalculées, sortie en append | `HISTORIQUE` : copie unique des données existantes |
   | Tout autre cas (type, format ou option inconnus, emplacement non déclaré) | `REFUSÉ`, avec la raison |

2. **Lakehouse Federation, classification explicite.** Aucune connexion n'est supposée compatible. Chaque source base de données reçoit un statut : `COMPATIBLE` (type pris en charge par Federation **et** connexion déclarée disponible avec son catalogue fédéré), `À CONFIRMER` (type pris en charge, connexion non déclarée), `NON DISPONIBLE` (déclarée indisponible), `INCOMPATIBLE` (type non pris en charge). Seul `COMPATIBLE` permet de référencer ou de copier par Federation.
3. **Noms Dataiku conservés.** Les tables portent le nom du dataset Dataiku. Les noms et identifiants originaux (projet, dataset, recette productrice, connexion, table ou chemin source, hash du JSON) sont **toujours** enregistrés dans les métadonnées de la table (`TBLPROPERTIES`) et dans `donnees.json`, y compris quand une contrainte Unity Catalog empêche d'utiliser le nom tel quel. Dans ce cas, ou si deux noms ne diffèrent que par la casse, le dataset est `REFUSÉ` avec un nom proposé : jamais de renommage silencieux.
4. **Option B pour les étapes** (décision du 2026-11-25) : la fusion des recettes est conservée et chaque étape porte le nom de sa recette principale (celle qui écrit la sortie de l'étape). L'en-tête de chaque fichier et le rapport de simplification gardent la correspondance avec **toutes** les recettes Dataiku de l'étape.
5. **Copies idempotentes.** Un notebook de copie par dataset, relançable sans duplication ni écrasement accidentel : par défaut, une table déjà chargée n'est pas réécrite, et seules les partitions absentes sont ajoutées ; le remplacement complet n'a lieu qu'avec le mode explicite `replace`. Jamais d'ajout aveugle.
6. **Validation des datasets** après chaque copie : schéma (colonnes manquantes ou en trop), types (schéma Dataiku déclaré), partitions (colonne présente, partitions de la source toutes chargées), nombre de lignes, et permissions (lecture de la source, écriture dans le schéma cible ; vérification préalable optionnelle par le SDK Databricks).
7. Les sources non branchées dont la décision est `COPIE` reçoivent leur notebook de copie à la place de la tâche d'ingestion qui échoue (phase 7).

Livrables : `out/<P>/donnees/` (rapport `donnees.md`, `donnees.json`, notebooks de copie), commande `dku2dbx data`, hypothèses marquées dans `docs/hypotheses.md`.

Terminé quand : sur `DEMO_SIMPLE`, `DEMO_COMPLEXE` et l'export du tutoriel Dataiku Cloud, chaque dataset reçoit une décision et un statut Federation explicites ; les notebooks de copie s'exécutent deux fois sur PySpark local sans doublon ; la validation détecte un schéma, un type et une partition faux ; les étapes portent le nom de leur recette principale ; tests verts et `ruff` propre.

**Phase 11 validée le 2026-11-25.** Limitations conservées explicitement, à traiter plus tard :
- les tests réels de Lakehouse Federation et de Delta (propriétés, *column mapping*, droits) restent à faire sur un véritable workspace Databricks ;
- la gestion des noms de colonnes incompatibles avec Delta doit être étendue aux tables générées par le bundle (phase 6) ;
- les copies d'historique (`HISTORIQUE`) doivent être intégrées aux tâches du bundle ;
- la lecture des fichiers sur place (table externe, sans copie) reste à développer.

---

## Phase 12 — Lecteur par API Dataiku

**Objectif :** lire un projet directement sur l'instance Dataiku, sans export ZIP fait à la main.

Tâches :
1. Implémenter `reader/api_reader.py` (interface prévue en phase 1), en lecture seule ; la clé est lue dans une variable d'environnement, jamais affichée ni écrite.
2. Commandes `dku2dbx list` (projets de l'instance) et `--api PROJET` sur les commandes qui lisent un export.
3. **Comparaison complète API / ZIP** : sur un même projet, l'intégralité des métadonnées et des configurations extraites (datasets, recettes, payloads, steps Prepare, scénarios, variables, zones, partitions, schémas, objets exposés et étrangers) doit être identique ; chaque différence est listée champ par champ.

Terminé quand : sur l'instance d'essai Dataiku Cloud, la comparaison API / ZIP ne montre aucune différence non expliquée, et les tests (faux client) sont verts.

---

## Phase 13 — Pilotage par agent IA (MCP), après validation du pilote

**Objectif :** piloter la migration en langage naturel, sans remplacer le moteur déterministe.

Tâches :
1. Un serveur MCP expose les commandes existantes comme outils (`analyze`, `simplify`, `connect`, `data`, `generate`, `residue`, `reconcile`).
2. L'agent **appelle** ces outils : il ne traduit rien lui-même. Le moteur déterministe reste la source de vérité ; les règles 1, 2 et 5 de `CLAUDE.md` s'appliquent (LLM validé par la sécurité uniquement).

Terminé quand : un parcours complet sur `DEMO_SIMPLE` est piloté par l'agent et produit exactement les mêmes fichiers que la CLI.

---

## Pilote et passage à l'échelle (après la phase 9)

1. Choisir 2 ou 3 projets réels représentatifs avec l'équipe : un petit projet visuel, un gros projet Prepare, un projet avec partitions et scénario.
2. Les dérouler de bout en bout, en run parallèle avec Dataiku, jusqu'à réconciliation signée par le métier.
3. Calibrer `scoring.yaml` (jours réels par classe de recette) et compléter le registre des processeurs.
4. Seulement ensuite, lancer les vagues de la phase 2.

---

## Pièges sémantiques à couvrir par des tests

Chacun de ces points doit avoir au moins un test et une règle dans le traducteur concerné.

| Piège | Dataiku | Databricks / Spark | Règle |
|---|---|---|---|
| Chaîne vide et NULL | `isBlank`, `FillEmptyWithValue` et `coalesce` traitent `''` comme vide | `coalesce` ne remplace que NULL | Générer `coalesce(nullif(trim(col), ''), valeur)` ou l'équivalent |
| Arrondi des négatifs | Documenté : `round(-3.5) = -3` | `round` en HALF_UP, probablement `-4` | Tester, puis générer une expression qui reproduit Dataiku |
| Concaténation avec `+` | Concatène des chaînes (moteur DSS) | Addition numérique | Traduire en `concat()` selon le type |
| Dates et fuseau | Dates locales selon la configuration | Références de job en UTC | Paramètre de date explicite, documenter le décalage avec Paris |
| Patterns de date | — | `MM` = mois, `mm` = minutes | Table de conversion des formats testée |
| Fuzzy join | Damerau-Levenshtein (transposition = 1) | `levenshtein` simple (transposition = 2) | Classer `LLM_REVIEW` ou `MANUAL`, recalibrer les seuils |
| Top N à égalité | Ordre non déterministe | Idem | Ajouter une colonne de départage |
| Distances géographiques | — | `st_distance` est planaire (degrés) | Utiliser `st_distancesphere` pour des mètres |
| Typage | « Meanings » et inférence Dataiku | Types Spark stricts | Utiliser le schéma déclaré dans l'export, avec un `cast` explicite |
| Steps Prepare groupées | `metaType` différent de `PROCESSOR` | — | Lever une erreur, ne jamais filtrer en silence |
| Variables dans les noms de tables | `${projectKey}`, `${NODE_DC}` | — | Résoudre, sinon signaler |

---

## Prérequis à obtenir avant ou pendant le projet

- **Exports Dataiku** : qui peut exporter les projets (export ZIP « configuration seulement ») ? Quels projets pour le pilote ? Où les stocker de façon sécurisée ?
- **Databricks** : un workspace de dev avec un catalogue bac à sable Unity Catalog, un SQL warehouse et un profil CLI pour `databricks bundle validate` et `deploy`.
- **LLM** : un endpoint Model Serving validé par la sécurité. Les exports contiennent la logique métier et les noms de tables.
- **Dépôt Git** de l'organisation et accès à la CI.
- **Databricks** : demander si le prototype « Dataiku Sunset » présenté le 10/11/2026 peut être partagé ou réutilisé, pour éviter de refaire ce qui existe.
