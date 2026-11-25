# dku2dbx

Usine de migration de projets Dataiku DSS vers Databricks (Unity Catalog, Lakeflow Jobs, Lakeflow pipelines).
Le contexte et les règles sont dans [`CLAUDE.md`](CLAUDE.md), le plan dans [`ROADMAP.md`](ROADMAP.md) et l'avancement dans [`docs/journal.md`](docs/journal.md).

## Démarrage

Prérequis : [uv](https://docs.astral.sh/uv/). Java 17+ est aussi nécessaire pour les tests PySpark locaux.

```bash
uv sync                                         # crée .venv et installe les dépendances (uv.lock)
uv run pre-commit install                       # installe les hooks Git
uv run dku2dbx --help                           # CLI
uv run dku2dbx analyze exports/ --out out/      # analyse : inventaire, classement, score, vagues
uv run dku2dbx analyze export.zip --dump-ir     # un seul projet, avec en plus out/<projet>/ir.json
uv run dku2dbx simplify export.zip              # étapes après fusion -> out/<projet>/simplify.md
uv run dku2dbx translate export.zip             # recettes -> out/<projet>/sql/*.sql et python/*.py
uv run dku2dbx residue export.zip               # résidu LLM -> out/<projet>/residue/RESIDU.md
uv run dku2dbx connect export.zip               # sources -> Unity Catalog, out/<projet>/a_brancher.md
uv run dku2dbx data export.zip                  # datasets : référencer, copier, recalculer -> out/<projet>/donnees/
uv run dku2dbx generate export.zip              # bundle Databricks -> out/<projet>/bundle/
uv run python tests/fixtures/make_fixture.py    # régénère les exports synthétiques
uv run pytest                                   # tests
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Les exports réels vont dans `exports/` et les artefacts générés dans `out/`. Ces deux dossiers sont ignorés par Git.

## Analyser (phase 2)

`dku2dbx analyze` lit un export ZIP, ou tous les ZIP d'un dossier, et écrit dans `out/` :

| Fichier | Contenu |
|---|---|
| `<projet>/analyse.md`, `analyse.json` | inventaire, classe de chaque recette (`AUTO`, `LLM_REVIEW`, `MANUAL`), niveau de complexité et conditions atteintes, effort, dépendances |
| `portefeuille.csv` | une ligne par projet |
| `vagues.md` | ordre de migration : producteurs avant consommateurs, cycles regroupés |
| `catalogue_processeurs.md`, `.csv` | processeurs Prepare du plus fréquent au moins fréquent (ordre de la phase 4) |

La classe d'une recette est sa **classe cible** prévue par la ROADMAP ; la colonne « traducteur disponible » dit si le traducteur existe déjà. Seuils et jours d'effort sont dans `config/scoring.yaml`, **à calibrer sur le pilote** (`--scoring` pour une autre grille). Un projet lu via l'API par le pont `dku2dbx_bridge` s'analyse de la même façon : `dku2dbx analyze out/<projet>/virtual_export.zip`.

## Simplifier (phase 5)

`dku2dbx simplify <export.zip> [--no-fusion]` fusionne les chaînes de recettes en étapes : une table intermédiaire lue par une seule recette devient une CTE de l'étape qui la consomme. Une table reste matérialisée si c'est une sortie finale ou exposée, si plusieurs recettes la lisent, si elle est partitionnée, en append, porte des checks ou sert à un scénario, ou si elle touche une recette `LLM_REVIEW`, `MANUAL` ou non traduisible. `simplify.md` donne les étapes, les tables supprimées et la raison de chaque table conservée ; `--no-fusion` garde une étape par recette.

## Brancher les sources (phase 7)

Les vraies correspondances « connexion Dataiku → catalogue.schéma Unity Catalog » vont dans `config/connections.local.yaml`, **ignoré par Git** (modèle commenté et fictif : `config/connections.yaml`). On y met aussi les `shared_projects` (datasets partagés par d'autres projets) et les `instance_variables` (`${NODE_DC}`…).

`dku2dbx connect <export.zip> [--connections fichier]` écrit `a_brancher.md` (sources branchées, sources à brancher avec leur raison, graphe où les sources à brancher sont en pointillés). `dku2dbx generate` lit le même mapping : les sources branchées sont lues en `catalog.schema.table`, chaque source non branchée reçoit une tâche d'ingestion qui échoue tant qu'elle n'est pas branchée. `--check-uc --profile X` vérifiera les tables dans Unity Catalog (SDK Databricks, optionnel).

## Migrer les datasets (phase 11)

L'export ZIP ne contient aucune donnée. `dku2dbx data <export.zip> [--data fichier] [--connections fichier]` décide, pour chaque dataset, comment ses données arrivent dans Unity Catalog, et écrit `out/<projet>/donnees/` : `donnees.md` (décision et raison par dataset), `donnees.json` (identifiants Dataiku d'origine) et un notebook de copie par dataset à copier.

| Décision | Quand |
|---|---|
| `BRANCHÉ` | source déjà branchée (phase 7) |
| `RÉFÉRENCE` | base lue sur place par Lakehouse Federation (connexion déclarée, `access: reference`) |
| `COPIE` | base fédérée avec `access: copy`, fichiers d'une connexion dont la racine est déclarée, fichiers déposés dans Dataiku |
| `RECALCUL` | dataset produit par une recette : le job le reconstruit |
| `HISTORIQUE` | dataset produit dont l'historique ne se recalcule pas (append, partitions jamais recalculées) : copié une fois |
| `REFUSÉ` | tout le reste, avec la raison (rien n'est supposé) |

Les vraies valeurs vont dans `config/data.local.yaml`, **ignoré par Git** (modèle fictif : `config/data.yaml`). Une connexion n'est jamais supposée compatible avec Lakehouse Federation : il faut la déclarer `federation: available` avec son `foreign_catalog`.

Les copies sont **idempotentes** : par défaut (`mode=skip`), une table déjà chargée n'est pas réécrite et seules les partitions absentes sont ajoutées ; `mode=replace` remplace la table, sur demande explicite. Après la copie, le notebook valide schéma, types, partitions, nombre de lignes et métadonnées. Chaque table garde ses identifiants Dataiku dans ses propriétés `dku2dbx.dataiku.*`. `--check-uc --profile X --principal <identité> --target catalog.schema` vérifie les droits par le SDK Databricks.

`dku2dbx generate` lit la même configuration : une source non branchée dont la décision est `COPIE` reçoit son notebook de copie comme tâche d'ingestion, au lieu d'une tâche qui échoue. Les étapes du bundle portent le nom de leur recette principale (option B, décision du 2026-11-25).

## Générer le bundle (phase 6)

`dku2dbx generate <export.zip> [--target job|pipeline] [--out] [--no-fusion]` écrit un bundle Databricks déployable (`databricks bundle deploy`) :

- **cible A, `job`** (défaut) : un notebook SQL par étape, orchestrés par un Lakeflow Job ;
- **cible B, `pipeline`** : un pipeline Lakeflow de vues matérialisées, puis des tâches SQL pour les étapes partitionnées, en append ou à migrer à la main.

Le bundle contient `databricks.yml` (targets `dev` / `prod`, variables, permissions), `resources/` (jobs, pipeline), `src/` (SQL) et **`GENERATION.md`**, à lire en premier : étapes à migrer à la main, scénarios, partitions, variables, checks non traduits. Les scénarios deviennent des jobs planifiés, les variables Dataiku des paramètres, les partitions journalières des écritures `REPLACE WHERE`. Avant de déployer, renseigner `catalog`, `schema` et `admin_group` (valeurs fictives entre crochets).

## Traduire (phase 3)

`dku2dbx translate <export.zip>` traduit de façon déterministe les recettes visuelles (Sync, Sample/Filter, Distinct, Stack, Join, Group, Window, Split, Sort, Top N, Pivot) et les recettes Prepare (une CTE par étape, langage de formules compris) en Spark SQL :

- un fichier `out/<projet>/sql/<recette>.sql` par sortie (`<recette>__<sortie>.sql` quand il y en a plusieurs), qui commence par un en-tête de traçabilité (projet, recette, version, SHA-256 de la définition et du payload) et les **avertissements** à relire ;
- `out/<projet>/translate_report.json` : recettes traduites, recettes **non prises en charge** avec l'élément Dataiku en cause, recettes sans traducteur.

Une option non traduite n'est jamais devinée ni ignorée : la recette est listée comme non prise en charge. Les tables sont nommées comme les datasets Dataiku ; les phases 6 et 7 les remplaceront par `catalog.schema.table`.

## Recettes de code et résidu LLM (phase 8)

- **Recettes SQL** (`sql_query`) : transpilées par sqlglot depuis le dialecte de la base des entrées, tables physiques remplacées par les datasets, variables en paramètres de job. Elles sont traitées comme les autres étapes SQL du bundle, mais leur en-tête porte `TO REVIEW`.
- **Recettes Python** : seuls les appels `dataiku` sont réécrits (lecture `spark.table(...).toPandas()`, écriture `saveAsTable`, variables, dossiers en Volumes) ; le code métier reste en pandas. On obtient `out/<projet>/python/<recette>.py`, et un notebook Python dans le bundle, lui aussi marqué `TO REVIEW`.
- **Résidu** : `dku2dbx residue <export.zip> [--llm-config] [--connections]` prend les recettes `LLM_REVIEW` qu'aucune règle n'a traitées. Pour chacune, il écrit le JSON d'entrée dans `residue/inputs/` (configuration seulement, aucune donnée). Si `config/llm.local.yaml` (ignoré par Git), sinon `config/llm.yaml`, désigne un endpoint **validé par la sécurité** (Model Serving Databricks, ou endpoint interne compatible OpenAI comme le vLLM La Poste : voir les exemples dans `config/llm.yaml` ; le vLLM peut aussi être déclaré dans le `.env` par `VLLM_BASE_URL`, `VLLM_MODEL` et `SSL_CA_BUNDLE`), il l'appelle avec le prompt versionné (`src/dku2dbx/translate/llm/prompts/`) et vérifie la réponse sans l'exécuter (`ast` et `ruff`, ou sqlglot). La réponse est alors écrite dans `residue/outputs/` avec l'en-tête `REVIEW_REQUIRED`. `RESIDU.md` liste chaque fichier à relire. Rien n'entre dans le bundle sans relecture.

## Réconcilier (phase 9)

`dku2dbx reconcile <export.zip> [--config] [--out]` génère dans `out/<projet>/reconcile/` :

- `reconcile.py` : un notebook Databricks autonome (rien à installer sur le workspace) qui compare chaque table produite par le flux migré avec la sortie Dataiku du même dataset, en quatre couches : schéma, nombre de lignes (au total et par partition), agrégats par colonne, lignes (jointure sur la clé métier, ou `EXCEPT ALL` sans clé). Paramètres : `catalog`, `schema` (tables Databricks), `ref_catalog`, `ref_schema` (sorties Dataiku dans Unity Catalog). Il écrit un résultat par table dans la table Delta `dku2dbx_reconciliation` et **échoue** si une table est en `ÉCART` ;
- `RECONCILIATION.md` : le plan (tables, clés, tolérances, partitions) ;
- `ECARTS_ACCEPTES.md` : le registre des écarts acceptés, à remplir par le métier (jamais réécrit).

Statut par table : `IDENTIQUE`, `ÉCART TOLÉRÉ` (dans les tolérances, ou élargissement de type sans perte) ou `ÉCART`. Clés et tolérances : `config/reconcile.local.yaml` (ignoré par Git), sinon `config/reconcile.yaml`.

## Interface : Databricks App (phase 10)

Le parcours complet dans un navigateur : déposer un export ZIP → Analyser → Simplifier → Brancher (formulaire par connexion) → Migrer (bundle et résidu à **télécharger**, le déploiement reste un geste humain après relecture) → Réconcilier (clé et tolérances par table, notebook à télécharger). L'app appelle exactement le code de la CLI (`src/dku2dbx/webapp/session.py`).

```bash
uv run streamlit run app/app.py --server.address localhost   # en local : http://localhost:8501
cd app && databricks bundle deploy && databricks bundle run dku2dbx_app   # sur un workspace
```

`app/app.yaml` déclare deux variables : `DKU2DBX_EXPORTS_VOLUME` (Volume Unity Catalog à accès restreint où copier les exports) et `DKU2DBX_LLM_ENDPOINT` (endpoint Model Serving validé pour le résidu). Vides par défaut.

## Clé d'API Dataiku

La clé n'est jamais écrite dans le code ni dans Git (règle 4 de `CLAUDE.md`). Elle vit dans un fichier `.env` local, ignoré par Git. Un hook `pre-commit` refuse en plus tout commit d'un fichier `.env`.

```bash
cp .env.example .env               # puis remplir DKU_DSS_URL et DKU_API_KEY dans .env
uv run --env-file .env <commande>  # charge les variables pour cette commande
```

Pour éviter de répéter `--env-file`, définir une fois `UV_ENV_FILE=.env` dans l'environnement : `export UV_ENV_FILE=.env` sous Linux ou macOS, `setx UV_ENV_FILE .env` sous Windows.

Recommandations :
- utiliser une clé **personnelle**, en lecture seule si possible, plutôt qu'une clé administrateur ;
- créer `.env` **uniquement sur le poste qui accède à Dataiku** ;
- si la clé a fuité (commit, capture, message), la **révoquer** immédiatement dans Dataiku (Profil > API keys).
