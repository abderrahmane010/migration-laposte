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
uv run dku2dbx translate export.zip             # recettes visuelles -> out/<projet>/sql/*.sql
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

## Traduire (phase 3)

`dku2dbx translate <export.zip>` traduit de façon déterministe les recettes visuelles (Sync, Sample/Filter, Distinct, Stack, Join, Group, Window, Split, Sort, Top N, Pivot) et les recettes Prepare (une CTE par étape, langage de formules compris) en Spark SQL :

- un fichier `out/<projet>/sql/<recette>.sql` par sortie (`<recette>__<sortie>.sql` quand il y en a plusieurs), qui commence par un en-tête de traçabilité (projet, recette, version, SHA-256 de la définition et du payload) et les **avertissements** à relire ;
- `out/<projet>/translate_report.json` : recettes traduites, recettes **non prises en charge** avec l'élément Dataiku en cause, recettes sans traducteur.

Une option non traduite n'est jamais devinée ni ignorée : la recette est listée comme non prise en charge. Les tables sont nommées comme les datasets Dataiku ; les phases 6 et 7 les remplaceront par `catalog.schema.table`.

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
