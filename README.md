# dku2dbx

Usine de migration de projets Dataiku DSS vers Databricks (Unity Catalog, Lakeflow Jobs, Lakeflow pipelines).
Le contexte et les règles sont dans [`CLAUDE.md`](CLAUDE.md), le plan dans [`ROADMAP.md`](ROADMAP.md) et l'avancement dans [`docs/journal.md`](docs/journal.md).

## Démarrage

Prérequis : [uv](https://docs.astral.sh/uv/). Java 17+ est aussi nécessaire pour les tests PySpark locaux.

```bash
uv sync                                         # crée .venv et installe les dépendances (uv.lock)
uv run pre-commit install                       # installe les hooks Git
uv run dku2dbx --help                           # CLI
uv run dku2dbx analyze export.zip --dump-ir     # écrit out/<projet>/ir.json (modèle interne)
uv run python tests/fixtures/make_fixture.py    # régénère les exports synthétiques
uv run pytest                                   # tests
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Les exports réels vont dans `exports/` et les artefacts générés dans `out/`. Ces deux dossiers sont ignorés par Git.

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
