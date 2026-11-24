# CLAUDE.md — Projet `dku2dbx` : usine de migration Dataiku → Databricks

Ce fichier est lu automatiquement par Claude Code à chaque session. Il donne le contexte, les règles non négociables et les conventions du projet. Le plan détaillé, phase par phase, est dans `ROADMAP.md`.

## Contexte

- Le Groupe La Poste veut migrer des projets Dataiku DSS complexes (dizaines à centaines de recettes, partitions, scénarios, plugins) vers Databricks (Unity Catalog, Lakeflow Jobs, Lakeflow pipelines, Lakeflow Designer).
- Aucun outil officiel ne convertit Dataiku. Ni Lakebridge ni Genie Migrate n'acceptent Dataiku en entrée, et Dataiku n'expose aucune API de conversion des recettes visuelles en code.
- Ce dépôt construit donc notre propre outillage. Il lit les **exports ZIP de projets Dataiku** (configuration seulement, aucune donnée), les analyse, les convertit en code Databricks, génère un bundle déployable, puis aide à prouver l'équivalence des résultats.
- Référence d'inspiration : un prototype Databricks (« Dataiku Sunset ») suit le même parcours en quatre étapes : Analyser → Simplifier → Brancher → Migrer. Nous ne disposons pas de son code.

## Règles non négociables

1. **Déterministe d'abord, LLM en dernier recours.** Chaque construction Dataiku connue est traduite par du code déterministe et testé. Le LLM ne traite que le résidu, et sa sortie est toujours marquée `REVIEW_REQUIRED`.
2. **Ne jamais inventer ni ignorer une transformation.** Un type de recette, de processeur ou de fonction de formule inconnu lève `UnsupportedConstruct` et apparaît dans le rapport. Rien n'est ignoré en silence, sauf les steps Prepare `"disabled": true`, qui sont listées dans le rapport.
3. **Traçabilité totale.** Chaque fichier généré commence par un en-tête qui indique le projet et la recette Dataiku d'origine, la version du générateur et le hash du JSON source.
4. **Pas de données réelles ni de secrets dans Git.** Les exports réels vont dans `exports/`, ignoré par Git. Les fixtures de test sont synthétiques ou anonymisées. Aucun token dans le code : utiliser des variables d'environnement et des profils Databricks CLI.
5. **Aucun envoi d'export ou de code métier vers une API externe.** Le LLM n'est appelé que via un endpoint validé par la sécurité : un endpoint Model Serving Databricks, ou un endpoint interne La Poste compatible OpenAI (vLLM, décision du 2026-11-24). Il est configuré dans `config/llm.local.yaml` (ignoré par Git) ou `config/llm.yaml`. Sans endpoint configuré, l'étape LLM est désactivée.
6. **Tests obligatoires.** Pas de traducteur sans tests unitaires, snapshot du SQL généré et test d'exécution sur PySpark local avec de petites données synthétiques.
7. **Une phase de `ROADMAP.md` à la fois.** À la fin de chaque phase : tests verts, `ruff` propre, résumé dans `docs/journal.md`, puis arrêt pour validation humaine avant la phase suivante.
8. **Hypothèses explicites.** Le format interne des exports Dataiku n'est pas documenté officiellement. Toute hypothèse sur un champ JSON est marquée `# ASSUMPTION:` dans le code, accompagnée d'une fixture, et listée dans `docs/hypotheses.md`.

## Pile technique

- Python 3.11+, packaging `pyproject.toml` (gestionnaire `uv`), CLI `typer`.
- Modèle interne : `pydantic` v2.
- SQL : `sqlglot` (dialecte `databricks`) pour parser, formater et valider le SQL généré.
- Graphes : `networkx`. Templates : `jinja2`. Grammaire des formules Dataiku : `lark`.
- Tests : `pytest`, `syrupy` (snapshots), `pyspark` en local pour exécuter le SQL généré, `chispa` ou `pyspark.testing.assertDataFrameEqual`.
- Qualité : `ruff`, `mypy` (mode strict sur `src/`), `pre-commit`.
- Databricks : Declarative Automation Bundles (ex-Databricks Asset Bundles, CLI `databricks bundle`), Lakeflow Jobs, Lakeflow pipelines (`from pyspark import pipelines as dp`), Unity Catalog.

## Arborescence cible

```
dku2dbx/
├── CLAUDE.md
├── ROADMAP.md
├── pyproject.toml
├── config/
│   ├── scoring.yaml          # poids du score de complexité
│   ├── connections.yaml      # connexion Dataiku -> catalog.schema Unity Catalog
│   └── llm.yaml              # endpoint Model Serving validé (vide par défaut)
├── src/dku2dbx/
│   ├── reader/               # lecture des exports ZIP -> modèle interne
│   ├── model/                # modèle interne (IR) pydantic
│   ├── analyze/              # inventaire, catalogue de processeurs, score, vagues
│   ├── simplify/             # fusion des chaînes de recettes
│   ├── translate/
│   │   ├── visual/           # Join, Group, Window, Stack, Split, Distinct, Pivot, TopN, Sort, Sync
│   │   ├── prepare/          # registre des processeurs + traducteur de formules
│   │   ├── code/             # recettes SQL et Python (réécriture d'idiomes)
│   │   └── llm/              # résidu, sortie REVIEW_REQUIRED
│   ├── generate/             # notebooks SQL, pipelines, jobs, bundle
│   ├── connect/              # « brancher » : mapping des sources vers UC
│   ├── reconcile/            # harnais de comparaison des sorties
│   └── cli.py
├── templates/                # jinja2 : bundle, jobs, notebooks
├── tests/
│   ├── fixtures/exports/     # exports synthétiques ou anonymisés
│   └── ...
├── exports/                  # exports réels — ignoré par Git
├── out/                      # artefacts générés — ignoré par Git
└── docs/
    ├── journal.md
    ├── hypotheses.md
    └── pieges_semantiques.md
```

## Ce que l'on sait du format d'export Dataiku (DSS 15, à revérifier sur nos exports)

- Le ZIP contient `export-manifest.json` (version DSS, `exportedWithPlugins`, `foreignObjects`) et un dossier `project_config/`.
- `project_config/` contient `params.json` (dont `exposedObjects` pour les partages entre projets), `datasets/<nom>.json` et `recipes/<nom>.json`.
- Un dataset contient `type`, `managed`, `params.connection/catalog/schema/table/mode`, `partitioning.dimensions`, `schema` et `checks`. Les noms de tables peuvent contenir des variables (`${projectKey}`, `${NODE_DC}`) à résoudre.
- Une recette contient `type`, `inputs.main.items[].ref/deps`, `outputs.main.items[].ref/appendMode`, `params.engineParams` et `neverRecomputeExistingPartitions`.
- Chaque recette visuelle a un fichier de payload dont l'extension est son type : `.shaker` (Prepare), `.grouping`, `.join`, `.window`, `.pivot`, `.split`, `.vstack`, `.distinct`, etc.
- Une step Prepare a la forme `{"metaType": "PROCESSOR", "type": "<Processeur>", "params": {...}, "disabled": false}`. Les steps peuvent être regroupées (autre `metaType`) : ne pas filtrer, lever une erreur tant que ce n'est pas géré.
- Les recettes de code (Python, SQL) stockent leur script dans un payload texte. Son emplacement exact dans le ZIP est à confirmer sur nos exports.

## Conventions

- Noms de code, de fonctions et de commits en anglais ; documentation et rapports en français.
- Commits petits et atomiques, au format Conventional Commits (`feat:`, `fix:`, `test:`…).
- Toute sortie générée est reproductible : même entrée et même version du générateur produisent les mêmes fichiers, octet pour octet.
- Les pièges sémantiques connus sont listés dans `docs/pieges_semantiques.md` (repris de `ROADMAP.md`) et chacun a au moins un test.
