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

Il reste à faire : les dossiers gérés (managed folders) ne sont pas encore modélisés. Ils produiraient le même type de référence manquante, mais aucun n'a été rencontré.
