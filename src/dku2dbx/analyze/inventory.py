"""Per-project analysis: inventory, measures, classification, score and effort."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from dku2dbx.analyze.classify import (
    FORMULA_PROCESSORS,
    PREPARE_TYPE,
    active_processors,
    classify,
    processors,
)
from dku2dbx.analyze.model import CrossProjectLink, ProjectAnalysis
from dku2dbx.analyze.scoring import ScoringConfig, effort, score
from dku2dbx.model.ir import Project, RecipeCategory

# ASSUMPTION (H-29): a Python scenario has type "custom_python"; a step-based
# scenario running Python has a step of type "custom_python".
PYTHON_SCENARIO_TYPES = frozenset({"custom_python"})


def _sorted_counts(values: Iterable[str]) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def cross_project_links(project: Project) -> list[CrossProjectLink]:
    """Datasets shared with other projects, seen from this project's export."""
    key = project.key
    links: set[tuple[str, str, str, str]] = set()
    for rcp in project.recipes:
        for inp in rcp.inputs:
            if inp.project_key and inp.project_key != key:
                links.add((inp.project_key, key, inp.name, "input"))
    for fo in project.foreign_objects:
        if fo.project_key != key:
            links.add((fo.project_key, key, fo.id, "foreign"))
    for exp in project.exposed_objects:
        for target in exp.target_projects:
            if target != key:
                links.add((key, target, exp.local_name, "exposed"))
    return [
        CrossProjectLink(producer=p, consumer=c, dataset=d, source=s)
        for p, c, d, s in sorted(links)
    ]


def analyze_project(project: Project, source_file: str, config: ScoringConfig) -> ProjectAnalysis:
    recipes = project.recipes
    categories = Counter(r.category for r in recipes)
    prepares = [r for r in recipes if r.type == PREPARE_TYPE]
    active = [p for r in prepares for p in active_processors(r)]
    formula_prepares = sum(
        sum(p in FORMULA_PROCESSORS for p in active_processors(r)) >= config.formula_rich_min_steps
        for r in prepares
    )
    python_scenarios = sum(
        s.type in PYTHON_SCENARIO_TYPES or any(st.type in PYTHON_SCENARIO_TYPES for st in s.steps)
        for s in project.scenarios
    )
    used_plugins = sorted(p.plugin_id for p in project.plugins if p.used_by_recipes)
    partitioned = sum(bool(d.partitioning) for d in project.datasets)
    code = categories[RecipeCategory.CODE]

    measures: dict[str, float] = {
        "recipes": len(recipes),
        "code_share": round(code / len(recipes), 4) if recipes else 0.0,
        "formula_prepares": formula_prepares,
        "partitioned": partitioned,
        "python_scenarios": python_scenarios,
        "plugins": len(used_plugins),
        "saved_models": len(project.saved_models),
    }
    classes = classify(project)
    level, triggers = score(measures, config)
    disabled = sorted(f"{r.name}: {p}" for r in prepares for p in processors(r, disabled=True))
    return ProjectAnalysis(
        key=project.key,
        name=project.name,
        source_file=source_file,
        measures=measures,
        recipes_by_type=_sorted_counts(r.type for r in recipes),
        recipes_by_category=_sorted_counts(r.category.value for r in recipes),
        recipes_by_class=_sorted_counts(c.target_class.value for c in classes),
        datasets_by_type=_sorted_counts(d.type for d in project.datasets),
        counts={
            "datasets": len(project.datasets),
            "partitioned_datasets": partitioned,
            "zones": len(project.zones),
            "recipes": len(recipes),
            "code_recipes": code,
            "prepare_recipes": len(prepares),
            "ml_recipes": categories[RecipeCategory.ML],
            "plugin_recipes": categories[RecipeCategory.PLUGIN],
            "scenarios": len(project.scenarios),
            "python_scenarios": python_scenarios,
            "saved_models": len(project.saved_models),
            "issues": len(project.issues),
        },
        plugins=used_plugins,
        processors=_sorted_counts(active),
        disabled_steps=disabled,
        issues_by_kind=_sorted_counts(i.kind.value for i in project.issues),
        level=level,
        triggers=triggers,
        effort_days=effort([c.target_class for c in classes], len(project.scenarios), config),
        recipes=classes,
        links=cross_project_links(project),
    )
