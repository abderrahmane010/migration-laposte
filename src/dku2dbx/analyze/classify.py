"""Target class of each recipe: AUTO, LLM_REVIEW or MANUAL.

The class is the one the roadmap plans for the recipe type (decision of
2026-11-23: "target class + state"). ``translator_available`` says whether the
translator already exists; it turns true as phases 3 and 4 add translators to
``AVAILABLE_TRANSLATORS``, so the classification updates itself.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

from dku2dbx.analyze.model import RecipeClassification, TargetClass
from dku2dbx.model.ir import IssueKind, PrepareStep, Project, Recipe, RecipeCategory, Status
from dku2dbx.reader.known_types import CODE_RECIPE_TYPES, GROUP_META_TYPE, PROCESSOR_META_TYPE

# Recipe types whose deterministic translator exists. Empty until phase 3: each
# translator phase adds its types here (and its tests).
AVAILABLE_TRANSLATORS: frozenset[str] = frozenset()

# Phase 3, "Sync/Filter -> ... -> Pivot". "sampling" is the DSS Sample/Filter recipe.
PHASE_3_TYPES = frozenset(
    {
        "sync",
        "sampling",
        "distinct",
        "vstack",
        "join",
        "grouping",
        "window",
        "split",
        "sort",
        "topn",
        "pivot",
    }
)
PREPARE_TYPE = "shaker"
# Python is rewritten by idioms then by the LLM (phase 8). SQL code recipes have no
# planned translator (decision of 2026-11-23): residue, reviewed.
LLM_CODE_TYPES = frozenset(
    {"python", "pyspark", "sql_query", "sql_script", "hive", "impala", "spark_sql_query"}
)
# Out of scope of v1 (ROADMAP): R, Shell, Scala.
MANUAL_CODE_TYPES = CODE_RECIPE_TYPES - LLM_CODE_TYPES
# Visual recipes no phase translates. Fuzzy join: Damerau-Levenshtein has no exact
# Spark equivalent (pieges_semantiques.md) -> reviewed. The others read or write
# folders or geometry -> by hand.
LLM_VISUAL_TYPES = frozenset({"fuzzyjoin", "upsert"})

# ASSUMPTION (H-27): Prepare processors that need a reference dataset or a non-SQL
# computation (phase 4, task 4), by DSS type id. To be completed with the catalogue.
MANUAL_PROCESSORS = frozenset({"ZipCodeGeocoder", "GeoIPProcessor", "UserAgentClassifier"})
# ASSUMPTION (H-28): Prepare processors that carry a Dataiku formula.
FORMULA_PROCESSORS = frozenset({"CreateColumnWithGREL", "FilterOnFormula"})

_PAYLOAD_ISSUES = (
    IssueKind.MISSING_PAYLOAD,
    IssueKind.INVALID_PAYLOAD,
    IssueKind.MULTIPLE_PAYLOADS,
)


def iter_steps(steps: list[PrepareStep] | None) -> Iterator[PrepareStep]:
    """Every step, groups included, depth first."""
    for step in steps or []:
        yield step
        yield from iter_steps(step.steps)


def steps_with_state(
    steps: list[PrepareStep] | None, parent_disabled: bool = False
) -> Iterator[tuple[PrepareStep, bool]]:
    """Every step with its effective state: a disabled group disables its steps."""
    for step in steps or []:
        disabled = parent_disabled or step.disabled
        yield step, disabled
        yield from steps_with_state(step.steps, disabled)


def processors(recipe: Recipe, *, disabled: bool) -> list[str]:
    """Types of the processors of a Prepare recipe that are (not) disabled."""
    return [
        step.type
        for step, is_disabled in steps_with_state(recipe.prepare_steps)
        if is_disabled is disabled and step.meta_type == PROCESSOR_META_TYPE and step.type
    ]


def active_processors(recipe: Recipe) -> list[str]:
    return processors(recipe, disabled=False)


_Rule = tuple[Callable[[Recipe], bool], TargetClass, int | None, Callable[[Recipe], str | None]]

# First matching rule wins: (condition, class, planned phase, reason).
_RULES: list[_Rule] = [
    (
        lambda r: r.status is Status.UNKNOWN,
        TargetClass.MANUAL,
        None,
        lambda r: f"type de recette inconnu '{r.type}'",
    ),
    (lambda r: r.type in PHASE_3_TYPES, TargetClass.AUTO, 3, lambda r: None),
    (lambda r: r.type == PREPARE_TYPE, TargetClass.AUTO, 4, lambda r: None),
    (lambda r: r.type in LLM_CODE_TYPES, TargetClass.LLM_REVIEW, 8, lambda r: None),
    (
        lambda r: r.type in LLM_VISUAL_TYPES,
        TargetClass.LLM_REVIEW,
        8,
        lambda r: "aucun traducteur déterministe prévu",
    ),
    (
        lambda r: r.type in MANUAL_CODE_TYPES,
        TargetClass.MANUAL,
        None,
        lambda r: f"langage hors périmètre v1 ({r.type})",
    ),
    (
        lambda r: r.category is RecipeCategory.ML,
        TargetClass.MANUAL,
        None,
        lambda r: "recette ML hors périmètre v1",
    ),
    (
        lambda r: r.category is RecipeCategory.PLUGIN,
        TargetClass.MANUAL,
        None,
        lambda r: f"recette de plugin ({r.plugin_id})",
    ),
]


def _base_class(recipe: Recipe) -> tuple[TargetClass, int | None, str | None]:
    for matches, target, phase, reason in _RULES:
        if matches(recipe):
            return target, phase, reason(recipe)
    return TargetClass.MANUAL, None, f"aucun traducteur prévu pour '{recipe.type}'"


def classify(project: Project) -> list[RecipeClassification]:
    payload_issues = {
        (i.object_name, i.kind.value) for i in project.issues if i.kind in _PAYLOAD_ISSUES
    }
    result = []
    for rcp in project.recipes:
        target, phase, reason = _base_class(rcp)
        reasons = [reason] if reason else []
        if rcp.type == PREPARE_TYPE:
            manual = sorted(set(active_processors(rcp)) & MANUAL_PROCESSORS)
            unknown_meta = sorted(
                {
                    s.meta_type
                    for s in iter_steps(rcp.prepare_steps)
                    if s.meta_type not in (PROCESSOR_META_TYPE, GROUP_META_TYPE)
                }
            )
            if manual:
                target = TargetClass.MANUAL
                reasons.append("processeur(s) à référentiel : " + ", ".join(manual))
            if unknown_meta:
                target = TargetClass.MANUAL
                reasons.append("steps de metaType inconnu : " + ", ".join(unknown_meta))
        issues = sorted(kind for name, kind in payload_issues if name == rcp.name)
        if issues and target is TargetClass.AUTO:
            target = TargetClass.MANUAL
            reasons.append("payload inexploitable : " + ", ".join(issues))
        result.append(
            RecipeClassification(
                name=rcp.name,
                type=rcp.type,
                category=rcp.category.value,
                target_class=target,
                planned_phase=phase if target is not TargetClass.MANUAL else None,
                translator_available=(
                    target is TargetClass.AUTO and rcp.type in AVAILABLE_TRANSLATORS
                ),
                reasons=reasons,
            )
        )
    return result
