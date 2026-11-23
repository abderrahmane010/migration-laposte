"""Group recipe (ROADMAP phase 3).

ASSUMPTION (H-38), format observed on a real project: ``keys[].column`` and
``values[].{column, <aggregate flags>}``; ``globalCount`` adds a ``count`` column. An
aggregate output column is named ``<column>_<suffix>``; each declared output column is
matched by name to exactly one aggregate (no assumption on their order) and cast to
its declared type. Only count, sum, avg, min and max are mapped: stddev (sample or
population?), first/last (order-dependent), concat... are reported until verified.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    columns_with_types,
    partition_warnings,
    payload_dict,
    require_empty,
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import post_filter, where_clause
from dku2dbx.translate.visual.types import spark_type

_Agg = Callable[[exp.Expr], exp.Expr]

# flag -> (output name suffix, SQL aggregate)
AGGREGATES: dict[str, tuple[str, _Agg]] = {
    "count": ("count", lambda c: exp.Count(this=c)),
    "sum": ("sum", lambda c: exp.Sum(this=c)),
    "avg": ("avg", lambda c: exp.Avg(this=c)),
    "min": ("min", lambda c: exp.Min(this=c)),
    "max": ("max", lambda c: exp.Max(this=c)),
}
# Flags that exist in the payload but are not mapped: refused when enabled.
UNMAPPED_FLAGS = (
    "countDistinct",
    "sum2",
    "stddev",
    "first",
    "last",
    "firstLastNotNull",
    "concat",
    "concatDistinct",
)
GLOBAL_COUNT_NAME = "count"


def _enabled(recipe: Recipe, entry: dict[str, Any], where: str) -> list[str]:
    for flag in UNMAPPED_FLAGS:
        if entry.get(flag):
            raise UnsupportedConstruct(recipe.name, f"{where}.{flag}", "aggregate not mapped yet")
    return [flag for flag in AGGREGATES if entry.get(flag) is True]


def _keys(recipe: Recipe, payload: dict[str, Any], available: list[str]) -> list[str]:
    keys = []
    for key in payload.get("keys") or []:
        name = str(key.get("column"))
        if name not in available:
            raise UnsupportedConstruct(recipe.name, "keys", f"unknown column '{name}'")
        if _enabled(recipe, key, "keys[]"):
            raise UnsupportedConstruct(recipe.name, "keys[]", "aggregate on a key column")
        keys.append(name)
    return keys


def _aggregates(
    recipe: Recipe, payload: dict[str, Any], available: list[str]
) -> dict[str, exp.Expr]:
    """Output column name -> aggregate expression."""
    candidates: dict[str, exp.Expr] = {}
    if payload.get("globalCount"):
        candidates[GLOBAL_COUNT_NAME] = exp.Count(this=exp.Star())
    for value in payload.get("values") or []:
        name = str(value.get("column"))
        if name not in available:
            raise UnsupportedConstruct(recipe.name, "values", f"unknown column '{name}'")
        for flag in _enabled(recipe, value, "values[]"):
            suffix, build = AGGREGATES[flag]
            out_name = f"{name}_{suffix}"
            if out_name in candidates:
                raise UnsupportedConstruct(recipe.name, "values", f"'{out_name}' defined twice")
            candidates[out_name] = build(column(name))
    return candidates


def _projection(
    recipe: Recipe,
    declared: list[tuple[str, str | None]],
    keys: list[str],
    candidates: dict[str, exp.Expr],
) -> list[exp.Expr]:
    """Declared output columns, in order: keys as they are, aggregates cast to their type."""
    remaining = dict(candidates)
    projection: list[exp.Expr] = []
    for name, dku_type in declared:
        if name in keys:
            projection.append(column(name))
        elif name in remaining:
            cast = exp.Cast(this=remaining.pop(name), to=spark_type(dku_type, recipe.name, name))
            projection.append(exp.alias_(cast, name, quoted=True))
        else:
            raise UnsupportedConstruct(
                recipe.name, "output schema", f"column '{name}' matches no key nor aggregate"
            )
    if remaining:
        raise UnsupportedConstruct(
            recipe.name, "output schema", f"aggregates absent from the output: {sorted(remaining)}"
        )
    if [n for n, _ in declared if n in keys] != keys:
        raise UnsupportedConstruct(recipe.name, "output schema", "key columns missing or moved")
    return projection


def translate_grouping(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_empty(recipe, payload, "computedColumns", "outputColumnNameOverrides")
    available = schema(project, source.ref, recipe)
    keys = _keys(recipe, payload, available)
    declared = columns_with_types(project, target.ref, recipe)
    projection = _projection(recipe, declared, keys, _aggregates(recipe, payload, available))
    query = exp.select(*projection).from_(table(source.ref))
    query = where_clause(query, recipe, payload.get("preFilter"), "preFilter", available)
    if keys:
        query = query.group_by(*[column(k) for k in keys])
    final = post_filter(query, recipe, payload.get("postFilter"), [n for n, _ in declared])
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(final), append=target.append_mode)
        ],
        warnings=partition_warnings(recipe),
    )
