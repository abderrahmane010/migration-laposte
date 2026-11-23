"""Pivot recipe, explicit values (ROADMAP phase 3).

ASSUMPTION (H-43), format observed on a real project: ``pivots[]`` with
``keyColumns``, ``valueLimit``, ``explicitValues``, ``globalCount`` and
``valueColumns[]`` (aggregate flags); rows identified by ``explicitIdentifiers``
(``identifierColumnsSelection: EXPLICIT``). Only explicit values are translated
(``valueLimit: EXPLICIT``, supposed): a dynamic pivot (``TOP_N``, observed) creates
columns from the data, which a fixed SELECT cannot do, so it is reported (ROADMAP).
Output columns are named ``<value>_count`` and ``<value>_<column>_<aggregate>``,
matched by name to the declared output and cast to their declared type.
"""

from __future__ import annotations

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
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import where_clause
from dku2dbx.translate.visual.group import AGGREGATES, UNMAPPED_FLAGS
from dku2dbx.translate.visual.types import spark_type


def _pivot(recipe: Recipe, payload: dict[str, Any]) -> dict[str, Any]:
    require_empty(recipe, payload, "computedColumns", "customAggregates", "otherColumns")
    pivots = payload.get("pivots") or []
    if len(pivots) != 1:
        raise UnsupportedConstruct(recipe.name, "pivots", f"{len(pivots)} pivots (1 mapped)")
    pivot: dict[str, Any] = pivots[0]
    limit = pivot.get("valueLimit")
    if limit != "EXPLICIT":
        raise UnsupportedConstruct(
            recipe.name,
            "pivots[].valueLimit",
            f"'{limit}': dynamic pivot values (columns depend on the data); freeze the "
            "list of values or write code",
        )
    if payload.get("modalitySlugification", "NONE") != "NONE":
        raise UnsupportedConstruct(
            recipe.name, "modalitySlugification", f"'{payload['modalitySlugification']}'"
        )
    if payload.get("identifierColumnsSelection", "EXPLICIT") != "EXPLICIT":
        raise UnsupportedConstruct(
            recipe.name,
            "identifierColumnsSelection",
            f"'{payload.get('identifierColumnsSelection')}' is not mapped",
        )
    return pivot


def _candidates(
    recipe: Recipe, pivot: dict[str, Any], key: exp.Column, types: dict[str, str | None]
) -> dict[str, exp.Expr]:
    values = [str(v) for v in pivot.get("explicitValues") or []]
    if not values or len(set(values)) != len(values):
        raise UnsupportedConstruct(recipe.name, "pivots[].explicitValues", "empty or duplicated")
    result: dict[str, exp.Expr] = {}
    for value in values:
        hit = exp.EQ(this=key.copy(), expression=exp.Literal.string(value))
        if pivot.get("globalCount"):
            result[f"{value}_count"] = exp.Count(
                this=exp.Case(ifs=[exp.If(this=hit.copy(), true=exp.Literal.number(1))])
            )
        for spec in pivot.get("valueColumns") or []:
            name = str(spec.get("column"))
            if name not in types:
                raise UnsupportedConstruct(recipe.name, "valueColumns", f"unknown column '{name}'")
            for flag in UNMAPPED_FLAGS:
                if spec.get(flag):
                    raise UnsupportedConstruct(
                        recipe.name, f"valueColumns[].{flag}", "aggregate not mapped yet"
                    )
            for flag, (suffix, build) in AGGREGATES.items():
                if spec.get(flag) is True:
                    masked = exp.Case(ifs=[exp.If(this=hit.copy(), true=column(name))])
                    result[f"{value}_{name}_{suffix}"] = build(masked)
    return result


def translate_pivot(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    pivot = _pivot(recipe, payload)
    types = dict(columns_with_types(project, source.ref, recipe))
    keys = [str(k) for k in pivot.get("keyColumns") or []]
    if len(keys) != 1 or keys[0] not in types or types[keys[0]] != "string":
        raise UnsupportedConstruct(
            recipe.name, "pivots[].keyColumns", f"{keys}: one text column expected"
        )
    identifiers = [str(c) for c in payload.get("explicitIdentifiers") or []]
    unknown = [c for c in identifiers if c not in types]
    if unknown:
        raise UnsupportedConstruct(recipe.name, "explicitIdentifiers", f"unknown {unknown}")
    candidates = _candidates(recipe, pivot, column(keys[0]), types)

    projection: list[exp.Expr] = []
    for name, dku_type in columns_with_types(project, target.ref, recipe):
        if name in identifiers:
            projection.append(column(name))
        elif name in candidates:
            cast = exp.Cast(this=candidates.pop(name), to=spark_type(dku_type, recipe.name, name))
            projection.append(exp.alias_(cast, name, quoted=True))
        else:
            raise UnsupportedConstruct(
                recipe.name, "output schema", f"column '{name}' matches no identifier nor value"
            )
    if candidates:
        raise UnsupportedConstruct(
            recipe.name,
            "output schema",
            f"pivoted columns absent from the output: {sorted(candidates)}",
        )
    query = exp.select(*projection).from_(table(source.ref))
    query = where_clause(query, recipe, payload.get("preFilter"), "preFilter", list(types))
    if identifiers:
        query = query.group_by(*[column(c) for c in identifiers])
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(query), append=target.append_mode)
        ],
        warnings=partition_warnings(recipe),
    )
