"""Distinct, Stack and Sort recipes (ROADMAP phase 3)."""

from __future__ import annotations

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    check_output,
    columns_with_types,
    main_inputs,
    partition_warnings,
    payload_dict,
    require_empty,
    require_false,
    require_no_filter,
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.types import spark_type


def _translation(
    recipe: Recipe, target: str, append: bool, query: exp.Expr, warnings: list[str]
) -> Translation:
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[TranslatedOutput(dataset=target, sql=render(query), append=append)],
        warnings=[*partition_warnings(recipe), *warnings],
    )


def translate_distinct(project: Project, recipe: Recipe) -> Translation:
    """Remove duplicates on the key columns (all columns when no key), with an optional count.

    ASSUMPTION (H-33): payload keys[].column (observed), globalCount adds the number of
    duplicates as the single extra output column; outputColumnNameOverrides empty.
    """
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_no_filter(recipe, payload.get("preFilter"), "preFilter")
    require_no_filter(recipe, payload.get("postFilter"), "postFilter")
    require_empty(recipe, payload, "outputColumnNameOverrides", "computedColumns")
    keys = [str(k.get("column")) for k in payload.get("keys") or []]
    available = schema(project, source.ref, recipe)
    keys = keys or available
    unknown = [k for k in keys if k not in available]
    if unknown:
        raise UnsupportedConstruct(recipe.name, "keys", f"unknown columns: {', '.join(unknown)}")
    declared = schema(project, target.ref, recipe)
    if payload.get("globalCount"):
        if len(declared) != len(keys) + 1:
            raise UnsupportedConstruct(
                recipe.name, "globalCount", "the output schema has no single count column"
            )
        count_name = declared[-1]
        query = (
            exp.select(
                *[column(k) for k in keys],
                exp.alias_(exp.Count(this=exp.Star()), count_name, quoted=True),
            )
            .from_(table(source.ref))
            .group_by(*[column(k) for k in keys])
        )
        produced = [*keys, count_name]
    else:
        query = exp.select(*[column(k) for k in keys]).distinct().from_(table(source.ref))
        produced = keys
    check_output(recipe, produced, declared, target.ref)
    return _translation(recipe, target.ref, target.append_mode, query, [])


def translate_vstack(project: Project, recipe: Recipe) -> Translation:
    """Stack (UNION mode): selected columns, NULL where an input lacks one, origin column.

    ASSUMPTION (H-09d): mode UNION, selectedColumns (names), virtualInputs[].{index,
    originLabel, preFilter}, addOriginColumn/originColumnName (all observed).
    """
    payload = payload_dict(recipe)
    target = single_output(recipe)
    inputs = main_inputs(recipe)
    if payload.get("mode") != "UNION":
        raise UnsupportedConstruct(recipe.name, "mode", f"'{payload.get('mode')}' is not mapped")
    require_no_filter(recipe, payload.get("postFilter"), "postFilter")
    selected = [str(c) for c in payload.get("selectedColumns") or []]
    if not selected:
        raise UnsupportedConstruct(recipe.name, "selectedColumns", "no column selected")
    out_types = dict(columns_with_types(project, target.ref, recipe))
    origin = str(payload.get("originColumnName")) if payload.get("addOriginColumn") else None
    virtual = sorted(payload.get("virtualInputs") or [], key=lambda v: int(v.get("index", 0)))
    if [int(v.get("index", -1)) for v in virtual] != list(range(len(inputs))):
        raise UnsupportedConstruct(recipe.name, "virtualInputs", "one entry per input expected")

    selects: list[exp.Select] = []
    for vin, inp in zip(virtual, inputs, strict=True):
        require_no_filter(recipe, vin.get("preFilter"), f"virtualInputs[{vin['index']}].preFilter")
        available = set(schema(project, inp.ref, recipe))
        projection: list[exp.Expr] = []
        for name in selected:
            if name in available:
                projection.append(column(name))
            else:
                cast = exp.Cast(
                    this=exp.Null(), to=spark_type(out_types.get(name), recipe.name, name)
                )
                projection.append(exp.alias_(cast, name, quoted=True))
        if origin:
            label = str(vin.get("originLabel", inp.ref))
            projection.append(exp.alias_(exp.Literal.string(label), origin, quoted=True))
        selects.append(exp.select(*projection).from_(table(inp.ref)))
    query: exp.Expr = selects[0]
    for nxt in selects[1:]:
        query = exp.union(query, nxt, distinct=False)
    check_output(recipe, [*selected, *([origin] if origin else [])], list(out_types), target.ref)
    return _translation(recipe, target.ref, target.append_mode, query, [])


def translate_sort(project: Project, recipe: Recipe) -> Translation:
    """Sort: ORDER BY the payload orders (ASSUMPTION H-34: orders[].{column, desc})."""
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_no_filter(recipe, payload.get("preFilter"), "preFilter")
    require_empty(recipe, payload, "outputColumnNameOverrides", "computedColumns")
    require_false(recipe, payload, "rank", "rowNumber", "denseRank")
    declared = schema(project, target.ref, recipe)
    available = set(schema(project, source.ref, recipe))
    if not set(declared) <= available:
        raise UnsupportedConstruct(recipe.name, "output schema", "columns absent from the input")
    orders = payload.get("orders") or []
    if not orders:
        raise UnsupportedConstruct(recipe.name, "orders", "no sort column")
    query = (
        exp.select(*[column(c) for c in declared])
        .from_(table(source.ref))
        .order_by(
            # ASSUMPTION (H-34): NULLs sort last in both directions (made explicit: Spark's
            # default puts them first in ascending order). To verify against Dataiku.
            *[
                exp.Ordered(
                    this=column(str(o["column"])), desc=bool(o.get("desc")), nulls_first=False
                )
                for o in orders
            ]
        )
    )
    warning = (
        "l'ordre des lignes n'est pas conservé par une table Delta : il ne vaut que pour "
        "la lecture de ce SELECT"
    )
    return _translation(recipe, target.ref, target.append_mode, query, [warning])
