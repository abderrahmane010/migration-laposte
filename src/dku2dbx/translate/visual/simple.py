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
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import post_filter, where_clause
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
    query = where_clause(query, recipe, payload.get("preFilter"), "preFilter", available)
    check_output(recipe, produced, declared, target.ref)
    final = post_filter(query, recipe, payload.get("postFilter"), declared)
    return _translation(recipe, target.ref, target.append_mode, final, [])


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
        one = exp.select(*projection).from_(table(inp.ref))
        where = f"virtualInputs[{vin['index']}].preFilter"
        selects.append(where_clause(one, recipe, vin.get("preFilter"), where, available))
    query: exp.Expr = selects[0]
    for nxt in selects[1:]:
        query = exp.union(query, nxt, distinct=False)
    produced = [*selected, *([origin] if origin else [])]
    check_output(recipe, produced, list(out_types), target.ref)
    final = post_filter(query, recipe, payload.get("postFilter"), produced)
    return _translation(recipe, target.ref, target.append_mode, final, [])


# Sort numbering options -> Spark window functions.
NUMBERINGS = {"rowNumber": exp.RowNumber, "rank": exp.Rank, "denseRank": exp.DenseRank}


def translate_sort(project: Project, recipe: Recipe) -> Translation:
    """Sort: ORDER BY the payload orders (ASSUMPTION H-34: orders[].{column, desc}).

    ASSUMPTION (H-36): a numbering option (rowNumber, rank, denseRank) adds one output
    column, numbered in the sort order; its name is the one output column absent from
    the input. Several options at once are not mapped (their column order is unknown).
    """
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_empty(recipe, payload, "outputColumnNameOverrides", "computedColumns")
    declared = schema(project, target.ref, recipe)
    available = schema(project, source.ref, recipe)
    orders = payload.get("orders") or []
    if not orders:
        raise UnsupportedConstruct(recipe.name, "orders", "no sort column")
    # ASSUMPTION (H-34): NULLs sort last in both directions (made explicit: Spark's
    # default puts them first in ascending order). To verify against Dataiku.
    ordering = [
        exp.Ordered(this=column(str(o["column"])), desc=bool(o.get("desc")), nulls_first=False)
        for o in orders
    ]
    numberings = [k for k in NUMBERINGS if payload.get(k)]
    extra = [c for c in declared if c not in available]
    if len(numberings) > 1:
        raise UnsupportedConstruct(recipe.name, "+".join(numberings), "several numberings")
    if len(extra) != len(numberings):
        raise UnsupportedConstruct(
            recipe.name, "output schema", f"columns absent from the input: {extra}"
        )
    projection: list[exp.Expr] = []
    for name in declared:
        if name in extra:
            window = exp.Window(
                this=NUMBERINGS[numberings[0]](),
                order=exp.Order(expressions=[o.copy() for o in ordering]),
            )
            projection.append(exp.alias_(window, name, quoted=True))
        else:
            projection.append(column(name))
    query = exp.select(*projection).from_(table(source.ref))
    query = where_clause(query, recipe, payload.get("preFilter"), "preFilter", available)
    query = query.order_by(*ordering)
    warning = (
        "l'ordre des lignes n'est pas conservé par une table Delta : il ne vaut que pour "
        "la lecture de ce SELECT"
    )
    return _translation(recipe, target.ref, target.append_mode, query, [warning])
