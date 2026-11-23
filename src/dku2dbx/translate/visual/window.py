"""Window recipe (ROADMAP phase 3).

ASSUMPTION (H-39), format observed on a real project: one ``windows[]`` entry
(partitioningColumns, orders, frame limits) and ``values[]`` with aggregate flags,
lag/lead offsets (``lagValues``/``leadValues``, comma-separated); numbering flags at
the root. Computed columns are named ``<col>_<agg>``, ``<col>_lag<N>``,
``<col>_lead<N>``, ``rank``, ``rowNumber``, ``denseRank``; each declared output column
is either an input column (kept as is) or matched by name to one computation.

ASSUMPTION (H-40): the frame of the aggregates. Without limits: the whole partition.
With limits (ROWS): N PRECEDING when ``limitPreceding``, M FOLLOWING when
``limitFollowing``, UNBOUNDED on a side without limit. The frame used is written in
the header of every generated file, for review. RANGE frames are not mapped.
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
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import post_filter, where_clause
from dku2dbx.translate.visual.group import AGGREGATES, UNMAPPED_FLAGS
from dku2dbx.translate.visual.types import spark_type

NUMBERINGS = {"rank": exp.Rank, "rowNumber": exp.RowNumber, "denseRank": exp.DenseRank}
UNMAPPED_ROOT = ("cumeDist", "ntile")
UNMAPPED_VALUE = (*UNMAPPED_FLAGS, "lagDiff", "leadDiff")


def _offsets(recipe: Recipe, text: Any, what: str) -> list[int]:
    try:
        values = [int(v) for v in str(text or "").split(",") if v.strip()]
    except ValueError:
        raise UnsupportedConstruct(
            recipe.name, what, f"offsets '{text}' are not integers"
        ) from None
    if not values or any(v < 1 for v in values):
        raise UnsupportedConstruct(recipe.name, what, f"offsets '{text}' are not positive")
    return values


def _frame(recipe: Recipe, window: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """sqlglot frame spec and its description for the header (H-40)."""
    if not window.get("enableLimits"):
        spec: dict[str, Any] = {
            "kind": "ROWS",
            "start": "UNBOUNDED",
            "start_side": "PRECEDING",
            "end": "UNBOUNDED",
            "end_side": "FOLLOWING",
        }
        return spec, "toute la partition (aucune limite)"
    if window.get("windowLimitMode", "ROWS") != "ROWS":
        raise UnsupportedConstruct(
            recipe.name, "windowLimitMode", f"'{window.get('windowLimitMode')}' is not mapped"
        )
    start = (
        str(int(window.get("precedingRows", 0))) if window.get("limitPreceding") else "UNBOUNDED"
    )
    end = str(int(window.get("followingRows", 0))) if window.get("limitFollowing") else "UNBOUNDED"
    spec = {
        "kind": "ROWS",
        "start": exp.Literal.number(start) if start != "UNBOUNDED" else start,
        "start_side": "PRECEDING",
        "end": exp.Literal.number(end) if end != "UNBOUNDED" else end,
        "end_side": "FOLLOWING",
    }
    return spec, f"ROWS BETWEEN {start} PRECEDING AND {end} FOLLOWING"


def _computations(
    recipe: Recipe, payload: dict[str, Any], available: list[str]
) -> dict[str, tuple[exp.Expr, bool]]:
    """Output name -> (function, uses the frame)."""
    for flag in UNMAPPED_ROOT:
        if payload.get(flag):
            raise UnsupportedConstruct(recipe.name, flag, "not mapped yet")
    result: dict[str, tuple[exp.Expr, bool]] = {
        name: (fn(), False) for name, fn in NUMBERINGS.items() if payload.get(name)
    }
    for value in payload.get("values") or []:
        col = str(value.get("column"))
        if col not in available:
            raise UnsupportedConstruct(recipe.name, "values", f"unknown column '{col}'")
        for flag in UNMAPPED_VALUE:
            if value.get(flag):
                raise UnsupportedConstruct(recipe.name, f"values[].{flag}", "not mapped yet")
        for flag, (suffix, build) in AGGREGATES.items():
            if value.get(flag) is True:
                result[f"{col}_{suffix}"] = (build(column(col)), True)
        for kind, fn in (("lag", exp.Lag), ("lead", exp.Lead)):
            if value.get(kind):
                for n in _offsets(recipe, value.get(f"{kind}Values"), f"values[].{kind}Values"):
                    result[f"{col}_{kind}{n}"] = (
                        fn(this=column(col), offset=exp.Literal.number(n)),
                        False,
                    )
    return result


def translate_window(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_empty(recipe, payload, "computedColumns", "outputColumnNameOverrides")
    windows = payload.get("windows") or []
    if len(windows) != 1:
        raise UnsupportedConstruct(recipe.name, "windows", f"{len(windows)} windows (1 mapped)")
    window = windows[0]
    available = schema(project, source.ref, recipe)
    partition = (
        list(window.get("partitioningColumns") or []) if window.get("enablePartitioning") else []
    )
    orders = list(window.get("orders") or []) if window.get("enableOrdering") else []
    for name in [*partition, *[o.get("column") for o in orders]]:
        if name not in available:
            raise UnsupportedConstruct(recipe.name, "windows[]", f"unknown column '{name}'")
    frame, frame_text = _frame(recipe, window)
    computations = _computations(recipe, payload, available)
    if any(not uses_frame for _, uses_frame in computations.values()) and not orders:
        raise UnsupportedConstruct(recipe.name, "windows[].orders", "lag/lead/rank need an order")

    def over(fn: exp.Expr, uses_frame: bool) -> exp.Window:
        spec: dict[str, Any] = {"this": fn}
        if partition:
            spec["partition_by"] = [column(str(c)) for c in partition]
        if orders:
            spec["order"] = exp.Order(
                expressions=[
                    exp.Ordered(
                        this=column(str(o["column"])), desc=bool(o.get("desc")), nulls_first=False
                    )  # H-34
                    for o in orders
                ]
            )
        if uses_frame:
            spec["spec"] = exp.WindowSpec(**frame)
        return exp.Window(**spec)

    projection: list[exp.Expr] = []
    remaining = dict(computations)
    for name, dku_type in columns_with_types(project, target.ref, recipe):
        if name in remaining:
            fn, uses_frame = remaining.pop(name)
            cast = exp.Cast(this=over(fn, uses_frame), to=spark_type(dku_type, recipe.name, name))
            projection.append(exp.alias_(cast, name, quoted=True))
        elif name in available:
            projection.append(column(name))
        else:
            raise UnsupportedConstruct(
                recipe.name, "output schema", f"column '{name}' matches no input nor computation"
            )
    if remaining:
        raise UnsupportedConstruct(
            recipe.name,
            "output schema",
            f"computations absent from the output: {sorted(remaining)}",
        )
    produced = [str(p.alias_or_name) for p in projection]
    query = exp.select(*projection).from_(table(source.ref))
    query = where_clause(query, recipe, payload.get("preFilter"), "preFilter", available)
    final = post_filter(query, recipe, payload.get("postFilter"), produced)
    warnings = (
        [f"cadre de fenêtre supposé pour les agrégats (H-40) : {frame_text}"]
        if any(uses for _, uses in computations.values())
        else []
    )
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(final), append=target.append_mode)
        ],
        warnings=[*partition_warnings(recipe), *warnings],
    )
