"""Top N recipe (ROADMAP phase 3).

ASSUMPTION (H-42), format observed on a real project: ``firstRows``/``lastRows``,
``orders[].{column, desc}``, ``keys[]`` (groups, ``{column}``; empty observed),
``retrievedColumnsSelectionMode`` (EXPLICIT observed) with ``retrievedColumns``. The N
first rows in the sort order (and/or the M last ones) of each group are kept.

Ties make the selection non-deterministic, in Dataiku as in Spark
(pieges_semantiques.md, "Top N à égalité"): the other input columns are appended to
the order as a tie-breaker, and a warning says so.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    partition_warnings,
    payload_dict,
    require_empty,
    require_false,
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import where_clause

FIRST, LAST = "__dku2dbx_first", "__dku2dbx_last"


def _ordered(name: str, desc: bool, reverse: bool) -> exp.Ordered:
    # H-34: NULLs last; the reverse order is the exact mirror (NULLs first).
    return exp.Ordered(this=column(name), desc=desc != reverse, nulls_first=reverse)


def _keys(recipe: Recipe, payload: dict[str, Any], available: list[str]) -> list[str]:
    keys = []
    for key in payload.get("keys") or []:
        if not isinstance(key, dict) or str(key.get("column")) not in available:
            raise UnsupportedConstruct(recipe.name, "keys", f"group key {key!r} not mapped")
        keys.append(str(key["column"]))
    return keys


def translate_topn(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    source, target = single_input(recipe), single_output(recipe)
    require_empty(recipe, payload, "computedColumns", "outputColumnNameOverrides")
    require_false(recipe, payload, "rank", "rowNumber", "denseRank", "duplicateCount")
    available = schema(project, source.ref, recipe)
    if FIRST in available or LAST in available:
        raise UnsupportedConstruct(recipe.name, "schema", "reserved column name in the input")
    first, last = int(payload.get("firstRows", 0)), int(payload.get("lastRows", 0))
    if first < 0 or last < 0 or first + last == 0:
        raise UnsupportedConstruct(recipe.name, "firstRows/lastRows", f"{first}/{last}")
    orders = [(str(o.get("column")), bool(o.get("desc"))) for o in payload.get("orders") or []]
    if not orders or any(name not in available for name, _ in orders):
        raise UnsupportedConstruct(recipe.name, "orders", "missing or unknown sort column")
    keys = _keys(recipe, payload, available)

    declared = schema(project, target.ref, recipe)
    if payload.get("retrievedColumnsSelectionMode") == "EXPLICIT":
        retrieved = [str(c) for c in payload.get("retrievedColumns") or []]
        if retrieved != declared:
            raise UnsupportedConstruct(
                recipe.name, "retrievedColumns", f"{retrieved} differs from the output {declared}"
            )
    elif payload.get("retrievedColumnsSelectionMode") not in (None, "ALL"):
        raise UnsupportedConstruct(
            recipe.name,
            "retrievedColumnsSelectionMode",
            f"'{payload.get('retrievedColumnsSelectionMode')}' is not mapped",
        )
    if not set(declared) <= set(available):
        raise UnsupportedConstruct(recipe.name, "output schema", "columns absent from the input")

    sort_names = {name for name, _ in orders}
    tie_break = [c for c in available if c not in sort_names and c not in keys]
    full = [*orders, *[(c, False) for c in tie_break]]

    def numbering(reverse: bool, alias: str) -> exp.Expr:
        spec: dict[str, Any] = {
            "this": exp.RowNumber(),
            "order": exp.Order(expressions=[_ordered(n, d, reverse) for n, d in full]),
        }
        if keys:
            spec["partition_by"] = [column(k) for k in keys]
        return exp.alias_(exp.Window(**spec), alias, quoted=True)

    inner = exp.select(*[column(c) for c in available]).from_(table(source.ref))
    conditions: list[exp.Expr] = []
    if first:
        inner = inner.select(numbering(False, FIRST))
        conditions.append(exp.LTE(this=column(FIRST), expression=exp.Literal.number(first)))
    if last:
        inner = inner.select(numbering(True, LAST))
        conditions.append(exp.LTE(this=column(LAST), expression=exp.Literal.number(last)))
    inner = where_clause(inner, recipe, payload.get("preFilter"), "preFilter", available)
    sub = exp.Subquery(
        this=inner, alias=exp.TableAlias(this=exp.to_identifier("ranked", quoted=True))
    )
    query = exp.select(*[column(c) for c in declared]).from_(sub).where(exp.or_(*conditions))
    warning = (
        "égalités départagées par les autres colonnes de l'entrée "
        f"({', '.join(tie_break) or 'aucune'}) : Dataiku ne garantit pas le même choix"
    )
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(query), append=target.append_mode)
        ],
        warnings=[*partition_warnings(recipe), warning],
    )
