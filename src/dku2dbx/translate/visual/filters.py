"""Dataiku filters (pre-filter, post-filter, Sample/Filter, split) -> SQL conditions.

ASSUMPTION (H-09h, H-35): a filter is {enabled, distinct, uiData: {mode, conditions[]}};
a condition tests the column ``input`` with ``operator`` against ``string`` or ``num``.
Only the vocabulary observed on a real project is mapped (mode "&&"; operators
"== [string]", ">  [number]" — two spaces, verbatim — and "not empty"). Anything else
raises UnsupportedConstruct: an operator is never guessed.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Recipe
from dku2dbx.translate.sql import column

_Condition = Callable[[exp.Column, dict[str, Any]], exp.Expr]


def _not_empty(col: exp.Column, _: dict[str, Any]) -> exp.Expr:
    # ASSUMPTION (H-35): "not empty" = neither NULL nor the empty string (Dataiku treats
    # '' as empty, see pieges_semantiques.md "Chaîne vide et NULL").
    as_text = exp.Cast(this=col.copy(), to=exp.DataType.build("STRING"))
    return exp.and_(
        exp.Not(this=exp.Is(this=col, expression=exp.Null())),
        exp.NEQ(this=as_text, expression=exp.Literal.string("")),
    )


OPERATORS: dict[str, _Condition] = {
    "== [string]": lambda c, cond: exp.EQ(
        this=c, expression=exp.Literal.string(str(cond.get("string", "")))
    ),
    ">  [number]": lambda c, cond: exp.GT(this=c, expression=exp.Literal.number(cond["num"])),
    "not empty": _not_empty,
}
MODES = {"&&": exp.and_}


def condition(recipe: Recipe, flt: Any, where: str, columns: Collection[str]) -> exp.Expr | None:
    """The SQL condition of a filter, or None when it is absent or disabled."""
    if flt is None:
        return None
    if not isinstance(flt, dict):
        raise UnsupportedConstruct(recipe.name, where, "filter is not a JSON object")
    if not flt.get("enabled"):
        return None
    if flt.get("distinct"):
        raise UnsupportedConstruct(recipe.name, f"{where}.distinct", "option not mapped yet")
    ui = flt.get("uiData") or {}
    mode = ui.get("mode")
    if mode not in MODES:
        detail = (
            "formula filter (formula language: phase 4)"
            if mode == "CUSTOM"
            else (f"'{mode}' is not mapped")
        )
        raise UnsupportedConstruct(recipe.name, f"{where}.mode", detail)
    parts = []
    for cond in ui.get("conditions") or []:
        operator = str(cond.get("operator"))
        if operator not in OPERATORS:
            raise UnsupportedConstruct(
                recipe.name, f"{where}.operator", f"'{operator}' is not mapped"
            )
        name = str(cond.get("input", ""))
        if name not in columns:
            raise UnsupportedConstruct(
                recipe.name, f"{where}.input", f"column '{name}' is not an input column"
            )
        parts.append(OPERATORS[operator](column(name), cond))
    if not parts:
        raise UnsupportedConstruct(recipe.name, where, "enabled filter without condition")
    return MODES[mode](*parts)


def where_clause(
    query: exp.Select, recipe: Recipe, flt: Any, where: str, columns: Collection[str]
) -> exp.Select:
    """Apply a filter on the rows read by ``query`` (a pre-filter)."""
    cond = condition(recipe, flt, where, columns)
    return query if cond is None else query.where(cond)


def post_filter(query: exp.Expr, recipe: Recipe, flt: Any, output_columns: list[str]) -> exp.Expr:
    """Apply a post-filter on the rows ``query`` produces (wrapped in a subquery)."""
    cond = condition(recipe, flt, "postFilter", output_columns)
    if cond is None:
        return query
    sub = exp.Subquery(
        this=query.copy(), alias=exp.TableAlias(this=exp.to_identifier("filtered", quoted=True))
    )
    return exp.select(*[column(c) for c in output_columns]).from_(sub).where(cond)
