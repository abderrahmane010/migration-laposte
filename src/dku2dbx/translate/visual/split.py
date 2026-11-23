"""Split recipe, VALUES mode (ROADMAP phase 3): one SELECT per output.

ASSUMPTION (H-41), format observed on a real project: ``mode: VALUES``, ``column``,
``valueSplits[].{value, caseSensitive, outputIndex}``, ``defaultOutputIndex``; an
output index is a position in the recipe outputs. A row goes to the output of its
value; the default output gets every other row, NULL included (a negative default
index drops them). ``caseSensitive: false`` compares in lower case. Only text
columns are compared: comparing a number with a text value depends on its format.
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
    main_outputs,
    partition_warnings,
    payload_dict,
    require_empty,
    schema,
    single_input,
)
from dku2dbx.translate.visual.filters import condition


def _matches(col: exp.Column, values: list[str], case_sensitive: bool) -> exp.Expr:
    if case_sensitive:
        return exp.In(this=col, expressions=[exp.Literal.string(v) for v in values])
    return exp.In(
        this=exp.Lower(this=col),
        expressions=[exp.Literal.string(v.lower()) for v in values],
    )


def _rules(
    recipe: Recipe, payload: dict[str, Any], n_outputs: int
) -> dict[int, list[tuple[str, bool]]]:
    """Output index -> [(value, case sensitive)]."""
    rules: dict[int, list[tuple[str, bool]]] = {}
    seen: set[str] = set()
    for rule in payload.get("valueSplits") or []:
        index = int(rule.get("outputIndex", -1))
        if not 0 <= index < n_outputs:
            raise UnsupportedConstruct(recipe.name, "valueSplits[].outputIndex", f"{index}")
        value = str(rule.get("value", ""))
        case = bool(rule.get("caseSensitive", True))
        key = value if case else value.lower()
        if key in seen:
            raise UnsupportedConstruct(recipe.name, "valueSplits", f"value '{value}' listed twice")
        seen.add(key)
        rules.setdefault(index, []).append((value, case))
    if not rules:
        raise UnsupportedConstruct(recipe.name, "valueSplits", "no value rule")
    return rules


def translate_split(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    source = single_input(recipe)
    outputs = main_outputs(recipe)
    if len(outputs) != len(recipe.outputs):
        raise UnsupportedConstruct(recipe.name, "outputs", "outputs outside the main role")
    if payload.get("mode") != "VALUES":
        raise UnsupportedConstruct(recipe.name, "mode", f"'{payload.get('mode')}' is not mapped")
    require_empty(recipe, payload, "computedColumns", "writeComputedColumnsInOutput")
    types = dict(columns_with_types(project, source.ref, recipe))
    name = str(payload.get("column", ""))
    if name not in types:
        raise UnsupportedConstruct(recipe.name, "column", f"unknown column '{name}'")
    if types[name] != "string":
        raise UnsupportedConstruct(
            recipe.name, "column", f"'{name}' is {types[name]}: only text columns are compared"
        )
    rules = _rules(recipe, payload, len(outputs))
    default = int(payload.get("defaultOutputIndex", -1))
    if default >= len(outputs):
        raise UnsupportedConstruct(recipe.name, "defaultOutputIndex", f"{default}")
    pre = condition(recipe, payload.get("preFilter"), "preFilter", list(types))

    col = column(name)
    all_rules = [r for index in sorted(rules) for r in rules[index]]
    translated = []
    for index, out in enumerate(outputs):
        declared = schema(project, out.ref, recipe)
        missing = [c for c in declared if c not in types]
        if missing:
            raise UnsupportedConstruct(
                recipe.name,
                "output schema",
                f"'{out.ref}': columns absent from the input {missing}",
            )
        parts = [
            _matches(col.copy(), [v for v, c in rules[index] if c is case], case)
            for case in (True, False)
            if index in rules and any(c is case for _, c in rules[index])
        ]
        if index == default:
            matched = [
                _matches(col.copy(), [v for v, c in all_rules if c is case], case)
                for case in (True, False)
                if any(c is case for _, c in all_rules)
            ]
            parts.append(
                exp.or_(
                    exp.Is(this=col.copy(), expression=exp.Null()),
                    exp.Not(this=exp.paren(exp.or_(*matched))),
                )
            )
        if not parts:
            raise UnsupportedConstruct(recipe.name, "outputs", f"'{out.ref}' receives no row")
        where = exp.or_(*parts)
        if pre is not None:
            where = exp.and_(exp.paren(pre.copy()), exp.paren(where))
        query = exp.select(*[column(c) for c in declared]).from_(table(source.ref)).where(where)
        translated.append(
            TranslatedOutput(dataset=out.ref, sql=render(query), append=out.append_mode)
        )
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=translated,
        warnings=partition_warnings(recipe),
    )
