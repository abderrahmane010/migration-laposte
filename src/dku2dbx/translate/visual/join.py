"""Join recipe (ROADMAP phase 3).

ASSUMPTION (H-37), format observed on a real project:
- ``virtualInputs[i].index`` is the recipe input read as table ``i``; ``joins[]`` chain
  the tables in order, ``table1`` being a table already joined and ``table2`` the new one;
- a condition ``{column1: {table, name}, column2: {table, name}, type: "EQ"}``;
- ``selectedColumns[] {name, table, type}`` are the output columns, in order;
- join types INNER (observed), LEFT, RIGHT, FULL (supposed). Keys compare as in SQL:
  NULL never matches NULL (pieges_semantiques.md, "Jointure et NULL").
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    check_output,
    main_inputs,
    partition_warnings,
    payload_dict,
    require_empty,
    schema,
    single_output,
)
from dku2dbx.translate.visual.filters import condition, post_filter

JOIN_TYPES = {"INNER": "INNER", "LEFT": "LEFT", "RIGHT": "RIGHT", "FULL": "FULL"}
CONDITION_TYPES = frozenset({"EQ"})


def _alias(index: int) -> str:
    return f"t{index}"


def _source(project: Project, recipe: Recipe, ref: str, vin: dict[str, Any], i: int) -> exp.Expr:
    """The table read as t<i>: the dataset itself, or a filtered subquery."""
    where = f"virtualInputs[{i}]"
    require_empty(recipe, vin, "computedColumns")
    if vin.get("autoSelectColumns"):
        raise UnsupportedConstruct(recipe.name, f"{where}.autoSelectColumns", "not mapped yet")
    mode = vin.get("outputColumnsSelectionMode", "MANUAL")
    if mode != "MANUAL":
        raise UnsupportedConstruct(
            recipe.name, f"{where}.outputColumnsSelectionMode", f"'{mode}' is not mapped"
        )
    cond = condition(
        recipe, vin.get("preFilter"), f"{where}.preFilter", schema(project, ref, recipe)
    )
    if cond is None:
        return table(ref, alias=_alias(i))
    inner = exp.select(exp.Star()).from_(table(ref)).where(cond)
    return exp.Subquery(
        this=inner, alias=exp.TableAlias(this=exp.to_identifier(_alias(i), quoted=True))
    )


def _on(recipe: Recipe, join: dict[str, Any], columns: dict[int, set[str]], n: int) -> exp.Expr:
    parts = []
    for cond in join.get("on") or []:
        kind = cond.get("type")
        if kind not in CONDITION_TYPES:
            raise UnsupportedConstruct(recipe.name, "joins[].on[].type", f"'{kind}' is not mapped")
        for option in ("caseInsensitive", "normalizeText", "strict"):
            if cond.get(option):
                raise UnsupportedConstruct(recipe.name, f"joins[].on[].{option}", "not mapped yet")
        sides = []
        for key in ("column1", "column2"):
            ref = cond.get(key) or {}
            t, name = int(ref.get("table", -1)), str(ref.get("name", ""))
            if t not in columns or t > n or name not in columns[t]:
                raise UnsupportedConstruct(
                    recipe.name, f"joins[].on[].{key}", f"column '{name}' of table {t} unknown"
                )
            sides.append(column(name, table=_alias(t)))
        parts.append(exp.EQ(this=sides[0], expression=sides[1]))
    if not parts:
        raise UnsupportedConstruct(recipe.name, "joins[].on", "join without condition")
    if join.get("conditionsMode", "AND") != "AND":
        raise UnsupportedConstruct(
            recipe.name, "joins[].conditionsMode", f"'{join.get('conditionsMode')}' is not mapped"
        )
    return exp.and_(*parts)


def translate_join(project: Project, recipe: Recipe) -> Translation:
    payload = payload_dict(recipe)
    target = single_output(recipe)
    inputs = main_inputs(recipe)
    require_empty(recipe, payload, "computedColumns", "enableAutoCastInJoinConditions")
    virtual = payload.get("virtualInputs") or []
    if [int(v.get("index", -1)) for v in virtual] != list(range(len(inputs))) or len(
        recipe.inputs
    ) != len(inputs):
        raise UnsupportedConstruct(recipe.name, "virtualInputs", "one entry per input, in order")
    columns = {i: set(schema(project, inp.ref, recipe)) for i, inp in enumerate(inputs)}

    query = exp.select().from_(_source(project, recipe, inputs[0].ref, virtual[0], 0))
    joined = {0}
    for join in payload.get("joins") or []:
        left, right = int(join.get("table1", -1)), int(join.get("table2", -1))
        if left not in joined or right in joined or right not in columns:
            raise UnsupportedConstruct(
                recipe.name, "joins[]", f"tables {left} -> {right} do not chain in order"
            )
        kind = JOIN_TYPES.get(str(join.get("type")))
        if kind is None:
            raise UnsupportedConstruct(
                recipe.name, "joins[].type", f"'{join.get('type')}' is not mapped"
            )
        if not join.get("outerJoinOnTheLeft", True):
            raise UnsupportedConstruct(recipe.name, "joins[].outerJoinOnTheLeft", "not mapped yet")
        if any((join.get("rightLimit") or {}).values()):
            raise UnsupportedConstruct(recipe.name, "joins[].rightLimit", "not mapped yet")
        query = query.join(
            _source(project, recipe, inputs[right].ref, virtual[right], right),
            on=_on(recipe, join, columns, max(joined | {right})),
            join_type=kind,
        )
        joined.add(right)
    if joined != set(columns):
        raise UnsupportedConstruct(recipe.name, "joins", "an input is never joined")

    selected = payload.get("selectedColumns")
    if not isinstance(selected, list) or not selected:
        raise UnsupportedConstruct(recipe.name, "selectedColumns", "explicit list expected")
    produced = []
    for sel in selected:
        t, name = int(sel.get("table", -1)), str(sel.get("name", ""))
        if t not in columns or name not in columns[t]:
            raise UnsupportedConstruct(
                recipe.name, "selectedColumns", f"column '{name}' of table {t} unknown"
            )
        query = query.select(exp.alias_(column(name, table=_alias(t)), name, quoted=True))
        produced.append(name)
    if len(set(produced)) != len(produced):
        raise UnsupportedConstruct(recipe.name, "selectedColumns", "duplicate output names")
    declared = schema(project, target.ref, recipe)
    check_output(recipe, produced, declared, target.ref)
    final = post_filter(query, recipe, payload.get("postFilter"), produced)
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(final), append=target.append_mode)
        ],
        warnings=partition_warnings(recipe),
    )
