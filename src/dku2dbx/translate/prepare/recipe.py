"""Prepare (shaker) recipe -> one CTE per step (ROADMAP phase 4).

Steps run in order; each one reads the previous CTE and the columns it left. A
disabled step (or a step of a disabled group) is skipped and listed in the warnings.
Groups (``metaType: GROUP``, H-08) are handled explicitly: their steps run in place.
Any other metaType, or a processor absent from the registry, raises
UnsupportedConstruct, with the reference-data processors pointing to MANUAL.
"""

from __future__ import annotations

from collections.abc import Iterator

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import PrepareStep, Project, Recipe
from dku2dbx.reader.known_types import GROUP_META_TYPE, PROCESSOR_META_TYPE
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.prepare.processors import MANUAL_PROCESSORS, PROCESSORS, StepContext
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    columns_with_types,
    partition_warnings,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.types import spark_type


def _flatten(
    recipe: Recipe, steps: list[PrepareStep] | None, disabled: bool = False
) -> Iterator[tuple[PrepareStep, bool]]:
    for step in steps or []:
        off = disabled or step.disabled
        if step.meta_type == GROUP_META_TYPE:
            yield from _flatten(recipe, step.steps, off)
        elif step.meta_type == PROCESSOR_META_TYPE:
            yield step, off
        else:
            raise UnsupportedConstruct(
                recipe.name, "metaType", f"Prepare step with metaType '{step.meta_type}'"
            )


def _cte(index: int) -> str:
    return f"step_{index}"


def translate_prepare(project: Project, recipe: Recipe) -> Translation:
    source, target = single_input(recipe), single_output(recipe)
    if recipe.prepare_steps is None:
        raise UnsupportedConstruct(recipe.name, "payload", "Prepare steps not readable")
    columns = columns_with_types(project, source.ref, recipe)
    ctes: list[tuple[str, exp.Select]] = []
    previous: exp.Expr = table(source.ref)
    warnings = partition_warnings(recipe)
    for step, off in _flatten(recipe, recipe.prepare_steps):
        ptype = step.type or ""
        if off:
            warnings.append(f"étape désactivée ignorée : {ptype}")
            continue
        translator = PROCESSORS.get(ptype)
        if translator is None:
            detail = (
                "reference-data processor: MANUAL (UDF or custom operator)"
                if ptype in MANUAL_PROCESSORS
                else "processor not mapped yet"
            )
            raise UnsupportedConstruct(recipe.name, ptype, detail)
        result = translator(
            StepContext(recipe.name, ptype, columns, dict(project.variables)), step.params
        )
        warnings += result.warnings
        query = exp.select(
            *[
                e if isinstance(e, exp.Column) and e.name == n else exp.alias_(e, n, quoted=True)
                for n, _, e in result.columns
            ]
        ).from_(previous)
        if result.where is not None:
            query = query.where(result.where)
        name = _cte(len(ctes))
        ctes.append((name, query))
        previous = table(name)
        columns = [(n, t) for n, t, _ in result.columns]

    declared = columns_with_types(project, target.ref, recipe)
    available = dict(columns)
    if {n for n, _ in declared} != set(available):
        raise UnsupportedConstruct(
            recipe.name,
            "output schema",
            f"'{target.ref}' declares {sorted(n for n, _ in declared)}, "
            f"the steps produce {sorted(available)}",
        )
    final = exp.select(
        *[
            column(n)
            if available[n] == t
            else exp.alias_(
                exp.Cast(this=column(n), to=spark_type(t, recipe.name, n)), n, quoted=True
            )
            for n, t in declared
        ]
    ).from_(previous)
    for name, query in ctes:
        final = final.with_(exp.to_identifier(name, quoted=True), as_=query)
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(final), append=target.append_mode)
        ],
        warnings=warnings,
    )
