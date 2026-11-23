"""Sync and Sample/Filter recipes (ROADMAP phase 3, first in order)."""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.sql import column, render, table
from dku2dbx.translate.visual.common import (
    partition_warnings,
    schema,
    single_input,
    single_output,
)
from dku2dbx.translate.visual.filters import where_clause

# ASSUMPTION (H-30): sync copies the input columns that the output schema keeps,
# in the output order; STRICT_SYNC (seen on a real project) is the only mode mapped.
SYNC_SCHEMA_MODES = frozenset({"STRICT_SYNC"})


def _projection(project: Project, recipe: Recipe, source: str, target: str) -> exp.Select:
    available = set(schema(project, source, recipe))
    wanted = schema(project, target, recipe)
    missing = [c for c in wanted if c not in available]
    if missing:
        raise UnsupportedConstruct(
            recipe.name, "schema", f"output columns absent from the input: {', '.join(missing)}"
        )
    return exp.select(*[column(c) for c in wanted]).from_(table(source))


def translate_sync(project: Project, recipe: Recipe) -> Translation:
    source, target = single_input(recipe), single_output(recipe)
    mode = recipe.params.get("schemaMode", "STRICT_SYNC")
    if mode not in SYNC_SCHEMA_MODES:
        raise UnsupportedConstruct(recipe.name, "schemaMode", f"'{mode}' is not mapped")
    query = _projection(project, recipe, source.ref, target.ref)
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(query), append=target.append_mode)
        ],
        warnings=partition_warnings(recipe),
    )


def translate_sampling(project: Project, recipe: Recipe) -> Translation:
    """Sample/Filter: the filter is the payload, the sampling is params.selection."""
    source, target = single_input(recipe), single_output(recipe)
    # ASSUMPTION (H-31): the payload is the filter; params.selection holds samplingMethod,
    # maxRecords, partitionSelectionMethod, ordering and its own filter.
    selection: dict[str, Any] = recipe.params.get("selection") or {}
    if (selection.get("ordering") or {}).get("enabled"):
        raise UnsupportedConstruct(recipe.name, "selection.ordering", "ordered sampling")
    if selection.get("partitionSelectionMethod", "ALL") != "ALL":
        raise UnsupportedConstruct(
            recipe.name, "partitionSelectionMethod", str(selection["partitionSelectionMethod"])
        )
    query = _projection(project, recipe, source.ref, target.ref)
    columns = schema(project, source.ref, recipe)
    query = where_clause(query, recipe, recipe.payload, "filter", columns)
    query = where_clause(query, recipe, selection.get("filter"), "selection.filter", columns)
    warnings = partition_warnings(recipe)
    method = selection.get("samplingMethod")
    if method == "HEAD_SEQUENTIAL":
        query = query.limit(int(selection.get("maxRecords", 0)))
        warnings.append(
            "échantillon « premières lignes » : sans ordre de lecture garanti, Spark ne "
            "renvoie pas forcément les mêmes lignes que Dataiku"
        )
    else:
        # Only HEAD_SEQUENTIAL is mapped; random sampling has no equivalent result.
        raise UnsupportedConstruct(recipe.name, "samplingMethod", f"'{method}' is not mapped")
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=target.ref, sql=render(query), append=target.append_mode)
        ],
        warnings=warnings,
    )
