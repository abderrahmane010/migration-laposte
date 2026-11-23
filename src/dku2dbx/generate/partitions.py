"""Dataiku partitions -> date column, REPLACE WHERE and input predicates (phase 6, task 2).

ASSUMPTION (H-53):
- one time dimension of period DAY is mapped; it becomes a DATE column named like the
  dimension (added to the table when the Dataiku schema does not have it);
- the partition a run builds is the job parameter ``date`` (YYYY-MM-DD), or, when it
  is empty, today in the ``timezone`` job parameter minus ``date_offset`` days: a job
  scheduled at 05:00 in Paris that builds PREVIOUS_DAY gets yesterday *in Paris*, not
  in UTC (pieges_semantiques.md, "Dates et fuseau");
- a partitioned output is written with ``INSERT INTO … REPLACE WHERE <col> = <date>``
  (Delta rejects a row outside the partition: inconsistencies fail, never hide);
- an input dependency ``equals`` (same day) reads only that day of the input; any other
  dependency function (time range, latest available…) raises UnsupportedConstruct.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Dataset, Project, Recipe
from dku2dbx.translate.sql import DIALECT, column

PARTITION_DATE_SQL = (
    "COALESCE(TRY_CAST(NULLIF(:date, '') AS DATE), "
    "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), :timezone) AS DATE), "
    "CAST(:date_offset AS INT)))"
)
# Scenario partitionsSpec -> default of the date_offset job parameter.
PARTITION_SPECS = {"CURRENT_DAY": "0", "PREVIOUS_DAY": "1"}
MAPPED_DEPENDENCIES = frozenset({"equals"})


@dataclass(frozen=True)
class DayPartition:
    column: str
    in_schema: bool
    """True when the Dataiku schema already has the column (SQL-style partitioning)."""


def partition_date() -> exp.Expr:
    return sqlglot.parse_one(PARTITION_DATE_SQL, read=DIALECT)


def day_partition(dataset: Dataset, recipe: str) -> DayPartition | None:
    """The day partition of a dataset, None if it is not partitioned."""
    dims = dataset.partitioning
    if not dims:
        return None
    if len(dims) != 1:
        raise UnsupportedConstruct(
            recipe, "partitioning", f"{len(dims)} dimensions on {dataset.name}"
        )
    dim = dims[0]
    period = dim.params.get("period")
    if dim.type != "time" or period != "DAY":
        raise UnsupportedConstruct(
            recipe, "partitioning", f"{dim.type} dimension ({period}) on {dataset.name}"
        )
    return DayPartition(dim.name, dim.name in {c.name for c in dataset.columns})


def filter_partitioned_inputs(query: exp.Query, recipe: Recipe, project: Project) -> exp.Query:
    """Read only the partition being built of the inputs with a same-day dependency."""
    filters: dict[str, exp.Expr] = {}
    for inp in recipe.inputs:
        functions = {str(d.get("func")) for d in inp.deps}
        if not functions:
            continue
        unknown = sorted(functions - MAPPED_DEPENDENCIES)
        if unknown:
            raise UnsupportedConstruct(
                recipe.name, "partition dependency", f"{unknown} on input {inp.ref}"
            )
        try:
            dataset = project.dataset(inp.ref)
        except KeyError:
            raise UnsupportedConstruct(
                recipe.name, "partition dependency", f"input {inp.ref} is not a local dataset"
            ) from None
        part = day_partition(dataset, recipe.name)
        if part is None:
            raise UnsupportedConstruct(
                recipe.name, "partition dependency", f"input {inp.ref} is not partitioned"
            )
        filters[inp.ref] = exp.EQ(this=column(part.column), expression=partition_date())
    if not filters:
        return query

    def wrap(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Table) and node.name in filters and not node.args.get("db"):
            inner = (
                exp.select(exp.Star())
                .from_(exp.Table(this=node.this.copy()))
                .where(filters[node.name].copy())
            )
            alias = node.alias or node.name
            return exp.Subquery(
                this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
            )
        return node

    result = query.transform(wrap)
    if not isinstance(result, exp.Query):  # transform keeps the root type
        raise TypeError("unexpected transform result")
    return result
