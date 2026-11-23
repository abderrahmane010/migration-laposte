"""SQL building blocks: identifiers, formatting and validation (sqlglot, databricks)."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

DIALECT = "databricks"


def column(name: str, table: str | None = None) -> exp.Column:
    """A quoted column reference (any character is safe in a name)."""
    return exp.column(name, table=table, quoted=True)


def table(name: str, alias: str | None = None) -> exp.Table:
    """A quoted table reference.

    Decision of 2026-11-23: the table is the Dataiku dataset name; phases 6-7
    replace it with catalog.schema.table.
    """
    ref = exp.Table(this=exp.to_identifier(name, quoted=True))
    if alias is not None:
        ref.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
    return ref


def render(query: exp.Expr) -> str:
    """Pretty Databricks SQL, checked by parsing it back."""
    sql = query.sql(dialect=DIALECT, pretty=True)
    sqlglot.parse_one(sql, read=DIALECT)  # raises if sqlglot cannot read its own output
    return sql
