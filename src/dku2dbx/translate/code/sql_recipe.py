"""Dataiku SQL query recipes -> Databricks SQL (ROADMAP phase 8, task 1, SQL side).

ASSUMPTION (H-56): an ``sql_query`` recipe holds one query in its text payload. Dataiku
substitutes the ``${variables}`` textually, runs the query on the database of its
inputs and writes the result into its single output dataset. The physical tables the
query reads are those of the recipe inputs (``params.table`` / ``params.schema``).

The translation is deterministic and always flagged "to review" (decision of
2026-11-24): sqlglot transpiles from the dialect of the input connection, but engine
specific semantics (collations, implicit casts...) cannot be proven equivalent.
- a variable that is a whole string literal (``'${start_date}'``) becomes a value, or
  a job parameter in the bundle; a numeric or boolean variable used bare becomes a
  typed value; a variable glued to a name (``${env}_table``) is substituted by its
  value; any other use raises UnsupportedConstruct;
- each table is mapped to the input dataset whose physical table it is; an unknown
  table, a function sqlglot does not know, several statements or a non-SELECT raise
  UnsupportedConstruct (the recipe goes to the LLM residue);
- an integer division is reported: PostgreSQL and SQL Server truncate, Databricks
  does not (pieges_semantiques.md).
"""

from __future__ import annotations

import re
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Dataset, Project, Recipe
from dku2dbx.translate.model import TranslatedOutput, Translation
from dku2dbx.translate.prepare.formula import JobParameter
from dku2dbx.translate.sql import render, table

# ASSUMPTION (H-56): Dataiku dataset type of the inputs -> sqlglot dialect.
DIALECTS = {
    "PostgreSQL": "postgres",
    "Greenplum": "postgres",
    "Redshift": "redshift",
    "Oracle": "oracle",
    "SQLServer": "tsql",
    "Synapse": "tsql",
    "MySQL": "mysql",
    "Snowflake": "snowflake",
    "BigQuery": "bigquery",
    "Teradata": "teradata",
    "Databricks": "databricks",
}
# Dialects where integer / integer is an integer (truncated).
INTEGER_DIVISION = frozenset({"postgres", "redshift", "tsql", "teradata"})
_VARIABLE = re.compile(r"\$\{([^}]*)\}")
_NAME_CHAR = re.compile(r"[A-Za-z0-9_]")
_SENTINEL = "__dku2dbx_var_{}__"


def _unsupported(recipe: Recipe, element: str, detail: str) -> UnsupportedConstruct:
    return UnsupportedConstruct(recipe.name, element, detail)


def _dialect(project: Project, recipe: Recipe) -> str:
    types = set()
    for inp in recipe.inputs:
        try:
            types.add(project.dataset(inp.ref).type)
        except KeyError:
            raise _unsupported(
                recipe, "input", f"{inp.ref} is not a dataset of the project"
            ) from None
    if len(types) != 1:
        raise _unsupported(recipe, "connection", f"inputs of several types {sorted(types)}")
    kind = types.pop()
    if kind not in DIALECTS:
        raise _unsupported(recipe, "connection", f"dataset type {kind} has no mapped dialect")
    return DIALECTS[kind]


def _value(recipe: Recipe, name: str, value: Any) -> exp.Expr:
    """A variable as a typed SQL value (a job parameter in the bundle)."""
    if isinstance(value, JobParameter):
        marker = exp.Placeholder(this=value.name)
        types = {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE"}
        target = types.get(type(value.value))
        if isinstance(value.value, str):
            return marker
        if target is None:
            raise _unsupported(recipe, "variable", f"${{{name}}} is not a scalar")
        return exp.Cast(this=marker, to=exp.DataType.build(target))
    if isinstance(value, bool):
        return exp.true() if value else exp.false()
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    if isinstance(value, str):
        return exp.Literal.string(value)
    raise _unsupported(recipe, "variable", f"${{{name}}} is not a scalar")


def _plain(value: Any) -> str:
    return str(value.value if isinstance(value, JobParameter) else value)


def _substitute(
    project: Project, recipe: Recipe, text: str, warnings: list[str]
) -> tuple[str, dict[str, exp.Expr]]:
    """Text with sentinels in place of the variables, and the value of each sentinel."""
    values: dict[str, exp.Expr] = {}
    variables: dict[str, Any] = {**project.variables, "projectKey": project.key}
    parts: list[str] = []
    end = 0
    for match in _VARIABLE.finditer(text):
        name, start, stop = match.group(1), match.start(), match.end()
        if name not in variables:
            raise _unsupported(recipe, "variable", f"${{{name}}} cannot be resolved")
        value = variables[name]
        before, after = text[start - 1 : start], text[stop : stop + 1]
        if before == "'" and after == "'":  # '${x}': the whole literal
            sentinel = _SENTINEL.format(len(values))
            values[sentinel] = _value(recipe, name, value)
            parts += [text[end : start - 1], sentinel]
            end = stop + 1
            continue
        if _in_string(text, start):
            raise _unsupported(recipe, "variable", f"${{{name}}} inside a longer string")
        glued = bool(_NAME_CHAR.match(before) or _NAME_CHAR.match(after))
        if glued or name == "projectKey":  # part of a table or column name
            if name != "projectKey" and isinstance(value, JobParameter):
                warnings.append(f"variable ${{{name}}} figée à sa valeur dans un nom")
            parts += [text[end:start], _plain(value)]
        elif isinstance(value, (bool, int, float)) or (
            isinstance(value, JobParameter) and not isinstance(value.value, str)
        ):
            sentinel = _SENTINEL.format(len(values))
            values[sentinel] = _value(recipe, name, value)
            parts += [text[end:start], sentinel]
        else:
            raise _unsupported(recipe, "variable", f"${{{name}}} (text) used outside quotes")
        end = stop
    parts.append(text[end:])
    return "".join(parts), values


def _in_string(text: str, position: int) -> bool:
    """True when ``position`` is inside a single-quoted literal ('' escapes a quote)."""
    return text[:position].count("'") % 2 == 1


def _physical(dataset: Dataset) -> tuple[str, str | None] | None:
    if dataset.table_resolved is None or "${" in dataset.table_resolved:
        return None
    schema = dataset.db_schema_resolved
    return dataset.table_resolved.lower(), (schema.lower() if schema else None)


def _map_tables(query: exp.Expr, project: Project, recipe: Recipe) -> exp.Expr:
    """Physical tables of the inputs -> dataset names (phases 6-7 qualify them)."""
    by_table: dict[str, list[tuple[str | None, str]]] = {}
    for inp in recipe.inputs:
        physical = _physical(project.dataset(inp.ref))
        if physical is not None:
            by_table.setdefault(physical[0], []).append((physical[1], inp.ref))
    ctes = {cte.alias_or_name.lower() for cte in query.find_all(exp.CTE)}

    def swap(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.Table) or not node.name:
            return node
        name, db = node.name.lower(), (node.db or "").lower() or None
        if db is None and not node.catalog and name in ctes:
            return node
        matches = [ds for schema, ds in by_table.get(name, []) if db is None or db == schema]
        if node.catalog or len(matches) != 1:
            raise _unsupported(
                recipe, "table", f"{node.sql()} is not the table of exactly one input dataset"
            )
        alias = node.alias or None
        return table(matches[0], alias=alias)

    return query.transform(swap)


def _check_columns(project: Project, recipe: Recipe, query: exp.Query, output: str) -> list[str]:
    declared = [c.name for c in project.dataset(output).columns]
    selects = query.selects
    if any(isinstance(s, exp.Star) or (isinstance(s, exp.Column) and s.is_star) for s in selects):
        return ["SELECT * : colonnes de sortie non vérifiées par rapport au schéma Dataiku"]
    names = [s.alias_or_name for s in selects]
    if [n.lower() for n in names] != [d.lower() for d in declared]:
        raise _unsupported(recipe, "output schema", f"query columns {names} differ from {declared}")
    return []


def translate_sql_query(project: Project, recipe: Recipe) -> Translation:
    if len(recipe.outputs) != 1:
        raise _unsupported(recipe, "outputs", f"{len(recipe.outputs)} outputs (1 expected)")
    if not recipe.payload_raw or not recipe.payload_raw.strip():
        raise _unsupported(recipe, "payload", "empty SQL script")
    dialect = _dialect(project, recipe)
    warnings: list[str] = []
    text, values = _substitute(project, recipe, recipe.payload_raw, warnings)
    try:
        statements = [s for s in sqlglot.parse(text, read=dialect) if s is not None]
    except ParseError as exc:
        raise _unsupported(recipe, "sql", f"cannot be parsed as {dialect}: {exc}") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise _unsupported(recipe, "sql", "one SELECT statement expected")
    query = statements[0]
    unknown = sorted({f.name for f in query.find_all(exp.Anonymous)})
    if unknown:
        raise _unsupported(recipe, "function", f"{unknown} not known in {dialect}")

    def fill(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return values[node.name].copy()
        return node

    mapped = _map_tables(statements[0].transform(fill), project, recipe)
    if not isinstance(mapped, exp.Query):
        raise TypeError("unexpected transform result")
    query = mapped
    output = recipe.outputs[0]
    warnings += _check_columns(project, recipe, query, output.ref)
    if dialect in INTEGER_DIVISION and any(query.find_all(exp.Div)):
        warnings.append(
            f"division : en {dialect}, entier / entier tronque ; en Databricks, non "
            "(vérifier les types des opérandes)"
        )
    if dialect != "databricks":
        warnings.insert(0, f"SQL transpilé depuis {dialect} par sqlglot : à relire")
    return Translation(
        recipe=recipe.name,
        type=recipe.type,
        outputs=[
            TranslatedOutput(dataset=output.ref, sql=render(query), append=output.append_mode)
        ],
        warnings=warnings,
        review=True,
    )
