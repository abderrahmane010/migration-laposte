"""Prepare processors -> SQL (ROADMAP phase 4, task 1), in the order of the catalogue.

A processor receives the columns available before it (name -> Dataiku type) and
returns the columns after it, each with its SQL expression, plus an optional row
condition. Parameters follow H-47 (fixtures, to confirm on the real format); an
option or mode not listed here raises UnsupportedConstruct.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.translate.prepare.dates import spark_pattern
from dku2dbx.translate.prepare.formula import (
    BOOLEAN,
    NUMERIC_DKU_TYPES,
    Context,
    translate_formula,
)
from dku2dbx.translate.sql import column
from dku2dbx.translate.visual.types import spark_type

# ASSUMPTION (H-27): Prepare processors that need a reference dataset or a non-SQL
# computation (phase 4, task 4), by DSS type id: MANUAL, with a proposed target (a
# Unity Catalog UDF or a Lakeflow Designer custom operator). To complete.
MANUAL_PROCESSORS = frozenset({"ZipCodeGeocoder", "GeoIPProcessor", "UserAgentClassifier"})

KIND_TO_DKU = {"string": "string", "number": "double", "boolean": "boolean", "date": "date"}


@dataclass
class StepResult:
    columns: list[tuple[str, str | None, exp.Expr]]
    """Columns after the step, in order: (name, Dataiku type, expression)."""
    where: exp.Expr | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StepContext:
    recipe: str
    processor: str
    columns: list[tuple[str, str | None]]
    variables: dict[str, Any]

    def fail(self, element: str, detail: str) -> UnsupportedConstruct:
        return UnsupportedConstruct(self.recipe, f"{self.processor}.{element}", detail)

    def passthrough(self) -> list[tuple[str, str | None, exp.Expr]]:
        return [(n, t, column(n)) for n, t in self.columns]

    def formula_context(self) -> Context:
        return Context(self.recipe, dict(self.columns), self.variables)

    def names(self) -> list[str]:
        return [n for n, _ in self.columns]


def _target_columns(ctx: StepContext, params: dict[str, Any]) -> list[str]:
    # ASSUMPTION (H-47): appliesTo SINGLE_COLUMN or COLUMNS with a "columns" list.
    applies = params.get("appliesTo", "SINGLE_COLUMN")
    if applies not in ("SINGLE_COLUMN", "COLUMNS"):
        raise ctx.fail("appliesTo", f"'{applies}' is not mapped")
    names = [str(c) for c in params.get("columns") or []]
    if not names or (applies == "SINGLE_COLUMN" and len(names) != 1):
        raise ctx.fail("columns", f"{names} for appliesTo {applies}")
    unknown = [c for c in names if c not in ctx.names()]
    if unknown:
        raise ctx.fail("columns", f"columns not available at this step: {unknown}")
    return names


def create_column_with_formula(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{column, expression}: add the column, or replace it in place."""
    name, formula = str(params.get("column", "")), params.get("expression")
    if not name or not isinstance(formula, str):
        raise ctx.fail("params", "column and expression expected")
    typed = translate_formula(formula, ctx.formula_context())
    created = (name, KIND_TO_DKU[typed.kind], typed.sql)
    columns = ctx.passthrough()
    if name in ctx.names():
        columns = [created if n == name else (n, t, e) for n, t, e in columns]
    else:
        columns.append(created)
    return StepResult(columns)


def fill_empty_with_value(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{columns, value}: '' and NULL become the value (Dataiku treats '' as empty)."""
    targets = set(_target_columns(ctx, params))
    value = str(params.get("value", ""))
    columns = []
    for name, dku_type, expr in ctx.passthrough():
        if name not in targets:
            columns.append((name, dku_type, expr))
            continue
        if dku_type == "string":
            filled = exp.Coalesce(
                this=exp.Nullif(this=expr, expression=exp.Literal.string("")),
                expressions=[exp.Literal.string(value)],
            )
        elif dku_type in NUMERIC_DKU_TYPES:
            try:
                float(value)
            except ValueError:
                raise ctx.fail("value", f"'{value}' is not a number for '{name}'") from None
            literal = exp.Cast(
                this=exp.Literal.number(value), to=spark_type(dku_type, ctx.recipe, name)
            )
            filled = exp.Coalesce(this=expr, expressions=[literal])
        else:
            raise ctx.fail("columns", f"'{name}' has type {dku_type!r}, not mapped")
        columns.append((name, dku_type, filled))
    return StepResult(columns)


def columns_selector(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{keep, columns}: keep only these columns, or remove them."""
    targets = set(_target_columns(ctx, {**params, "appliesTo": "COLUMNS"}))
    if params.get("appliesTo", "COLUMNS") != "COLUMNS":
        raise ctx.fail("appliesTo", f"'{params.get('appliesTo')}' is not mapped")
    keep = params.get("keep")
    if not isinstance(keep, bool):
        raise ctx.fail("keep", "boolean expected")
    columns = [c for c in ctx.passthrough() if (c[0] in targets) == keep]
    if not columns:
        raise ctx.fail("columns", "no column left")
    return StepResult(columns)


STRING_MODES: dict[str, Callable[[exp.Expr], exp.Expr]] = {
    "TO_UPPER": lambda e: exp.Upper(this=e),
    "TO_LOWER": lambda e: exp.Lower(this=e),
    "TRIM": lambda e: exp.Trim(this=e),
}


def string_transformer(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{columns, mode}: TO_UPPER, TO_LOWER, TRIM on text columns."""
    targets = set(_target_columns(ctx, params))
    mode = params.get("mode")
    if mode not in STRING_MODES:
        raise ctx.fail("mode", f"'{mode}' is not mapped")
    columns = []
    for name, dku_type, expr in ctx.passthrough():
        if name in targets and dku_type != "string":
            raise ctx.fail("columns", f"'{name}' is {dku_type}, not text")
        new = STRING_MODES[str(mode)](expr) if name in targets else expr
        columns.append((name, dku_type, new))
    return StepResult(columns)


def _row_action(ctx: StepContext, params: dict[str, Any], condition: exp.Expr) -> exp.Expr:
    # ASSUMPTION (H-47): KEEP_ROW keeps the rows where the condition is true (NULL is not
    # true); REMOVE_ROW removes them, so a NULL condition keeps the row.
    action = params.get("action")
    if action == "KEEP_ROW":
        return exp.Coalesce(this=exp.paren(condition), expressions=[exp.false()])
    if action == "REMOVE_ROW":
        return exp.Not(this=exp.Coalesce(this=exp.paren(condition), expressions=[exp.false()]))
    raise ctx.fail("action", f"'{action}' is not mapped")


def filter_on_formula(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{expression, action}: keep or remove the rows where the formula is true."""
    formula = params.get("expression")
    if not isinstance(formula, str):
        raise ctx.fail("expression", "formula expected")
    typed = translate_formula(formula, ctx.formula_context())
    if typed.kind != BOOLEAN:
        raise ctx.fail("expression", f"the formula is {typed.kind}, not boolean")
    return StepResult(ctx.passthrough(), where=_row_action(ctx, params, typed.sql))


def filter_on_value(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{columns, values, action}: exact full-string match only."""
    targets = _target_columns(ctx, params)
    if params.get("matchingMode", "FULL_STRING") != "FULL_STRING":
        raise ctx.fail("matchingMode", f"'{params.get('matchingMode')}' is not mapped")
    if params.get("normalizationMode", "EXACT") != "EXACT":
        raise ctx.fail("normalizationMode", f"'{params.get('normalizationMode')}' is not mapped")
    values = [str(v) for v in params.get("values") or []]
    if not values:
        raise ctx.fail("values", "no value")
    types = dict(ctx.columns)
    if any(types[c] != "string" for c in targets):
        raise ctx.fail("columns", "only text columns are compared")
    matches = exp.or_(
        *[
            exp.In(this=column(c), expressions=[exp.Literal.string(v) for v in values])
            for c in targets
        ]
    )
    return StepResult(ctx.passthrough(), where=_row_action(ctx, params, matches))


def column_renamer(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{renamings: [{from, to}]}."""
    renames = {str(r.get("from")): str(r.get("to")) for r in params.get("renamings") or []}
    unknown = [c for c in renames if c not in ctx.names()]
    if not renames or unknown:
        raise ctx.fail("renamings", f"empty or unknown columns {unknown}")
    columns = [(renames.get(n, n), t, e) for n, t, e in ctx.passthrough()]
    if len({n for n, _, _ in columns}) != len(columns):
        raise ctx.fail("renamings", "a renaming creates a duplicate column")
    return StepResult(columns)


def date_parser(ctx: StepContext, params: dict[str, Any]) -> StepResult:
    """{columns, formats, timezone_id, lang}: parse text into a Dataiku date (timestamp).

    ASSUMPTION (H-48): the column is parsed in place (an "outCol" is not mapped); the
    first format that matches wins; the text is a local time in ``timezone_id`` and is
    stored in UTC (the Spark session runs in UTC); an unparsable value gives NULL.
    """
    targets = set(_target_columns(ctx, params))
    if params.get("outCol"):
        raise ctx.fail("outCol", "parsing into another column is not mapped yet")
    lang = params.get("lang", "auto")
    formats = [str(f) for f in params.get("formats") or []]
    if not formats:
        raise ctx.fail("formats", "no format")
    patterns = [spark_pattern(f, ctx.recipe, f"{ctx.processor}.formats") for f in formats]
    if lang not in ("auto", "en", "english") and any("MMM" in f for f in formats):
        raise ctx.fail("lang", f"'{lang}' for month names")
    zone = str(params.get("timezone_id") or "UTC")
    columns = []
    for name, dku_type, expr in ctx.passthrough():
        if name not in targets:
            columns.append((name, dku_type, expr))
            continue
        if dku_type != "string":
            raise ctx.fail("columns", f"'{name}' is {dku_type}, not text")
        parsed = [
            exp.func("try_to_timestamp", expr.copy(), exp.Literal.string(p)) for p in patterns
        ]
        local = (
            parsed[0] if len(parsed) == 1 else exp.Coalesce(this=parsed[0], expressions=parsed[1:])
        )
        utc = exp.func("to_utc_timestamp", local, exp.Literal.string(zone))
        columns.append((name, "date", utc))
    return StepResult(
        columns,
        warnings=[f"{ctx.processor} : heure lue dans le fuseau {zone}, stockée en UTC"],
    )


# Processor type -> translator, by decreasing frequency in the phase 2 catalogue.
PROCESSORS: dict[str, Callable[[StepContext, dict[str, Any]], StepResult]] = {
    "CreateColumnWithGREL": create_column_with_formula,
    "FillEmptyWithValue": fill_empty_with_value,
    "ColumnsSelector": columns_selector,
    "StringTransformer": string_transformer,
    "FilterOnFormula": filter_on_formula,
    "FilterOnValue": filter_on_value,
    "ColumnRenamer": column_renamer,
    "DateParser": date_parser,
}
