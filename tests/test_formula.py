"""Phase 4: Dataiku formula language -> Spark SQL (grammar, functions, traps, refusals)."""

from __future__ import annotations

from typing import Any

import pytest
from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.translate.prepare.formula import (
    FUNCTIONS,
    Context,
    translate_formula,
)

COLUMNS = {"a": "double", "n": "bigint", "s": "string", "b": "boolean", "geo": "geopoint"}
CTX = Context("r", COLUMNS, {"rate": 2, "label": "L", "flag": True, "obj": {"x": 1}})


def sql(formula: str) -> str:
    return translate_formula(formula, CTX).sql.sql(dialect="databricks")


def evaluate(spark: Any, formula: str, rows: list[tuple[Any, ...]]) -> list[Any]:
    spark.createDataFrame(rows, "a double, n long, s string, b boolean").createOrReplaceTempView(
        "t"
    )
    expr = translate_formula(formula, CTX).sql
    query = exp.select(exp.alias_(expr, "v")).from_("t")
    return [r["v"] for r in spark.sql(query.sql(dialect="databricks")).collect()]


ROW = [(2.5, 7, "Hello", True)]


@pytest.mark.spark
@pytest.mark.parametrize(
    ("formula", "expected"),
    [
        ("a * 2 + n", 12.0),
        ("n % 4", 3),
        ("-a", -2.5),
        ("(n - 1) / 2", 3.0),
        ("toUppercase(s)", "HELLO"),
        ("s.toLowercase()", "hello"),
        ("length(s)", 5),
        ("substring(s, 1, 3)", "el"),
        ("substring(s, 2)", "llo"),
        ('startsWith(s, "He") && !endsWith(s, "x")', True),
        ('contains(s, "ell") || false', True),
        ('if(n > 5, "big", "small")', "big"),
        ('if(n > 50, "big")', None),
        ("abs(-a)", 2.5),
        ("floor(a)", 2),
        ("ceil(a)", 3),
        ("sqrt(n + 2)", 3.0),
        ("min(a, n, 1)", 1.0),
        ("max(a, n)", 7.0),
        ('val("s") == "Hello"', True),
        ('strval("n") + "!"', "7!"),
        ('numval("n") / 2', 3.5),
        ("n * ${rate}", 14),
        ("s + ${label}", "HelloL"),
        ("b == ${flag}", True),
        ('trim("  x ")', "x"),
        ("'single' + \"double\"", "singledouble"),
        ('"it\\"s"', 'it"s'),
    ],
)
def test_functions_on_spark(spark: Any, formula: str, expected: Any) -> None:
    assert evaluate(spark, formula, ROW) == [expected]


# --------------------------------------------------------------------------- #
# Semantic traps (pieges_semantiques.md)
# --------------------------------------------------------------------------- #
@pytest.mark.spark
@pytest.mark.parametrize(
    ("formula", "expected"),
    [
        ("round(-3.5)", -3),  # Dataiku documents -3 (half up); Spark ROUND gives -4
        ("round(2.5)", 3),
        ("round(-2.5)", -2),
        ("round(a + 0.004, 2)", 2.5),
        ("round(1.005 + 1.0, 2)", 2.01),
    ],
)
def test_round_half_up_including_negatives(spark: Any, formula: str, expected: Any) -> None:
    assert evaluate(spark, formula, ROW) == [pytest.approx(expected)]


@pytest.mark.spark
def test_plus_concatenates_with_a_string(spark: Any) -> None:
    assert evaluate(spark, "s + n", ROW) == ["Hello7"]
    assert evaluate(spark, "n + n", ROW) == [14]


@pytest.mark.spark
def test_blank_means_null_or_empty_string(spark: Any) -> None:
    rows = [(1.0, 1, None, True), (1.0, 1, "", True), (1.0, 1, "x", True)]
    assert evaluate(spark, "isBlank(s)", rows) == [True, True, False]
    assert evaluate(spark, "isNonBlank(s)", rows) == [False, False, True]
    assert evaluate(spark, 'coalesce(s, "default")', rows) == ["default", "default", "x"]


@pytest.mark.spark
def test_division_by_zero_gives_null_not_an_error(spark: Any) -> None:
    assert evaluate(spark, "a / 0", ROW) == [None]


# --------------------------------------------------------------------------- #
# Refusals: never guessed
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("formula", "detail"),
    [
        ("unknownFn(a)", "function 'unknownFn' is not mapped"),
        ("ghost + 1", "column 'ghost' is not available at this step"),
        ("geo", "column 'geo' has type 'geopoint', not mapped"),
        ("${missing}", "variable '${missing}' cannot be resolved"),
        ("${obj}", "variable '${obj}' is not a scalar"),
        ("a * s", "'*' between number and string"),
        ("a == s", "comparison between number and string"),
        ("a && b", "boolean operator on a non-boolean value"),
        ("if(n, 1, 2)", "if() expects a condition and 1 or 2 values"),
        ('if(b, 1, "x")', "if() branches have different types ['number', 'string']"),
        ("round(a, n)", "round() digits must be a literal"),
        ("toUppercase(n)", "string function() argument 1 must be string, got number"),
        ("val(s)", "val() expects a literal column name"),
        ("a +", "syntax not mapped: UnexpectedToken"),
        ("a = 1", "syntax not mapped: UnexpectedCharacters"),
    ],
)
def test_formulas_never_guess(formula: str, detail: str) -> None:
    with pytest.raises(UnsupportedConstruct) as info:
        translate_formula(formula, CTX)
    assert (info.value.construct, info.value.detail) == ("formula", detail)


def test_column_names_are_quoted() -> None:
    assert sql('val("s")') == "`s`"


def test_function_table_is_explicit() -> None:
    # Adding a function is a decision with its tests: the table is reviewed here.
    assert sorted(FUNCTIONS) == sorted(
        [
            "abs",
            "ceil",
            "coalesce",
            "contains",
            "endsWith",
            "floor",
            "if",
            "isBlank",
            "isNonBlank",
            "length",
            "max",
            "min",
            "round",
            "sqrt",
            "startsWith",
            "strip",
            "substring",
            "toLowercase",
            "toUppercase",
            "trim",
        ]
    )
