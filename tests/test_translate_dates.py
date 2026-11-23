"""Phase 4: date patterns (SimpleDateFormat -> Spark) and the DateParser processor."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.translate.prepare.dates import spark_pattern
from tests.fixtures import make_fixture as mf
from tests.test_translate_prepare import _prepare
from tests.test_translate_simple import _result

ALL = "order_id:bigint,customer_id:bigint,order_date:date,amount:double,status:string"


@pytest.mark.parametrize(
    ("dataiku", "spark"),
    [
        ("yyyy-MM-dd", "yyyy-MM-dd"),
        ("dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss"),
        ("yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        ("d/M/yy", "d/M/yy"),
    ],
)
def test_pattern_table(dataiku: str, spark: str) -> None:
    assert spark_pattern(dataiku, "r", "f") == spark


@pytest.mark.parametrize(
    ("pattern", "detail"),
    [
        ("YYYY-MM-dd", "'YYYY' in 'YYYY-MM-dd' is not mapped (meaning differs)"),  # week year
        ("yyyy-MM-dd u", "'u' in 'yyyy-MM-dd u' is not mapped (meaning differs)"),  # weekday
        ("dd MMM yyyy", "'MMM' in 'dd MMM yyyy' is not mapped (meaning differs)"),  # names
        ("HH:mm:ss.SS", "'SS' in 'HH:mm:ss.SS' is not mapped (meaning differs)"),
        ("yyyy-MM-ddTHH", "'T' in 'yyyy-MM-ddTHH' is not mapped (meaning differs)"),
        ("yyyy-MM-dd Z", "'Z' in 'yyyy-MM-dd Z' is not mapped (meaning differs)"),
        ("yyyy-MM-dd'T", "unclosed quote in 'yyyy-MM-dd'T'"),
        ("yyyy#MM", "character '#' in 'yyyy#MM'"),
    ],
)
def test_patterns_never_guess(pattern: str, detail: str) -> None:
    with pytest.raises(UnsupportedConstruct) as info:
        spark_pattern(pattern, "r", "f")
    assert info.value.detail == detail


def _parser(**params: Any) -> dict[str, Any]:
    return mf.step(
        "DateParser",
        {
            "columns": ["status"],
            "formats": ["yyyy-MM-dd HH:mm:ss"],
            "lang": "auto",
            "timezone_id": "Europe/Paris",
            "appliesTo": "SINGLE_COLUMN",
            **params,
        },
    )


@pytest.mark.spark
def test_date_parser_on_spark_with_paris_time(tmp_path: Path, spark: Any) -> None:
    step = _parser(formats=["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy"])
    tr = _result(_prepare(tmp_path, [step], ALL.replace("status:string", "status:date")), "r")
    assert any("Europe/Paris" in w for w in tr.warnings)
    spark.createDataFrame(
        [
            (1, "2026-01-15 10:00:00"),
            (2, "2026-07-15 10:00:00"),
            (3, "15/07/2026"),
            (4, "not a date"),
            (5, None),
        ],
        "order_id long, status string",
    ).selectExpr(
        "order_id",
        "1L AS customer_id",
        "CAST(NULL AS TIMESTAMP) AS order_date",
        "1.0D AS amount",
        "status",
    ).createOrReplaceTempView("orders_raw")
    query = (
        f"SELECT order_id, date_format(status, 'yyyy-MM-dd HH:mm') AS d FROM ({tr.outputs[0].sql})"
    )
    got = sorted(tuple(r) for r in spark.sql(query).collect())
    assert got == [
        (1, "2026-01-15 09:00"),  # winter: Paris = UTC+1
        (2, "2026-07-15 08:00"),  # summer: Paris = UTC+2
        (3, "2026-07-14 22:00"),  # second format, midnight in Paris
        (4, None),  # unparsable: NULL, the job does not fail
        (5, None),
    ]


@pytest.mark.parametrize(
    ("params", "element"),
    [
        ({"formats": []}, "DateParser.formats"),
        ({"formats": ["YYYY"]}, "DateParser.formats"),
        ({"outCol": "parsed"}, "DateParser.outCol"),
        ({"columns": ["amount"]}, "DateParser.columns"),
    ],
)
def test_date_parser_never_guesses(tmp_path: Path, params: dict[str, Any], element: str) -> None:
    item = _result(_prepare(tmp_path, [_parser(**params)], ALL), "r")
    assert item.element == element
