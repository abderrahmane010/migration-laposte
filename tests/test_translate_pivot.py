"""Phase 3: Pivot translator, explicit values (PySpark, dynamic pivot reported)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.model.ir import Project
from tests.fixtures import make_fixture as mf
from tests.test_translate_simple import ORDERS, _result
from tests.test_translate_sync import SqlSnapshot, fixture, read_files


def test_pivot_sql_snapshot(snapshot: SnapshotAssertion) -> None:
    tr = _result(fixture("DEMO_COMPLEXE"), "pivot_status")
    assert tr.outputs[0].sql + "\n" == snapshot.use_extension(SqlSnapshot)


def _payload(
    values: list[str], value_columns: list[dict[str, Any]] | None = None, **pivot: Any
) -> dict[str, Any]:
    return {
        "pivots": [
            {
                "keyColumns": ["status"],
                "valueLimit": "EXPLICIT",
                "explicitValues": values,
                "topnLimit": 20,
                "minOccLimit": 0,
                "globalCount": True,
                "valueColumns": value_columns or [],
                **pivot,
            }
        ],
        "identifierColumnsSelection": "EXPLICIT",
        "explicitIdentifiers": ["customer_id"],
        "otherColumns": [],
        "modalitySlugification": "NONE",
        "customAggregates": [],
        "computedColumns": [],
        "preFilter": mf.no_filter(),
    }


def _pivot(tmp_path: Path, payload: dict[str, Any], cols: str) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*cols.split(","))))
    p.add_recipe(
        mf.recipe(p.key, "r", "pivot", [mf.ref("orders_raw")], [mf.out_ref("out")]), payload
    )
    return read_files(tmp_path, p.files())


ROWS = [
    (1, 1, None, 10.0, "A"),
    (2, 1, None, 30.0, "A"),
    (3, 1, None, 5.0, "B"),
    (4, 2, None, 7.0, "B"),
    (5, 2, None, 1.0, None),
]


@pytest.mark.spark
def test_pivot_counts_and_aggregates_on_spark(tmp_path: Path, spark: Any) -> None:
    payload = _payload(["A", "B"], [mf.agg("amount", "sum", "max")])
    cols = (
        "customer_id:bigint,A_count:bigint,A_amount_sum:double,A_amount_max:double,"
        "B_count:bigint,B_amount_sum:double,B_amount_max:double"
    )
    tr = _result(_pivot(tmp_path, payload, cols), "r")
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    got = sorted(tuple(r) for r in spark.sql(tr.outputs[0].sql).collect())
    # a value absent from a group gives 0 for the count and NULL for the aggregates
    assert got == [(1, 2, 40.0, 30.0, 1, 5.0, 5.0), (2, 0, None, None, 1, 7.0, 7.0)]


def test_dynamic_pivot_is_reported(tmp_path: Path) -> None:
    payload = _payload([], valueLimit="TOP_N")
    item = _result(_pivot(tmp_path, payload, "customer_id:bigint"), "r")
    assert item.element == "pivots[].valueLimit"
    assert "dynamic pivot values" in item.detail


@pytest.mark.parametrize(
    ("payload", "cols", "element"),
    [
        (_payload(["A", "A"]), "customer_id:bigint,A_count:bigint", "pivots[].explicitValues"),
        (
            _payload(["A"], keyColumns=["status", "customer_id"]),
            "customer_id:bigint",
            "pivots[].keyColumns",
        ),
        (_payload(["A"], keyColumns=["order_id"]), "customer_id:bigint", "pivots[].keyColumns"),
        (
            _payload(["A"], [{**mf.agg("amount"), "stddev": True}]),
            "customer_id:bigint,A_count:bigint",
            "valueColumns[].stddev",
        ),
        (
            {**_payload(["A"]), "modalitySlugification": "SOFT"},
            "customer_id:bigint",
            "modalitySlugification",
        ),
        (
            {**_payload(["A"]), "otherColumns": [{"column": "x"}]},
            "customer_id:bigint",
            "otherColumns",
        ),
        (_payload(["A"]), "customer_id:bigint", "output schema"),
        (_payload(["A"]), "customer_id:bigint,A_count:bigint,Z_count:bigint", "output schema"),
    ],
)
def test_pivot_never_guesses(
    tmp_path: Path, payload: dict[str, Any], cols: str, element: str
) -> None:
    assert _result(_pivot(tmp_path, payload, cols), "r").element == element
