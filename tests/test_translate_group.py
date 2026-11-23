"""Phase 3: Group translator (SQL snapshots, PySpark, refusals)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.model.ir import Project
from tests.fixtures import make_fixture as mf
from tests.test_translate_filters import cond, flt
from tests.test_translate_simple import ORDERS, _result
from tests.test_translate_sync import SqlSnapshot, fixture, read_files

CASES = [
    ("DEMO_SIMPLE", "group_by_customer"),
    ("DEMO_COMPLEXE", "group_daily_depot"),
    ("DEMO_COMPLEXE", "group_routes"),
]


@pytest.mark.parametrize(("key", "recipe"), CASES)
def test_group_sql_snapshot(key: str, recipe: str, snapshot: SnapshotAssertion) -> None:
    assert _result(fixture(key), recipe).outputs[0].sql + "\n" == snapshot.use_extension(
        SqlSnapshot
    )


def _group(tmp_path: Path, payload: dict[str, Any], out_cols: str) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*out_cols.split(","))))
    p.add_recipe(
        mf.recipe(p.key, "r", "grouping", [mf.ref("orders_raw")], [mf.out_ref("out")]), payload
    )
    return read_files(tmp_path, p.files())


ALL_AGGS = ",".join(
    [
        "status:string",
        "count:bigint",
        "amount_count:bigint",
        "amount_sum:double",
        "amount_avg:double",
        "amount_min:double",
        "amount_max:double",
    ]
)


@pytest.mark.spark
def test_group_aggregates_run_on_spark(tmp_path: Path, spark: Any) -> None:
    payload = mf.grouping_payload(
        ["status"], [mf.agg("amount", "count", "sum", "avg", "min", "max")]
    )
    tr = _result(_group(tmp_path, payload, ALL_AGGS), "r")
    spark.createDataFrame(
        [
            (1, 1, None, 2.0, "A"),
            (2, 1, None, 4.0, "A"),
            (3, 1, None, None, "A"),
            (4, 1, None, 10.0, "B"),
        ],
        ORDERS,
    ).createOrReplaceTempView("orders_raw")
    got = sorted(tuple(r) for r in spark.sql(tr.outputs[0].sql).collect())
    # COUNT(*) counts the NULL row, COUNT(amount) does not; AVG ignores NULL
    assert got == [("A", 3, 2, 6.0, 3.0, 2.0, 4.0), ("B", 1, 1, 10.0, 10.0, 10.0, 10.0)]


@pytest.mark.spark
def test_output_columns_are_matched_by_name_not_order(tmp_path: Path, spark: Any) -> None:
    payload = mf.grouping_payload(["status"], [mf.agg("amount", "sum", "avg")])
    payload["globalCount"] = False
    tr = _result(
        _group(tmp_path, payload, "status:string,amount_avg:double,amount_sum:double"), "r"
    )
    spark.createDataFrame(
        [(1, 1, None, 2.0, "A"), (2, 1, None, 4.0, "A")], ORDERS
    ).createOrReplaceTempView("orders_raw")
    assert [tuple(r) for r in spark.sql(tr.outputs[0].sql).collect()] == [("A", 3.0, 6.0)]


@pytest.mark.spark
def test_group_pre_filter_on_spark(tmp_path: Path, spark: Any) -> None:
    payload = mf.grouping_payload(["status"], [])
    payload["preFilter"] = flt(cond(">  [number]", "amount", num=3))
    tr = _result(_group(tmp_path, payload, "status:string,count:bigint"), "r")
    spark.createDataFrame(
        [(1, 1, None, 2.0, "A"), (2, 1, None, 4.0, "A")], ORDERS
    ).createOrReplaceTempView("orders_raw")
    assert [tuple(r) for r in spark.sql(tr.outputs[0].sql).collect()] == [("A", 1)]


def _values(**flags: bool) -> list[dict[str, Any]]:
    return [{**mf.agg("amount"), **flags}]


@pytest.mark.parametrize(
    ("update", "cols", "element"),
    [
        ({"values": _values(stddev=True)}, "status:string,count:bigint", "values[].stddev"),
        ({"values": _values(first=True)}, "status:string,count:bigint", "values[].first"),
        (
            {"values": _values(countDistinct=True)},
            "status:string,count:bigint",
            "values[].countDistinct",
        ),
        ({"values": _values(sum=True)}, "status:string,count:bigint", "output schema"),
        ({}, "status:string,count:bigint,ghost:double", "output schema"),
        ({"keys": [{"column": "ghost"}]}, "ghost:string,count:bigint", "keys"),
        (
            {"outputColumnNameOverrides": {"count": "n"}},
            "status:string,count:bigint",
            "outputColumnNameOverrides",
        ),
        ({}, "status:string,count:geopoint", "column type"),
    ],
)
def test_group_never_guesses(
    tmp_path: Path, update: dict[str, Any], cols: str, element: str
) -> None:
    payload = {**mf.grouping_payload(["status"], []), **update}
    assert _result(_group(tmp_path, payload, cols), "r").element == element
