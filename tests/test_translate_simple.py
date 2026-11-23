"""Phase 3: Distinct, Stack and Sort translators (SQL snapshots, PySpark, refusals)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.model.ir import Project
from dku2dbx.translate.engine import translate_project
from tests.fixtures import make_fixture as mf
from tests.test_translate_sync import SqlSnapshot, fixture, read_files

CASES = [
    ("DEMO_COMPLEXE", "distinct_depots"),
    ("DEMO_COMPLEXE", "stack_failed"),
    ("DEMO_COMPLEXE", "sort_depot_trend"),
    ("DEMO_SIMPLE", "stack_orders"),
]


def _result(project: Project, name: str) -> Any:
    result = translate_project(project)
    for item in [*result.translated, *result.unsupported]:
        if item.recipe == name:
            return item
    raise AssertionError(name)


@pytest.mark.parametrize(("key", "recipe"), CASES)
def test_sql_snapshot(key: str, recipe: str, snapshot: SnapshotAssertion) -> None:
    tr = _result(fixture(key), recipe)
    assert tr.outputs[0].sql + "\n" == snapshot.use_extension(SqlSnapshot)


ORDERS = "order_id long, customer_id long, order_date timestamp, amount double, status string"


def _with(tmp_path: Path, rtype: str, payload: Any, out_cols: str, n_inputs: int = 1) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*out_cols.split(","))))
    inputs = [mf.ref("orders_raw"), mf.ref("customers")][:n_inputs]
    p.add_recipe(mf.recipe(p.key, "r", rtype, inputs, [mf.out_ref("out")]), payload)
    return read_files(tmp_path, p.files())


def _orders(spark: Any, rows: list[tuple[Any, ...]]) -> None:
    spark.createDataFrame(rows, ORDERS).createOrReplaceTempView("orders_raw")


def _distinct(keys: list[str], count: bool = False) -> dict[str, Any]:
    return {
        "keys": [{"column": k} for k in keys],
        "globalCount": count,
        "outputColumnNameOverrides": {},
        "preFilter": mf.no_filter(),
        "postFilter": mf.no_filter(),
    }


# --------------------------------------------------------------------------- #
# Distinct
# --------------------------------------------------------------------------- #
@pytest.mark.spark
def test_distinct_on_keys_with_count_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    project = _with(
        tmp_path, "distinct", _distinct(["status"], count=True), "status:string,count:bigint"
    )
    tr = _result(project, "r")
    _orders(spark, [(1, 1, None, 1.0, "NEW"), (2, 1, None, 2.0, "NEW"), (3, 2, None, 3.0, "DONE")])
    got = sorted(tuple(r) for r in spark.sql(tr.outputs[0].sql).collect())
    assert got == [("DONE", 1), ("NEW", 2)]


@pytest.mark.spark
def test_distinct_on_all_columns_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    cols = "order_id:bigint,customer_id:bigint,order_date:date,amount:double,status:string"
    tr = _result(_with(tmp_path, "distinct", _distinct([]), cols), "r")
    assert "SELECT DISTINCT" in tr.outputs[0].sql
    _orders(spark, [(1, 1, None, 1.0, "NEW"), (1, 1, None, 1.0, "NEW"), (2, 1, None, 1.0, "NEW")])
    assert len(spark.sql(tr.outputs[0].sql).collect()) == 2


@pytest.mark.parametrize(
    ("payload", "cols", "element"),
    [
        (_distinct(["status"]), "status:string,amount:double", "output schema"),
        (_distinct(["ghost"]), "ghost:string", "keys"),
        (_distinct(["status"], count=True), "status:string", "globalCount"),
        (
            {**_distinct(["status"]), "preFilter": {"enabled": True}},
            "status:string",
            "preFilter.mode",
        ),
        (
            {**_distinct(["status"]), "outputColumnNameOverrides": {"status": "s"}},
            "status:string",
            "outputColumnNameOverrides",
        ),
    ],
)
def test_distinct_never_guesses(tmp_path: Path, payload: Any, cols: str, element: str) -> None:
    assert _result(_with(tmp_path, "distinct", payload, cols), "r").element == element


# --------------------------------------------------------------------------- #
# Stack
# --------------------------------------------------------------------------- #
@pytest.mark.spark
def test_stack_fills_missing_columns_and_origin_on_spark(spark: Any) -> None:
    tr = _result(fixture("DEMO_SIMPLE"), "stack_orders")
    assert "CAST(NULL AS DOUBLE) AS `amount_eur`" in tr.outputs[0].sql
    spark.createDataFrame(
        [(1, 1, None, 2.0, "NEW", 2.0)], ORDERS + ", amount_eur double"
    ).createOrReplaceTempView("orders_prepared")
    spark.createDataFrame([(9, 2, None, 5.0, "OLD")], ORDERS).createOrReplaceTempView(
        "orders_archive"
    )
    got = sorted(
        (r["order_id"], r["amount_eur"], r["original_dataset"])
        for r in spark.sql(tr.outputs[0].sql).collect()
    )
    assert got == [(1, 2.0, "orders_prepared"), (9, None, "orders_archive")]


def _stack(**overrides: Any) -> dict[str, Any]:
    payload = mf.vstack_payload(2, ["a", "b"])
    payload["selectedColumns"] = ["customer_id"]
    payload["addOriginColumn"] = False
    return {**payload, **overrides}


@pytest.mark.parametrize(
    ("payload", "element"),
    [
        (_stack(mode="INTERSECT"), "mode"),
        (_stack(postFilter={"enabled": True}), "postFilter.mode"),
        (_stack(selectedColumns=[]), "selectedColumns"),
        (
            _stack(
                virtualInputs=[
                    {"index": 0, "preFilter": {"enabled": True}},
                    {"index": 1, "preFilter": mf.no_filter()},
                ]
            ),
            "virtualInputs[0].preFilter.mode",
        ),
    ],
)
def test_stack_never_guesses(tmp_path: Path, payload: Any, element: str) -> None:
    project = _with(tmp_path, "vstack", payload, "customer_id:bigint", n_inputs=2)
    assert _result(project, "r").element == element


def test_stack_unknown_type_for_a_missing_column(tmp_path: Path) -> None:
    payload = _stack(selectedColumns=["customer_id", "segment"])
    project = _with(tmp_path, "vstack", payload, "customer_id:bigint,segment:geopoint", 2)
    item = _result(project, "r")
    assert (item.element, item.detail) == (
        "column type",
        "type 'geopoint' of column 'segment' is not mapped",
    )


# --------------------------------------------------------------------------- #
# Sort
# --------------------------------------------------------------------------- #
@pytest.mark.spark
def test_sort_runs_on_spark_and_warns(tmp_path: Path, spark: Any) -> None:
    payload = {"orders": [{"column": "status", "desc": False}, {"column": "amount", "desc": True}]}
    tr = _result(_with(tmp_path, "sort", payload, "status:string,amount:double"), "r")
    assert any("Delta" in w for w in tr.warnings)
    rows = [(1, 1, None, 1.0, "B"), (2, 1, None, 3.0, "A"), (3, 1, None, 5.0, "B")]
    _orders(spark, [*rows, (4, 1, None, 2.0, None), (5, 1, None, None, "A")])
    got = [tuple(r) for r in spark.sql(tr.outputs[0].sql).collect()]
    # H-34: NULLs last in both directions
    assert got == [("A", 3.0), ("A", None), ("B", 5.0), ("B", 1.0), (None, 2.0)]


@pytest.mark.parametrize(
    ("payload", "element"),
    [
        ({"orders": [{"column": "status"}], "rowNumber": True}, "output schema"),
        ({"orders": [{"column": "status"}], "rowNumber": True, "rank": True}, "rowNumber+rank"),
        ({"orders": []}, "orders"),
        ({"orders": [{"column": "status"}], "preFilter": {"enabled": True}}, "preFilter.mode"),
    ],
)
def test_sort_never_guesses(tmp_path: Path, payload: Any, element: str) -> None:
    assert _result(_with(tmp_path, "sort", payload, "status:string"), "r").element == element


@pytest.mark.spark
def test_sort_row_number_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    payload = {"orders": [{"column": "amount", "desc": True}], "rowNumber": True}
    tr = _result(_with(tmp_path, "sort", payload, "amount:double,row_number:bigint"), "r")
    assert "ROW_NUMBER() OVER (ORDER BY `amount` DESC" in tr.outputs[0].sql
    _orders(spark, [(1, 1, None, 1.0, "A"), (2, 1, None, 3.0, "A"), (3, 1, None, 2.0, "A")])
    got = [tuple(r) for r in spark.sql(tr.outputs[0].sql).collect()]
    assert got == [(3.0, 1), (2.0, 2), (1.0, 3)]


@pytest.mark.spark
def test_stack_post_filter_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    flt = {
        "enabled": True,
        "distinct": False,
        "uiData": {
            "mode": "&&",
            "conditions": [{"input": "customer_id", "operator": ">  [number]", "num": 1}],
        },
    }
    project = _with(tmp_path, "vstack", _stack(postFilter=flt), "customer_id:bigint", n_inputs=2)
    tr = _result(project, "r")
    _orders(spark, [(1, 1, None, 1.0, "A"), (2, 2, None, 1.0, "A")])
    spark.createDataFrame(
        [(3, "x", "y")], "customer_id long, name string, segment string"
    ).createOrReplaceTempView("customers")
    assert sorted(r[0] for r in spark.sql(tr.outputs[0].sql).collect()) == [2, 3]
