"""Phase 3: Top N translator (first/last rows, groups, tie-breaker, PySpark, refusals)."""

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


def test_topn_sql_snapshot(snapshot: SnapshotAssertion) -> None:
    tr = _result(fixture("DEMO_COMPLEXE"), "topn_late_depots")
    assert tr.outputs[0].sql + "\n" == snapshot.use_extension(SqlSnapshot)
    assert any("départagées" in w for w in tr.warnings)


def _payload(first: int = 2, last: int = 0, **extra: Any) -> dict[str, Any]:
    return {
        "firstRows": first,
        "lastRows": last,
        "orders": [{"column": "amount", "desc": True}],
        "keys": [],
        "retrievedColumnsSelectionMode": "EXPLICIT",
        "retrievedColumns": ["order_id", "amount"],
        "rank": False,
        "rowNumber": False,
        "denseRank": False,
        "duplicateCount": False,
        "computedColumns": [],
        "outputColumnNameOverrides": {},
        "preFilter": mf.no_filter(),
        **extra,
    }


def _topn(
    tmp_path: Path, payload: dict[str, Any], cols: str = "order_id:bigint,amount:double"
) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*cols.split(","))))
    p.add_recipe(
        mf.recipe(p.key, "r", "topn", [mf.ref("orders_raw")], [mf.out_ref("out")]), payload
    )
    return read_files(tmp_path, p.files())


ROWS = [
    (1, 1, None, 10.0, "A"),
    (2, 1, None, 30.0, "A"),
    (3, 2, None, 20.0, "B"),
    (4, 2, None, 30.0, "B"),  # tie with order 2 on amount
    (5, 2, None, None, "B"),
]


def _ids(spark: Any, tr: Any) -> list[int]:
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    return sorted(r["order_id"] for r in spark.sql(tr.outputs[0].sql).collect())


@pytest.mark.spark
def test_first_rows_with_a_deterministic_tie_break(tmp_path: Path, spark: Any) -> None:
    tr = _result(_topn(tmp_path, _payload(first=1)), "r")
    # orders 2 and 4 tie on 30.0: the tie-break (order_id first) always keeps order 2
    assert _ids(spark, tr) == [2]
    assert _ids(spark, tr) == [2]


@pytest.mark.spark
def test_first_and_last_rows(tmp_path: Path, spark: Any) -> None:
    tr = _result(_topn(tmp_path, _payload(first=1, last=1)), "r")
    # NULL amount sorts last (H-34): it is the "last row"
    assert _ids(spark, tr) == [2, 5]


@pytest.mark.spark
def test_top_n_per_group(tmp_path: Path, spark: Any) -> None:
    payload = _payload(first=1, keys=[{"column": "customer_id"}])
    assert _ids(spark, _result(_topn(tmp_path, payload), "r")) == [2, 4]


@pytest.mark.spark
def test_pre_filter_before_ranking(tmp_path: Path, spark: Any) -> None:
    payload = _payload(first=1, preFilter=flt(cond("== [string]", "status", string="B")))
    assert _ids(spark, _result(_topn(tmp_path, payload), "r")) == [4]


@pytest.mark.parametrize(
    ("payload", "element"),
    [
        (_payload(first=0), "firstRows/lastRows"),
        (_payload(rank=True), "rank"),
        (_payload(duplicateCount=True), "duplicateCount"),
        (_payload(orders=[]), "orders"),
        (_payload(retrievedColumns=["amount", "order_id"]), "retrievedColumns"),
        (_payload(retrievedColumnsSelectionMode="MAGIC"), "retrievedColumnsSelectionMode"),
        (_payload(keys=["customer_id"]), "keys"),
    ],
)
def test_topn_never_guesses(tmp_path: Path, payload: dict[str, Any], element: str) -> None:
    assert _result(_topn(tmp_path, payload), "r").element == element
