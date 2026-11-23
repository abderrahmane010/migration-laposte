"""Phase 3: Window translator (frames, lag/lead, numbering, PySpark, refusals)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.model.ir import Project
from tests.fixtures import make_fixture as mf
from tests.test_translate_simple import ORDERS, _result
from tests.test_translate_sync import SqlSnapshot, fixture, read_files

CASES = [("DEMO_SIMPLE", "window_orders"), ("DEMO_COMPLEXE", "window_depot_trend")]


@pytest.mark.parametrize(("key", "recipe"), CASES)
def test_window_sql_snapshot(key: str, recipe: str, snapshot: SnapshotAssertion) -> None:
    assert _result(fixture(key), recipe).outputs[0].sql + "\n" == snapshot.use_extension(
        SqlSnapshot
    )


def _window(tmp_path: Path, payload: dict[str, Any], out_cols: str) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*out_cols.split(","))))
    p.add_recipe(
        mf.recipe(p.key, "r", "window", [mf.ref("orders_raw")], [mf.out_ref("out")]), payload
    )
    return read_files(tmp_path, p.files())


ROWS = [
    (1, 1, None, 1.0, "A"),
    (2, 1, None, 2.0, "A"),
    (3, 1, None, 3.0, "A"),
    (4, 2, None, 10.0, "B"),
]


def _run(spark: Any, sql: str) -> list[tuple[Any, ...]]:
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    return sorted(tuple(r) for r in spark.sql(sql).collect())


@pytest.mark.spark
def test_sum_over_the_whole_partition(tmp_path: Path, spark: Any) -> None:
    payload = mf.window_payload(
        ["customer_id"], [("order_id", False)], [mf.window_value("amount", "sum")]
    )
    tr = _result(_window(tmp_path, payload, "order_id:bigint,amount_sum:double"), "r")
    assert any("toute la partition" in w for w in tr.warnings)
    assert _run(spark, tr.outputs[0].sql) == [(1, 6.0), (2, 6.0), (3, 6.0), (4, 10.0)]


@pytest.mark.spark
def test_limited_frame_moving_sum(tmp_path: Path, spark: Any) -> None:
    payload = mf.window_payload(
        ["customer_id"], [("order_id", False)], [mf.window_value("amount", "sum")]
    )
    payload["windows"][0].update(
        {
            "enableLimits": True,
            "limitPreceding": True,
            "precedingRows": 1,
            "limitFollowing": True,
            "followingRows": 0,
        }
    )
    tr = _result(_window(tmp_path, payload, "order_id:bigint,amount_sum:double"), "r")
    assert "ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING" in tr.warnings[-1]
    assert _run(spark, tr.outputs[0].sql) == [(1, 1.0), (2, 3.0), (3, 5.0), (4, 10.0)]


@pytest.mark.spark
def test_unlimited_following_side(tmp_path: Path, spark: Any) -> None:
    payload = mf.window_payload(
        ["customer_id"], [("order_id", False)], [mf.window_value("amount", "sum")]
    )
    payload["windows"][0].update({"enableLimits": True, "limitPreceding": True, "precedingRows": 1})
    tr = _result(_window(tmp_path, payload, "order_id:bigint,amount_sum:double"), "r")
    # H-40: the side without limit is UNBOUNDED
    assert _run(spark, tr.outputs[0].sql) == [(1, 6.0), (2, 6.0), (3, 5.0), (4, 10.0)]


@pytest.mark.spark
def test_lag_lead_and_numbering(tmp_path: Path, spark: Any) -> None:
    payload = mf.window_payload(
        ["customer_id"],
        [("order_id", False)],
        [mf.window_value("amount", lag="1,2", lead="1")],
        rank=True,
    )
    payload["rowNumber"] = True
    cols = (
        "order_id:bigint,amount_lag1:double,amount_lag2:double,amount_lead1:double,"
        "rank:bigint,rowNumber:bigint"
    )
    tr = _result(_window(tmp_path, payload, cols), "r")
    assert not tr.warnings  # no aggregate: no frame to review
    assert _run(spark, tr.outputs[0].sql) == [
        (1, None, None, 2.0, 1, 1),
        (2, 1.0, None, 3.0, 2, 2),
        (3, 2.0, 1.0, None, 3, 3),
        (4, None, None, None, 1, 1),
    ]


def _base(**root: Any) -> dict[str, Any]:
    payload = mf.window_payload(
        ["customer_id"], [("order_id", False)], [mf.window_value("amount", "sum")]
    )
    payload.update(root)
    return payload


@pytest.mark.parametrize(
    ("payload", "cols", "element"),
    [
        (_base(cumeDist=True), "amount_sum:double", "cumeDist"),
        (_base(ntile=True), "amount_sum:double", "ntile"),
        (
            _base(values=[{**mf.window_value("amount"), "stddev": True}]),
            "order_id:bigint",
            "values[].stddev",
        ),
        (
            _base(values=[{**mf.window_value("amount", lag="1"), "lagDiff": True}]),
            "order_id:bigint",
            "values[].lagDiff",
        ),
        (
            _base(values=[mf.window_value("amount", lag="x")]),
            "order_id:bigint",
            "values[].lagValues",
        ),
        (_base(windows=[]), "amount_sum:double", "windows"),
        (_base(), "order_id:bigint", "output schema"),
        (_base(), "amount_sum:double,ghost:double", "output schema"),
    ],
)
def test_window_never_guesses(
    tmp_path: Path, payload: dict[str, Any], cols: str, element: str
) -> None:
    assert _result(_window(tmp_path, payload, cols), "r").element == element


def test_range_frame_is_not_mapped(tmp_path: Path) -> None:
    payload = _base()
    payload["windows"][0].update({"enableLimits": True, "windowLimitMode": "RANGE"})
    assert (
        _result(_window(tmp_path, payload, "amount_sum:double"), "r").element == "windowLimitMode"
    )


def test_lag_needs_an_order(tmp_path: Path) -> None:
    payload = mf.window_payload(["customer_id"], [], [mf.window_value("amount", lag="1")])
    item = _result(_window(tmp_path, payload, "amount_lag1:double"), "r")
    assert item.element == "windows[].orders"
