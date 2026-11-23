"""Phase 3: Split translator, VALUES mode (one SELECT per output, PySpark, refusals)."""

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


def test_split_sql_snapshot(snapshot: SnapshotAssertion) -> None:
    tr = _result(fixture("DEMO_COMPLEXE"), "split_status")
    assert [o.dataset for o in tr.outputs] == ["events_delivered", "events_failed", "events_other"]
    sql = "\n\n".join(f"-- {o.dataset}\n{o.sql}" for o in tr.outputs) + "\n"
    assert sql == snapshot.use_extension(SqlSnapshot)


def _payload(
    splits: list[tuple[str, int]], default: int = 2, case: bool = True, **extra: Any
) -> dict[str, Any]:
    return {
        "mode": "VALUES",
        "column": "status",
        "valueSplits": [{"value": v, "caseSensitive": case, "outputIndex": i} for v, i in splits],
        "defaultOutputIndex": default,
        "computedColumns": [],
        "writeComputedColumnsInOutput": False,
        "preFilter": mf.no_filter(),
        **extra,
    }


def _split(tmp_path: Path, payload: dict[str, Any], column: str = "status") -> Project:
    p = mf.build_demo_simple()
    names = ["a", "b", "rest"]
    for n in names:
        p.add_dataset(mf.dataset(p.key, n, cols=mf.columns("order_id:bigint", f"{column}:string")))
    p.add_recipe(
        mf.recipe(p.key, "r", "split", [mf.ref("orders_raw")], [mf.out_ref(n) for n in names]),
        payload,
    )
    return read_files(tmp_path, p.files())


ROWS = [
    (1, 1, None, 1.0, "DONE"),
    (2, 1, None, 1.0, "done"),
    (3, 1, None, 1.0, "NEW"),
    (4, 1, None, 1.0, None),
    (5, 1, None, 1.0, "OTHER"),
]


def _ids(spark: Any, tr: Any) -> dict[str, list[int]]:
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    return {
        o.dataset: sorted(r["order_id"] for r in spark.sql(o.sql).collect()) for o in tr.outputs
    }


@pytest.mark.spark
def test_every_row_lands_in_exactly_one_output(tmp_path: Path, spark: Any) -> None:
    tr = _result(_split(tmp_path, _payload([("DONE", 0), ("NEW", 1)])), "r")
    got = _ids(spark, tr)
    assert got == {"a": [1], "b": [3], "rest": [2, 4, 5]}  # NULL goes to the default output
    assert sorted(i for ids in got.values() for i in ids) == [1, 2, 3, 4, 5]


@pytest.mark.spark
def test_case_insensitive_values(tmp_path: Path, spark: Any) -> None:
    tr = _result(_split(tmp_path, _payload([("done", 0), ("new", 1)], case=False)), "r")
    assert _ids(spark, tr) == {"a": [1, 2], "b": [3], "rest": [4, 5]}


@pytest.mark.spark
def test_no_default_output_drops_the_other_rows(tmp_path: Path, spark: Any) -> None:
    payload = _payload([("DONE", 0), ("NEW", 1), ("OTHER", 2)], default=-1)
    assert _ids(spark, _result(_split(tmp_path, payload), "r")) == {"a": [1], "b": [3], "rest": [5]}


@pytest.mark.spark
def test_pre_filter_applies_to_every_output(tmp_path: Path, spark: Any) -> None:
    payload = _payload(
        [("DONE", 0), ("NEW", 1)], preFilter=flt(cond(">  [number]", "order_id", num=2))
    )
    assert _ids(spark, _result(_split(tmp_path, payload), "r")) == {
        "a": [],
        "b": [3],
        "rest": [4, 5],
    }


@pytest.mark.parametrize(
    ("payload", "element"),
    [
        (_payload([("DONE", 0)], mode="FILTERS"), "mode"),
        (
            _payload([("DONE", 0)], preFilter={**flt(cond("not empty")), "distinct": True}),
            "preFilter.distinct",
        ),
        (_payload([("DONE", 0), ("DONE", 1)]), "valueSplits"),
        (_payload([("DONE", 5)]), "valueSplits[].outputIndex"),
        (_payload([]), "valueSplits"),
        (_payload([("DONE", 0)], default=-1), "outputs"),  # output "b" receives no row
        (_payload([("DONE", 0)], column="ghost"), "column"),
        (
            _payload([("DONE", 0)], writeComputedColumnsInOutput=True),
            "writeComputedColumnsInOutput",
        ),
    ],
)
def test_split_never_guesses(tmp_path: Path, payload: dict[str, Any], element: str) -> None:
    assert _result(_split(tmp_path, payload), "r").element == element


def test_numeric_column_is_not_compared_as_text(tmp_path: Path) -> None:
    item = _result(_split(tmp_path, {**_payload([("1", 0)]), "column": "order_id"}), "r")
    assert (item.element, item.detail) == (
        "column",
        "'order_id' is bigint: only text columns are compared",
    )
