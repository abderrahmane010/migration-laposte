"""Phase 3: Join translator (SQL snapshots, PySpark, NULL keys, refusals)."""

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
    ("DEMO_SIMPLE", "join_orders_customers"),
    ("DEMO_COMPLEXE", "join_delivered_depots"),
    ("DEMO_COMPLEXE", "join_routes"),
    ("DEMO_COMPLEXE", "join_b2b_scores"),
]


@pytest.mark.parametrize(("key", "recipe"), CASES)
def test_join_sql_snapshot(key: str, recipe: str, snapshot: SnapshotAssertion) -> None:
    assert _result(fixture(key), recipe).outputs[0].sql + "\n" == snapshot.use_extension(
        SqlSnapshot
    )


def _join(
    tmp_path: Path, payload_update: dict[str, Any] | None = None, kind: str = "LEFT"
) -> Project:
    """orders_raw (t0) joined with customers (t1) on customer_id -> out."""
    p = mf.build_demo_simple()
    p.add_dataset(
        mf.dataset(
            p.key, "out", cols=mf.columns("order_id:bigint", "status:string", "segment:string")
        )
    )
    payload = mf.join_payload(2, [mf.join_on(0, 1, kind, [("customer_id", "customer_id")])])
    payload["selectedColumns"] = [
        {"name": "order_id", "table": 0, "type": "bigint"},
        {"name": "status", "table": 0, "type": "string"},
        {"name": "segment", "table": 1, "type": "string"},
    ]
    payload.update(payload_update or {})
    p.add_recipe(
        mf.recipe(
            p.key, "r", "join", [mf.ref("orders_raw"), mf.ref("customers")], [mf.out_ref("out")]
        ),
        payload,
    )
    return read_files(tmp_path, p.files())


def _views(spark: Any) -> None:
    spark.createDataFrame(
        [(1, 10, None, 1.0, "NEW"), (2, 20, None, 2.0, "DONE"), (3, None, None, 3.0, "NEW")],
        ORDERS,
    ).createOrReplaceTempView("orders_raw")
    spark.createDataFrame(
        [(10, "Ann", "B2B"), (None, "Nobody", "NULLKEY")],
        "customer_id long, name string, segment string",
    ).createOrReplaceTempView("customers")


@pytest.mark.spark
@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("INNER", [(1, "NEW", "B2B")]),
        # NULL keys never match (H-37): order 3 keeps a NULL segment
        ("LEFT", [(1, "NEW", "B2B"), (2, "DONE", None), (3, "NEW", None)]),
    ],
)
def test_join_runs_on_spark(tmp_path: Path, spark: Any, kind: str, expected: list[Any]) -> None:
    tr = _result(_join(tmp_path, kind=kind), "r")
    _views(spark)
    got = sorted(tuple(r) for r in spark.sql(tr.outputs[0].sql).collect())
    assert got == expected


@pytest.mark.spark
def test_join_pre_filter_is_a_subquery(tmp_path: Path, spark: Any) -> None:
    virtual = mf.join_payload(2, [])["virtualInputs"]
    virtual[0]["preFilter"] = flt(cond("== [string]", "status", string="NEW"))
    tr = _result(_join(tmp_path, {"virtualInputs": virtual}), "r")
    assert "WHERE" in tr.outputs[0].sql and "AS `t0`" in tr.outputs[0].sql
    _views(spark)
    got = sorted(tuple(r) for r in spark.sql(tr.outputs[0].sql).collect())
    assert got == [(1, "NEW", "B2B"), (3, "NEW", None)]


def _on(**overrides: Any) -> list[dict[str, Any]]:
    join = mf.join_on(0, 1, "LEFT", [("customer_id", "customer_id")])
    join["on"][0].update(overrides)
    return [join]


@pytest.mark.parametrize(
    ("update", "element"),
    [
        ({"joins": _on(caseInsensitive=True)}, "joins[].on[].caseInsensitive"),
        ({"joins": _on(normalizeText=True)}, "joins[].on[].normalizeText"),
        ({"joins": _on(type="WITHIN_RANGE")}, "joins[].on[].type"),
        ({"joins": _on(column2={"table": 1, "name": "ghost"})}, "joins[].on[].column2"),
        (
            {"joins": [mf.join_on(0, 1, "CROSS", [("customer_id", "customer_id")])]},
            "joins[].type",
        ),
        ({"joins": [{**_on()[0], "rightLimit": {"maxMatches": 1}}]}, "joins[].rightLimit"),
        ({"joins": [mf.join_on(1, 0, "LEFT", [("customer_id", "customer_id")])]}, "joins[]"),
        ({"joins": []}, "joins"),
        ({"computedColumns": [{"name": "x"}]}, "computedColumns"),
        ({"enableAutoCastInJoinConditions": True}, "enableAutoCastInJoinConditions"),
        ({"selectedColumns": [{"name": "segment", "table": 0}]}, "selectedColumns"),
        (
            {
                "selectedColumns": [
                    {"name": "customer_id", "table": 0},
                    {"name": "customer_id", "table": 1},
                    {"name": "segment", "table": 1},
                ]
            },
            "selectedColumns",
        ),
        ({"selectedColumns": [{"name": "order_id", "table": 0}]}, "output schema"),
    ],
)
def test_join_never_guesses(tmp_path: Path, update: dict[str, Any], element: str) -> None:
    assert _result(_join(tmp_path, update), "r").element == element
