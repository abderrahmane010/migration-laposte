"""Phase 4: Prepare recipes (processor chain as CTEs, PySpark, refusals, coverage)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.analyze.portfolio import processor_catalogue
from dku2dbx.model.ir import Project
from dku2dbx.translate.prepare.processors import MANUAL_PROCESSORS, PROCESSORS
from tests.fixtures import make_fixture as mf
from tests.test_translate_simple import ORDERS, _result
from tests.test_translate_sync import SqlSnapshot, fixture, read_files

CASES = [
    ("DEMO_SIMPLE", "prepare_orders"),
    ("DEMO_COMPLEXE", "prepare_events"),
    ("DEMO_COMPLEXE", "prepare_anomalies"),
    ("DEMO_COMPLEXE", "prepare_b2b"),
]


@pytest.mark.parametrize(("key", "recipe"), CASES)
def test_prepare_sql_snapshot(key: str, recipe: str, snapshot: SnapshotAssertion) -> None:
    assert _result(fixture(key), recipe).outputs[0].sql + "\n" == snapshot.use_extension(
        SqlSnapshot
    )


@pytest.mark.spark
def test_demo_simple_prepare_runs_on_spark(spark: Any) -> None:
    tr = _result(fixture("DEMO_SIMPLE"), "prepare_orders")
    assert tr.warnings == ["étape désactivée ignorée : ColumnRenamer"]
    spark.createDataFrame(
        [
            (1, 1, None, 10.004, "NEW"),
            (2, 1, None, 5.0, None),
            (3, 1, None, 7.0, ""),
            (4, 1, None, 1.0, "CANCELLED"),
        ],
        ORDERS,
    ).createOrReplaceTempView("orders_raw")
    got = sorted(
        (r["order_id"], r["status"], r["amount_eur"])
        for r in spark.sql(tr.outputs[0].sql).collect()
    )
    # FillEmptyWithValue: NULL and '' -> UNKNOWN; round half up; CANCELLED removed
    assert got == [(1, "NEW", 10.0), (2, "UNKNOWN", 5.0), (3, "UNKNOWN", 7.0)]


def _prepare(tmp_path: Path, steps: list[dict[str, Any]], out_cols: str) -> Project:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*out_cols.split(","))))
    p.add_recipe(
        mf.recipe(p.key, "r", "shaker", [mf.ref("orders_raw")], [mf.out_ref("out")]),
        mf.shaker(steps),
    )
    return read_files(tmp_path, p.files())


ALL = "order_id:bigint,customer_id:bigint,order_date:date,amount:double,status:string"
ROWS = [(1, 1, None, 2.0, " a "), (2, 2, None, None, "B"), (3, 3, None, 4.0, None)]


def _run(spark: Any, tr: Any, *names: str) -> list[tuple[Any, ...]]:
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    return sorted(tuple(r[n] for n in names) for r in spark.sql(tr.outputs[0].sql).collect())


@pytest.mark.spark
@pytest.mark.parametrize(
    ("step", "out_cols", "names", "expected"),
    [
        (
            mf.step(
                "StringTransformer",
                {"columns": ["status"], "mode": "TRIM", "appliesTo": "SINGLE_COLUMN"},
            ),
            ALL,
            ("order_id", "status"),
            [(1, "a"), (2, "B"), (3, None)],
        ),
        (
            mf.step(
                "StringTransformer",
                {"columns": ["status"], "mode": "TO_LOWER", "appliesTo": "SINGLE_COLUMN"},
            ),
            ALL,
            ("order_id", "status"),
            [(1, " a "), (2, "b"), (3, None)],
        ),
        (
            mf.step(
                "FillEmptyWithValue",
                {"columns": ["amount"], "value": "0", "appliesTo": "SINGLE_COLUMN"},
            ),
            ALL,
            ("order_id", "amount"),
            [(1, 2.0), (2, 0.0), (3, 4.0)],
        ),
        (
            mf.step(
                "ColumnsSelector",
                {"keep": True, "columns": ["order_id", "status"], "appliesTo": "COLUMNS"},
            ),
            "order_id:bigint,status:string",
            ("order_id",),
            [(1,), (2,), (3,)],
        ),
        (
            mf.step("FilterOnFormula", {"expression": "amount > 3", "action": "KEEP_ROW"}),
            ALL,
            ("order_id",),
            [(3,)],  # NULL amount: condition not true, row removed
        ),
        (
            mf.step("FilterOnFormula", {"expression": "amount > 3", "action": "REMOVE_ROW"}),
            ALL,
            ("order_id",),
            [(1,), (2,)],  # NULL amount: kept
        ),
        (
            mf.step(
                "FilterOnValue",
                {
                    "columns": ["status"],
                    "values": ["B"],
                    "action": "REMOVE_ROW",
                    "appliesTo": "SINGLE_COLUMN",
                    "matchingMode": "FULL_STRING",
                    "normalizationMode": "EXACT",
                },
            ),
            ALL,
            ("order_id",),
            [(1,), (3,)],
        ),
        (
            mf.step("ColumnRenamer", {"renamings": [{"from": "status", "to": "state"}]}),
            ALL.replace("status:string", "state:string"),
            ("order_id", "state"),
            [(1, " a "), (2, "B"), (3, None)],
        ),
        (
            mf.step(
                "CreateColumnWithGREL",
                {"column": "amount", "expression": "coalesce(amount, -1) * 2"},
            ),
            ALL,
            ("order_id", "amount"),
            [(1, 4.0), (2, -2.0), (3, 8.0)],
        ),
    ],
)
def test_each_processor_on_spark(
    tmp_path: Path,
    spark: Any,
    step: dict[str, Any],
    out_cols: str,
    names: tuple[str, ...],
    expected: list[tuple[Any, ...]],
) -> None:
    tr = _result(_prepare(tmp_path, [step], out_cols), "r")
    assert _run(spark, tr, *names) == expected


@pytest.mark.spark
def test_a_formula_sees_the_columns_of_the_previous_steps(tmp_path: Path, spark: Any) -> None:
    steps = [
        mf.step("CreateColumnWithGREL", {"column": "double", "expression": "amount * 2"}),
        mf.step("CreateColumnWithGREL", {"column": "label", "expression": 'status + "-" + double'}),
    ]
    tr = _result(_prepare(tmp_path, steps, ALL + ",double:double,label:string"), "r")
    assert _run(spark, tr, "order_id", "label") == [(1, " a -4.0"), (2, None), (3, None)]


def test_disabled_group_skips_its_steps(tmp_path: Path) -> None:
    group = mf.step_group("off", [mf.step("UnknownProcessor", {})])
    group["disabled"] = True
    tr = _result(_prepare(tmp_path, [group], ALL), "r")
    assert tr.warnings == ["étape désactivée ignorée : UnknownProcessor"]


@pytest.mark.parametrize(
    ("steps", "out_cols", "element"),
    [
        ([mf.step("UnknownProcessor", {})], ALL, "UnknownProcessor"),
        ([mf.step("ZipCodeGeocoder", {})], ALL, "ZipCodeGeocoder"),
        ([{"metaType": "STRANGE", "disabled": False}], ALL, "metaType"),
        (
            [mf.step("ColumnsSelector", {"keep": False, "columns": ["ghost"]})],
            ALL,
            "ColumnsSelector.columns",
        ),
        (
            [mf.step("FillEmptyWithValue", {"columns": ["amount"], "value": "n/a"})],
            ALL,
            "FillEmptyWithValue.value",
        ),
        (
            [mf.step("StringTransformer", {"columns": ["status"], "mode": "TO_TITLE"})],
            ALL,
            "StringTransformer.mode",
        ),
        (
            [mf.step("StringTransformer", {"columns": ["amount"], "mode": "TRIM"})],
            ALL,
            "StringTransformer.columns",
        ),
        (
            [mf.step("FilterOnFormula", {"expression": "amount * 2", "action": "KEEP_ROW"})],
            ALL,
            "FilterOnFormula.expression",
        ),
        (
            [
                mf.step(
                    "FilterOnValue",
                    {
                        "columns": ["status"],
                        "values": ["B"],
                        "action": "KEEP_ROW",
                        "matchingMode": "SUBSTRING",
                    },
                )
            ],
            ALL,
            "FilterOnValue.matchingMode",
        ),
        (
            [mf.step("CreateColumnWithGREL", {"column": "x", "expression": "nope(a)"})],
            ALL + ",x:double",
            "formula",
        ),
        ([], "order_id:bigint", "output schema"),
    ],
)
def test_prepare_never_guesses(
    tmp_path: Path, steps: list[dict[str, Any]], out_cols: str, element: str
) -> None:
    assert _result(_prepare(tmp_path, steps, out_cols), "r").element == element


def test_manual_processor_points_to_a_target(tmp_path: Path) -> None:
    item = _result(_prepare(tmp_path, [mf.step("ZipCodeGeocoder", {})], ALL), "r")
    assert "MANUAL" in item.detail and "UDF" in item.detail


def test_phase_4_end_criterion_catalogue_coverage() -> None:
    """ROADMAP phase 4: processors covering >= 80 % of the catalogue occurrences."""
    catalogue = processor_catalogue([fixture("DEMO_SIMPLE"), fixture("DEMO_COMPLEXE")])
    total = sum(s.occurrences for s in catalogue)
    covered = sum(s.occurrences for s in catalogue if s.type in PROCESSORS)
    assert covered / total >= 0.8
    uncovered = {s.type for s in catalogue if s.occurrences and s.type not in PROCESSORS}
    assert uncovered <= MANUAL_PROCESSORS  # only reference-data processors are left
