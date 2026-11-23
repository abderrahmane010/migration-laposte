"""Phase 3: Dataiku filters -> SQL conditions (observed vocabulary only, H-35)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.fixtures import make_fixture as mf
from tests.test_translate_simple import ORDERS, _result
from tests.test_translate_sync import read_files

SELECTION = {
    "samplingMethod": "HEAD_SEQUENTIAL",
    "maxRecords": 100,
    "partitionSelectionMethod": "ALL",
    "filter": mf.no_filter(),
    "ordering": {"enabled": False, "rules": []},
}
COLS = "order_id:bigint,customer_id:bigint,order_date:date,amount:double,status:string"


def cond(operator: str, column: str = "status", **values: Any) -> dict[str, Any]:
    return {"input": column, "operator": operator, "col": "", "string": "", "num": 0, **values}


def flt(*conditions: dict[str, Any], mode: str = "&&", **extra: Any) -> dict[str, Any]:
    return {
        "enabled": True,
        "distinct": False,
        "uiData": {"mode": mode, "conditions": list(conditions)},
        **extra,
    }


def sampled(tmp_path: Path, payload: Any) -> Any:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "out", cols=mf.columns(*COLS.split(","))))
    rcp = mf.recipe(p.key, "r", "sampling", [mf.ref("orders_raw")], [mf.out_ref("out")])
    rcp["params"]["selection"] = SELECTION
    p.add_recipe(rcp, payload)
    return _result(read_files(tmp_path, p.files()), "r")


ROWS = [
    (1, 1, None, 5.0, "DONE"),
    (2, 1, None, 15.0, "DONE"),
    (3, 2, None, 25.0, ""),
    (4, 2, None, 35.0, None),
    (5, 3, None, 45.0, "done"),
]


@pytest.mark.spark
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (flt(cond("== [string]", string="DONE")), [1, 2]),  # case-sensitive
        (flt(cond(">  [number]", "amount", num=20)), [3, 4, 5]),
        (flt(cond("not empty")), [1, 2, 5]),  # NULL and '' are empty
        (flt(cond("not empty"), cond(">  [number]", "amount", num=10)), [2, 5]),
    ],
)
def test_filter_operators_on_spark(
    tmp_path: Path, spark: Any, payload: Any, expected: list[int]
) -> None:
    tr = sampled(tmp_path, payload)
    spark.createDataFrame(ROWS, ORDERS).createOrReplaceTempView("orders_raw")
    assert sorted(r["order_id"] for r in spark.sql(tr.outputs[0].sql).collect()) == expected


@pytest.mark.spark
def test_string_values_are_escaped(tmp_path: Path, spark: Any) -> None:
    value = "it's'; DROP TABLE x --"
    tr = sampled(tmp_path, flt(cond("== [string]", string=value)))
    assert "'it\\'s\\'; DROP TABLE x --'" in tr.outputs[0].sql
    spark.createDataFrame(
        [(1, 1, None, 1.0, value), (2, 1, None, 1.0, "x")], ORDERS
    ).createOrReplaceTempView("orders_raw")
    assert [r["order_id"] for r in spark.sql(tr.outputs[0].sql).collect()] == [1]


@pytest.mark.parametrize(
    ("payload", "element", "detail"),
    [
        (flt(cond("contains [string]")), "filter.operator", "'contains [string]' is not mapped"),
        (
            flt(cond("== [string]", "ghost")),
            "filter.input",
            "column 'ghost' is not an input column",
        ),
        (flt(cond("not empty"), mode="||"), "filter.mode", "'||' is not mapped"),
        (flt(cond("not empty"), distinct=True), "filter.distinct", "option not mapped yet"),
        (flt(), "filter", "enabled filter without condition"),
        (
            mf.formula_filter("amount > 0"),
            "filter.mode",
            "formula filter (formula language: phase 4)",
        ),
    ],
)
def test_filters_never_guess(tmp_path: Path, payload: Any, element: str, detail: str) -> None:
    item = sampled(tmp_path, payload)
    assert (item.element, item.detail) == (element, detail)


def test_disabled_filter_adds_nothing(tmp_path: Path) -> None:
    tr = sampled(tmp_path, {**flt(cond("contains [string]")), "enabled": False})
    assert "WHERE" not in tr.outputs[0].sql
