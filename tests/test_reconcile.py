"""Phase 9: reconciliation (four layers, statuses, config, plan, generated notebook)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import pytest
from pyspark.sql import DataFrame, SparkSession
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.single_file import SingleFileSnapshotExtension, WriteMode
from typer.testing import CliRunner

from dku2dbx.cli import app
from dku2dbx.generate.bundle import PY_CELL
from dku2dbx.reconcile.compare import (
    DIFFERENT,
    IDENTICAL,
    TOLERATED,
    compare_aggregates,
    compare_counts,
    compare_rows,
    compare_schema,
    reconcile_table,
    report_markdown,
    status,
    within,
)
from dku2dbx.reconcile.config import (
    ReconcileConfig,
    ReconcileConfigError,
    load_reconcile_config,
)
from dku2dbx.reconcile.notebook import (
    REGISTRY,
    Reconciliation,
    plan_markdown,
    plan_reconciliation,
    reconcile_notebook,
    registry_markdown,
    write_reconciliation,
)
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR
from tests.test_translate_sync import fixture

ORDERS = "order_id bigint, customer string, amount double, day string"
ROWS = [(1, "a", 10.0, "2026-01-01"), (2, "b", 20.0, "2026-01-01"), (3, "c", 30.0, "2026-01-02")]


class MarkdownSnapshot(SingleFileSnapshotExtension):
    _write_mode = WriteMode.TEXT
    file_extension = "md"


def _df(spark: SparkSession, rows: list[tuple[Any, ...]], schema: str = ORDERS) -> DataFrame:
    return spark.createDataFrame(rows, schema)


def _spec(**rule: Any) -> dict[str, Any]:
    return {"dataset": "orders", **rule}


def _severities(diffs: list[dict[str, str]]) -> list[tuple[str, str, str]]:
    return [(d["layer"], d["item"], d["severity"]) for d in diffs]


# --------------------------------------------------------------------------- #
# Tolerance and status
# --------------------------------------------------------------------------- #
def test_within_and_status() -> None:
    assert within(100.0, 100.5, 1.0, 0.0)
    assert within(100.0, 101.0, 0.0, 0.01)  # 1 % of 100
    assert not within(100.0, 101.1, 0.0, 0.01)
    assert within(None, None, 0.0, 0.0)
    assert not within(None, 0.0, 1.0, 1.0)
    assert status([]) == IDENTICAL
    assert status([{"severity": "tolerated"}]) == TOLERATED
    assert status([{"severity": "tolerated"}, {"severity": "diff"}]) == DIFFERENT


# --------------------------------------------------------------------------- #
# The four layers
# --------------------------------------------------------------------------- #
@pytest.mark.spark
def test_identical_tables(spark: SparkSession) -> None:
    result = reconcile_table(_df(spark, ROWS), _df(spark, list(reversed(ROWS))), _spec())
    assert result["status"] == IDENTICAL and result["differences"] == []
    keyed = reconcile_table(_df(spark, ROWS), _df(spark, ROWS), _spec(key=["order_id"]))
    assert keyed["status"] == IDENTICAL


@pytest.mark.spark
def test_schema_layer(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    widened = _df(spark, ROWS, "order_id bigint, customer string, amount double, day string")
    narrowed = _df(
        spark, [(1, "a", 10, "x")], "order_id int, customer string, amount int, x string"
    )
    assert compare_schema(ref, widened, []) == []
    diffs = compare_schema(ref, narrowed, [])
    assert _severities(diffs) == [
        ("schéma", "day", "diff"),  # missing
        ("schéma", "x", "diff"),  # extra
        ("schéma", "order_id", "diff"),  # bigint -> int loses values
        ("schéma", "amount", "diff"),  # double -> int
    ]
    small = _df(spark, [(1,)], "n int")
    big = _df(spark, [(1,)], "n bigint")
    assert _severities(compare_schema(small, big, [])) == [("schéma", "n", "tolerated")]
    assert compare_schema(ref, narrowed, ["day", "x", "order_id", "amount"]) == []


@pytest.mark.spark
def test_schema_nullability_is_tolerated(spark: SparkSession) -> None:
    from pyspark.sql.types import LongType, StructField, StructType  # noqa: PLC0415

    nullable = spark.createDataFrame([(1,)], StructType([StructField("n", LongType(), True)]))
    strict = spark.createDataFrame([(1,)], StructType([StructField("n", LongType(), False)]))
    diffs = compare_schema(nullable, strict, [])
    assert diffs[0]["detail"] == "nullable True -> False" and diffs[0]["severity"] == "tolerated"


@pytest.mark.spark
def test_count_layer(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    assert compare_counts(ref, _df(spark, ROWS[:2]), []) == [
        {"layer": "comptes", "item": "total", "detail": "3 lignes -> 2 lignes", "severity": "diff"}
    ]
    moved = [*ROWS[:2], (3, "c", 30.0, "2026-01-01")]  # same total, other partition
    diffs = compare_counts(ref, _df(spark, moved), ["day"])
    assert [d["item"] for d in diffs] == ["day=2026-01-01", "day=2026-01-02"]
    assert diffs[0]["detail"] == "2 lignes -> 3 lignes"
    assert diffs[1]["detail"] == "1 lignes -> 0 lignes"
    assert compare_counts(ref, ref, ["day"]) == []
    assert compare_counts(ref, ref, ["absent"])[0]["detail"].startswith("colonne de partition")


@pytest.mark.spark
def test_aggregate_layer(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    columns = ["order_id", "customer", "amount", "day"]
    nudged = _df(spark, [(1, "a", 10.001, "2026-01-01"), *ROWS[1:]])
    diffs = compare_aggregates(ref, nudged, columns, (0.01, 0.0))
    assert {d["severity"] for d in diffs} == {"tolerated"}  # sum and min within 0.01
    assert {d["detail"].split()[0] for d in diffs} == {"min", "sum"}
    diffs = compare_aggregates(ref, nudged, columns)  # no tolerance
    assert {d["severity"] for d in diffs} == {"diff"}
    nulls = _df(spark, [(1, None, 10.0, "2026-01-01"), *ROWS[1:]])
    diffs = compare_aggregates(ref, nulls, columns, (1e9, 1.0))
    assert ("agrégats", "customer", "diff") in _severities(diffs)  # counts never tolerated
    assert any(d["detail"] == "nulls 0 -> 1" for d in diffs)


@pytest.mark.spark
def test_aggregate_layer_skips_complex_types(spark: SparkSession) -> None:
    schema = "id int, tags map<string,int>"
    ref = spark.createDataFrame([(1, {"x": 1})], schema)
    assert compare_aggregates(ref, ref, ["id", "tags"]) == []


@pytest.mark.spark
def test_row_layer_with_key(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    columns = ["order_id", "customer", "amount", "day"]
    changed = _df(
        spark,
        [(1, "a", 10.004, "2026-01-01"), (2, "B", 20.0, "2026-01-01"), (4, "d", 1.0, "2026-01-02")],
    )
    diffs = compare_rows(ref, changed, ["order_id"], columns, (0.01, 0.0))
    assert _severities(diffs) == [
        ("lignes", "clé", "diff"),  # 3 only in Dataiku
        ("lignes", "clé", "diff"),  # 4 only in Databricks
        ("lignes", "customer", "diff"),
        ("lignes", "amount", "tolerated"),
    ]
    assert diffs[2]["detail"] == "1 ligne(s) différente(s), ex. order_id=2 : b -> B"
    strict = compare_rows(ref, changed, ["order_id"], columns)
    assert ("lignes", "amount", "diff") in _severities(strict)
    duplicated = _df(spark, [*ROWS, ROWS[0]])
    assert (
        "clé en double côté Databricks"
        in compare_rows(ref, duplicated, ["order_id"], columns)[0]["detail"]
    )
    assert compare_rows(ref, ref, ["absent"], columns)[0]["detail"].startswith("colonne(s) de clé")


@pytest.mark.spark
def test_row_layer_without_key(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    columns = ["order_id", "customer", "amount", "day"]
    assert compare_rows(ref, _df(spark, list(reversed(ROWS))), [], columns) == []
    diffs = compare_rows(ref, _df(spark, [*ROWS[:2], ROWS[1]]), [], columns)
    assert [d["detail"] for d in diffs] == [
        "1 ligne(s) seulement côté Dataiku",
        "1 ligne(s) seulement côté Databricks",
    ]


@pytest.mark.spark
def test_null_values_compare_equal(spark: SparkSession) -> None:
    rows = [(1, None, None, None)]
    result = reconcile_table(_df(spark, rows), _df(spark, rows), _spec(key=["order_id"]))
    assert result["status"] == IDENTICAL


@pytest.mark.spark
def test_table_statuses(spark: SparkSession) -> None:
    ref = _df(spark, ROWS)
    near = _df(spark, [(1, "a", 10.004, "2026-01-01"), *ROWS[1:]])
    far = _df(spark, [(1, "a", 99.0, "2026-01-01"), *ROWS[1:]])
    other = _df(spark, [(9, "z", 0.0, "1999-01-01")])
    rule = {"key": ["order_id"], "abs_tolerance": 0.01, "partition_columns": ["day"]}
    assert reconcile_table(ref, near, _spec(**rule))["status"] == TOLERATED
    assert reconcile_table(ref, far, _spec(**rule))["status"] == DIFFERENT
    result = reconcile_table(ref, other, _spec(**rule))
    assert result["status"] == DIFFERENT
    assert {d["layer"] for d in result["differences"]} == {"comptes", "agrégats", "lignes"}
    report = report_markdown("P", [result])
    assert "| orders | ÉCART |" in report and "## orders (ÉCART)" in report


# --------------------------------------------------------------------------- #
# Configuration and plan
# --------------------------------------------------------------------------- #
def test_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "reconcile.yaml"
    path.write_text(
        "defaults: {rel_tolerance: 0.001}\n"
        "tables:\n  orders: {key: [order_id], abs_tolerance: 0.01}\n  old: {skip: true}\n",
        encoding="utf-8",
    )
    config, source = load_reconcile_config(path)
    assert source == str(path)
    assert config.rule("orders") == {
        "key": ["order_id"],
        "abs_tolerance": 0.01,
        "rel_tolerance": 0.001,
        "skip": False,
    }
    assert config.rule("other") == {"rel_tolerance": 0.001, "skip": False}
    path.write_text("tables: {orders: {keys: [x]}}\n", encoding="utf-8")
    with pytest.raises(ReconcileConfigError, match="invalid"):
        load_reconcile_config(path)
    with pytest.raises(ReconcileConfigError, match="not found"):
        load_reconcile_config(tmp_path / "absent.yaml")
    monkeypatch.chdir(tmp_path)
    assert load_reconcile_config()[1] == "(aucun fichier : ni clé ni tolérance)"
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "reconcile.yaml").write_text("{}\n", encoding="utf-8")
    (tmp_path / "config" / "reconcile.local.yaml").write_text("{}\n", encoding="utf-8")
    assert load_reconcile_config()[1] == "config/reconcile.local.yaml"


def test_repository_config_is_valid() -> None:
    config, _ = load_reconcile_config(Path("config/reconcile.yaml"))
    assert isinstance(config, ReconcileConfig)


def test_plan_on_the_complex_fixture() -> None:
    config = ReconcileConfig.model_validate(
        {
            "tables": {
                "route_kpis": {"key": ["route_id"], "abs_tolerance": 0.01},
                "depots_unique": {"skip": True},
                "ghost": {"key": ["x"]},
                "events_other": {"partition_columns": []},
            }
        }
    )
    plan = plan_reconciliation(fixture("DEMO_COMPLEXE"), config, "test")
    names = [s["dataset"] for s in plan.specs]
    assert len(names) == 19 and "depots_unique" not in names
    assert plan.skipped == ["depots_unique"]
    specs = {s["dataset"]: s for s in plan.specs}
    assert specs["route_kpis"]["key"] == ["route_id"]
    assert specs["events_clean"]["partition_columns"] == ["day"]  # dimension by default
    assert specs["events_other"]["partition_columns"] == []  # overridden
    assert plan.warnings == ["ghost : déclarée dans la configuration, mais pas produite"]
    tolerant = ReconcileConfig.model_validate({"defaults": {"abs_tolerance": 1.0}})
    warnings = plan_reconciliation(fixture("DEMO_SIMPLE"), tolerant, "t").warnings
    assert len(warnings) == 3 and all("tolérance sans clé" in w for w in warnings)


def _simple_plan() -> Reconciliation:
    config = ReconcileConfig.model_validate(
        {"tables": {"customer_stats": {"key": ["customer_id"], "rel_tolerance": 0.001}}}
    )
    return plan_reconciliation(fixture("DEMO_SIMPLE"), config, "config/reconcile.yaml")


def test_plan_snapshot(snapshot: SnapshotAssertion) -> None:
    assert plan_markdown(_simple_plan()) == snapshot.use_extension(MarkdownSnapshot)


def test_registry_snapshot(snapshot: SnapshotAssertion) -> None:
    assert registry_markdown(_simple_plan()) == snapshot.use_extension(MarkdownSnapshot)


def test_registry_is_never_overwritten(tmp_path: Path) -> None:
    plan = plan_reconciliation(fixture("DEMO_SIMPLE"), ReconcileConfig(), "t")
    write_reconciliation(plan, tmp_path)
    (tmp_path / REGISTRY).write_text("décisions du métier\n", encoding="utf-8")
    written = write_reconciliation(plan, tmp_path)
    assert (tmp_path / REGISTRY).read_text(encoding="utf-8") == "décisions du métier\n"
    assert [p.name for p in written] == ["reconcile.py", "RECONCILIATION.md"]


def test_notebook_is_reproducible_and_traceable() -> None:
    plan = plan_reconciliation(fixture("DEMO_SIMPLE"), ReconcileConfig(), "t")
    text = reconcile_notebook(plan)
    assert text == reconcile_notebook(plan)
    assert text.startswith("# Databricks notebook source\n# Generated by dku2dbx")
    assert "# Dataset orders_enriched: JSON SHA-256 " in text
    assert "from __future__" not in text
    for cell in text.split(PY_CELL):
        compile(cell, "<cell>", "exec")  # every cell is valid Python on its own


# --------------------------------------------------------------------------- #
# The generated notebook runs on local PySpark
# --------------------------------------------------------------------------- #
class _Exit(Exception):  # noqa: N818 (mirrors dbutils.notebook.exit)
    pass


class FakeDbutils:
    """widgets.text/get and notebook.exit, as the notebook uses them."""

    exits: ClassVar[list[str]] = []

    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.widgets = self
        self.notebook = self

    def text(self, name: str, default: str) -> None:
        self.values.setdefault(name, default)

    def get(self, name: str) -> str:
        return self.values[name]

    def exit(self, value: str) -> None:
        raise _Exit(value)


def _run(text: str, spark: SparkSession, values: dict[str, str]) -> dict[str, Any]:
    scope: dict[str, Any] = {"spark": spark, "dbutils": FakeDbutils(values)}
    for cell in text.split(PY_CELL):  # Databricks compiles every cell alone
        exec(compile(cell, "<cell>", "exec"), scope)
    return scope


@pytest.mark.spark
def test_generated_notebook_runs(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_ref")
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_new")
    tables = {
        "orders_enriched": (ROWS, ROWS),
        "customer_stats": (ROWS, [(1, "a", 10.004, "2026-01-01"), *ROWS[1:]]),
        "orders_ranked": (ROWS, ROWS[:1]),
    }
    for name, (ref, new) in tables.items():
        _df(spark, ref).write.mode("overwrite").saveAsTable(f"dku_ref.{name}")
        _df(spark, new).write.mode("overwrite").saveAsTable(f"dku_new.{name}")
    config = ReconcileConfig.model_validate(
        {"tables": {"customer_stats": {"key": ["order_id"], "abs_tolerance": 0.01}}}
    )
    text = reconcile_notebook(plan_reconciliation(fixture("DEMO_SIMPLE"), config, "t"))
    values = {
        "catalog": "spark_catalog",
        "schema": "dku_new",
        "ref_catalog": "spark_catalog",
        "ref_schema": "dku_ref",
    }
    with pytest.raises(AssertionError, match=r"ÉCART for orders_ranked \(details in"):
        _run(text, spark, dict(values))
    stored = {
        r["table"]: r["status"] for r in spark.table("dku_new.dku2dbx_reconciliation").collect()
    }
    assert stored == {
        "orders_enriched": IDENTICAL,
        "customer_stats": TOLERATED,
        "orders_ranked": DIFFERENT,
    }
    # Fixed: the job succeeds and returns the results.
    _df(spark, ROWS).write.mode("overwrite").saveAsTable("dku_new.orders_ranked")
    spark.sql("DROP TABLE dku_ref.orders_enriched")  # a missing table is an ÉCART, not a crash
    with pytest.raises(AssertionError, match="orders_enriched"):
        _run(text, spark, dict(values))
    _df(spark, ROWS).write.mode("overwrite").saveAsTable("dku_ref.orders_enriched")
    with pytest.raises(_Exit) as done:
        _run(text, spark, dict(values))
    assert '"status": "IDENTIQUE"' in str(done.value)
    assert spark.table("dku_new.dku2dbx_reconciliation").count() == 9  # appended per run
    with pytest.raises(ValueError, match="catalog and schema"):
        _run(text, spark, {})


def test_cli_reconcile(tmp_path: Path) -> None:
    export = str(DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip")
    result = CliRunner().invoke(app, ["reconcile", export, "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "3 table(s) à comparer (0 avec clé, 3 par EXCEPT ALL), 0 exclue(s)" in result.output
    target = tmp_path / "DEMO_SIMPLE" / "reconcile"
    assert {p.name for p in target.iterdir()} == {
        "reconcile.py",
        "RECONCILIATION.md",
        "ECARTS_ACCEPTES.md",
    }
    bad = tmp_path / "bad.yaml"
    bad.write_text("tables: [", encoding="utf-8")
    result = CliRunner().invoke(app, ["reconcile", export, "--config", str(bad)])
    assert result.exit_code == 1 and "invalid reconciliation configuration" in result.output
