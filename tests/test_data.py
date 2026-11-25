"""Phase 11: dataset migration (decision, idempotent copies, validation, names)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from syrupy.assertion import SnapshotAssertion
from typer.testing import CliRunner

from dku2dbx.cli import app
from dku2dbx.connect.resolve import ConnectionsConfig, branch_sources
from dku2dbx.data.config import DataConfig, DataConfigError, load_data_config
from dku2dbx.data.load import (
    ALREADY_LOADED,
    LOADED,
    PARTITIONS_ADDED,
    REPLACED,
    conform,
    load_table,
    validate,
)
from dku2dbx.data.naming import case_collisions, name_problem, proposed_name
from dku2dbx.data.notebook import copy_notebook, notebook_name, report_markdown, write_data
from dku2dbx.data.permissions import check_permissions
from dku2dbx.data.plan import DataPlan, Federation, Strategy, plan_data
from dku2dbx.model.ir import Project
from tests.fixtures import make_fixture as mf
from tests.test_analyze import MarkdownSnapshot
from tests.test_reconcile import FakeDbutils, _Exit
from tests.test_translate_sync import fixture, read_files

K = "DEMO_SIMPLE"
COLS = mf.columns("order_id:bigint", "label:string", "amount:double", "day:string")


def _sql_ds(name: str, **kw: Any) -> dict[str, Any]:
    ds = mf.dataset(K, name, cols=kw.pop("cols", COLS), connection="pg_src", **kw)
    ds["params"]["schema"] = "src"
    ds["params"]["table"] = name
    return ds


def _file_ds(name: str, *, fmt: str = "csv", **params: Any) -> dict[str, Any]:
    ds = mf.dataset(K, name, cols=COLS, ds_type=params.pop("ds_type", "Filesystem"))
    ds["params"] = {"connection": "fs_conn", "path": params.pop("path", f"/{K}.{name}")}
    ds["formatType"] = fmt
    ds["formatParams"] = {
        "style": "unix",
        "charset": "utf8",
        "separator": "\t",
        "quoteChar": '"',
        "escapeChar": "\\",
        "parseHeaderRow": False,
        "dateSerializationFormat": "ISO",
        **params,
    }
    return ds


def _project(tmp_path: Path, *extra: dict[str, Any]) -> Project:
    p = mf.build_demo_simple()
    p.datasets += list(extra)
    return read_files(tmp_path, p.files())


def _config(**connections: Any) -> DataConfig:
    return DataConfig.model_validate({"connections": connections})


AVAILABLE = {"federation": "available", "foreign_catalog": "fed"}
LOCAL_COPY = {"federation": "available", "foreign_catalog": "spark_catalog", "access": "copy"}
SPEC_COLUMNS = [
    ["order_id", "bigint"],
    ["label", "string"],
    ["amount", "double"],
    ["day", "string"],
]


# --------------------------------------------------------------------------- #
# Decision per dataset
# --------------------------------------------------------------------------- #
def test_nothing_is_assumed_without_configuration() -> None:
    plan = plan_data(fixture(K), DataConfig(), "t")
    sources = [d for d in plan.decisions if d.role == "source"]
    assert {d.strategy for d in sources} == {Strategy.REFUSED}
    assert {d.federation for d in sources} == {Federation.TO_CONFIRM}
    assert all("à confirmer" in d.reason for d in sources)
    produced = [d for d in plan.decisions if d.role == "produit"]
    assert {d.strategy for d in produced} == {Strategy.RECOMPUTE}
    assert plan.copies == []


@pytest.mark.parametrize(
    ("ds_type", "declaration", "federation", "strategy"),
    [
        ("PostgreSQL", None, Federation.TO_CONFIRM, Strategy.REFUSED),
        ("PostgreSQL", {"federation": "unavailable"}, Federation.UNAVAILABLE, Strategy.REFUSED),
        ("Vertica", AVAILABLE, Federation.INCOMPATIBLE, Strategy.REFUSED),
        ("Oracle", AVAILABLE, Federation.COMPATIBLE, Strategy.REFERENCE),
        ("Snowflake", {**AVAILABLE, "access": "copy"}, Federation.COMPATIBLE, Strategy.COPY),
        (
            "PostgreSQL",
            {**AVAILABLE, "foreign_catalog": "[FED]"},
            Federation.COMPATIBLE,
            Strategy.REFUSED,
        ),
    ],
)
def test_federation_is_classified_explicitly(
    tmp_path: Path,
    ds_type: str,
    declaration: dict[str, str] | None,
    federation: Federation,
    strategy: Strategy,
) -> None:
    project = _project(tmp_path, _sql_ds("src_table", ds_type=ds_type))
    config = _config(**({"pg_src": declaration} if declaration else {}))
    decision = plan_data(project, config, "t").decision("src_table")
    assert (decision.federation, decision.strategy) == (federation, strategy)
    if strategy is Strategy.REFERENCE:
        assert decision.reference == "fed.src.src_table"
        assert decision.read is None
    if strategy is Strategy.COPY:
        assert decision.read == {"kind": "table", "name": "`fed`.`src`.`src_table`"}
        assert decision.columns[0] == ["order_id", "bigint"]
    if "[FED]" in str(declaration):
        assert "valeur fictive" in decision.reason


def test_a_branched_source_is_not_copied() -> None:
    project = fixture(K)
    mapping = ConnectionsConfig.model_validate(
        {"connections": {"pg_sources": {"catalog": "cat", "schema": "src"}}}
    )
    branching = branch_sources(project, mapping, "t")
    plan = plan_data(project, _config(pg_sources=AVAILABLE), "t", branching)
    decision = plan.decision("customers")
    assert decision.strategy is Strategy.BRANCHED
    assert decision.reference == branching.resolved["customers"]
    assert decision.federation is Federation.COMPATIBLE


def test_history_that_cannot_be_recomputed_is_copied_once() -> None:
    plan = plan_data(
        fixture("DEMO_COMPLEXE"), _config(pg_managed={**AVAILABLE, "access": "copy"}), "t"
    )
    appended = plan.decision("events_anomalies")
    assert appended.strategy is Strategy.HISTORY
    assert "mode append de stack_failed" in appended.reason
    # Partitions never recomputed, but the day dimension is not a column: refused, never guessed.
    never = plan.decision("events_sync")
    assert never.strategy is Strategy.REFUSED
    assert "jamais recalculées par sync_events" in never.reason
    assert "colonne de partition day absente" in never.reason


def test_files_are_copied_from_their_declared_root(tmp_path: Path) -> None:
    project = _project(
        tmp_path,
        _file_ds("events_files", path="/${projectKey}.events"),
        _file_ds("excel_files", style="excel", parseHeaderRow=True, separator=","),
        _file_ds("pq_files", fmt="parquet"),
        _file_ds("xls_files", fmt="excel"),
        _file_ds("odd_files", style="exotic"),
        _file_ds("skip_files", skipRowsBeforeHeader=2),
        _file_ds("node_files", path="/${NODE_DC}/x"),
        _file_ds("hive_files", dateSerializationFormat="HIVE", style="escape_only_no_quote"),
    )
    plan = plan_data(project, _config(fs_conn={"root": "/Volumes/c/s/v/dss/"}), "t")
    events = plan.decision("events_files")
    assert events.strategy is Strategy.COPY
    assert events.read == {
        "kind": "files",
        "format": "csv",
        "path": "/Volumes/c/s/v/dss/DEMO_SIMPLE.events",
        "options": {
            "encoding": "UTF-8",
            "escape": "\\",
            "header": "false",
            "mode": "FAILFAST",
            "quote": '"',
            "sep": "\t",
        },
        "schema": "`order_id` BIGINT, `label` STRING, `amount` DOUBLE, `day` STRING",
    }
    excel = plan.decision("excel_files").read or {}
    assert (excel["options"]["escape"], excel["options"]["header"]) == ('"', "true")
    hive = plan.decision("hive_files").read or {}
    assert hive["options"]["quote"] == ""
    assert hive["options"]["timestampFormat"] == "yyyy-MM-dd HH:mm:ss[.SSS]"
    assert (plan.decision("pq_files").read or {})["format"] == "parquet"
    refused = {
        "xls_files": "format de fichier 'excel'",
        "odd_files": "style CSV 'exotic'",
        "skip_files": "skipRowsBeforeHeader",
        "node_files": "variable non résolue dans le chemin",
    }
    for name, reason in refused.items():
        decision = plan.decision(name)
        assert decision.strategy is Strategy.REFUSED, name
        assert reason in decision.reason, (name, decision.reason)


def test_files_need_a_declared_root_and_no_path_partitions(tmp_path: Path) -> None:
    partitioned = _file_ds("part_files")
    partitioned["partitioning"]["dimensions"] = [{"name": "day", "type": "time", "params": {}}]
    uploaded = _file_ds("uploaded", ds_type="UploadedFiles", fmt="csv")
    project = _project(tmp_path, _file_ds("lost_files"), partitioned, uploaded)
    plan = plan_data(project, DataConfig(), "t")
    assert "root dans data.yaml" in plan.decision("lost_files").reason
    assert "uploads_root non déclaré" in plan.decision("uploaded").reason
    config = DataConfig.model_validate(
        {"connections": {"fs_conn": {"root": "/Volumes/c/s/v"}}, "uploads_root": "/Volumes/c/s/up"}
    )
    plan = plan_data(project, config, "t")
    assert "partitionné (motif de chemin)" in plan.decision("part_files").reason
    upload = plan.decision("uploaded")
    assert upload.strategy is Strategy.COPY
    assert (upload.read or {})["path"] == "/Volumes/c/s/up/uploaded"


def test_shared_datasets_are_migrated_with_their_project() -> None:
    plan = plan_data(fixture("DEMO_COMPLEXE"), DataConfig(), "t")
    shared = plan.decision("DEMO_SIMPLE.customer_stats")
    assert (shared.role, shared.strategy) == ("partagé", Strategy.REFUSED)
    assert "à migrer avec lui" in shared.reason


# --------------------------------------------------------------------------- #
# Names and metadata
# --------------------------------------------------------------------------- #
def test_names_rules() -> None:
    assert name_problem("orders_2020") is None
    assert "caractères interdits" in str(name_problem("my orders"))
    assert "255" in str(name_problem("x" * 256))
    assert proposed_name("My Orders.v2") == "my_orders_v2"
    assert case_collisions(["Orders", "orders", "other"]) == {
        "Orders": ["orders"],
        "orders": ["Orders"],
    }


def test_original_identifiers_are_kept_even_when_the_name_is_refused(tmp_path: Path) -> None:
    project = _project(
        tmp_path, _sql_ds("bad name"), _sql_ds("Twin"), _sql_ds("twin"), _sql_ds("good")
    )
    plan = plan_data(project, _config(pg_src={**AVAILABLE, "access": "copy"}), "t")
    bad = plan.decision("bad name")
    assert (bad.strategy, bad.uc_name, bad.read) == (Strategy.REFUSED, "bad_name", None)
    assert bad.properties["dku2dbx.dataiku.dataset"] == "bad name"
    assert "renommage à décider, proposition bad_name" in bad.reason
    assert "même nom que twin à la casse près" in str(plan.decision("Twin").name_note)
    good = plan.decision("good")
    assert good.strategy is Strategy.COPY
    assert good.properties == {
        "dku2dbx.dataiku.connection": "pg_src",
        "dku2dbx.dataiku.dataset": "good",
        "dku2dbx.dataiku.json_sha256": project.dataset("good").source_sha256,
        "dku2dbx.dataiku.project": K,
        "dku2dbx.dataiku.schema": "src",
        "dku2dbx.dataiku.table": "good",
        "dku2dbx.dataiku.type": "PostgreSQL",
        "dku2dbx.generator_version": good.properties["dku2dbx.generator_version"],
    }
    assert "- bad name : caractères interdits" in report_markdown(plan)


def test_a_produced_dataset_records_its_recipe() -> None:
    plan = plan_data(fixture(K), DataConfig(), "t")
    assert plan.decision("orders_enriched").properties["dku2dbx.dataiku.recipe"] == (
        "join_orders_customers"
    )


# --------------------------------------------------------------------------- #
# Configuration, report, CLI
# --------------------------------------------------------------------------- #
def test_configuration_validation(tmp_path: Path) -> None:
    with pytest.raises(DataConfigError, match="not found"):
        load_data_config(tmp_path / "missing.yaml")
    bad = tmp_path / "data.yaml"
    bad.write_text("connections:\n  pg:\n    federation: available\n", encoding="utf-8")
    with pytest.raises(DataConfigError, match="foreign_catalog"):
        load_data_config(bad)
    bad.write_text("connections:\n  pg:\n    federation: maybe\n", encoding="utf-8")
    with pytest.raises(DataConfigError):
        load_data_config(bad)
    config, source = load_data_config(Path("config/data.yaml"))
    assert source == "config/data.yaml"
    assert config.connections["pg_sources_exemple"].federation is not None


def test_template_values_are_never_used(tmp_path: Path) -> None:
    config, _ = load_data_config(Path("config/data.yaml"))
    project = _project(tmp_path, _file_ds("uploaded", ds_type="UploadedFiles"))
    decision = plan_data(project, config, "t").decision("uploaded")
    assert decision.strategy is Strategy.REFUSED
    assert "valeur fictive pour uploads_root" in decision.reason


def _complex_plan() -> DataPlan:
    config = _config(
        pg_sources={**AVAILABLE, "access": "copy"},
        pg_managed={"federation": "unavailable"},
        hdfs_sources={"root": "/Volumes/c/s/v/hdfs"},
    )
    return plan_data(fixture("DEMO_COMPLEXE"), config, "config/data.local.yaml")


def test_report_snapshot(snapshot: SnapshotAssertion) -> None:
    assert report_markdown(_complex_plan()) == snapshot(extension_class=MarkdownSnapshot)


def test_write_data_is_reproducible_and_drops_stale_notebooks(tmp_path: Path) -> None:
    plan = _complex_plan()
    stale = tmp_path / "notebooks" / "copie_old.py"
    stale.parent.mkdir(parents=True)
    stale.write_text("old", encoding="utf-8")
    first = {p: p.read_bytes() for p in write_data(plan, tmp_path)}
    assert not stale.exists()
    assert {p: p.read_bytes() for p in write_data(plan, tmp_path)} == first
    names = sorted(p.name for p in first)
    assert names == [
        "copie_customers_b2b.py",
        "copie_depots.py",
        "copie_routes_ref.py",
        "donnees.json",
        "donnees.md",
    ]
    data = json.loads((tmp_path / "donnees.json").read_text(encoding="utf-8"))
    assert data["decisions"][0]["properties"]["dku2dbx.dataiku.project"] == "DEMO_COMPLEXE"


def test_cli_data(tmp_path: Path) -> None:
    config = tmp_path / "data.yaml"
    config.write_text(
        "connections:\n  pg_sources:\n    federation: available\n"
        "    foreign_catalog: fed\n    access: copy\n",
        encoding="utf-8",
    )
    export = mf.DEFAULT_OUT_DIR / f"{K}.zip"
    result = CliRunner().invoke(
        app, ["data", str(export), "--data", str(config), "--out", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    assert "8 dataset(s) — 3 COPIE, 5 RECALCUL" in result.output
    assert (tmp_path / K / "donnees" / "notebooks" / "copie_customers.py").is_file()
    result = CliRunner().invoke(app, ["data", str(export), "--check-uc"])
    assert result.exit_code == 1
    assert "--principal" in result.output


# --------------------------------------------------------------------------- #
# Permissions (fake SDK client)
# --------------------------------------------------------------------------- #
class _Privilege:
    def __init__(self, value: str) -> None:
        self.privilege = value


class _Assignment:
    def __init__(self, privileges: list[str]) -> None:
        self.privileges = [_Privilege(p) for p in privileges]


class _Effective:
    def __init__(self, privileges: list[str]) -> None:
        self.privilege_assignments = [_Assignment(privileges)]


class FakeGrants:
    def __init__(self, granted: dict[str, list[str]]) -> None:
        self.granted = granted
        self.grants = self
        self.calls: list[tuple[str, str]] = []

    def get_effective(self, securable_type: Any, full_name: str, *, principal: str) -> Any:
        assert principal == "sp-dku2dbx"
        self.calls.append((str(securable_type), full_name))
        if full_name not in self.granted:
            raise PermissionError(full_name)
        return _Effective(self.granted[full_name])


def test_permissions_are_checked_with_the_sdk(tmp_path: Path) -> None:
    project = _project(tmp_path, _sql_ds("src_table"), _file_ds("vol_files"), _file_ds("abfss"))
    project.dataset("abfss").params["connection"] = "abfss_conn"
    project.dataset("abfss").connection = "abfss_conn"
    config = _config(
        pg_src={**AVAILABLE, "access": "copy"},
        fs_conn={"root": "/Volumes/land/raw/files"},
        abfss_conn={"root": "abfss://dss@account.dfs.core.windows.net/"},
    )
    plan = plan_data(project, config, "t")
    client = FakeGrants(
        {
            "fed": ["USE_CATALOG"],
            "fed.src": ["USE_SCHEMA"],
            "fed.src.src_table": ["SELECT"],
            "land.raw.files": [],
            "dev": ["ALL_PRIVILEGES"],
            "dev.mig": ["USE_SCHEMA", "CREATE_TABLE"],
        }
    )
    result = check_permissions(plan, client, principal="sp-dku2dbx", catalog="dev", schema="mig")
    assert result["src_table"] == []
    assert result["vol_files"] == ["READ_VOLUME manquant sur volume land.raw.files"]
    assert result["abfss"] == [
        "external location de abfss://dss@account.dfs.core.windows.net/DEMO_SIMPLE.abfss : "
        "READ FILES à vérifier à la main"
    ]
    assert len(client.calls) == len(set(client.calls))  # each securable read once
    assert "orders_enriched" not in result  # recomputed: nothing to read
    text = report_markdown(plan, result)
    assert "- vol_files : READ_VOLUME manquant" in text
    assert "Non vérifiées" in report_markdown(plan)


# --------------------------------------------------------------------------- #
# Copies on local PySpark
# --------------------------------------------------------------------------- #
def _run(text: str, spark: SparkSession, values: dict[str, str]) -> dict[str, Any]:
    scope: dict[str, Any] = {"spark": spark, "dbutils": FakeDbutils(values)}
    for cell in text.split("\n\n# COMMAND ----------\n\n"):
        exec(compile(cell, "<cell>", "exec"), scope)
    return scope


def _result(text: str, spark: SparkSession, **values: str) -> dict[str, Any]:
    with pytest.raises(_Exit) as done:
        _run(text, spark, {"catalog": "spark_catalog", "schema": "dku_copy", **values})
    result: dict[str, Any] = json.loads(str(done.value))
    return result


ROWS = [(1, "a", 1.5, "d1"), (2, "b", 2.5, "d1"), (3, "c", 3.5, "d2")]
SCHEMA = "order_id bigint, label string, amount double, day string"


@pytest.mark.spark
def test_copy_from_a_federated_table_runs_twice_without_duplicates(
    tmp_path: Path, spark: SparkSession
) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS src")
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_copy")
    spark.createDataFrame(ROWS, SCHEMA).write.mode("overwrite").saveAsTable("src.orders_src")
    project = _project(tmp_path, _sql_ds("orders_src"))
    config = _config(
        pg_src={"federation": "available", "foreign_catalog": "spark_catalog", "access": "copy"}
    )
    plan = plan_data(project, config, "t")
    text = copy_notebook(plan, plan.decision("orders_src"))
    assert text.startswith("# Databricks notebook source\n# Generated by dku2dbx")
    assert "from __future__" not in text
    first = _result(text, spark)
    assert (first["status"], first["problems"]) == (LOADED, [])
    assert _result(text, spark)["status"] == ALREADY_LOADED  # run again: nothing rewritten
    assert spark.table("dku_copy.orders_src").count() == 3
    props = {r[0]: r[1] for r in spark.sql("SHOW TBLPROPERTIES dku_copy.orders_src").collect()}
    assert props["dku2dbx.dataiku.dataset"] == "orders_src"
    assert props["dku2dbx.dataiku.project"] == K
    # The source changed: skip keeps the copy and says so; replace is explicit.
    spark.createDataFrame([*ROWS, (4, "d", 4.5, "d3")], SCHEMA).write.mode("overwrite").saveAsTable(
        "src.orders_src"
    )
    with pytest.raises(AssertionError, match="source modifiée depuis la copie"):
        _run(text, spark, {"catalog": "spark_catalog", "schema": "dku_copy"})
    assert _result(text, spark, mode="replace")["status"] == REPLACED
    assert spark.table("dku_copy.orders_src").count() == 4
    with pytest.raises(ValueError, match="unknown mode"):
        _run(text, spark, {"catalog": "spark_catalog", "schema": "dku_copy", "mode": "append"})
    with pytest.raises(ValueError, match="catalog and schema"):
        _run(text, spark, {})


@pytest.mark.spark
def test_partitioned_copy_only_adds_missing_partitions(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_copy")
    spec = {
        "dataset": "events",
        "target": "dku_copy.events_part",
        "columns": [
            ["order_id", "bigint"],
            ["label", "string"],
            ["amount", "double"],
            ["day", "string"],
        ],
        "partition_column": "day",
        "properties": {"dku2dbx.dataiku.dataset": "events"},
    }
    spark.sql("DROP TABLE IF EXISTS dku_copy.events_part")
    first = spark.createDataFrame(ROWS[:2], SCHEMA)
    assert load_table(spark, first, spec, "skip") == LOADED
    both = spark.createDataFrame(ROWS, SCHEMA)
    assert load_table(spark, both, spec, "skip") == PARTITIONS_ADDED
    assert load_table(spark, both, spec, "skip") == ALREADY_LOADED
    assert spark.table(spec["target"]).count() == 3
    assert validate(spark, both, spec) == []
    with_new_day = spark.createDataFrame([*ROWS, (9, "z", 9.0, "d9"), (10, "y", 1.0, None)], SCHEMA)
    problems = validate(spark, with_new_day, spec)
    assert "2 partition(s) de la source absente(s) : None, d9" in problems
    assert load_table(spark, with_new_day, spec, "skip") == PARTITIONS_ADDED
    assert load_table(spark, with_new_day, spec, "skip") == ALREADY_LOADED  # NULL is null-safe
    assert "1 ligne(s) sans partition (day vide)" in validate(spark, with_new_day, spec)


@pytest.mark.spark
def test_validation_detects_schema_type_partition_and_metadata(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_copy")
    spark.createDataFrame(
        [(1, "a", "1.5", "x")], "order_id bigint, label string, amount string, extra string"
    ).write.mode("overwrite").saveAsTable("dku_copy.wrong")
    spec = {
        "dataset": "wrong",
        "target": "dku_copy.wrong",
        "columns": [
            ["order_id", "bigint"],
            ["label", "string"],
            ["amount", "double"],
            ["day", "string"],
        ],
        "partition_column": "day",
        "properties": {"dku2dbx.dataiku.dataset": "wrong"},
    }
    source = spark.createDataFrame(ROWS, SCHEMA)
    assert validate(spark, source, spec) == [
        "colonne absente : day",
        "colonne en trop : extra",
        "type de amount : string au lieu de double",
        "colonne de partition absente : day",
        "1 ligne(s) dans la table, 3 dans la source (source modifiée depuis la copie ? "
        "relancer avec mode=replace si c'est voulu)",
        "métadonnées Dataiku absentes : dku2dbx.dataiku.dataset",
    ]


@pytest.mark.spark
def test_conversion_never_loses_values_silently(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("1", "x"), ("oops", "y"), (None, "z")], "order_id string, label string"
    )
    spec = {"dataset": "t", "columns": [["order_id", "bigint"], ["label", "string"]]}
    with pytest.raises(ValueError, match=r"do not convert to the Dataiku type.*'order_id': 1"):
        conform(df, spec)
    ok = conform(df.where("order_id <> 'oops' OR order_id IS NULL"), spec)
    assert ok.schema.simpleString() == "struct<order_id:bigint,label:string>"
    with pytest.raises(ValueError, match=r"columns missing in the source: \['amount'\]"):
        conform(df, {"dataset": "t", "columns": [["amount", "double"]]})


@pytest.mark.spark
def test_copy_of_dataiku_csv_files(tmp_path: Path, spark: SparkSession) -> None:
    folder = tmp_path / "dss" / f"{K}.events"
    folder.mkdir(parents=True)
    lines = '1\ta\t1.5\td1\n2\t"b\tc"\t2.5\td2\n3\t\t3.5\td3\n'
    (folder / "out-s0.csv").write_text(lines, encoding="utf-8")
    project = _project(tmp_path, _file_ds("events", path=f"/{K}.events"))
    plan = plan_data(project, _config(fs_conn={"root": str(tmp_path / "dss")}), "t")
    decision = plan.decision("events")
    text = copy_notebook(plan, decision)
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_copy")
    spark.sql("DROP TABLE IF EXISTS dku_copy.events")
    result = _result(text, spark)
    assert (result["status"], result["problems"]) == (LOADED, [])
    rows = sorted(tuple(r) for r in spark.table("dku_copy.events").collect())
    # An empty value becomes NULL, even for a string (pitfall documented, phase 11).
    assert rows == [(1, "a", 1.5, "d1"), (2, "b\tc", 2.5, "d2"), (3, None, 3.5, "d3")]
    # A malformed file fails the copy (FAILFAST) instead of becoming NULLs.
    (folder / "out-s1.csv").write_text("x\ty\tnot-a-number\td3\n", encoding="utf-8")
    spark.sql("DROP TABLE dku_copy.events")
    with pytest.raises(Exception, match=r"(?i)malformed|parse|FAILFAST"):
        _run(text, spark, {"catalog": "spark_catalog", "schema": "dku_copy"})
    assert notebook_name(decision) == "copie_events.py"


@pytest.mark.spark
def test_an_unreadable_source_fails_clearly(tmp_path: Path, spark: SparkSession) -> None:
    project = _project(tmp_path, _sql_ds("absent_table"))
    config = _config(
        pg_src={"federation": "available", "foreign_catalog": "spark_catalog", "access": "copy"}
    )
    plan = plan_data(project, config, "t")
    text = copy_notebook(plan, plan.decision("absent_table"))
    with pytest.raises(RuntimeError, match="source of absent_table unreadable"):
        _run(text, spark, {"catalog": "spark_catalog", "schema": "dku_copy"})


def test_only_copies_get_a_notebook() -> None:
    plan = plan_data(fixture(K), DataConfig(), "t")
    with pytest.raises(ValueError, match="nothing to copy"):
        copy_notebook(plan, plan.decision("orders_enriched"))


# --------------------------------------------------------------------------- #
# Bundle: copies replace the failing ingestion tasks; tables keep Dataiku metadata
# --------------------------------------------------------------------------- #
def test_bundle_copies_unbranched_sources_and_keeps_dataiku_metadata(tmp_path: Path) -> None:
    config = tmp_path / "data.yaml"
    config.write_text(
        "connections:\n  pg_sources:\n    federation: available\n"
        "    foreign_catalog: fed\n    access: copy\n",
        encoding="utf-8",
    )
    mapping = tmp_path / "connections.yaml"
    mapping.write_text("connections: {}\n", encoding="utf-8")  # nothing branched
    export = mf.DEFAULT_OUT_DIR / f"{K}.zip"
    out = tmp_path / "bundle"
    args = ["generate", str(export), "--out", str(out), "--connections", str(mapping)]
    result = CliRunner().invoke(app, [*args, "--data", str(config)])
    assert result.exit_code == 0, result.output
    assert "Sources copiées par le job (phase 11) : customers, orders_archive, orders_raw" in (
        result.output
    )
    ingest = sorted(p.name for p in (out / "src").glob("ingest_*"))
    assert ingest == ["ingest_01.py", "ingest_02.py", "ingest_03.py"]
    assert "copy_dataset(spark, SPEC, MODE)" in (out / "src" / "ingest_01.py").read_text()
    jobs = (out / "resources" / "jobs.yml").read_text(encoding="utf-8")
    assert "../src/ingest_01.py" in jobs
    assert "../src/ingest_01.sql" not in jobs
    generation = (out / "GENERATION.md").read_text(encoding="utf-8")
    assert "- customers : copie (notebook" in generation
    step = (out / "src" / "join_orders_customers.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE `orders_enriched` SET TBLPROPERTIES (" in step
    assert "'dku2dbx.dataiku.recipe'='join_orders_customers'" in step
    # Without a data configuration, the ingestion tasks still fail explicitly (phase 7).
    empty = tmp_path / "empty.yaml"
    empty.write_text("{}\n", encoding="utf-8")
    result = CliRunner().invoke(app, [*args, "--data", str(empty)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in (out / "src").glob("ingest_*"))[0] == "ingest_01.sql"
    assert "raise_error" in (out / "src" / "ingest_01.sql").read_text(encoding="utf-8")


def test_pipeline_views_and_python_steps_keep_dataiku_metadata() -> None:
    from dku2dbx.generate.bundle import generate_job_bundle  # noqa: PLC0415

    bundle = generate_job_bundle(fixture(K), target="pipeline")
    view = bundle.files["src/pipeline/join_orders_customers.sql"]
    assert "TBLPROPERTIES (\n  'dku2dbx.dataiku.connection'='pg_managed'," in view
    complex_bundle = generate_job_bundle(fixture("DEMO_COMPLEXE"))
    python = complex_bundle.files["src/python_scoring.py"]
    assert 'spark.sql("ALTER TABLE `customer_scores` SET TBLPROPERTIES (' in python


@pytest.mark.spark
def test_a_column_name_delta_refuses_gets_column_mapping(
    tmp_path: Path, spark: SparkSession
) -> None:
    """Seen on a real export (Dataiku Cloud tutorial): a column named "Adj Close"."""
    cols = mf.columns("order_id:bigint", "Adj Close:double")
    ds = _sql_ds("prices", cols=cols)
    project = _project(tmp_path, ds)
    plan = plan_data(project, _config(pg_src=LOCAL_COPY), "t")
    decision = plan.decision("prices")
    assert decision.column_mapping is True
    assert plan.decision("customers").column_mapping is False
    spark.sql("CREATE DATABASE IF NOT EXISTS src")
    spark.createDataFrame([(1, 2.5)], "order_id bigint, `Adj Close` double").write.mode(
        "overwrite"
    ).saveAsTable("src.prices")
    spark.sql("DROP TABLE IF EXISTS dku_copy.prices")
    spark.sql("CREATE DATABASE IF NOT EXISTS dku_copy")
    result = _result(copy_notebook(plan, decision), spark)
    assert (result["status"], result["problems"]) == (LOADED, [])
    props = {r[0]: r[1] for r in spark.sql("SHOW TBLPROPERTIES dku_copy.prices").collect()}
    assert props["delta.columnMapping.mode"] == "name"
    assert [tuple(r) for r in spark.table("dku_copy.prices").collect()] == [(1, 2.5)]
