"""Phase 8: SQL query recipes, transpiled deterministically (snapshot, PySpark, refusals)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from dku2dbx.generate.bundle import generate_job_bundle
from dku2dbx.model.ir import Project
from dku2dbx.translate.engine import TO_REVIEW, translate_project, write_translation
from dku2dbx.translate.model import Translation, Unsupported
from tests.fixtures import make_fixture as mf
from tests.test_translate_sync import SqlSnapshot, fixture, read_files

NAME = "sql_anomaly_report"
OUT_COLS = "depot_id:int,depot_name:string,anomalies:bigint,last_anomaly_ts:date"


def _result(project: Project, name: str = NAME) -> Translation | Unsupported:
    result = translate_project(project)
    for item in [*result.translated, *result.unsupported]:
        if item.recipe == name:
            return item
    raise AssertionError(name)


def _translated(project: Project) -> Translation:
    item = _result(project)
    assert isinstance(item, Translation), item
    return item


def _refused(project: Project) -> Unsupported:
    item = _result(project)
    assert isinstance(item, Unsupported), item
    return item


def _with_sql(
    tmp_path: Path,
    sql: str,
    out_cols: str = OUT_COLS,
    variables: dict[str, Any] | None = None,
    input_type: str = "PostgreSQL",
) -> Project:
    """DEMO_SIMPLE plus two inputs and an SQL recipe running ``sql``."""
    p = mf.build_demo_simple()
    p.variables.update(variables or {})
    cols = mf.columns("depot_id:int", "event_ts:date", "status:string")
    p.add_dataset(mf.dataset(p.key, "anomalies_clean", cols=cols, ds_type=input_type))
    depots = mf.columns("depot_id:int", "depot_name:string")
    p.add_dataset(mf.dataset(p.key, "depots_clean", cols=depots, ds_type=input_type))
    p.add_dataset(mf.dataset(p.key, "report", cols=mf.columns(*out_cols.split(","))))
    rcp = mf.recipe(
        p.key,
        NAME,
        "sql_query",
        [mf.ref("anomalies_clean"), mf.ref("depots_clean")],
        [mf.out_ref("report")],
    )
    p.add_recipe(rcp, sql)
    return read_files(tmp_path, p.files())


REPORT = """\
SELECT a.depot_id, d.depot_name, COUNT(*) AS anomalies, MAX(a.event_ts) AS last_anomaly_ts
FROM ${projectKey}_anomalies_clean a
LEFT JOIN dku_${projectKey}.${projectKey}_depots_clean d ON a.depot_id = d.depot_id
WHERE a.event_ts >= '${start_date}' AND a.status <> 'OK'
GROUP BY a.depot_id, d.depot_name
"""


def test_fixture_sql_snapshot(snapshot: SnapshotAssertion) -> None:
    tr = _translated(fixture("DEMO_COMPLEXE"))
    assert tr.review
    assert tr.warnings == ["SQL transpilé depuis postgres par sqlglot : à relire"]
    assert tr.outputs[0].sql + "\n" == snapshot.use_extension(SqlSnapshot)


def test_written_file_is_flagged_to_review(tmp_path: Path) -> None:
    project = fixture("DEMO_COMPLEXE")
    paths = write_translation(project, translate_project(project), tmp_path)
    [path] = [p for p in paths if p.name == f"{NAME}.sql"]
    assert f"-- {TO_REVIEW}" in path.read_text(encoding="utf-8")


@pytest.mark.spark
def test_report_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    project = _with_sql(tmp_path, REPORT, variables={"start_date": "2026-01-01"})
    tr = _translated(project)
    assert "'2026-01-01'" in tr.outputs[0].sql
    spark.createDataFrame(
        [
            (1, "2026-01-02 10:00:00", "KO"),
            (1, "2026-01-03 10:00:00", "KO"),
            (1, "2026-01-04 10:00:00", "OK"),  # not an anomaly
            (2, "2025-12-31 10:00:00", "KO"),  # before start_date
            (3, "2026-02-01 08:00:00", "LATE"),  # no depot
        ],
        "depot_id int, event_ts string, status string",
    ).selectExpr(
        "depot_id", "CAST(event_ts AS TIMESTAMP) AS event_ts", "status"
    ).createOrReplaceTempView("anomalies_clean")
    spark.createDataFrame(
        [(1, "Lyon"), (2, "Lille")], "depot_id int, depot_name string"
    ).createOrReplaceTempView("depots_clean")
    rows = spark.sql(tr.outputs[0].sql).selectExpr("*", "CAST(last_anomaly_ts AS STRING) AS ts")
    got = sorted((r[0], r[1], r[2], r["ts"]) for r in rows.collect())
    # Computed by hand.
    assert got == [(1, "Lyon", 2, "2026-01-03 10:00:00"), (3, None, 1, "2026-02-01 08:00:00")]


def test_variables_become_job_parameters_in_the_bundle(tmp_path: Path) -> None:
    sql = REPORT.replace("<> 'OK'", "<> 'OK' AND a.depot_id < ${max_depot}")
    project = _with_sql(tmp_path, sql, variables={"start_date": "2026-01-01", "max_depot": 10})
    bundle = generate_job_bundle(project)
    [step] = [g for g in bundle.steps if NAME in g.step.recipes]
    assert step.translated
    notebook = bundle.files[f"src/{step.step.id}.sql"]
    assert ">= :dku_start_date" in notebook
    assert "< CAST(:dku_max_depot AS BIGINT)" in notebook
    assert f"{TO_REVIEW} ({NAME})" in notebook


def test_number_variable_and_glued_variable(tmp_path: Path) -> None:
    sql = (
        "SELECT a.depot_id, d.depot_name, COUNT(*) AS anomalies, "
        "MAX(a.event_ts) AS last_anomaly_ts FROM ${projectKey}_anomalies_clean a "
        "JOIN ${env}_depots_clean d ON a.depot_id = d.depot_id "
        "WHERE a.depot_id > ${min_depot} GROUP BY a.depot_id, d.depot_name"
    )
    project = _with_sql(tmp_path, sql, variables={"min_depot": 3, "env": "DEMO_SIMPLE"})
    out = _translated(project).outputs[0].sql
    assert "a.depot_id > 3" in out
    assert "JOIN `depots_clean` AS `d`" in out


def test_integer_division_is_reported(tmp_path: Path) -> None:
    sql = REPORT.replace("COUNT(*) AS anomalies", "COUNT(*) / 2 AS anomalies")
    tr = _translated(_with_sql(tmp_path, sql, variables={"start_date": "x"}))
    assert any(w.startswith("division : en postgres") for w in tr.warnings)


def test_select_star_is_not_checked_but_reported(tmp_path: Path) -> None:
    sql = "SELECT * FROM ${projectKey}_depots_clean"
    tr = _translated(_with_sql(tmp_path, sql, out_cols="depot_id:int,depot_name:string"))
    assert any("SELECT *" in w for w in tr.warnings)


def test_databricks_input_is_not_transpiled(tmp_path: Path) -> None:
    sql = "SELECT depot_id, depot_name FROM ${projectKey}_depots_clean"
    project = _with_sql(
        tmp_path, sql, out_cols="depot_id:int,depot_name:string", input_type="Databricks"
    )
    assert _translated(project).warnings == []


@pytest.mark.parametrize(
    ("sql", "element", "detail"),
    [
        ("SELECT * FROM other_table", "table", "other_table"),
        ("SELECT * FROM x.${projectKey}_depots_clean", "table", "x."),
        ("SELECT 1; SELECT 2", "sql", "one SELECT"),
        ("INSERT INTO t SELECT 1", "sql", "one SELECT"),
        ("SELECT FROM WHERE (", "sql", "cannot be parsed"),
        ("SELECT my_udf(depot_id) FROM ${projectKey}_depots_clean", "function", "MY_UDF"),
        ("SELECT * FROM t WHERE d = '${unknown}'", "variable", "cannot be resolved"),
        ("SELECT * FROM t WHERE d = 'since ${start_date}'", "variable", "longer string"),
        ("SELECT * FROM t WHERE d = ${start_date}", "variable", "outside quotes"),
        ("SELECT depot_id AS id FROM ${projectKey}_depots_clean", "output schema", "differ"),
        ("", "payload", "empty"),
    ],
)
def test_refusals(tmp_path: Path, sql: str, element: str, detail: str) -> None:
    out_cols = "depot_id:int" if element == "output schema" else OUT_COLS
    project = _with_sql(tmp_path, sql, out_cols=out_cols, variables={"start_date": "2026-01-01"})
    refused = _refused(project)
    assert refused.element == element
    assert detail.lower() in refused.detail.lower()


def test_unmapped_input_type_is_refused(tmp_path: Path) -> None:
    project = _with_sql(tmp_path, "SELECT 1", input_type="HDFS")
    refused = _refused(project)
    assert (refused.element, refused.detail) == (
        "connection",
        "dataset type HDFS has no mapped dialect",
    )
