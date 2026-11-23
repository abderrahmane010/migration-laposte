"""Phase 3: Sync and Sample/Filter translators, the translation engine and CLI."""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.single_file import SingleFileSnapshotExtension, WriteMode
from typer.testing import CliRunner

from dku2dbx.analyze.classify import classify
from dku2dbx.cli import app
from dku2dbx.model.ir import Project
from dku2dbx.reader.zip_reader import read_export
from dku2dbx.translate.engine import header, translate_project, write_translation
from dku2dbx.translate.visual import TRANSLATORS
from tests.fixtures import make_fixture as mf
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR


class SqlSnapshot(SingleFileSnapshotExtension):
    _write_mode = WriteMode.TEXT
    file_extension = "sql"


def fixture(key: str) -> Project:
    return read_export(DEFAULT_OUT_DIR / f"{key}.zip")


def read_files(tmp_path: Path, files: dict[str, bytes]) -> Project:
    path = tmp_path / "export.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for entry, data in sorted(files.items()):
            zf.writestr(entry, data)
    return read_export(path)


def _project(tmp_path: Path, rtype: str, payload: Any, **params: Any) -> Project:
    """DEMO_SIMPLE plus one recipe orders_raw -> copy (output keeps 3 columns, reordered)."""
    p = mf.build_demo_simple()
    p.add_dataset(
        mf.dataset(
            p.key, "copy", cols=mf.columns("status:string", "order_id:bigint", "amount:double")
        )
    )
    rcp = mf.recipe(p.key, "copy_orders", rtype, [mf.ref("orders_raw")], [mf.out_ref("copy")])
    rcp["params"].update(params)
    p.add_recipe(rcp, payload)
    return read_files(tmp_path, p.files())


def _one(project: Project, name: str = "copy_orders") -> Any:
    result = translate_project(project)
    found = [t for t in result.translated if t.recipe == name]
    missing = [u for u in result.unsupported if u.recipe == name]
    return found[0] if found else missing[0]


SELECTION = {
    "samplingMethod": "HEAD_SEQUENTIAL",
    "maxRecords": 2,
    "partitionSelectionMethod": "ALL",
    "filter": mf.no_filter(),
    "ordering": {"enabled": False, "rules": []},
}


# --------------------------------------------------------------------------- #
# Sync
# --------------------------------------------------------------------------- #
def test_sync_sql_snapshot(snapshot: SnapshotAssertion) -> None:
    [tr] = [t for t in translate_project(fixture("DEMO_COMPLEXE")).translated if t.type == "sync"]
    assert tr.outputs[0].sql + "\n" == snapshot.use_extension(SqlSnapshot)
    assert tr.warnings == [
        "dépendance de partition de l'entrée 'parcels_events' non traduite ici (phase 6)"
    ]


def test_sync_keeps_output_columns_in_output_order(tmp_path: Path) -> None:
    tr = _one(_project(tmp_path, "sync", {}, schemaMode="STRICT_SYNC"))
    assert tr.outputs[0].sql == "SELECT\n  `status`,\n  `order_id`,\n  `amount`\nFROM `orders_raw`"


@pytest.mark.spark
def test_sync_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    tr = _one(_project(tmp_path, "sync", {}, schemaMode="STRICT_SYNC"))
    rows = [(1, 10, "2026-01-01", 5.0, "NEW"), (2, 11, "2026-01-02", 7.5, "DONE")]
    spark.createDataFrame(
        rows, "order_id long, customer_id long, order_date string, amount double, status string"
    ).createOrReplaceTempView("orders_raw")
    got = [tuple(r) for r in spark.sql(tr.outputs[0].sql).collect()]
    assert sorted(got) == [("DONE", 2, 7.5), ("NEW", 1, 5.0)]


def test_sync_unknown_schema_mode(tmp_path: Path) -> None:
    item = _one(_project(tmp_path, "sync", {}, schemaMode="FREE_SCHEMA"))
    assert (item.element, item.detail) == ("schemaMode", "'FREE_SCHEMA' is not mapped")


def test_sync_output_column_missing_from_input(tmp_path: Path) -> None:
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "copy", cols=mf.columns("order_id:bigint", "ghost:string")))
    p.add_recipe(
        mf.recipe(p.key, "copy_orders", "sync", [mf.ref("orders_raw")], [mf.out_ref("copy")]), {}
    )
    item = _one(read_files(tmp_path, p.files()))
    assert item.element == "schema"
    assert "ghost" in item.detail


# --------------------------------------------------------------------------- #
# Sample/Filter
# --------------------------------------------------------------------------- #
def test_head_sampling_is_a_limit_with_a_warning(tmp_path: Path) -> None:
    tr = _one(_project(tmp_path, "sampling", mf.no_filter(), selection=SELECTION))
    assert tr.outputs[0].sql.endswith("FROM `orders_raw`\nLIMIT 2")
    assert any("premières lignes" in w for w in tr.warnings)


@pytest.mark.spark
def test_head_sampling_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    tr = _one(_project(tmp_path, "sampling", mf.no_filter(), selection=SELECTION))
    spark.createDataFrame(
        [(i, i, "2026-01-01", 1.0, "NEW") for i in range(5)],
        "order_id long, customer_id long, order_date string, amount double, status string",
    ).createOrReplaceTempView("orders_raw")
    assert len(spark.sql(tr.outputs[0].sql).collect()) == 2


@pytest.mark.parametrize(
    ("payload", "selection", "element"),
    [
        (mf.formula_filter("amount > 0"), SELECTION, "filter"),
        (mf.no_filter(), {**SELECTION, "filter": {"enabled": True}}, "selection.filter"),
        (mf.no_filter(), {**SELECTION, "samplingMethod": "RANDOM_FIXED_NB"}, "samplingMethod"),
        (mf.no_filter(), {**SELECTION, "ordering": {"enabled": True}}, "selection.ordering"),
        (
            mf.no_filter(),
            {**SELECTION, "partitionSelectionMethod": "LATEST_N"},
            "partitionSelectionMethod",
        ),
    ],
)
def test_sampling_never_guesses(
    tmp_path: Path, payload: Any, selection: dict[str, Any], element: str
) -> None:
    item = _one(_project(tmp_path, "sampling", payload, selection=selection))
    assert item.element == element


# --------------------------------------------------------------------------- #
# Engine, header, CLI, classification
# --------------------------------------------------------------------------- #
def test_header_traces_the_source() -> None:
    project = fixture("DEMO_COMPLEXE")
    tr = next(t for t in translate_project(project).translated if t.type == "sync")
    recipe = project.recipe(tr.recipe)
    text = header(project, recipe, tr, tr.outputs[0])
    assert "-- Dataiku project: DEMO_COMPLEXE" in text
    assert "-- Dataiku recipe: sync_events (sync)" in text
    assert f"-- Recipe JSON SHA-256: {recipe.source_sha256}" in text
    assert "-- Payload SHA-256: " in text
    assert "-- WARNING: dépendance de partition" in text


def test_every_recipe_is_accounted_for() -> None:
    project = fixture("DEMO_COMPLEXE")
    result = translate_project(project)
    names = (
        {t.recipe for t in result.translated}
        | {u.recipe for u in result.unsupported}
        | set(result.without_translator)
    )
    assert names == {r.name for r in project.recipes}


def test_written_files_are_reproducible(tmp_path: Path) -> None:
    project = fixture("DEMO_COMPLEXE")
    result = translate_project(project)
    first = write_translation(project, result, tmp_path / "a")
    second = write_translation(project, result, tmp_path / "b")
    expected = sorted(f"{t.recipe}.sql" for t in result.translated)
    assert [p.name for p in first] == [*expected, "translate_report.json"]
    for a, b in zip(first, second, strict=True):
        assert a.read_bytes() == b.read_bytes()
    sync = tmp_path / "a" / "DEMO_COMPLEXE" / "sql" / "sync_events.sql"
    assert sync.read_text().endswith("FROM `parcels_events`;\n")


def test_cli_translate(tmp_path: Path) -> None:
    export = str(DEFAULT_OUT_DIR / "DEMO_COMPLEXE.zip")
    result = CliRunner().invoke(app, ["translate", export, "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    covered = sum(r.type in TRANSLATORS for r in fixture("DEMO_COMPLEXE").recipes)
    assert f"{covered} recette(s) traduite(s), 0 non prise(s) en charge" in result.output
    assert (tmp_path / "DEMO_COMPLEXE" / "sql" / "sync_events.sql").is_file()


def test_classification_sees_the_new_translators() -> None:
    project = fixture("DEMO_COMPLEXE")
    available = {c.name for c in classify(project) if c.translator_available}
    assert "sync_events" in available
    assert available == {
        r.name for r in project.recipes if r.type in TRANSLATORS and r.name != "prepare_depots"
    }
