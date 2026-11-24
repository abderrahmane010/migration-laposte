"""Phase 10: the app journey (session module, then the Streamlit pages with AppTest)."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from dku2dbx.webapp import session as journey
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR

APP = Path(__file__).parents[1] / "app" / "app.py"
SIMPLE = (DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip").read_bytes()
COMPLEX = (DEFAULT_OUT_DIR / "DEMO_COMPLEXE.zip").read_bytes()
PG = {"connection": "pg_sources", "catalog": "cat", "schema": "src", "table": "{table}"}


def _names(archive: bytes) -> set[str]:
    return set(zipfile.ZipFile(io.BytesIO(archive)).namelist())


# --------------------------------------------------------------------------- #
# Session module
# --------------------------------------------------------------------------- #
def test_open_export_checks_the_upload(tmp_path: Path) -> None:
    with pytest.raises(journey.SessionError, match="pas une archive ZIP"):
        journey.open_export(b"not a zip", "x.zip", tmp_path)
    session = journey.open_export(SIMPLE, "DEMO_SIMPLE.zip", tmp_path / "p")
    assert session.project.key == "DEMO_SIMPLE"
    assert session.export.read_bytes() == SIMPLE


def test_open_export_refuses_large_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(journey, "MAX_EXPORT_BYTES", 10)
    with pytest.raises(journey.SessionError, match="plus de"):
        journey.open_export(SIMPLE, "x.zip", tmp_path)


def test_full_journey_on_demo_simple(tmp_path: Path) -> None:
    session = journey.open_export(SIMPLE, "DEMO_SIMPLE.zip", tmp_path)
    result, report = journey.analysis(session)
    assert sum(result.recipes_by_class.values()) == len(session.project.recipes)
    assert "DEMO_SIMPLE" in report
    flow, report = journey.simplification(session)
    assert len(flow.steps) == 3 and report
    assert journey.sources_by_connection(session) == {
        "pg_sources": ["customers", "orders_archive", "orders_raw"]
    }
    branching, report = journey.connect(session, [PG])
    assert branching.resolved == {
        "customers": "cat.src.customers",
        "orders_archive": "cat.src.orders_2020",  # {table}: the Dataiku table name
        "orders_raw": "cat.src.DEMO_SIMPLE_orders_raw",  # ${projectKey} resolved
    }
    assert branching.unresolved == {} and "Branchées" in report
    migration = journey.migrate(session)
    names = _names(migration.archive)
    assert {"bundle/databricks.yml", "bundle/resources/jobs.yml", "bundle/GENERATION.md"} <= names
    assert "residue/RESIDU.md" in names
    archive = zipfile.ZipFile(io.BytesIO(migration.archive))
    assert not any("/src/ingest" in n for n in names)  # every source is branched
    assert "`cat`.`src`.`customers`" in archive.read("bundle/src/step_01.sql").decode()
    assert migration.archive == journey.migrate(session).archive  # reproducible
    tables = journey.reconcile_tables(session)
    assert tables == ["orders_enriched", "customer_stats", "orders_ranked"]
    rules = [{"table": t, "key": "", "abs_tolerance": 0.0, "rel_tolerance": 0.0} for t in tables]
    rules[1] = {**rules[1], "key": "customer_id", "rel_tolerance": 0.001}
    rules[2] = {**rules[2], "skip": True}
    files = journey.reconcile(session, rules)
    assert [s["dataset"] for s in files.plan.specs] == ["orders_enriched", "customer_stats"]
    assert files.plan.specs[1]["key"] == ["customer_id"]
    assert _names(files.archive) == {"reconcile.py", "RECONCILIATION.md", "ECARTS_ACCEPTES.md"}
    assert files.notebook.startswith("# Databricks notebook source")


def test_rows_without_catalog_stay_unbranched(tmp_path: Path) -> None:
    session = journey.open_export(COMPLEX, "DEMO_COMPLEXE.zip", tmp_path)
    branching, _ = journey.connect(session, [PG, {"connection": "hdfs_sources", "catalog": " "}])
    assert "parcels_events" in branching.unresolved
    assert branching.resolved["depots"] == "cat.src.depots"


def test_llm_endpoint_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    assert not journey.llm_config().enabled
    monkeypatch.setenv(journey.LLM_ENDPOINT_ENV, "validated-endpoint")
    config = journey.llm_config()
    assert config.enabled and config.provider == "databricks"


def test_no_volume_no_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(journey.EXPORTS_VOLUME_ENV, raising=False)
    assert journey.store_export(SIMPLE, "x.zip") is None


# --------------------------------------------------------------------------- #
# Streamlit pages (AppTest runs the script headless)
# --------------------------------------------------------------------------- #
def _app() -> AppTest:
    at = AppTest.from_file(str(APP), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    return at


def _go(at: AppTest, step: str) -> AppTest:
    at.sidebar.radio(key="step").set_value(step).run()
    assert not at.exception, at.exception
    return at


def test_pages_ask_for_an_export_first() -> None:
    at = _app()
    for step in ("2. Analyser", "4. Brancher", "5. Migrer", "6. Réconcilier"):
        _go(at, step)
        assert "Déposez d'abord un export ZIP" in at.info[0].value


def test_app_journey_on_demo_simple() -> None:
    at = _app()
    at.file_uploader[0].upload("DEMO_SIMPLE.zip", SIMPLE, "application/zip").run()
    assert not at.exception, at.exception
    assert "Projet **DEMO_SIMPLE**" in at.success[0].value

    _go(at, "2. Analyser")
    assert [m.label for m in at.metric] == [
        "Recettes",
        "Niveau",
        "Effort estimé",
        "Points à examiner",
    ]
    _go(at, "3. Simplifier")
    assert at.metric[1].value == "3"  # steps

    _go(at, "4. Brancher")
    at.session_state["connections_editor"] = {
        "edited_rows": {0: {"catalog": "cat", "schema": "src"}},
        "added_rows": [],
        "deleted_rows": [],
    }
    at.button[0].click().run()
    assert not at.exception, at.exception
    assert at.success[0].value == "3 source(s) branchée(s), 0 à brancher."

    _go(at, "5. Migrer")
    assert not at.warning  # sources are branched
    at.button[0].click().run()
    assert not at.exception, at.exception
    assert at.metric[0].label == "Étapes" and at.metric[0].value == "3"
    assert at.download_button[0].label == "Télécharger le bundle (ZIP)"

    _go(at, "6. Réconcilier")
    at.button[0].click().run()
    assert not at.exception, at.exception
    assert at.download_button[0].label.startswith("Télécharger le notebook")
    assert any("Plan de réconciliation — DEMO_SIMPLE" in m.value for m in at.markdown)


def test_app_reports_an_unreadable_export() -> None:
    at = _app()
    at.file_uploader[0].upload("bad.zip", b"not a zip", "application/zip").run()
    assert not at.exception, at.exception
    assert at.error[0].value.startswith("Export illisible")
