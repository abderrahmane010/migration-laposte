"""Phase 7: branch the Dataiku sources onto Unity Catalog tables."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from dku2dbx.cli import app
from dku2dbx.connect.report import markdown
from dku2dbx.connect.resolve import (
    ConnectionsConfig,
    ConnectionsError,
    branch_sources,
    load_connections,
)
from dku2dbx.connect.uc_check import check_tables
from dku2dbx.generate.bundle import generate_job_bundle
from dku2dbx.reader.zip_reader import ZipExportReader
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR
from tests.test_translate_sync import fixture

MAPPING: dict[str, Any] = {
    "connections": {
        "pg_sources": {"catalog": "lab"},  # schema: the Dataiku one
        "hdfs_sources": {"catalog": "lab", "schema": "raw", "table": "{table}"},
    },
    "shared_projects": {"DEMO_SIMPLE": {"catalog": "lab", "schema": "demo_simple"}},
}


def branch(key: str, mapping: dict[str, Any], instance: dict[str, Any] | None = None) -> Any:
    project = ZipExportReader(DEFAULT_OUT_DIR / f"{key}.zip", instance).read()
    return branch_sources(project, ConnectionsConfig.model_validate(mapping), "test")


def test_sources_are_resolved_through_their_connection() -> None:
    result = branch("DEMO_SIMPLE", MAPPING)
    assert result.resolved == {
        "customers": "lab.crm.customers",
        "orders_archive": "lab.sales_archive.orders_2020",
        "orders_raw": "lab.sales.DEMO_SIMPLE_orders_raw",  # table ${projectKey}_orders_raw
    }
    assert result.unresolved == {}


def test_unresolved_variable_and_instance_variables() -> None:
    result = branch("DEMO_COMPLEXE", MAPPING)
    assert result.unresolved == {
        "parcels_events": "variable non résolue dans la table Dataiku (${NODE_DC}_parcels_events)"
    }
    resolved = branch("DEMO_COMPLEXE", MAPPING, {"NODE_DC": "paris"})
    assert resolved.resolved["parcels_events"] == "lab.raw.paris_parcels_events"
    assert resolved.partitioned == ["parcels_events"]  # needs its date column (H-53)


def test_a_variable_the_template_does_not_use_does_not_block() -> None:
    mapping = {
        **MAPPING,
        "connections": {
            **MAPPING["connections"],
            "hdfs_sources": {
                "catalog": "lab",
                "schema": "raw",
                "table": "{dataset}",
                "lowercase": True,
            },
        },
    }
    assert branch("DEMO_COMPLEXE", mapping).resolved["parcels_events"] == "lab.raw.parcels_events"


def test_unknown_connection_and_shared_project() -> None:
    result = branch("DEMO_COMPLEXE", {"connections": {}})
    assert result.unresolved["depots"] == "connexion « pg_sources » absente du mapping"
    assert result.unresolved["DEMO_SIMPLE.customer_stats"] == (
        "dataset partagé du projet DEMO_SIMPLE, absent de shared_projects"
    )
    assert branch("DEMO_COMPLEXE", MAPPING).resolved["DEMO_SIMPLE.customer_stats"] == (
        "lab.demo_simple.customer_stats"
    )


def test_fictitious_values_are_never_used() -> None:
    config, _ = load_connections(Path("config/connections.yaml"))
    project = fixture("DEMO_SIMPLE")
    result = branch_sources(project, config, "x")
    assert result.resolved == {}  # the shared file only holds fictitious examples
    mapping = {"connections": {"pg_sources": {"catalog": "[CATALOGUE_SOURCES]"}}}
    assert "valeur fictive" in branch("DEMO_SIMPLE", mapping).unresolved["customers"]


def test_uploaded_files_go_to_a_volume(tmp_path: Path) -> None:
    from tests.fixtures import make_fixture as mf  # noqa: PLC0415
    from tests.test_translate_sync import read_files  # noqa: PLC0415

    p = mf.build_demo_simple()
    next(d for d in p.datasets if d["name"] == "customers")["type"] = "UploadedFiles"
    result = branch_sources(read_files(tmp_path, p.files()), ConnectionsConfig(), "x")
    assert "Volume Unity Catalog" in result.unresolved["customers"]


def test_local_file_first_and_invalid_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "connections.yaml").write_text("connections: {}\n")
    (tmp_path / "config" / "connections.local.yaml").write_text(yaml.safe_dump(MAPPING))
    monkeypatch.chdir(tmp_path)
    config, source = load_connections()
    assert source == "config/connections.local.yaml" and "pg_sources" in config.connections
    (tmp_path / "bad.yaml").write_text("connections: {x: {catalogue: lab}}\n")
    with pytest.raises(ConnectionsError, match="invalid connections file"):
        load_connections(tmp_path / "bad.yaml")
    with pytest.raises(ConnectionsError, match="not found"):
        load_connections(tmp_path / "absent.yaml")


def test_report_shows_unbranched_sources_dashed() -> None:
    project = fixture("DEMO_COMPLEXE")
    result = branch_sources(project, ConnectionsConfig.model_validate(MAPPING), "test")
    text = markdown(project, result)
    assert "| parcels_events | variable non résolue" in text
    assert "class n_parcels_events todo" in text
    assert "n_parcels_events -.-> r_n_sync_events" in text
    assert "n_depots --> r_n_distinct_depots" in text
    assert markdown(project, result) == text  # reproducible


# --------------------------------------------------------------------------- #
# Bundle: qualified sources, failing ingestion tasks
# --------------------------------------------------------------------------- #
def test_bundle_reads_branched_sources_and_waits_for_ingestion() -> None:
    project = fixture("DEMO_COMPLEXE")
    branching = branch_sources(project, ConnectionsConfig.model_validate(MAPPING), "test")
    for target in ("job", "pipeline"):
        bundle = generate_job_bundle(project, target=target, branching=branching)
        sql = "\n".join(t for p, t in bundle.files.items() if p.endswith(".sql"))
        assert "FROM `lab`.`ref`.`depots` AS `depots`" in sql
        assert "raise_error('dku2dbx: source parcels_events is not branched yet" in sql
        job = yaml.safe_load(bundle.files["resources/jobs.yml"])["resources"]["jobs"][
            "dku2dbx_demo_complexe_job"
        ]
        tasks = {
            t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in job["tasks"]
        }
        assert tasks["ingest_01"] == ["prepare_schema"]
        readers = [k for k, deps in tasks.items() if "ingest_01" in deps]
        assert readers  # the steps reading parcels_events wait for its ingestion
        assert "parcels_events : variable non résolue" in bundle.files["GENERATION.md"]


def test_cli_connect(tmp_path: Path) -> None:
    mapping = tmp_path / "map.yaml"
    mapping.write_text(yaml.safe_dump(MAPPING))
    export = str(DEFAULT_OUT_DIR / "DEMO_COMPLEXE.zip")
    args = ["connect", export, "--connections", str(mapping), "--out", str(tmp_path / "o")]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert "4 source(s) branchée(s), 1 à brancher" in result.output
    assert (tmp_path / "o" / "DEMO_COMPLEXE" / "a_brancher.md").is_file()
    assert (tmp_path / "o" / "DEMO_COMPLEXE" / "connect.json").is_file()


# --------------------------------------------------------------------------- #
# Optional Unity Catalog check (fake client: no workspace)
# --------------------------------------------------------------------------- #
class FakeTables:
    def __init__(self, tables: dict[str, list[str]]) -> None:
        self.known = tables

    def get(self, full_name: str) -> Any:
        if full_name not in self.known:
            raise LookupError(full_name)
        return SimpleNamespace(columns=[SimpleNamespace(name=c) for c in self.known[full_name]])


def test_unity_catalog_check() -> None:
    project = fixture("DEMO_SIMPLE")
    branching = branch_sources(project, ConnectionsConfig.model_validate(MAPPING), "test")
    customers = ["customer_id", "NAME", "segment"]  # case does not matter
    client = SimpleNamespace(
        tables=FakeTables(
            {
                "lab.crm.customers": customers,
                "lab.sales.DEMO_SIMPLE_orders_raw": ["order_id", "customer_id"],
            }
        )
    )
    problems = check_tables(project, branching, client)
    assert problems == {
        "orders_archive": "table lab.sales_archive.orders_2020 inaccessible (LookupError)",
        "orders_raw": (
            "colonnes absentes de lab.sales.DEMO_SIMPLE_orders_raw : amount, order_date, status"
        ),
    }


def test_inline_dataset_becomes_a_reference_table(tmp_path: Path) -> None:
    from tests.fixtures import make_fixture as mf  # noqa: PLC0415
    from tests.test_translate_sync import read_files  # noqa: PLC0415

    p = mf.build_demo_simple()
    next(d for d in p.datasets if d["name"] == "customers")["type"] = "Inline"
    result = branch_sources(read_files(tmp_path, p.files()), ConnectionsConfig(), "x")
    assert "table de référence" in result.unresolved["customers"]
    assert "Volume" not in result.unresolved["customers"]
