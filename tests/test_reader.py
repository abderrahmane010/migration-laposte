"""Phase 1: ZIP export reader -> internal representation (IR)."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import networkx as nx
import pytest
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.json import JSONSnapshotExtension

from dku2dbx.errors import ExportReadError
from dku2dbx.model import (
    FlowNodeKind,
    IssueKind,
    ObjectKind,
    Project,
    RecipeCategory,
    Status,
)
from dku2dbx.model.flow import to_networkx
from dku2dbx.reader.base import ProjectReader
from dku2dbx.reader.zip_reader import ZipExportReader, read_export
from tests.fixtures import make_fixture as mf

FIXTURES = mf.DEFAULT_OUT_DIR


def fixture_path(key: str) -> Path:
    return FIXTURES / f"{key}.zip"


@pytest.fixture(scope="module")
def simple() -> Project:
    return read_export(fixture_path("DEMO_SIMPLE"))


@pytest.fixture(scope="module")
def complexe() -> Project:
    return read_export(fixture_path("DEMO_COMPLEXE"))


def write_zip(tmp_path: Path, files: dict[str, bytes], name: str = "export.zip") -> Path:
    path = tmp_path / name
    path.write_bytes(mf.zip_bytes(files))
    return path


def kinds(project: Project) -> list[IssueKind]:
    return [i.kind for i in project.issues]


# --------------------------------------------------------------------------- #
# Both fixtures are read without error
# --------------------------------------------------------------------------- #
def test_read_demo_simple(simple: Project) -> None:
    assert simple.key == "DEMO_SIMPLE"
    assert simple.dss_version == "15.0.0"
    assert len(simple.datasets) == 8
    assert len(simple.recipes) == 5
    assert all(r.status is Status.KNOWN for r in simple.recipes)
    assert all(d.status is Status.KNOWN for d in simple.datasets)
    assert simple.issues == []
    assert simple.unparsed_entries == []


def test_read_demo_complexe(complexe: Project) -> None:
    assert complexe.key == "DEMO_COMPLEXE"
    assert len(complexe.datasets) == 26
    assert len(complexe.recipes) == 20
    assert len(complexe.scenarios) == 1
    assert {z.id for z in complexe.zones} == {"z_ingest", "z_kpi"}
    assert all(r.status is Status.KNOWN for r in complexe.recipes)
    assert complexe.unparsed_entries == []
    # Only expected issue: ${NODE_DC}, an instance variable absent from the export.
    assert kinds(complexe) == [IssueKind.UNRESOLVED_VARIABLE]


@pytest.mark.parametrize("key", ["DEMO_SIMPLE", "DEMO_COMPLEXE"])
def test_ir_snapshot(key: str, snapshot: SnapshotAssertion) -> None:
    """The serialised IR is stable (regenerate with: uv run pytest --snapshot-update)."""
    doc = ZipExportReader(fixture_path(key)).read_document()
    assert json.loads(doc.to_json()) == snapshot.use_extension(JSONSnapshotExtension)


@pytest.mark.parametrize("key", ["DEMO_SIMPLE", "DEMO_COMPLEXE"])
def test_ir_is_reproducible(key: str) -> None:
    first = ZipExportReader(fixture_path(key)).read_document().to_json()
    second = ZipExportReader(fixture_path(key)).read_document().to_json()
    assert first == second


def test_ir_meta_traceability() -> None:
    doc = ZipExportReader(fixture_path("DEMO_SIMPLE")).read_document()
    assert doc.meta.generator == "dku2dbx"
    assert doc.meta.source_file == "DEMO_SIMPLE.zip"
    assert len(doc.meta.source_sha256) == 64


# --------------------------------------------------------------------------- #
# Flow graph of DEMO_SIMPLE
# --------------------------------------------------------------------------- #
def test_demo_simple_flow_graph(simple: Project) -> None:
    g = to_networkx(simple.flow)
    assert set(g.nodes) == {
        "dataset:customer_stats",
        "dataset:customers",
        "dataset:orders_all",
        "dataset:orders_archive",
        "dataset:orders_enriched",
        "dataset:orders_prepared",
        "dataset:orders_ranked",
        "dataset:orders_raw",
        "recipe:group_by_customer",
        "recipe:join_orders_customers",
        "recipe:prepare_orders",
        "recipe:stack_orders",
        "recipe:window_orders",
    }
    assert set(g.edges) == {
        ("dataset:orders_raw", "recipe:prepare_orders"),
        ("recipe:prepare_orders", "dataset:orders_prepared"),
        ("dataset:orders_prepared", "recipe:stack_orders"),
        ("dataset:orders_archive", "recipe:stack_orders"),
        ("recipe:stack_orders", "dataset:orders_all"),
        ("dataset:orders_all", "recipe:join_orders_customers"),
        ("dataset:customers", "recipe:join_orders_customers"),
        ("recipe:join_orders_customers", "dataset:orders_enriched"),
        ("dataset:orders_enriched", "recipe:group_by_customer"),
        ("recipe:group_by_customer", "dataset:customer_stats"),
        ("dataset:orders_enriched", "recipe:window_orders"),
        ("recipe:window_orders", "dataset:orders_ranked"),
    }
    assert nx.is_directed_acyclic_graph(g)
    # Deterministic (lexicographic) topological order.
    assert simple.flow.topological_order == [
        "dataset:customers",
        "dataset:orders_archive",
        "dataset:orders_raw",
        "recipe:prepare_orders",
        "dataset:orders_prepared",
        "recipe:stack_orders",
        "dataset:orders_all",
        "recipe:join_orders_customers",
        "dataset:orders_enriched",
        "recipe:group_by_customer",
        "dataset:customer_stats",
        "recipe:window_orders",
        "dataset:orders_ranked",
    ]
    position = {n: i for i, n in enumerate(simple.flow.topological_order)}
    assert all(position[s] < position[t] for s, t in g.edges)


def test_foreign_dataset_node(complexe: Project) -> None:
    nodes = {n.id: n for n in complexe.flow.nodes}
    node = nodes["dataset:DEMO_SIMPLE.customer_stats"]
    assert node.kind is FlowNodeKind.FOREIGN_DATASET
    inp = next(i for i in complexe.recipe("python_scoring").inputs if i.project_key)
    assert (inp.project_key, inp.name) == ("DEMO_SIMPLE", "customer_stats")


# --------------------------------------------------------------------------- #
# Object details
# --------------------------------------------------------------------------- #
def test_variables_resolved_in_table_names(simple: Project) -> None:
    ds = simple.dataset("orders_prepared")
    assert ds.table == "${projectKey}_orders_prepared"
    assert ds.table_resolved == "DEMO_SIMPLE_orders_prepared"
    assert ds.db_schema_resolved == "dku_DEMO_SIMPLE"


def test_unresolved_variable_is_listed(complexe: Project) -> None:
    ds = complexe.dataset("parcels_events")
    assert ds.table_resolved == "${NODE_DC}_parcels_events"
    assert [(u.object_name, u.field, u.variable) for u in complexe.unresolved_variables] == [
        ("parcels_events", "params.table", "NODE_DC")
    ]


def test_instance_variables_resolve_the_rest() -> None:
    project = read_export(fixture_path("DEMO_COMPLEXE"), instance_variables={"NODE_DC": "dc01"})
    assert project.dataset("parcels_events").table_resolved == "dc01_parcels_events"
    assert project.unresolved_variables == []
    assert project.issues == []


def test_partitioned_dataset(complexe: Project) -> None:
    ds = complexe.dataset("parcels_events")
    assert ds.is_partitioned
    assert ds.partitioning[0].type == "time"
    assert ds.partitioning[0].params == {"period": "DAY"}
    assert not complexe.dataset("depots").is_partitioned
    sync = complexe.recipe("sync_events")
    assert sync.inputs[0].deps == [{"func": "equals", "params": {}}]
    assert sync.never_recompute_existing_partitions is True


def test_recipe_fields(simple: Project) -> None:
    join = simple.recipe("join_orders_customers")
    assert join.category is RecipeCategory.VISUAL
    assert join.engine == "SQL"
    assert [i.ref for i in join.inputs] == ["orders_all", "customers"]
    assert [o.ref for o in join.outputs] == ["orders_enriched"]
    assert join.payload_path == "project_config/recipes/join_orders_customers.join"
    assert join.payload["joins"][0]["type"] == "LEFT"
    assert len(join.source_sha256) == 64


def test_append_mode(complexe: Project) -> None:
    assert complexe.recipe("stack_failed").outputs[0].append_mode is True
    assert complexe.recipe("split_status").outputs[0].append_mode is False


def test_prepare_steps_including_disabled_and_grouped(simple: Project, complexe: Project) -> None:
    steps = simple.recipe("prepare_orders").prepare_steps
    assert steps is not None
    assert [s.type for s in steps] == [
        "FillEmptyWithValue",
        "CreateColumnWithGREL",
        "FilterOnValue",
        "ColumnRenamer",
    ]
    assert [s.disabled for s in steps] == [False, False, False, True]

    grouped = complexe.recipe("prepare_events").prepare_steps
    assert grouped is not None
    group = next(s for s in grouped if s.meta_type == "GROUP")
    assert group.name == "normalisation"
    assert [s.type for s in group.steps] == ["CreateColumnWithGREL", "FillEmptyWithValue"]
    assert len(grouped) == 4  # nothing filtered out


def test_code_recipes_keep_their_script(complexe: Project) -> None:
    python = complexe.recipe("python_scoring")
    assert python.category is RecipeCategory.CODE
    assert python.payload is None
    assert python.payload_raw is not None
    assert 'dataiku.Dataset("delivered_enriched").get_dataframe()' in python.payload_raw
    sql = complexe.recipe("sql_anomaly_report")
    assert sql.payload_path is not None
    assert sql.payload_path.endswith(".sql")
    assert sql.payload_raw is not None
    assert "GROUP BY" in sql.payload_raw


def test_plugin_usage(complexe: Project) -> None:
    rcp = complexe.recipe("plugin_geocode")
    assert rcp.category is RecipeCategory.PLUGIN
    assert rcp.plugin_id == "geo-toolkit"
    [plugin] = complexe.plugins
    assert (plugin.plugin_id, plugin.version, plugin.used_by_recipes) == (
        "geo-toolkit",
        "1.2.0",
        ["plugin_geocode"],
    )


def test_scenario(complexe: Project) -> None:
    [sc] = complexe.scenarios
    assert (sc.id, sc.type, sc.status, sc.active) == (
        "DAILY_BUILD",
        "step_based",
        Status.KNOWN,
        True,
    )
    assert [t.type for t in sc.triggers] == ["temporal", "ds_modified"]
    assert [s.type for s in sc.steps] == ["build_flowitem", "build_flowitem"]


def test_exposed_and_foreign_objects(simple: Project, complexe: Project) -> None:
    [exposed] = simple.exposed_objects
    assert (exposed.type, exposed.local_name, exposed.target_projects) == (
        "DATASET",
        "customer_stats",
        ["DEMO_COMPLEXE"],
    )
    [foreign] = complexe.foreign_objects
    assert (foreign.project_key, foreign.id) == ("DEMO_SIMPLE", "customer_stats")


def test_dataset_details(complexe: Project) -> None:
    kpis = complexe.dataset("daily_depot_kpis")
    assert kpis.checks[0]["type"] == "recordCountInRange"
    assert kpis.zone == "z_kpi"
    src = complexe.dataset("depots")
    assert (src.managed, src.connection, src.db_schema, src.table) == (
        False,
        "pg_sources",
        "ref",
        "depots",
    )
    assert [c.name for c in src.columns][:2] == ["depot_id", "depot_name"]


# --------------------------------------------------------------------------- #
# Unknown types are kept and reported, never dropped
# --------------------------------------------------------------------------- #
def _demo_with_unknowns() -> dict[str, bytes]:
    p = mf.build_demo_simple()
    p.add_dataset(
        mf.dataset(p.key, "mystery_out", ds_type="MysteryStore", cols=mf.columns("x:string"))
    )
    p.add_recipe(
        mf.recipe(
            p.key,
            "future_recipe",
            "brand_new_type",
            [mf.ref("orders_raw")],
            [mf.out_ref("mystery_out")],
        ),
        {"someSetting": 42},
    )
    p.scenarios.append({"id": "FUTURE", "type": "future_scenario", "params": {}})
    files = p.files()
    files["project_config/notebooks/exploration.ipynb"] = b"{}"
    files["project_config/recipes/prepare_orders.shaker"] = mf._json(
        {"steps": [{"metaType": "STRANGE", "disabled": False}]}
    )
    return files


def test_unknown_types_are_kept_and_reported(tmp_path: Path) -> None:
    project = read_export(write_zip(tmp_path, _demo_with_unknowns()))

    rcp = project.recipe("future_recipe")
    assert rcp.status is Status.UNKNOWN
    assert rcp.category is RecipeCategory.UNKNOWN
    assert rcp.raw is not None
    assert rcp.raw["type"] == "brand_new_type"
    assert rcp.payload == {"someSetting": 42}
    assert "recipe:future_recipe" in project.flow.topological_order

    ds = project.dataset("mystery_out")
    assert ds.status is Status.UNKNOWN
    assert ds.raw is not None

    [sc] = [s for s in project.scenarios if s.id == "FUTURE"]
    assert sc.status is Status.UNKNOWN

    steps = project.recipe("prepare_orders").prepare_steps
    assert steps is not None
    assert steps[0].meta_type == "STRANGE"

    assert project.unparsed_entries == ["project_config/notebooks/exploration.ipynb"]
    reported = {(i.kind, i.object_name) for i in project.issues}
    assert {
        (IssueKind.UNKNOWN_RECIPE_TYPE, "future_recipe"),
        (IssueKind.UNKNOWN_DATASET_TYPE, "mystery_out"),
        (IssueKind.UNKNOWN_SCENARIO_TYPE, "FUTURE"),
        (IssueKind.UNKNOWN_STEP_META_TYPE, "prepare_orders"),
        (IssueKind.UNPARSED_ENTRY, "DEMO_SIMPLE"),
    } <= reported


def test_missing_reference_and_payload(tmp_path: Path) -> None:
    p = mf.build_demo_simple()
    p.add_recipe(
        mf.recipe(p.key, "orphan", "sync", [mf.ref("ghost")], [mf.out_ref("orders_ranked")]),
        {},
    )
    files = p.files()
    del files["project_config/recipes/window_orders.window"]
    project = read_export(write_zip(tmp_path, files))
    nodes = {n.id: n.kind for n in project.flow.nodes}
    assert nodes["dataset:ghost"] is FlowNodeKind.MISSING
    reported = {(i.kind, i.object_name) for i in project.issues}
    assert (IssueKind.MISSING_REFERENCE, "orphan") in reported
    assert (IssueKind.MISSING_PAYLOAD, "window_orders") in reported


@pytest.mark.parametrize("payload", [None, b"", b"  \n"])
def test_sync_recipe_payload_is_optional(tmp_path: Path, payload: bytes | None) -> None:
    # H-25: through the API, every "sync" recipe of a real project had an empty payload.
    p = mf.build_demo_simple()
    p.add_recipe(
        mf.recipe(p.key, "copy_orders", "sync", [mf.ref("orders_raw")], [mf.out_ref("copy")]),
        {},
    )
    p.add_dataset(mf.dataset(p.key, "copy", cols=mf.columns("x:string")))
    files = p.files()
    del files["project_config/recipes/copy_orders.sync"]
    if payload is not None:
        files["project_config/recipes/copy_orders.sync"] = payload
    project = read_export(write_zip(tmp_path, files))
    assert project.recipe("copy_orders").payload is None
    assert [i for i in project.issues if i.object_name == "copy_orders"] == []


def test_other_visual_recipe_still_needs_a_payload(tmp_path: Path) -> None:
    files = mf.build_demo_simple().files()
    files["project_config/recipes/window_orders.window"] = b""
    project = read_export(write_zip(tmp_path, files))
    reported = {(i.kind, i.object_name) for i in project.issues}
    assert (IssueKind.INVALID_PAYLOAD, "window_orders") in reported


def _demo_with_saved_model(*, declare_model: bool) -> dict[str, bytes]:
    # H-26: a training recipe outputs a saved model, a scoring recipe reads it by id.
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "orders_scored", cols=mf.columns("x:string")))
    p.add_recipe(
        mf.recipe(
            p.key,
            "train_model",
            "prediction_training",
            [mf.ref("orders_raw")],
            [mf.out_ref("Ab12Cd34")],
        ),
        {},
    )
    rcp = mf.recipe(
        p.key,
        "score_orders",
        "prediction_scoring",
        [mf.ref("orders_raw")],
        [mf.out_ref("orders_scored")],
    )
    rcp["inputs"]["model"] = {"items": [mf.ref("Ab12Cd34")]}
    p.add_recipe(rcp, {})
    files = p.files()
    for name in ("train_model", "score_orders"):
        files.pop(f"project_config/recipes/{name}.json.payload")
    if declare_model:
        files["project_config/saved_models/Ab12Cd34.json"] = mf._json(
            {
                "id": "Ab12Cd34",
                "projectKey": p.key,
                "name": "Delay model",
                "savedModelType": "DSS_MANAGED",
                "miniTask": {"taskType": "PREDICTION", "predictionType": "REGRESSION"},
                "zone": "ghost_zone",
            }
        )
    return files


def test_saved_model_is_read_and_linked_in_the_flow(tmp_path: Path) -> None:
    project = read_export(write_zip(tmp_path, _demo_with_saved_model(declare_model=True)))
    [sm] = project.saved_models
    assert (sm.id, sm.name, sm.saved_model_type) == ("Ab12Cd34", "Delay model", "DSS_MANAGED")
    assert (sm.task_type, sm.prediction_type) == ("PREDICTION", "REGRESSION")
    assert sm.source_path == "project_config/saved_models/Ab12Cd34.json"

    nodes = {n.id: n.kind for n in project.flow.nodes}
    assert nodes["saved_model:Ab12Cd34"] is FlowNodeKind.SAVED_MODEL
    assert "dataset:Ab12Cd34" not in nodes
    edges = {(e.source, e.target) for e in project.flow.edges}
    assert ("recipe:train_model", "saved_model:Ab12Cd34") in edges
    assert ("saved_model:Ab12Cd34", "recipe:score_orders") in edges
    order = project.flow.topological_order
    assert order.index("recipe:train_model") < order.index("recipe:score_orders")
    assert [score.role for score in project.recipe("score_orders").inputs] == ["main", "model"]

    reported = {(i.kind, i.object_kind, i.object_name) for i in project.issues}
    assert (IssueKind.MISSING_REFERENCE, ObjectKind.SAVED_MODEL, "Ab12Cd34") in reported
    assert not any(
        i.kind is IssueKind.MISSING_REFERENCE and i.object_kind is ObjectKind.RECIPE
        for i in project.issues
    )
    assert project.unparsed_entries == []


def test_undeclared_saved_model_is_still_a_missing_reference(tmp_path: Path) -> None:
    project = read_export(write_zip(tmp_path, _demo_with_saved_model(declare_model=False)))
    assert project.saved_models == []
    nodes = {n.id: n.kind for n in project.flow.nodes}
    assert nodes["dataset:Ab12Cd34"] is FlowNodeKind.MISSING
    # The missing node is reported once, on the first recipe (in name order) using it.
    reported = {(i.kind, i.object_name) for i in project.issues}
    assert (IssueKind.MISSING_REFERENCE, "score_orders") in reported


def test_cycle_is_reported(tmp_path: Path) -> None:
    p = mf.build_demo_simple()
    p.add_recipe(
        mf.recipe(p.key, "loop", "sync", [mf.ref("orders_ranked")], [mf.out_ref("orders_raw")]),
        {},
    )
    project = read_export(write_zip(tmp_path, p.files()))
    assert IssueKind.FLOW_CYCLE in kinds(project)
    assert project.flow.topological_order == []


def test_manifest_under_a_single_top_folder(tmp_path: Path) -> None:
    files = {f"DEMO_SIMPLE/{k}": v for k, v in mf.build_demo_simple().files().items()}
    project = read_export(write_zip(tmp_path, files))
    assert project.key == "DEMO_SIMPLE"
    assert len(project.recipes) == 5
    assert project.unparsed_entries == []


# --------------------------------------------------------------------------- #
# Unreadable exports
# --------------------------------------------------------------------------- #
def test_not_a_zip(tmp_path: Path) -> None:
    path = tmp_path / "bad.zip"
    path.write_text("not a zip")
    with pytest.raises(ExportReadError, match="not a readable ZIP"):
        read_export(path)


def test_no_manifest(tmp_path: Path) -> None:
    with pytest.raises(ExportReadError, match=r"export-manifest\.json"):
        read_export(write_zip(tmp_path, {"project_config/params.json": b"{}"}))


def test_invalid_json(tmp_path: Path) -> None:
    files = mf.build_demo_simple().files()
    files["project_config/datasets/customers.json"] = b"{not json"
    with pytest.raises(ExportReadError, match=r"customers\.json.*not valid JSON"):
        read_export(write_zip(tmp_path, files))


# --------------------------------------------------------------------------- #
# Reader interface
# --------------------------------------------------------------------------- #
def test_zip_reader_interface() -> None:
    reader: ProjectReader = ZipExportReader(fixture_path("DEMO_SIMPLE"))
    assert reader.list_projects() == ["DEMO_SIMPLE"]
    assert reader.read_project("DEMO_SIMPLE").key == "DEMO_SIMPLE"
    with pytest.raises(KeyError):
        reader.read_project("OTHER")


def test_zip_entries_are_not_extracted(tmp_path: Path) -> None:
    """The reader works in memory: nothing is written next to the export."""
    path = write_zip(tmp_path, mf.build_demo_simple().files())
    read_export(path)
    assert [p.name for p in tmp_path.iterdir()] == ["export.zip"]
    with zipfile.ZipFile(path) as zf:
        assert "export-manifest.json" in zf.namelist()


def test_dss15_export_layout(tmp_path: Path) -> None:
    """Seen on a Dataiku Cloud export (DSS 15.2.2): manifest keys and split variables."""
    files = mf.build_demo_simple().files()
    manifest = json.loads(files["export-manifest.json"])
    manifest = {
        "generatedWithDSSVersion": "15.2.2",
        "originalProjectKey": "DEMO_SIMPLE",
        "exportedWithPlugins": [{"pluginId": "geoadmin", "version": "2.3.1"}],
        "foreignObjects": manifest["foreignObjects"],
    }
    files["export-manifest.json"] = json.dumps(manifest).encode()
    params = json.loads(files["project_config/params.json"])
    del params["projectKey"]
    files["project_config/params.json"] = json.dumps(params).encode()
    files["project_config/variables.json"] = json.dumps({"min_amount": 5, "x": "s"}).encode()
    files["project_config/localvariables.json"] = json.dumps({"x": "local"}).encode()
    path = write_zip(tmp_path, files)
    path = path.rename(tmp_path / "renamed_by_hand.zip")  # the key no longer comes from the name
    project = read_export(path)
    assert project.key == "DEMO_SIMPLE"
    assert project.dss_version == "15.2.2"
    assert [(p.plugin_id, p.version) for p in project.plugins] == [("geoadmin", "2.3.1")]
    assert project.variables == {"min_amount": 5, "x": "local"}  # local overrides standard
    assert "project_config/localvariables.json" not in project.unparsed_entries
