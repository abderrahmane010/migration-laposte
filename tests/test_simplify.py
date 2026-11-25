"""Phase 5: Flow simplification (each keep rule on hand-built graphs, snapshot, --no-fusion)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion
from typer.testing import CliRunner

from dku2dbx.cli import app
from dku2dbx.model.ir import Project
from dku2dbx.simplify.flow import SimplifyError, simplify_flow
from dku2dbx.simplify.report import markdown
from tests.fixtures import make_fixture as mf
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR
from tests.test_analyze import MarkdownSnapshot
from tests.test_translate_sync import fixture, read_files

COLS = mf.columns("x:string")


def chain(tmp_path: Path, change: Any = None) -> Project:
    """src -> r1 -> t1 -> r2 -> t2 -> r3 -> t3 (three Sync recipes, all AUTO)."""
    p = mf.Project(key="CHAIN", name="Chaîne")
    for name in ("src", "t1", "t2", "t3"):
        p.add_dataset(mf.dataset(p.key, name, cols=COLS))
    for i, (src, dst) in enumerate([("src", "t1"), ("t1", "t2"), ("t2", "t3")], start=1):
        p.add_recipe(mf.recipe(p.key, f"r{i}", "sync", [mf.ref(src)], [mf.out_ref(dst)]), {})
    if change is not None:
        change(p)
    return read_files(tmp_path, p.files())


def test_a_pure_chain_becomes_one_step(tmp_path: Path) -> None:
    flow = simplify_flow(chain(tmp_path))
    assert [(s.recipes, s.outputs, s.inlined) for s in flow.steps] == [
        (["r1", "r2", "r3"], ["t3"], ["t1", "t2"])
    ]
    assert flow.removed == ["t1", "t2"]
    assert flow.kept == {"t3": ["sortie finale (aucune recette ne la lit)"]}


def _dataset(p: mf.Project, name: str) -> dict[str, Any]:
    return next(d for d in p.datasets if d["name"] == name)


def _second_reader(p: mf.Project) -> None:
    p.add_dataset(mf.dataset(p.key, "t4", cols=COLS))
    p.add_recipe(mf.recipe(p.key, "r4", "sync", [mf.ref("t1")], [mf.out_ref("t4")]), {})


def _partitioned(p: mf.Project) -> None:
    _dataset(p, "t1")["partitioning"] = {
        "dimensions": [{"name": "day", "type": "time", "params": {"period": "DAY"}}]
    }


def _append(p: mf.Project) -> None:
    p.recipes[0][0]["outputs"]["main"]["items"][0]["appendMode"] = True


def _checks(p: mf.Project) -> None:
    _dataset(p, "t1")["checks"] = [{"type": "recordCountInRange", "minimum": 1, "id": "c"}]


def _scenario(p: mf.Project) -> None:
    p.scenarios.append(
        {
            "id": "S",
            "type": "step_based",
            "params": {
                "steps": [
                    {
                        "type": "build_flowitem",
                        "params": {"builds": [{"type": "DATASET", "itemId": "t1"}]},
                    }
                ]
            },
        }
    )


def _exposed(p: mf.Project) -> None:
    p.exposed_objects = [{"type": "DATASET", "localName": "t1", "rules": []}]


def _python_reader(p: mf.Project) -> None:
    rcp, _ = p.recipes[1]
    rcp["type"] = "python"
    p.recipes[1] = (rcp, "import dataiku\n")


def _refused_reader(p: mf.Project) -> None:
    p.recipes[1][0]["params"]["schemaMode"] = "FREE_SCHEMA"  # Sync translator refuses it


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (_second_reader, "lue par 2 recettes"),
        (_partitioned, "partitionnée"),
        (_append, "écrite en mode append"),
        (_checks, "porte des checks"),
        (_scenario, "lue ou construite par un scénario"),
        (_exposed, "exposée à d'autres projets"),
        (_python_reader, "entrée d'une recette LLM_REVIEW 'r2'"),
        (_refused_reader, "entrée d'une recette non traduisible 'r2' (schemaMode)"),
    ],
)
def test_each_keep_rule(tmp_path: Path, change: Any, reason: str) -> None:
    flow = simplify_flow(chain(tmp_path, change))
    assert reason in flow.kept["t1"]
    assert "t1" not in flow.removed
    assert not any("t1" in s.inlined for s in flow.steps)


def test_output_of_a_non_auto_recipe_is_kept(tmp_path: Path) -> None:
    flow = simplify_flow(chain(tmp_path, _python_reader))
    assert flow.kept["t2"] == ["sortie d'une recette LLM_REVIEW 'r2'"]
    assert [s.recipes for s in flow.steps] == [["r1"], ["r2"], ["r3"]]


def test_no_fusion_gives_one_step_per_recipe(tmp_path: Path) -> None:
    flow = simplify_flow(chain(tmp_path), fusion=False)
    assert [s.recipes for s in flow.steps] == [["r1"], ["r2"], ["r3"]]
    assert flow.removed == []
    assert all("option --no-fusion" in why for why in flow.kept.values())


def test_cycle_is_an_error(tmp_path: Path) -> None:
    def loop(p: mf.Project) -> None:
        p.add_recipe(mf.recipe(p.key, "back", "sync", [mf.ref("t2")], [mf.out_ref("src")]), {})

    with pytest.raises(SimplifyError, match="cycle"):
        simplify_flow(chain(tmp_path, loop))


def test_demo_simple_figures() -> None:
    flow = simplify_flow(fixture("DEMO_SIMPLE"))
    # prepare -> stack -> join merge (orders_prepared, orders_all read once);
    # orders_enriched is read by group and window: kept, 2 more steps.
    assert (flow.recipes_before, flow.removed) == (5, ["orders_all", "orders_prepared"])
    assert [s.recipes for s in flow.steps] == [
        ["prepare_orders", "stack_orders", "join_orders_customers"],
        ["group_by_customer"],
        ["window_orders"],
    ]
    assert flow.kept["orders_enriched"] == ["lue par 2 recettes"]
    assert flow.kept["customer_stats"] == [
        "sortie finale (aucune recette ne la lit)",
        "exposée à d'autres projets",
    ]


def test_demo_complexe_report_snapshot(snapshot: SnapshotAssertion) -> None:
    assert markdown(simplify_flow(fixture("DEMO_COMPLEXE"))) == snapshot.use_extension(
        MarkdownSnapshot
    )


@pytest.mark.parametrize("key", ["DEMO_SIMPLE", "DEMO_COMPLEXE"])
def test_no_fusion_on_the_fixtures(key: str) -> None:
    project = fixture(key)
    flow = simplify_flow(project, fusion=False)
    assert len(flow.steps) == len(project.recipes)
    assert all(len(s.recipes) == 1 for s in flow.steps)


@pytest.mark.parametrize("key", ["DEMO_SIMPLE", "DEMO_COMPLEXE"])
def test_every_recipe_is_in_exactly_one_step(key: str) -> None:
    project = fixture(key)
    names = [r for s in simplify_flow(project).steps for r in s.recipes]
    assert sorted(names) == sorted(r.name for r in project.recipes)


def test_cli_simplify(tmp_path: Path) -> None:
    export = str(DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip")
    result = CliRunner().invoke(app, ["simplify", export, "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "5 recettes -> 3 étape(s), 2 table(s) intermédiaire(s) supprimée(s)" in result.output
    report = tmp_path / "DEMO_SIMPLE" / "simplify.md"
    first = report.read_bytes()
    CliRunner().invoke(app, ["simplify", export, "--out", str(tmp_path)])
    assert report.read_bytes() == first  # reproducible
    assert (tmp_path / "DEMO_SIMPLE" / "simplify.json").is_file()
    result = CliRunner().invoke(app, ["simplify", export, "--no-fusion", "--out", str(tmp_path)])
    assert "5 recettes -> 5 étape(s), 0 table(s)" in result.output


def test_steps_are_named_after_their_main_recipe() -> None:
    """Option B (decision of 2026-11-25): the step keeps the name of its last recipe."""
    flow = simplify_flow(fixture("DEMO_SIMPLE"))
    step = flow.steps[0]
    assert step.id == step.main_recipe == "join_orders_customers"
    assert step.recipes == ["prepare_orders", "stack_orders", "join_orders_customers"]
    assert step.name_note is None
    assert "| join_orders_customers | prepare_orders, stack_orders, join_orders_customers |" in (
        markdown(flow)
    )


@pytest.mark.parametrize(
    ("recipe", "note"),
    [
        ("prepare_schema", "nom réservé"),
        ("ingest_01", "nom réservé"),
        ("compute.orders", "caractères interdits"),
        ("a" * 101, "caractères interdits"),
    ],
)
def test_a_recipe_name_unfit_for_a_task_key_falls_back(recipe: str, note: str) -> None:
    project = fixture("DEMO_SIMPLE")
    old = "window_orders"
    rcp = project.recipe(old)
    rcp.name = recipe
    nodes = project.flow.topological_order
    project.flow.topological_order = [n.replace(old, recipe) for n in nodes]
    flow = simplify_flow(project)
    step = next(s for s in flow.steps if s.main_recipe == recipe)
    assert re.fullmatch(r"step_\d\d", step.id)
    assert step.name_note is not None and note in step.name_note
    assert f"- {step.id} (recette {recipe}) : " in markdown(flow)


def test_case_collision_between_step_names_falls_back() -> None:
    project = fixture("DEMO_SIMPLE")
    rcp = project.recipe("window_orders")
    rcp.name = "GROUP_BY_CUSTOMER"
    nodes = project.flow.topological_order
    project.flow.topological_order = [n.replace("window_orders", rcp.name) for n in nodes]
    ids = [s.id for s in simplify_flow(project).steps]
    assert "group_by_customer" in ids
    assert "GROUP_BY_CUSTOMER" not in ids
