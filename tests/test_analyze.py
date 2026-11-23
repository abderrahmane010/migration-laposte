"""Phase 2: inventory, classification, score, effort, catalogue and waves."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.single_file import SingleFileSnapshotExtension, WriteMode

from dku2dbx.analyze import classify as classify_module
from dku2dbx.analyze.classify import classify
from dku2dbx.analyze.inventory import analyze_project
from dku2dbx.analyze.model import CrossProjectLink, Level, ProjectAnalysis, TargetClass
from dku2dbx.analyze.portfolio import analyze_portfolio, processor_catalogue, waves
from dku2dbx.analyze.report import project_markdown, write_outputs
from dku2dbx.analyze.scoring import ScoringConfig, effort, load_scoring, score
from dku2dbx.errors import ScoringConfigError
from dku2dbx.model.ir import Project
from dku2dbx.reader.zip_reader import read_export
from dku2dbx.translate.visual import TRANSLATORS
from tests.fixtures import make_fixture as mf
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR

CONFIG = load_scoring(Path("config/scoring.yaml"))


class MarkdownSnapshot(SingleFileSnapshotExtension):
    """Snapshot stored as a plain .md file (readable, no indentation)."""

    _write_mode = WriteMode.TEXT
    file_extension = "md"


def fixture(key: str) -> Project:
    return read_export(DEFAULT_OUT_DIR / f"{key}.zip")


def analysis(key: str) -> ProjectAnalysis:
    return analyze_project(fixture(key), f"{key}.zip", CONFIG)


def read_files(tmp_path: Path, files: dict[str, bytes], name: str = "export.zip") -> Project:
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as zf:
        for entry, data in sorted(files.items()):
            zf.writestr(entry, data)
    return read_export(path)


def classes(project: Project) -> dict[str, TargetClass]:
    return {c.name: c.target_class for c in classify(project)}


# --------------------------------------------------------------------------- #
# DEMO_SIMPLE: every figure checked by hand against make_fixture.build_demo_simple
# --------------------------------------------------------------------------- #
def test_demo_simple_figures() -> None:
    a = analysis("DEMO_SIMPLE")
    assert a.counts == {
        "code_recipes": 0,
        "datasets": 8,
        "issues": 0,
        "ml_recipes": 0,
        "partitioned_datasets": 0,
        "plugin_recipes": 0,
        "prepare_recipes": 1,
        "python_scenarios": 0,
        "recipes": 5,
        "saved_models": 0,
        "scenarios": 0,
        "zones": 0,
    }
    assert a.recipes_by_type == {"grouping": 1, "join": 1, "shaker": 1, "vstack": 1, "window": 1}
    assert a.datasets_by_type == {"PostgreSQL": 8}
    assert a.recipes_by_class == {"AUTO": 5}
    assert {r.name: r.planned_phase for r in a.recipes} == {
        "group_by_customer": 3,
        "join_orders_customers": 3,
        "prepare_orders": 4,
        "stack_orders": 3,
        "window_orders": 3,
    }
    # follows the translator registry (phase 3 fills it)
    assert all(r.translator_available == (r.type in TRANSLATORS) for r in a.recipes)
    # FillEmptyWithValue, CreateColumnWithGREL, FilterOnValue; ColumnRenamer is disabled
    assert a.processors == {"CreateColumnWithGREL": 1, "FillEmptyWithValue": 1, "FilterOnValue": 1}
    assert a.disabled_steps == ["prepare_orders: ColumnRenamer"]
    assert a.measures["formula_prepares"] == 0  # 1 formula step < 3
    assert (a.level, a.triggers) == (Level.LOW, [])
    assert a.effort_days == 1.25  # 5 AUTO x 0.25
    assert [(x.producer, x.consumer, x.dataset, x.source) for x in a.links] == [
        ("DEMO_SIMPLE", "DEMO_COMPLEXE", "customer_stats", "exposed")
    ]


def test_demo_simple_markdown_snapshot(snapshot: SnapshotAssertion) -> None:
    markdown = project_markdown(analysis("DEMO_SIMPLE"), wave=1)
    assert markdown == snapshot.use_extension(MarkdownSnapshot)


def test_demo_complexe_figures() -> None:
    a = analysis("DEMO_COMPLEXE")
    assert a.recipes_by_class == {"AUTO": 16, "LLM_REVIEW": 2, "MANUAL": 2}
    by_name = {r.name: r for r in a.recipes}
    assert by_name["python_scoring"].target_class is TargetClass.LLM_REVIEW
    assert by_name["sql_anomaly_report"].target_class is TargetClass.LLM_REVIEW
    assert by_name["plugin_geocode"].target_class is TargetClass.MANUAL
    depots = by_name["prepare_depots"]
    assert depots.target_class is TargetClass.MANUAL
    assert depots.planned_phase is None
    assert depots.reasons == ["processeur(s) à référentiel : ZipCodeGeocoder"]
    assert a.measures["code_share"] == 0.1
    assert a.level is Level.HIGH
    assert [(t.level, t.measure) for t in a.triggers] == [
        (Level.HIGH, "plugins"),
        (Level.MEDIUM, "code_share"),
        (Level.MEDIUM, "partitioned"),
        (Level.MEDIUM, "recipes"),
    ]
    # 16 x 0.25 + 2 x 1 + 2 x 2 + 1 scenario x 0.5
    assert a.effort_days == 10.5
    assert a.disabled_steps == ["prepare_events: DateParser"]


# --------------------------------------------------------------------------- #
# Classification rules
# --------------------------------------------------------------------------- #
def _with_recipes(*recipes: tuple[str, str, object]) -> dict[str, bytes]:
    p = mf.build_demo_simple()
    for name, rtype, payload in recipes:
        p.add_dataset(mf.dataset(p.key, f"{name}_out", cols=mf.columns("x:string")))
        p.add_recipe(
            mf.recipe(p.key, name, rtype, [mf.ref("orders_raw")], [mf.out_ref(f"{name}_out")]),
            payload,  # type: ignore[arg-type]
        )
    return p.files()


def test_classification_by_type(tmp_path: Path) -> None:
    files = _with_recipes(
        ("filter", "sampling", {}),
        ("fuzzy", "fuzzyjoin", {}),
        ("hive_code", "hive", {}),
        ("r_code", "r", {}),
        ("train", "prediction_training", {}),
        ("future", "brand_new_type", {}),
    )
    for name, ext, body in (("hive_code", "hql", b"SELECT 1"), ("r_code", "R", b"x <- 1")):
        files.pop(f"project_config/recipes/{name}.json.payload")
        files[f"project_config/recipes/{name}.{ext}"] = body
    got = classes(read_files(tmp_path, files))
    assert {k: got[k] for k in ("filter", "fuzzy", "hive_code", "r_code", "train", "future")} == {
        "filter": TargetClass.AUTO,
        "fuzzy": TargetClass.LLM_REVIEW,
        "hive_code": TargetClass.LLM_REVIEW,
        "r_code": TargetClass.MANUAL,
        "train": TargetClass.MANUAL,
        "future": TargetClass.MANUAL,
    }


def test_unusable_payload_makes_an_auto_recipe_manual(tmp_path: Path) -> None:
    files = mf.build_demo_simple().files()
    del files["project_config/recipes/window_orders.window"]
    [window] = [c for c in classify(read_files(tmp_path, files)) if c.name == "window_orders"]
    assert window.target_class is TargetClass.MANUAL
    assert window.reasons == ["payload inexploitable : MISSING_PAYLOAD"]


def test_unknown_step_meta_type_makes_prepare_manual(tmp_path: Path) -> None:
    files = mf.build_demo_simple().files()
    files["project_config/recipes/prepare_orders.shaker"] = mf._json(
        {"steps": [{"metaType": "STRANGE", "disabled": False}]}
    )
    assert classes(read_files(tmp_path, files))["prepare_orders"] is TargetClass.MANUAL


def test_translator_available_follows_the_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(classify_module, "AVAILABLE_TRANSLATORS", frozenset({"join"}))
    available = {c.name for c in classify(fixture("DEMO_SIMPLE")) if c.translator_available}
    assert available == {"join_orders_customers"}


# --------------------------------------------------------------------------- #
# Measures: formula-rich Prepare, disabled groups, Python scenarios
# --------------------------------------------------------------------------- #
def test_formula_rich_prepare_and_disabled_group(tmp_path: Path) -> None:
    grel = mf.step("CreateColumnWithGREL", {"column": "c", "expression": "1"})
    p = mf.build_demo_simple()
    p.add_dataset(mf.dataset(p.key, "rich_out", cols=mf.columns("x:string")))
    p.add_recipe(
        mf.recipe(p.key, "rich", "shaker", [mf.ref("orders_raw")], [mf.out_ref("rich_out")]),
        mf.shaker([grel, grel, mf.step_group("g", [grel])]),
    )
    p.add_dataset(mf.dataset(p.key, "poor_out", cols=mf.columns("x:string")))
    group = mf.step_group("off", [grel, grel])
    group["disabled"] = True
    p.add_recipe(
        mf.recipe(p.key, "poor", "shaker", [mf.ref("orders_raw")], [mf.out_ref("poor_out")]),
        mf.shaker([grel, group]),
    )
    a = analyze_project(read_files(tmp_path, p.files()), "x.zip", CONFIG)
    assert a.measures["formula_prepares"] == 1  # rich: 3 active; poor: 1 active
    assert a.disabled_steps.count("poor: CreateColumnWithGREL") == 2


def test_python_scenarios_are_counted(tmp_path: Path) -> None:
    p = mf.build_demo_simple()
    p.scenarios += [
        {"id": "PY", "type": "custom_python", "params": {}},
        {
            "id": "MIXED",
            "type": "step_based",
            "params": {"steps": [{"type": "custom_python", "params": {}}]},
        },
        {"id": "BUILD", "type": "step_based", "params": {"steps": [{"type": "build_flowitem"}]}},
    ]
    a = analyze_project(read_files(tmp_path, p.files()), "x.zip", CONFIG)
    assert a.measures["python_scenarios"] == 2
    assert a.level is Level.HIGH  # python_scenarios >= 1
    assert a.effort_days == 1.25 + 3 * 0.5


# --------------------------------------------------------------------------- #
# Scoring configuration
# --------------------------------------------------------------------------- #
def _config(**levels: dict[str, float]) -> ScoringConfig:
    return ScoringConfig.model_validate(
        {
            "levels": levels,
            "formula_rich_min_steps": 3,
            "effort_days": {"AUTO": 1, "LLM_REVIEW": 2, "MANUAL": 3, "per_scenario": 0.5},
        }
    )


MEASURES = {
    "recipes": 10,
    "code_share": 0.5,
    "formula_prepares": 0,
    "partitioned": 0,
    "python_scenarios": 0,
    "plugins": 0,
    "saved_models": 0,
}


def test_one_condition_is_enough_and_highest_level_wins() -> None:
    config = _config(VERY_HIGH={"recipes": 100}, HIGH={"code_share": 0.5}, MEDIUM={"recipes": 5})
    level, triggers = score(MEASURES, config)
    assert level is Level.HIGH
    assert [t.level for t in triggers] == [Level.HIGH, Level.MEDIUM]
    assert score(MEASURES, _config(MEDIUM={"recipes": 11})) == (Level.LOW, [])


def test_effort() -> None:
    config = _config()
    targets = [TargetClass.AUTO, TargetClass.LLM_REVIEW, TargetClass.MANUAL]
    assert effort(targets, scenarios=2, config=config) == 7.0


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("levels: {HIGH: {recipe: 3}}", "unknown measure"),
        ("levels: {LOW: {recipes: 3}}", "LOW is the default level"),
        ("levels: {EXTREME: {recipes: 3}}", "EXTREME"),
        ("levels: {}\nformula_rich_min_steps: 3\neffort_days: {AUTO: 1}", "LLM_REVIEW"),
        ("levels: [", "invalid scoring configuration"),
    ],
)
def test_invalid_scoring_config(tmp_path: Path, text: str, message: str) -> None:
    path = tmp_path / "scoring.yaml"
    path.write_text(text)
    with pytest.raises(ScoringConfigError, match=message):
        load_scoring(path)


def test_missing_scoring_file(tmp_path: Path) -> None:
    with pytest.raises(ScoringConfigError, match="cannot read"):
        load_scoring(tmp_path / "absent.yaml")


def test_repository_grid_is_the_default() -> None:
    assert load_scoring() == CONFIG
    assert CONFIG.effort_days.per_class(TargetClass.MANUAL) == 2.0


# --------------------------------------------------------------------------- #
# Portfolio: catalogue and waves
# --------------------------------------------------------------------------- #
def test_processor_catalogue_orders_phase_4() -> None:
    catalogue = processor_catalogue([fixture("DEMO_SIMPLE"), fixture("DEMO_COMPLEXE")])
    rows = [(s.type, s.occurrences, s.disabled, s.recipes, s.projects) for s in catalogue]
    assert rows[:2] == [
        ("CreateColumnWithGREL", 3, 0, 3, 2),
        ("FillEmptyWithValue", 3, 0, 3, 2),
    ]
    assert ("ColumnRenamer", 0, 1, 1, 1) in rows
    manual = {s.type for s in catalogue if s.target_class is TargetClass.MANUAL}
    assert manual == {"ZipCodeGeocoder"}


def _stub(key: str, *links: tuple[str, str]) -> ProjectAnalysis:
    return analysis("DEMO_SIMPLE").model_copy(
        update={
            "key": key,
            "links": [
                CrossProjectLink(producer=p, consumer=c, dataset="d", source="input")
                for p, c in links
            ],
        }
    )


def test_waves_producers_first_cycles_together_external_apart() -> None:
    projects = [
        _stub("A"),
        _stub("B", ("A", "B"), ("C", "B")),
        _stub("C", ("B", "C")),
        _stub("D", ("C", "D"), ("OUTSIDE", "D")),
    ]
    result, external = waves(projects)
    assert [(w.number, w.projects, w.cycles) for w in result] == [
        (1, ["A"], []),
        (2, ["B", "C"], [["B", "C"]]),
        (3, ["D"], []),
    ]
    assert [(x.producer, x.consumer) for x in external] == [("OUTSIDE", "D")]


def test_fixture_waves() -> None:
    projects = [fixture("DEMO_COMPLEXE"), fixture("DEMO_SIMPLE")]
    analyses = [analyze_project(p, p.key + ".zip", CONFIG) for p in projects]
    portfolio = analyze_portfolio(projects, analyses)
    assert [w.projects for w in portfolio.waves] == [["DEMO_SIMPLE"], ["DEMO_COMPLEXE"]]
    assert portfolio.external_links == []
    assert [a.key for a in portfolio.projects] == ["DEMO_COMPLEXE", "DEMO_SIMPLE"]


def test_outputs_are_written_and_reproducible(tmp_path: Path) -> None:
    projects = [fixture("DEMO_SIMPLE"), fixture("DEMO_COMPLEXE")]
    analyses = [analyze_project(p, p.key + ".zip", CONFIG) for p in projects]
    portfolio = analyze_portfolio(projects, analyses)
    first = write_outputs(portfolio, tmp_path / "a")
    second = write_outputs(portfolio, tmp_path / "b")
    names = [str(p.relative_to(tmp_path / "a")) for p in first]
    assert names == [
        "DEMO_COMPLEXE/analyse.json",
        "DEMO_COMPLEXE/analyse.md",
        "DEMO_SIMPLE/analyse.json",
        "DEMO_SIMPLE/analyse.md",
        "catalogue_processeurs.csv",
        "catalogue_processeurs.md",
        "portefeuille.csv",
        "vagues.md",
    ]
    for a, b in zip(first, second, strict=True):
        assert a.read_bytes() == b.read_bytes()
    csv_lines = (tmp_path / "a" / "portefeuille.csv").read_text().splitlines()
    assert csv_lines[0].startswith("projet,nom,vague,niveau,effort_jours")
    assert csv_lines[2].startswith("DEMO_SIMPLE,Démo simple (synthétique),1,LOW,1.25,5,5,0,0")
