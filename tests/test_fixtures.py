"""Phase 0: the synthetic export fixtures are well-formed and reproducible.

These tests check the *fixture generator only*, with the standard library.
Reading the exports into the internal model is phase 1.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections import Counter
from typing import Any

import pytest

from tests.fixtures.make_fixture import (
    CODE_PAYLOAD_EXT,
    DEFAULT_OUT_DIR,
    PROJECTS,
    VISUAL_PAYLOAD_EXT,
    build_zip,
)


def _open(key: str) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(build_zip(key)))


def _load(zf: zipfile.ZipFile, path: str) -> Any:
    return json.loads(zf.read(path))


def _recipes(zf: zipfile.ZipFile) -> list[dict[str, Any]]:
    names = sorted(
        n for n in zf.namelist() if n.startswith("project_config/recipes/") and n.endswith(".json")
    )
    return [_load(zf, n) for n in names]


def _datasets(zf: zipfile.ZipFile) -> dict[str, dict[str, Any]]:
    return {
        d["name"]: d
        for n in zf.namelist()
        if n.startswith("project_config/datasets/")
        for d in [_load(zf, n)]
    }


@pytest.mark.parametrize("key", list(PROJECTS))
def test_zip_is_reproducible(key: str) -> None:
    assert build_zip(key) == build_zip(key)


@pytest.mark.parametrize("key", list(PROJECTS))
def test_committed_fixture_is_up_to_date(key: str) -> None:
    """The ZIP in tests/fixtures/exports/ must match the generator, byte for byte."""
    path = DEFAULT_OUT_DIR / f"{key}.zip"
    assert path.exists(), f"Run: uv run python tests/fixtures/make_fixture.py ({path} missing)"
    assert path.read_bytes() == build_zip(key), "Fixture outdated: regenerate it."


@pytest.mark.parametrize("key", list(PROJECTS))
def test_manifest_and_params(key: str) -> None:
    with _open(key) as zf:
        manifest = _load(zf, "export-manifest.json")
        assert manifest["dssVersion"].startswith("15.")
        assert {"exportedWithPlugins", "foreignObjects"} <= manifest.keys()
        params = _load(zf, "project_config/params.json")
        assert params["projectKey"] == key
        assert "exposedObjects" in params


@pytest.mark.parametrize("key", list(PROJECTS))
def test_every_recipe_has_its_payload(key: str) -> None:
    with _open(key) as zf:
        names = set(zf.namelist())
        for rcp in _recipes(zf):
            base = f"project_config/recipes/{rcp['name']}"
            rtype = rcp["type"]
            if rtype in VISUAL_PAYLOAD_EXT:
                assert f"{base}.{VISUAL_PAYLOAD_EXT[rtype]}" in names
            elif rtype in CODE_PAYLOAD_EXT:
                assert f"{base}.{CODE_PAYLOAD_EXT[rtype]}" in names
            else:
                assert rtype.startswith("CustomCode_")
                assert f"{base}.{rtype}" in names


@pytest.mark.parametrize("key", list(PROJECTS))
def test_flow_references_are_consistent(key: str) -> None:
    """Each recipe input/output is a local dataset or a declared foreign object."""
    with _open(key) as zf:
        datasets = _datasets(zf)
        foreign = {
            f"{o['projectKey']}.{o['id']}"
            for o in _load(zf, "export-manifest.json")["foreignObjects"]
        }
        produced: Counter[str] = Counter()
        for rcp in _recipes(zf):
            for item in rcp["inputs"]["main"]["items"]:
                assert item["ref"] in datasets or item["ref"] in foreign, item["ref"]
            for item in rcp["outputs"]["main"]["items"]:
                assert item["ref"] in datasets, item["ref"]
                assert isinstance(item["appendMode"], bool)
                produced[item["ref"]] += 1
        assert all(count == 1 for count in produced.values()), "dataset built twice"
        # Every non-produced dataset is an external (non-managed) source.
        for name, ds in datasets.items():
            if name not in produced:
                assert ds["managed"] is False, name


@pytest.mark.parametrize("key", list(PROJECTS))
def test_prepare_steps_shape(key: str) -> None:
    with _open(key) as zf:
        for n in zf.namelist():
            if n.endswith(".shaker"):
                for s in _load(zf, n)["steps"]:
                    assert {"metaType", "disabled"} <= s.keys()
                    if s["metaType"] == "PROCESSOR":
                        assert {"type", "params"} <= s.keys()


def test_demo_simple_content() -> None:
    with _open("DEMO_SIMPLE") as zf:
        types = Counter(r["type"] for r in _recipes(zf))
        assert types == Counter({"shaker": 1, "join": 1, "grouping": 1, "window": 1, "vstack": 1})
        datasets = _datasets(zf)
        assert all(not d["partitioning"]["dimensions"] for d in datasets.values())
        assert not any(n.startswith("project_config/scenarios/") for n in zf.namelist())
        steps = _load(zf, "project_config/recipes/prepare_orders.shaker")["steps"]
        assert any(s["disabled"] for s in steps), "a disabled step is expected"


def test_demo_complexe_content() -> None:
    with _open("DEMO_COMPLEXE") as zf:
        recipes = _recipes(zf)
        types = Counter(r["type"] for r in recipes)
        assert 18 <= len(recipes) <= 22
        assert types["python"] == 1
        assert types["sql_query"] == 1
        assert sum(c for t, c in types.items() if t.startswith("CustomCode_")) == 1

        datasets = _datasets(zf)
        day_partitioned = [
            n
            for n, d in datasets.items()
            if any(
                dim["type"] == "time" and dim["params"]["period"] == "DAY"
                for dim in d["partitioning"]["dimensions"]
            )
        ]
        assert day_partitioned
        assert any("${NODE_DC}" in d["params"]["table"] for d in datasets.values())

        scenario = _load(zf, "project_config/scenarios/DAILY_BUILD.json")
        assert scenario["type"] == "step_based"
        assert scenario["params"]["steps"]

        manifest = _load(zf, "export-manifest.json")
        assert manifest["exportedWithPlugins"]

        # A grouped Prepare step is present, to exercise the "never filter silently" rule.
        steps = _load(zf, "project_config/recipes/prepare_events.shaker")["steps"]
        assert any(s["metaType"] != "PROCESSOR" for s in steps)


def test_cross_project_link() -> None:
    """DEMO_SIMPLE exposes customer_stats, DEMO_COMPLEXE consumes it (waves, phase 2)."""
    with _open("DEMO_SIMPLE") as zf:
        exposed = _load(zf, "project_config/params.json")["exposedObjects"]["objects"]
        assert {"DATASET", "customer_stats"} <= {exposed[0]["type"], exposed[0]["localName"]}
    with _open("DEMO_COMPLEXE") as zf:
        inputs = {i["ref"] for r in _recipes(zf) for i in r["inputs"]["main"]["items"]}
        assert "DEMO_SIMPLE.customer_stats" in inputs
