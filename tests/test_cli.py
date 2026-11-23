"""CLI: the six sub-commands exist; `analyze --dump-ir` writes the IR (phase 1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dku2dbx import __version__
from dku2dbx.cli import NOT_IMPLEMENTED_EXIT_CODE, app
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR

runner = CliRunner()

COMMANDS = ["analyze", "simplify", "translate", "generate", "connect", "reconcile"]
PLACEHOLDERS = ["connect", "reconcile"]


def test_help_lists_all_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in COMMANDS:
        assert command in result.output


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


@pytest.mark.parametrize("command", PLACEHOLDERS)
def test_placeholder_commands_fail_explicitly(command: str) -> None:
    """An empty command must never look like a successful run."""
    result = runner.invoke(app, [command])
    assert result.exit_code == NOT_IMPLEMENTED_EXIT_CODE
    assert "pas encore implémentée" in result.output


def test_analyze_dump_ir(tmp_path: Path) -> None:
    export = DEFAULT_OUT_DIR / "DEMO_COMPLEXE.zip"
    result = runner.invoke(app, ["analyze", str(export), "--dump-ir", "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    target = tmp_path / "DEMO_COMPLEXE" / "ir.json"
    assert f"IR écrite dans {target}" in result.output
    assert "UNRESOLVED_VARIABLE : 1" in result.output
    doc = json.loads(target.read_text(encoding="utf-8"))
    assert doc["meta"]["source_file"] == "DEMO_COMPLEXE.zip"
    assert doc["project"]["key"] == "DEMO_COMPLEXE"
    assert len(doc["project"]["recipes"]) == 20


def test_analyze_dump_ir_is_byte_reproducible(tmp_path: Path) -> None:
    export = str(DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip")
    for run in ("a", "b"):
        runner.invoke(app, ["analyze", export, "--dump-ir", "--out", str(tmp_path / run)])
    first = (tmp_path / "a" / "DEMO_SIMPLE" / "ir.json").read_bytes()
    assert first == (tmp_path / "b" / "DEMO_SIMPLE" / "ir.json").read_bytes()


def test_analyze_portfolio_writes_all_reports(tmp_path: Path) -> None:
    result = runner.invoke(app, ["analyze", str(DEFAULT_OUT_DIR), "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Projet DEMO_SIMPLE : 8 datasets, 5 recettes (AUTO 5)" in result.output
    assert "Niveau LOW, effort 1.25 j." in result.output
    assert "2 projet(s) analysé(s) en 2 vague(s)." in result.output
    written = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file())
    assert written == [
        "DEMO_COMPLEXE/analyse.json",
        "DEMO_COMPLEXE/analyse.md",
        "DEMO_SIMPLE/analyse.json",
        "DEMO_SIMPLE/analyse.md",
        "catalogue_processeurs.csv",
        "catalogue_processeurs.md",
        "portefeuille.csv",
        "vagues.md",
    ]


def test_analyze_portfolio_is_byte_reproducible(tmp_path: Path) -> None:
    for run in ("a", "b"):
        runner.invoke(app, ["analyze", str(DEFAULT_OUT_DIR), "--out", str(tmp_path / run)])
    for path in sorted((tmp_path / "a").rglob("*")):
        if path.is_file():
            twin = tmp_path / "b" / path.relative_to(tmp_path / "a")
            assert path.read_bytes() == twin.read_bytes(), path


def test_unreadable_export_does_not_stop_the_portfolio(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "DEMO_SIMPLE.zip").write_bytes((DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip").read_bytes())
    (exports / "broken.zip").write_text("nope")
    result = runner.invoke(app, ["analyze", str(exports), "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "Erreur de lecture" in result.output
    assert (tmp_path / "out" / "DEMO_SIMPLE" / "analyse.md").is_file()


def test_same_project_twice_is_an_error(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    exports.mkdir()
    for name in ("a.zip", "b.zip"):
        (exports / name).write_bytes((DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip").read_bytes())
    result = runner.invoke(app, ["analyze", str(exports), "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "présent deux fois (b.zip)" in result.output


def test_empty_export_folder(tmp_path: Path) -> None:
    result = runner.invoke(app, ["analyze", str(tmp_path), "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert "Aucun export .zip" in result.output


def test_invalid_scoring_grid(tmp_path: Path) -> None:
    grid = tmp_path / "scoring.yaml"
    grid.write_text("levels: {HIGH: {recipe: 1}}")
    export = str(DEFAULT_OUT_DIR / "DEMO_SIMPLE.zip")
    result = runner.invoke(app, ["analyze", export, "--scoring", str(grid), "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "Erreur de configuration" in result.output


def test_analyze_unreadable_export(tmp_path: Path) -> None:
    bad = tmp_path / "bad.zip"
    bad.write_text("nope")
    result = runner.invoke(app, ["analyze", str(bad), "--dump-ir", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "Erreur de lecture" in result.output
