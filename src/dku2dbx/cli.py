"""Command-line entry point for dku2dbx.

Sub-commands not implemented yet are placeholders: they print which roadmap
phase will implement them and exit with a non-zero code, so that no caller
mistakes an empty command for a successful run.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import typer

from dku2dbx import __version__
from dku2dbx.analyze.inventory import analyze_project
from dku2dbx.analyze.model import ProjectAnalysis
from dku2dbx.analyze.portfolio import analyze_portfolio
from dku2dbx.analyze.report import write_outputs
from dku2dbx.analyze.scoring import load_scoring
from dku2dbx.errors import ExportReadError, ScoringConfigError
from dku2dbx.model.ir import Project
from dku2dbx.reader.zip_reader import ZipExportReader
from dku2dbx.simplify.flow import SimplifyError, simplify_flow
from dku2dbx.simplify.report import write_report
from dku2dbx.translate.engine import translate_project, write_translation

app = typer.Typer(
    name="dku2dbx",
    help="Usine de migration Dataiku DSS → Databricks (Analyser → Simplifier → Brancher → Migrer).",
    no_args_is_help=True,
    add_completion=False,
)

NOT_IMPLEMENTED_EXIT_CODE = 2


def _not_implemented(command: str, phase: int) -> None:
    typer.echo(f"`{command}` n'est pas encore implémentée (prévue en phase {phase}).", err=True)
    raise typer.Exit(code=NOT_IMPLEMENTED_EXIT_CODE)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"dku2dbx {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Affiche la version et quitte.",
    ),
) -> None:
    """Usine de migration Dataiku DSS → Databricks."""


def _exports(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(p for p in path.iterdir() if p.suffix == ".zip" and p.is_file())
    return [path]


@app.command()
def analyze(
    export: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
        help="Export ZIP d'un projet Dataiku, ou dossier d'exports ZIP (configuration seulement).",
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
    dump_ir: bool = typer.Option(
        False, "--dump-ir", help="Écrire aussi le modèle interne (IR) : <out>/<projet>/ir.json."
    ),
    scoring: Path | None = typer.Option(
        None,
        "--scoring",
        help="Grille de score (défaut : config/scoring.yaml, sinon celle livrée avec dku2dbx).",
    ),
) -> None:
    """Analyser : inventaire, classement, score, effort et vagues (phases 1-2)."""
    try:
        config = load_scoring(scoring)
    except ScoringConfigError as exc:
        typer.echo(f"Erreur de configuration : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    exports = _exports(export)
    if not exports:
        typer.echo(f"Aucun export .zip dans {export}", err=True)
        raise typer.Exit(code=1)

    projects: list[Project] = []
    analyses: list[ProjectAnalysis] = []
    failed = 0
    for path in exports:
        try:
            doc = ZipExportReader(path).read_document()
        except ExportReadError as exc:
            # One unreadable export does not stop the portfolio: it is reported.
            typer.echo(f"Erreur de lecture : {exc}", err=True)
            failed += 1
            continue
        project = doc.project
        if any(p.key == project.key for p in projects):
            typer.echo(f"Erreur : projet {project.key} présent deux fois ({path.name})", err=True)
            failed += 1
            continue
        projects.append(project)
        analyses.append(analyze_project(project, path.name, config))
        if dump_ir:
            target = out / project.key / "ir.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(doc.to_json().encode("utf-8"))
        _print_project(project, analyses[-1])
        if dump_ir:
            typer.echo(f"IR écrite dans {target}")

    if projects:
        portfolio = analyze_portfolio(projects, analyses)
        write_outputs(portfolio, out)
        typer.echo(
            f"{len(projects)} projet(s) analysé(s) en {len(portfolio.waves)} vague(s). "
            f"Rapports dans {out}"
        )
    if failed:
        raise typer.Exit(code=1)


def _print_project(project: Project, analysis: ProjectAnalysis) -> None:
    classes = ", ".join(f"{k} {v}" for k, v in analysis.recipes_by_class.items())
    typer.echo(
        f"Projet {project.key} : {len(project.datasets)} datasets, "
        f"{len(project.recipes)} recettes ({classes or 'aucune'}), "
        f"{len(project.scenarios)} scénario(s). Niveau {analysis.level.value}, "
        f"effort {analysis.effort_days:g} j."
    )
    if project.issues:
        counts = Counter(issue.kind.value for issue in project.issues)
        typer.echo(f"{len(project.issues)} point(s) à examiner :")
        for kind, count in sorted(counts.items()):
            typer.echo(f"  - {kind} : {count}")


@app.command()
def simplify(
    export: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
    no_fusion: bool = typer.Option(
        False, "--no-fusion", help="Une étape par recette (réconciliation fine, débogage)."
    ),
) -> None:
    """Simplifier : fusionner les chaînes de recettes en étapes (phase 5)."""
    try:
        project = ZipExportReader(export).read()
        flow = simplify_flow(project, fusion=not no_fusion)
    except (ExportReadError, SimplifyError) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    write_report(flow, out)
    typer.echo(
        f"Projet {project.key} : {flow.recipes_before} recettes -> {len(flow.steps)} étape(s), "
        f"{len(flow.removed)} table(s) intermédiaire(s) supprimée(s), "
        f"{len(flow.kept)} conservée(s)."
    )
    typer.echo(f"Rapport dans {out / project.key / 'simplify.md'}")


@app.command()
def translate(
    export: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
) -> None:
    """Migrer : traduire les recettes visuelles en Spark SQL, un fichier .sql par sortie."""
    try:
        project = ZipExportReader(export).read()
    except ExportReadError as exc:
        typer.echo(f"Erreur de lecture : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    result = translate_project(project)
    write_translation(project, result, out)
    typer.echo(
        f"Projet {project.key} : {len(result.translated)} recette(s) traduite(s), "
        f"{len(result.unsupported)} non prise(s) en charge, "
        f"{len(result.without_translator)} sans traducteur pour l'instant."
    )
    for item in result.unsupported:
        typer.echo(f"  - {item.recipe} ({item.type}) : {item.element} — {item.detail}")
    typer.echo(f"SQL dans {out / project.key / 'sql'}, rapport dans translate_report.json")


@app.command()
def generate() -> None:
    """Migrer : générer notebooks, pipelines, jobs et bundle (phase 6)."""
    _not_implemented("generate", 6)


@app.command()
def connect() -> None:
    """Brancher : associer les sources Dataiku aux tables Unity Catalog (phase 7)."""
    _not_implemented("connect", 7)


@app.command()
def reconcile() -> None:
    """Réconcilier : comparer les sorties Dataiku et Databricks (phase 9)."""
    _not_implemented("reconcile", 9)


if __name__ == "__main__":  # pragma: no cover
    app()
