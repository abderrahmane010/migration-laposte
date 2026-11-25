"""Command-line entry point for dku2dbx.

Sub-commands not implemented yet are placeholders: they print which roadmap
phase will implement them and exit with a non-zero code, so that no caller
mistakes an empty command for a successful run.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import typer

from dku2dbx import __version__
from dku2dbx.analyze.inventory import analyze_project
from dku2dbx.analyze.model import ProjectAnalysis
from dku2dbx.analyze.portfolio import analyze_portfolio
from dku2dbx.analyze.report import write_outputs
from dku2dbx.analyze.scoring import load_scoring
from dku2dbx.connect.report import write_report as write_connect_report
from dku2dbx.connect.resolve import ConnectionsError, branch_sources, load_connections
from dku2dbx.connect.uc_check import check_tables, workspace_client
from dku2dbx.data.config import DataConfigError, load_data_config
from dku2dbx.data.notebook import REPORT as DATA_REPORT
from dku2dbx.data.notebook import copy_notebook, write_data
from dku2dbx.data.permissions import check_permissions, grants_client
from dku2dbx.data.plan import Strategy as DataStrategy
from dku2dbx.data.plan import plan_data
from dku2dbx.errors import ExportReadError, ScoringConfigError
from dku2dbx.generate.bundle import GenerateError, generate_job_bundle, write_bundle
from dku2dbx.model.ir import Project
from dku2dbx.reader.api_reader import ApiError, DataikuApiReader
from dku2dbx.reader.compare import COMPARISON, compare_projects
from dku2dbx.reader.compare import report_markdown as compare_report
from dku2dbx.reader.zip_reader import ZipExportReader
from dku2dbx.reconcile.config import ReconcileConfigError, load_reconcile_config
from dku2dbx.reconcile.notebook import (
    NOTEBOOK,
    PLAN,
    REGISTRY,
    plan_reconciliation,
    write_reconciliation,
)
from dku2dbx.simplify.flow import SimplifyError, simplify_flow
from dku2dbx.simplify.report import write_report
from dku2dbx.translate.engine import translate_project, write_translation
from dku2dbx.translate.llm.config import LLMConfigError, load_llm_config
from dku2dbx.translate.llm.report import write_residue_report
from dku2dbx.translate.llm.residue import Status, make_client, run_residue

app = typer.Typer(
    name="dku2dbx",
    help="Usine de migration Dataiku DSS → Databricks (Analyser → Simplifier → Brancher → Migrer).",
    no_args_is_help=True,
    add_completion=False,
)


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


def _read(
    export: Path | None, api: str | None, instance_variables: dict[str, Any] | None = None
) -> Project:
    """The project of an export ZIP, or read by the Dataiku API (phase 12): same engine."""
    if (export is None) == (api is None):
        raise ExportReadError("donner soit un export ZIP, soit --api <clé du projet>")
    if export is not None:
        return ZipExportReader(export, instance_variables).read()
    try:
        return DataikuApiReader(instance_variables=instance_variables).read_project(str(api))
    except KeyError as exc:
        raise ApiError(f"projet {api} absent de l'instance Dataiku (ou non accessible)") from exc


def _exports(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(p for p in path.iterdir() if p.suffix == ".zip" and p.is_file())
    return [path]


@app.command()
def analyze(  # noqa: PLR0912 (one branch per source and option)
    export: Path | None = typer.Argument(
        None,
        exists=True,
        readable=True,
        help="Export ZIP d'un projet Dataiku, ou dossier d'exports ZIP (configuration seulement).",
    ),
    api: list[str] | None = typer.Option(
        None, "--api", help="Lire ce projet par l'API Dataiku (clé, répétable)."
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
    if (export is None) == (not api):
        typer.echo("Erreur : donner soit un export ou un dossier, soit --api <clé>", err=True)
        raise typer.Exit(code=1)
    sources: list[Path | str] = list(_exports(export)) if export is not None else list(api or [])
    if not sources:
        typer.echo(f"Aucun export .zip dans {export}", err=True)
        raise typer.Exit(code=1)

    projects: list[Project] = []
    analyses: list[ProjectAnalysis] = []
    failed = 0
    for source in sources:
        try:
            if isinstance(source, Path):
                doc, name = ZipExportReader(source).read_document(), source.name
            else:
                doc, name = DataikuApiReader().read_document(source), f"API {source}"
        except KeyError:
            typer.echo(f"Erreur de lecture : projet {source} absent de l'instance", err=True)
            failed += 1
            continue
        except ExportReadError as exc:
            # One unreadable export does not stop the portfolio: it is reported.
            typer.echo(f"Erreur de lecture : {exc}", err=True)
            failed += 1
            continue
        project = doc.project
        if any(p.key == project.key for p in projects):
            typer.echo(f"Erreur : projet {project.key} présent deux fois ({name})", err=True)
            failed += 1
            continue
        projects.append(project)
        analyses.append(analyze_project(project, name, config))
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
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
    no_fusion: bool = typer.Option(
        False, "--no-fusion", help="Une étape par recette (réconciliation fine, débogage)."
    ),
) -> None:
    """Simplifier : fusionner les chaînes de recettes en étapes (phase 5)."""
    try:
        project = _read(export, api)
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
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
) -> None:
    """Migrer : traduire les recettes en Spark SQL (.sql par sortie) ou en Python (.py)."""
    try:
        project = _read(export, api)
    except ExportReadError as exc:
        typer.echo(f"Erreur de lecture : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    result = translate_project(project)
    write_translation(project, result, out)
    typer.echo(
        f"Projet {project.key} : {len(result.translated)} recette(s) traduite(s), "
        f"{len(result.rewritten)} recette(s) Python réécrite(s) (à relire), "
        f"{len(result.unsupported)} non prise(s) en charge, "
        f"{len(result.without_translator)} sans traducteur pour l'instant."
    )
    for item in result.unsupported:
        typer.echo(f"  - {item.recipe} ({item.type}) : {item.element} — {item.detail}")
    typer.echo(
        f"SQL dans {out / project.key / 'sql'}, Python dans {out / project.key / 'python'}, "
        "rapport dans translate_report.json"
    )


@app.command()
def generate(  # noqa: PLR0913, PLR0917 (one option per input)
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    target: str = typer.Option("job", "--target", help="job (notebooks SQL) ou pipeline."),
    out: Path | None = typer.Option(
        None, "--out", help="Dossier du bundle (défaut : out/<projet>/bundle)."
    ),
    no_fusion: bool = typer.Option(False, "--no-fusion", help="Une étape par recette."),
    connections: Path | None = typer.Option(
        None, "--connections", help="Mapping des sources (phase 7, défaut : config/…)."
    ),
    data_config: Path | None = typer.Option(
        None, "--data", help="Accès aux données (phase 11, défaut : config/data.local.yaml…)."
    ),
) -> None:
    """Migrer : générer le bundle Databricks, job de notebooks SQL ou pipeline (phase 6)."""
    if target not in ("job", "pipeline"):
        typer.echo(f"Erreur : cible inconnue {target!r} (job ou pipeline)", err=True)
        raise typer.Exit(code=1)
    try:
        config, source = load_connections(connections)
        project = _read(export, api, config.instance_variables)
        branching = branch_sources(project, config, source)
        data_plan = plan_data(project, *load_data_config(data_config), branching)
        copies = {d.dataset: copy_notebook(data_plan, d) for d in data_plan.copies}
        bundle = generate_job_bundle(
            project, fusion=not no_fusion, target=target, branching=branching, copies=copies
        )
    except (
        ExportReadError,
        SimplifyError,
        GenerateError,
        ConnectionsError,
        DataConfigError,
    ) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    target_dir = out or Path("out") / project.key / "bundle"
    write_bundle(bundle, target_dir)
    manual = [g for g in bundle.steps if not g.translated]
    typer.echo(
        f"Projet {project.key} : bundle de {len(bundle.steps)} étape(s) dans {target_dir}, "
        f"dont {len(manual)} à migrer à la main (notebook qui échoue explicitement)."
    )
    for g in manual:
        typer.echo(f"  - {g.step.id} : {'; '.join(g.reasons)}")
    if bundle.copies:
        typer.echo(f"Sources copiées par le job (phase 11) : {', '.join(bundle.copies)}")


@app.command()
def connect(  # noqa: PLR0913, PLR0917 (one option per input)
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    connections: Path | None = typer.Option(
        None,
        "--connections",
        help="Mapping (défaut : config/connections.local.yaml, sinon config/connections.yaml).",
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
    check_uc: bool = typer.Option(
        False, "--check-uc", help="Vérifier les tables dans Unity Catalog (profil Databricks)."
    ),
    profile: str | None = typer.Option(None, "--profile", help="Profil Databricks CLI."),
) -> None:
    """Brancher : associer les sources Dataiku aux tables Unity Catalog (phase 7)."""
    try:
        config, source = load_connections(connections)
        project = _read(export, api, config.instance_variables)
    except (ExportReadError, ConnectionsError) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    branching = branch_sources(project, config, source)
    write_connect_report(project, branching, out)
    typer.echo(
        f"Projet {project.key} : {len(branching.resolved)} source(s) branchée(s), "
        f"{len(branching.unresolved)} à brancher (mapping {source})."
    )
    typer.echo(f"Rapport dans {out / project.key / 'a_brancher.md'}")
    if check_uc:
        problems = check_tables(project, branching, workspace_client(profile))
        for name, problem in problems.items():
            typer.echo(f"  - {name} : {problem}")
        if problems:
            raise typer.Exit(code=1)


@app.command()
def residue(
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    llm_config: Path | None = typer.Option(
        None,
        "--llm-config",
        help="Endpoint validé (défaut : config/llm.local.yaml, sinon config/llm.yaml).",
    ),
    connections: Path | None = typer.Option(
        None, "--connections", help="Mapping des sources (phase 7, défaut : config/…)."
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
) -> None:
    """Migrer : préparer le résidu pour le LLM et, si un endpoint validé existe, l'appeler."""
    try:
        config = load_llm_config(llm_config)
        mapping, mapping_source = load_connections(connections)
        project = _read(export, api, mapping.instance_variables)
    except (ExportReadError, ConnectionsError, LLMConfigError) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    branching = branch_sources(project, mapping, mapping_source)
    client = make_client(config)
    target = out / project.key / "residue"
    result = run_residue(project, config, target, client=client, branching=branching)
    write_residue_report(result, target)
    counts = Counter(item.status for item in result.items)
    typer.echo(
        f"Projet {project.key} : {len(result.items)} recette(s) dans le résidu LLM "
        f"({', '.join(f'{n} {s}' for s, n in sorted(counts.items())) or 'aucune'}), "
        f"{len(result.deterministic_to_review)} réécriture(s) déterministe(s) à relire."
    )
    typer.echo(f"LLM : {result.llm}")
    if not config.enabled:
        typer.echo(f"Entrées prêtes ({Status.READY}) dans {target / 'inputs'}")
    typer.echo(f"Rapport dans {target / 'RESIDU.md'}")


@app.command()
def reconcile(
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    config_path: Path | None = typer.Option(
        None,
        "--config",
        help="Clés et tolérances (défaut : config/reconcile.local.yaml, sinon reconcile.yaml).",
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
) -> None:
    """Réconcilier : générer le notebook qui compare les sorties Dataiku et Databricks (phase 9)."""
    try:
        config, source = load_reconcile_config(config_path)
        project = _read(export, api)
        plan = plan_reconciliation(project, config, source)
    except (ExportReadError, ReconcileConfigError, SimplifyError) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    target = out / project.key / "reconcile"
    write_reconciliation(plan, target)
    keyed = sum(1 for s in plan.specs if s["key"])
    typer.echo(
        f"Projet {project.key} : {len(plan.specs)} table(s) à comparer ({keyed} avec clé, "
        f"{len(plan.specs) - keyed} par EXCEPT ALL), {len(plan.skipped)} exclue(s) "
        f"(configuration {source})."
    )
    for warning in plan.warnings:
        typer.echo(f"  - {warning}")
    typer.echo(f"Notebook {target / NOTEBOOK}, plan {target / PLAN}, registre {target / REGISTRY}")


@app.command()
def data(  # noqa: PLR0913, PLR0917 (one option per input)
    export: Path | None = typer.Argument(
        None, exists=True, dir_okay=False, readable=True, help="Export ZIP d'un projet Dataiku."
    ),
    api: str | None = typer.Option(None, "--api", help="Lire le projet par l'API Dataiku (clé)."),
    data_config: Path | None = typer.Option(
        None, "--data", help="Accès aux données (défaut : config/data.local.yaml, sinon data.yaml)."
    ),
    connections: Path | None = typer.Option(
        None, "--connections", help="Mapping des sources (phase 7, défaut : config/…)."
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
    check_uc: bool = typer.Option(
        False, "--check-uc", help="Vérifier les droits dans Unity Catalog (profil Databricks)."
    ),
    profile: str | None = typer.Option(None, "--profile", help="Profil Databricks CLI."),
    principal: str | None = typer.Option(
        None, "--principal", help="Identité du job (utilisateur ou service principal)."
    ),
    target: str | None = typer.Option(
        None, "--target", help="catalog.schema cible, pour --check-uc."
    ),
) -> None:
    """Migrer les datasets : référencer, copier une fois, recalculer ou refuser (phase 11)."""
    if check_uc and not (principal and target and target.count(".") == 1):
        typer.echo("Erreur : --check-uc demande --principal et --target catalog.schema", err=True)
        raise typer.Exit(code=1)
    try:
        mapping, mapping_source = load_connections(connections)
        config, source = load_data_config(data_config)
        project = _read(export, api, mapping.instance_variables)
        branching = branch_sources(project, mapping, mapping_source)
        plan = plan_data(project, config, source, branching)
    except (ExportReadError, ConnectionsError, DataConfigError) as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    permissions = None
    if check_uc and principal and target:
        catalog, schema = target.split(".")
        client = grants_client(profile)
        permissions = check_permissions(
            plan, client, principal=principal, catalog=catalog, schema=schema
        )
    folder = out / project.key / "donnees"
    write_data(plan, folder, permissions)
    counts = Counter(d.strategy for d in plan.decisions)
    summary = ", ".join(f"{counts[s]} {s.value}" for s in DataStrategy if counts[s])
    typer.echo(f"Projet {project.key} : {len(plan.decisions)} dataset(s) — {summary}.")
    typer.echo(f"Rapport dans {folder / DATA_REPORT} (configuration {source})")
    if permissions and any(permissions.values()):
        typer.echo("Droits manquants : voir la section Permissions du rapport.")
        raise typer.Exit(code=1)


@app.command(name="list")
def list_projects() -> None:
    """Lister les projets lisibles par l'API Dataiku (phase 12, lecture seule)."""
    try:
        keys = DataikuApiReader().list_projects()
    except ExportReadError as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"{len(keys)} projet(s) :")
    for key in keys:
        typer.echo(f"  - {key}")


@app.command()
def compare(
    project_key: str = typer.Argument(..., help="Clé du projet sur l'instance Dataiku."),
    export: Path = typer.Option(
        ..., "--zip", exists=True, dir_okay=False, readable=True, help="Export ZIP du projet."
    ),
    out: Path = typer.Option(Path("out"), "--out", help="Dossier de sortie."),
) -> None:
    """Comparer champ par champ le projet lu par l'API et lu depuis son export (phase 12)."""
    try:
        from_zip = ZipExportReader(export).read()
        from_api = _read(None, project_key)
    except ExportReadError as exc:
        typer.echo(f"Erreur : {exc}", err=True)
        raise typer.Exit(code=1) from exc
    differences = compare_projects(from_zip, from_api)
    unexplained = [d for d in differences if d.explanation is None]
    target = out / project_key / COMPARISON
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(compare_report(project_key, export.name, differences).encode("utf-8"))
    typer.echo(
        f"Projet {project_key} : {len(differences)} différence(s), dont "
        f"{len(unexplained)} non expliquée(s). Rapport dans {target}"
    )
    for d in unexplained[:20]:
        typer.echo(f"  - {d.path}")
    if from_zip.key != project_key:
        typer.echo(f"Attention : l'export est celui du projet {from_zip.key}", err=True)
    if unexplained or from_zip.key != project_key:
        raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
