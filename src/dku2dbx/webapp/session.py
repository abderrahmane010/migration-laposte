"""The migration journey of the app, without Streamlit (ROADMAP phase 10).

Each function is one step of the journey (Analyser → Simplifier → Brancher → Migrer →
Réconcilier) and calls exactly the code of the CLI, so the app adds no logic of its
own and is tested like the rest. Files are written under a private working directory
per upload; the bundle and the reconciliation are returned as ZIP archives to
download (decision of 2026-11-24: download only, deployment stays a human action
after review).
"""

from __future__ import annotations

import io
import os
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dku2dbx.analyze.inventory import analyze_project
from dku2dbx.analyze.model import ProjectAnalysis
from dku2dbx.analyze.report import project_markdown
from dku2dbx.analyze.scoring import load_scoring
from dku2dbx.connect.report import markdown as connect_markdown
from dku2dbx.connect.resolve import Branching, ConnectionsConfig, branch_sources, sources
from dku2dbx.errors import Dku2dbxError
from dku2dbx.generate.bundle import Bundle, generate_job_bundle, write_bundle
from dku2dbx.model.ir import Project
from dku2dbx.reader.zip_reader import ZipExportReader
from dku2dbx.reconcile.config import ReconcileConfig
from dku2dbx.reconcile.notebook import Reconciliation, plan_reconciliation, write_reconciliation
from dku2dbx.simplify.flow import SimplifiedFlow, simplify_flow
from dku2dbx.simplify.report import markdown as simplify_markdown
from dku2dbx.translate.llm.config import LLMConfig
from dku2dbx.translate.llm.report import residue_markdown, write_residue_report
from dku2dbx.translate.llm.residue import Residue, make_client, run_residue

MAX_EXPORT_BYTES = 200 * 1024 * 1024
EXPORTS_VOLUME_ENV = "DKU2DBX_EXPORTS_VOLUME"
LLM_ENDPOINT_ENV = "DKU2DBX_LLM_ENDPOINT"
FIXED_TIME = (1980, 1, 1, 0, 0, 0)  # ZIP entries: same bytes for the same files


class SessionError(Dku2dbxError):
    """The uploaded file cannot be used."""


@dataclass
class Session:
    workdir: Path
    export: Path
    project: Project
    connections: ConnectionsConfig = field(default_factory=ConnectionsConfig)
    branching: Branching | None = None


def open_export(data: bytes, filename: str, workdir: Path) -> Session:
    """Check and read an uploaded export ZIP (configuration only, never run)."""
    if len(data) > MAX_EXPORT_BYTES:
        raise SessionError(f"{filename}: plus de {MAX_EXPORT_BYTES // 2**20} Mo")
    if not zipfile.is_zipfile(io.BytesIO(data)):
        raise SessionError(f"{filename}: ce n'est pas une archive ZIP")
    workdir.mkdir(parents=True, exist_ok=True)
    export = workdir / "export.zip"
    export.write_bytes(data)
    project = ZipExportReader(export).read()
    return Session(workdir=workdir, export=export, project=project)


def analysis(session: Session) -> tuple[ProjectAnalysis, str]:
    result = analyze_project(session.project, session.export.name, load_scoring())
    return result, project_markdown(result, None)


ISSUE_HELP = {
    "UNKNOWN_DATASET_TYPE": "type de dataset inconnu : à ajouter ou à migrer à la main",
    "UNKNOWN_RECIPE_TYPE": "type de recette inconnu : la recette passe en migration manuelle",
    "UNKNOWN_SCENARIO_TYPE": "type de scénario inconnu : à recréer à la main",
    "UNKNOWN_STEP_META_TYPE": "step Prepare d'un genre inconnu (groupe ?) : jamais ignorée",
    "UNRESOLVED_VARIABLE": "variable ${…} introuvable : la définir (projet ou instance)",
    "MISSING_REFERENCE": "objet cité mais absent de l'export (dataset, plugin, projet partagé)",
    "MISSING_PAYLOAD": "fichier de paramètres de la recette absent de l'export",
    "MULTIPLE_PAYLOADS": "plusieurs fichiers de paramètres pour une recette",
    "INVALID_PAYLOAD": "fichier de paramètres illisible",
    "UNPARSED_ENTRY": "fichiers de l'export non utiles à la migration (dashboards, analyses…)",
    "FLOW_CYCLE": "cycle dans le Flow : le projet ne peut pas être ordonné",
}


def issues(session: Session) -> list[dict[str, str]]:
    """The points to examine, one row per issue, with what to do (the "Analyser" page)."""
    return [
        {
            "type": issue.kind.value,
            "objet": f"{issue.object_kind.value} {issue.object_name}",
            "message": issue.message,
            "que faire": ISSUE_HELP.get(issue.kind.value, ""),
        }
        for issue in session.project.issues
    ]


def simplification(session: Session) -> tuple[SimplifiedFlow, str]:
    flow = simplify_flow(session.project)
    return flow, simplify_markdown(flow)


def sources_by_connection(session: Session) -> dict[str, list[str]]:
    """The connections of the sources to branch (the form of the "Brancher" step)."""
    known = {d.name: d for d in session.project.datasets}
    grouped: dict[str, list[str]] = defaultdict(list)
    for name in sources(session.project):
        dataset = known.get(name)
        connection = dataset.connection if dataset else None
        grouped[connection or "(sans connexion ou projet partagé)"].append(name)
    return {k: sorted(v) for k, v in sorted(grouped.items())}


def connect(session: Session, rules: list[dict[str, Any]]) -> tuple[Branching, str]:
    """Branch the sources with the rules typed in the form.

    ``rules``: one row per connection, ``connection``, ``catalog``, ``schema`` (may be
    empty), ``table`` (template, default ``{table}``), ``lowercase``. A row without a
    catalog is left unbranched (its sources stay "à brancher").
    """
    connections = {
        r["connection"]: {
            "catalog": r["catalog"].strip(),
            "schema": (r.get("schema") or "").strip() or None,
            "table": (r.get("table") or "").strip() or "{table}",
            "lowercase": bool(r.get("lowercase")),
        }
        for r in rules
        if (r.get("catalog") or "").strip()
    }
    session.connections = ConnectionsConfig.model_validate({"connections": connections})
    session.branching = branch_sources(session.project, session.connections, "formulaire de l'app")
    return session.branching, connect_markdown(session.project, session.branching)


@dataclass
class Migration:
    bundle: Bundle
    residue: Residue
    report: str
    """GENERATION.md."""
    residue_report: str
    archive: bytes
    """The bundle and the residue, as a ZIP to download."""


def llm_config() -> LLMConfig:
    """Databricks Model Serving endpoint of the app (env var), or the LLM disabled."""
    endpoint = os.environ.get(LLM_ENDPOINT_ENV) or None
    return LLMConfig(provider="databricks", endpoint=endpoint)


def zip_directory(root: Path) -> bytes:
    """A reproducible ZIP of ``root`` (sorted entries, fixed dates)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(root).as_posix(), FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return buffer.getvalue()


def migrate(session: Session, target: str = "job", config: LLMConfig | None = None) -> Migration:
    """Generate the bundle and prepare (or run) the LLM residue."""
    out = session.workdir / "migration"
    bundle = generate_job_bundle(session.project, target=target, branching=session.branching)
    write_bundle(bundle, out / "bundle")
    llm = config or llm_config()
    residue = run_residue(
        session.project, llm, out / "residue", client=make_client(llm), branching=session.branching
    )
    write_residue_report(residue, out / "residue")
    return Migration(
        bundle=bundle,
        residue=residue,
        report=bundle.files["GENERATION.md"],
        residue_report=residue_markdown(residue),
        archive=zip_directory(out),
    )


@dataclass
class ReconciliationFiles:
    plan: Reconciliation
    notebook: str
    plan_report: str
    archive: bytes


def reconcile(session: Session, rules: list[dict[str, Any]]) -> ReconciliationFiles:
    """The reconciliation notebook, plan and registry, from the rules of the form.

    ``rules``: one row per table, ``table``, ``key`` (comma-separated), ``abs_tolerance``,
    ``rel_tolerance``, ``ignore_columns`` (comma-separated), ``skip``.
    """

    def split(value: Any) -> list[str]:
        return [v.strip() for v in str(value or "").split(",") if v.strip()]

    tables = {
        r["table"]: {
            "key": split(r.get("key")),
            "abs_tolerance": float(r.get("abs_tolerance") or 0.0),
            "rel_tolerance": float(r.get("rel_tolerance") or 0.0),
            "ignore_columns": split(r.get("ignore_columns")),
            "skip": bool(r.get("skip")),
        }
        for r in rules
    }
    config = ReconcileConfig.model_validate({"tables": tables})
    plan = plan_reconciliation(session.project, config, "formulaire de l'app")
    out = session.workdir / "reconcile"
    write_reconciliation(plan, out)
    return ReconciliationFiles(
        plan=plan,
        notebook=(out / "reconcile.py").read_text(encoding="utf-8"),
        plan_report=(out / "RECONCILIATION.md").read_text(encoding="utf-8"),
        archive=zip_directory(out),
    )


def reconcile_tables(session: Session) -> list[str]:
    """The tables of the "Réconcilier" form: what the migrated flow materializes."""
    return [d for step in simplify_flow(session.project).steps for d in step.outputs]


def store_export(data: bytes, filename: str) -> str | None:  # pragma: no cover (workspace)
    """Copy the export to the restricted Unity Catalog Volume, when the app has one."""
    volume = os.environ.get(EXPORTS_VOLUME_ENV)
    if not volume:
        return None
    from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]  # noqa: PLC0415

    path = f"{volume.rstrip('/')}/{Path(filename).name}"
    WorkspaceClient().files.upload(path, io.BytesIO(data), overwrite=True)
    return path
