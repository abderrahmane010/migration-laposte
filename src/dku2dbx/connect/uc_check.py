"""Optional check of the branched tables in Unity Catalog (ROADMAP phase 7, task 3).

Prepared but only run with ``--check-uc`` and a Databricks CLI profile (decision of
2026-11-24: no workspace yet). The client is the Databricks SDK ``WorkspaceClient``
(optional dependency, imported only then); tests use a fake client.
"""

from __future__ import annotations

from typing import Any, Protocol

from dku2dbx.connect.resolve import Branching
from dku2dbx.model.ir import Project


class _Tables(Protocol):
    def get(self, full_name: str) -> Any: ...


class WorkspaceLike(Protocol):
    @property
    def tables(self) -> _Tables: ...


def check_tables(project: Project, branching: Branching, client: WorkspaceLike) -> dict[str, str]:
    """Branched source -> problem (missing table, columns that differ). Empty: all good."""
    problems: dict[str, str] = {}
    datasets = {d.name: d for d in project.datasets}
    for name, full_name in sorted(branching.resolved.items()):
        try:
            info = client.tables.get(full_name)
        except Exception as exc:  # SDK errors vary (NotFound, PermissionDenied…): all reported
            problems[name] = f"table {full_name} inaccessible ({type(exc).__name__})"
            continue
        dataset = datasets.get(name)
        if dataset is None:
            continue
        expected = {c.name.lower() for c in dataset.columns}
        actual = {c.name.lower() for c in getattr(info, "columns", None) or []}
        missing = sorted(expected - actual)
        if missing:
            problems[name] = f"colonnes absentes de {full_name} : {', '.join(missing)}"
    return problems


def workspace_client(profile: str | None) -> WorkspaceLike:  # pragma: no cover (no workspace)
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]  # noqa: PLC0415 (optional dependency)
    except ImportError as exc:
        raise RuntimeError("databricks-sdk is not installed: uv add databricks-sdk") from exc
    client: WorkspaceLike = WorkspaceClient(profile=profile)
    return client
