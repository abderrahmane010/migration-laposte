"""Optional check of the Unity Catalog rights a dataset migration needs (phase 11).

Run only with ``dku2dbx data --check-uc`` and a Databricks CLI profile; tests use a fake
client. ASSUMPTION (H-68): ``WorkspaceClient().grants.get_effective(securable_type=…,
full_name=…, principal=…)`` returns ``privilege_assignments[].privileges[].privilege``
including the privileges inherited from the catalog and the schema; ``ALL_PRIVILEGES``
grants every privilege.
"""

from __future__ import annotations

from typing import Any, Protocol

from dku2dbx.data.plan import DataPlan, DatasetDecision, Strategy


class _Grants(Protocol):
    def get_effective(self, securable_type: Any, full_name: str, *, principal: str) -> Any: ...


class GrantsClient(Protocol):
    @property
    def grants(self) -> _Grants: ...


def _securable(kind: str) -> Any:  # pragma: no cover (SDK enum when it is installed)
    try:
        from databricks.sdk.service.catalog import SecurableType  # type: ignore[import-not-found]  # noqa: PLC0415
    except ImportError:
        return kind
    return SecurableType(kind)


def _needs(decision: DatasetDecision, catalog: str, schema: str) -> list[tuple[str, str, str]]:
    """(securable type, full name, privilege) needed by a decision."""
    needs = []
    read = decision.read or {}
    source: str | None = decision.reference
    if read.get("kind") == "table":
        source = str(read["name"]).replace("`", "")
    if source:
        cat, sch, _ = source.split(".", 2)
        needs += [
            ("catalog", cat, "USE_CATALOG"),
            ("schema", f"{cat}.{sch}", "USE_SCHEMA"),
            ("table", source, "SELECT"),
        ]
    path = str(read.get("path", ""))
    if read.get("kind") == "files" and path.startswith("/Volumes/"):
        cat, sch, vol = path.split("/")[2:5]
        needs.append(("volume", f"{cat}.{sch}.{vol}", "READ_VOLUME"))
    if decision.strategy in (Strategy.COPY, Strategy.HISTORY):
        needs += [
            ("catalog", catalog, "USE_CATALOG"),
            ("schema", f"{catalog}.{schema}", "USE_SCHEMA"),
            ("schema", f"{catalog}.{schema}", "CREATE_TABLE"),
        ]
    return needs


def _privileges(client: GrantsClient, kind: str, name: str, principal: str) -> set[str] | str:
    try:
        info = client.grants.get_effective(_securable(kind), name, principal=principal)
    except Exception as exc:  # SDK errors vary (NotFound, PermissionDenied…): all reported
        return f"{kind} {name} inaccessible ({type(exc).__name__})"
    found = set()
    for assignment in getattr(info, "privilege_assignments", None) or []:
        for privilege in getattr(assignment, "privileges", None) or []:
            value = getattr(privilege, "privilege", privilege)
            found.add(str(getattr(value, "value", value)))
    return found


def check_permissions(
    plan: DataPlan, client: GrantsClient, *, principal: str, catalog: str, schema: str
) -> dict[str, list[str]]:
    """Dataset -> missing rights (French); external locations are listed for a manual check."""
    cache: dict[tuple[str, str], set[str] | str] = {}
    result: dict[str, list[str]] = {}
    for decision in plan.decisions:
        if decision.strategy in (Strategy.RECOMPUTE, Strategy.REFUSED):
            continue
        missing: list[str] = []
        for kind, name, privilege in _needs(decision, catalog, schema):
            key = (kind, name)
            if key not in cache:
                cache[key] = _privileges(client, kind, name, principal)
            granted = cache[key]
            if isinstance(granted, str):
                problem = granted
            elif privilege in granted or "ALL_PRIVILEGES" in granted:
                continue
            else:
                problem = f"{privilege} manquant sur {kind} {name}"
            if problem not in missing:
                missing.append(problem)
        path = str((decision.read or {}).get("path", ""))
        if path and not path.startswith("/Volumes/"):
            missing.append(f"external location de {path} : READ FILES à vérifier à la main")
        result[decision.dataset] = missing
    return result


def grants_client(profile: str | None) -> GrantsClient:  # pragma: no cover (no workspace)
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]  # noqa: PLC0415 (optional dependency)
    except ImportError as exc:
        raise RuntimeError("databricks-sdk is not installed: uv add databricks-sdk") from exc
    client: GrantsClient = WorkspaceClient(profile=profile)
    return client
