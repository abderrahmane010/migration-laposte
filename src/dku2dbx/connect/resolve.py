"""Branch the Dataiku sources onto Unity Catalog tables (ROADMAP phase 7, "Brancher").

A source is a dataset that no recipe of the project produces. It is branched through
its Dataiku connection (``config/connections.local.yaml``, else ``connections.yaml``):
catalog, schema (or the Dataiku schema) and a table name template. A source that
cannot be branched (unknown connection, unresolved variable, uploaded file, shared
dataset of a project absent from the mapping) is listed "à brancher" with its reason;
the bundle then gets an ingestion task that fails until it is branched.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dku2dbx.errors import Dku2dbxError
from dku2dbx.model.ir import Dataset, Project

DEFAULT_LOCAL = Path("config/connections.local.yaml")
DEFAULT_SHARED = Path("config/connections.yaml")
_PLACEHOLDER = re.compile(r"\[[^\]]*\]")  # fictitious values of the template: [CATALOGUE]


class ConnectionsError(Dku2dbxError):
    """The connections file is missing or invalid."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConnectionRule(_Model):
    catalog: str
    schema_: str | None = Field(default=None, alias="schema")
    table: str = "{table}"
    lowercase: bool = False


class ProjectTarget(_Model):
    catalog: str
    schema_: str = Field(alias="schema")


class ConnectionsConfig(_Model):
    connections: dict[str, ConnectionRule] = Field(default_factory=dict)
    shared_projects: dict[str, ProjectTarget] = Field(default_factory=dict)
    instance_variables: dict[str, str | int | float | bool] = Field(default_factory=dict)
    """Dataiku instance variables (${NODE_DC}…), absent from exports (H-17, H-24)."""


def load_connections(path: Path | None = None) -> tuple[ConnectionsConfig, str]:
    """The mapping and the file it was read from (local file first)."""
    candidates = [path] if path is not None else [DEFAULT_LOCAL, DEFAULT_SHARED]
    for candidate in candidates:
        if candidate.is_file():
            try:
                data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
                return ConnectionsConfig.model_validate(data), str(candidate)
            except (OSError, yaml.YAMLError, ValidationError) as exc:
                raise ConnectionsError(f"{candidate}: invalid connections file\n{exc}") from exc
    if path is not None:
        raise ConnectionsError(f"{path}: not found")
    return ConnectionsConfig(), "(aucun fichier : aucune source branchée)"


class Branching(_Model):
    project: str
    source_file: str
    resolved: dict[str, str] = Field(default_factory=dict)
    """Source dataset (or PROJECT.dataset) -> catalog.schema.table."""
    unresolved: dict[str, str] = Field(default_factory=dict)
    """Source dataset -> reason (French, for the report)."""
    partitioned: list[str] = Field(default_factory=list)
    """Branched sources partitioned in Dataiku: their table needs the date column (H-53)."""


def _sources(project: Project) -> list[str]:
    produced = {o.ref for r in project.recipes for o in r.outputs}
    read = {i.ref for r in project.recipes for i in r.inputs}
    saved = {m.id for m in project.saved_models}
    return sorted(name for name in read - produced - saved)


def _table_name(rule: ConnectionRule, dataset: Dataset) -> tuple[str | None, str | None]:
    """(catalog.schema.table, None) or (None, reason)."""
    schema = rule.schema_ if rule.schema_ is not None else dataset.db_schema_resolved
    table = dataset.table_resolved or dataset.name
    # A Dataiku value only matters when the mapping uses it.
    used = [(schema, "le schéma")]
    if "{table}" in rule.table:
        used.append((table, "la table"))
    for value, what in used:
        if value is None:
            return None, f"{what} Dataiku est absent et le mapping ne le fixe pas"
        if "${" in value:
            return None, f"variable non résolue dans {what} Dataiku ({value})"
    try:
        name = rule.table.format(table=table, dataset=dataset.name, schema=schema)
    except (KeyError, IndexError):
        return None, f"modèle de table invalide « {rule.table} »"
    if rule.lowercase:
        name = name.lower()
    parts = [rule.catalog, str(schema), name]
    if any(_PLACEHOLDER.fullmatch(p) for p in parts):
        return None, "valeur fictive du mapping ([…]) : à remplacer dans connections.local.yaml"
    return ".".join(parts), None


def branch_sources(project: Project, config: ConnectionsConfig, source_file: str) -> Branching:
    result = Branching(project=project.key, source_file=source_file)
    datasets = {d.name: d for d in project.datasets}
    for name in _sources(project):
        dataset = datasets.get(name)
        if dataset is None:  # PROJECT.dataset shared by another project
            owner, _, local = name.rpartition(".")
            target = config.shared_projects.get(owner)
            if not owner:
                result.unresolved[name] = "ni dataset du projet ni dataset partagé"
            elif target is None:
                result.unresolved[name] = (
                    f"dataset partagé du projet {owner}, absent de shared_projects"
                )
            else:
                result.resolved[name] = f"{target.catalog}.{target.schema_}.{local}"
            continue
        if dataset.type == "UploadedFiles":
            result.unresolved[name] = (
                "dataset UploadedFiles : fichier à déposer dans un Volume Unity Catalog, "
                "puis à charger en table"
            )
            continue
        if dataset.type == "Inline":
            result.unresolved[name] = (
                "dataset Inline (données saisies dans Dataiku) : à recréer en table de "
                "référence Unity Catalog"
            )
            continue
        rule = config.connections.get(dataset.connection or "")
        if rule is None:
            result.unresolved[name] = f"connexion « {dataset.connection} » absente du mapping"
            continue
        full_name, reason = _table_name(rule, dataset)
        if full_name is None:
            result.unresolved[name] = reason or "?"
            continue
        result.resolved[name] = full_name
        if dataset.partitioning:
            result.partitioned.append(name)
    return result
