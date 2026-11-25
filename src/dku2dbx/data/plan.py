"""Decide, for every dataset of a project, how its data reaches Unity Catalog (phase 11).

Never copy by default (decision of 2026-11-25). Each dataset gets one strategy, and the
reason in French for the report:
- ``BRANCHÉ``: a source already mapped to a Unity Catalog table (phase 7);
- ``RÉFÉRENCE``: a source read in place through Lakehouse Federation (no copy);
- ``COPIE``: a source copied once into the target schema (Federation or files);
- ``RECALCUL``: produced by a recipe, rebuilt by the job;
- ``HISTORIQUE``: produced by a recipe, but its existing data cannot be rebuilt
  (partitions never recomputed, append output): copied once;
- ``REFUSÉ``: anything else, never guessed.

Lakehouse Federation is classified explicitly, connection by connection: a database
type it supports is ``COMPATIBLE`` only when its connection is declared available in
config/data.yaml; otherwise ``À CONFIRMER``, ``NON DISPONIBLE`` or ``INCOMPATIBLE``.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dku2dbx.connect.resolve import Branching, sources
from dku2dbx.data.config import Access, DataConfig, FederationDeclaration
from dku2dbx.data.naming import (
    case_collisions,
    dataiku_properties,
    name_problem,
    producer,
    proposed_name,
)
from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Dataset, Project
from dku2dbx.reader.variables import find_variables, resolve
from dku2dbx.translate.visual.types import spark_type


class Strategy(StrEnum):
    BRANCHED = "BRANCHÉ"
    REFERENCE = "RÉFÉRENCE"
    COPY = "COPIE"
    RECOMPUTE = "RECALCUL"
    HISTORY = "HISTORIQUE"
    REFUSED = "REFUSÉ"


class Federation(StrEnum):
    COMPATIBLE = "COMPATIBLE"
    TO_CONFIRM = "À CONFIRMER"
    UNAVAILABLE = "NON DISPONIBLE"
    INCOMPATIBLE = "INCOMPATIBLE"
    NOT_APPLICABLE = "—"


COPIED = frozenset({Strategy.COPY, Strategy.HISTORY})

# ASSUMPTION (H-65): Dataiku dataset types of the databases Lakehouse Federation can
# read (MySQL, PostgreSQL, SQL Server, Oracle, Redshift, Snowflake, BigQuery, Teradata,
# Azure Synapse). To check on the target workspace: the list depends on its version.
FEDERATION_TYPES = frozenset(
    {
        "PostgreSQL",
        "MySQL",
        "SQLServer",
        "Oracle",
        "Redshift",
        "Snowflake",
        "BigQuery",
        "Teradata",
        "Synapse",
    }
)
# Other SQL databases known to Dataiku: no Federation connector.
OTHER_SQL_TYPES = frozenset(
    {"Vertica", "Greenplum", "DB2", "Netezza", "SAPHANA", "Athena", "JDBC", "Trino"}
)
# ASSUMPTION (H-66): file datasets keep their location in params.path, relative to the
# root of their connection; uploaded files live in the Dataiku uploads folder.
FILE_TYPES = frozenset({"Filesystem", "HDFS", "S3", "Azure", "GCS"})
UPLOADED = "UploadedFiles"
# ASSUMPTION (H-67): CSV style -> Spark CSV options (quote, escape); formats read with
# the declared schema; dates as written by Dataiku (ISO, or HIVE "yyyy-MM-dd HH:mm:ss").
SIMPLE_FORMATS = frozenset({"parquet", "orc", "json"})
CSV_STYLES = frozenset({"excel", "unix", "escape_only_no_quote"})
CHARSETS = {"utf8": "UTF-8", "utf-8": "UTF-8", "latin1": "ISO-8859-1", "iso-8859-1": "ISO-8859-1"}
DATE_FORMATS = {"ISO": None, "HIVE": "yyyy-MM-dd HH:mm:ss[.SSS]"}
PLACEHOLDER = re.compile(r"\[[A-Z0-9_]+\]")
# ASSUMPTION (H-69): Delta refuses these characters in a column name unless the table has
# column mapping ('delta.columnMapping.mode' = 'name'); Dataiku accepts them.
DELTA_FORBIDDEN = re.compile(r"[ ,;{}()\n\t=]")


def _declared(value: str, what: str) -> str:
    """A value of data.yaml, refused while it is still a fictitious example."""
    if PLACEHOLDER.search(value):
        raise _Refusal(f"valeur fictive pour {what} ({value}) : à remplacer dans data.local.yaml")
    return value


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatasetDecision(_Model):
    dataset: str
    role: str
    """source, produit (by a recipe of the project) or partagé (another project)."""
    strategy: Strategy
    federation: Federation = Federation.NOT_APPLICABLE
    reason: str
    uc_name: str
    """Physical table name: the Dataiku name, or the proposed one when refused for it."""
    name_note: str | None = None
    reference: str | None = None
    """BRANCHÉ: the mapped table; RÉFÉRENCE: the foreign table to map in connections.yaml."""
    properties: dict[str, str] = Field(default_factory=dict)
    """Original Dataiku identifiers, kept in the table properties."""
    read: dict[str, Any] | None = None
    """COPIE and HISTORIQUE: how the copy reads the data (load.py spec)."""
    columns: list[list[str]] = Field(default_factory=list)
    partition_column: str | None = None
    column_mapping: bool = False
    """A column name Delta refuses without column mapping (space, comma…): enabled (H-69)."""


class DataPlan(_Model):
    project: str
    source: str
    decisions: list[DatasetDecision]

    def decision(self, dataset: str) -> DatasetDecision:
        return next(d for d in self.decisions if d.dataset == dataset)

    @property
    def copies(self) -> list[DatasetDecision]:
        return [d for d in self.decisions if d.strategy in COPIED]


class _Refusal(Exception):  # noqa: N818 (a decision, not an error)
    def __init__(self, reason: str, federation: Federation = Federation.NOT_APPLICABLE) -> None:
        super().__init__(reason)
        self.reason, self.federation = reason, federation


def _columns(dataset: Dataset) -> list[list[str]]:
    if not dataset.columns:
        raise _Refusal("schéma Dataiku vide : impossible de valider la copie")
    try:
        return [
            [c.name, spark_type(c.type, dataset.name, c.name).sql(dialect="databricks").lower()]
            for c in dataset.columns
        ]
    except UnsupportedConstruct as exc:
        raise _Refusal(f"type de colonne non pris en charge ({exc.detail})") from exc


def _partition(dataset: Dataset, *, files: bool) -> str | None:
    dims = dataset.partitioning
    if not dims:
        return None
    if files:
        raise _Refusal("dataset de fichiers partitionné (motif de chemin) : non pris en charge")
    if len(dims) != 1:
        raise _Refusal(f"{len(dims)} dimensions de partition : non pris en charge")
    if dims[0].name not in {c.name for c in dataset.columns}:
        raise _Refusal(f"colonne de partition {dims[0].name} absente du schéma Dataiku")
    return dims[0].name


def _resolved(value: str | None, what: str, project: Project) -> str:
    if not value:
        raise _Refusal(f"{what} Dataiku absent")
    text, missing = resolve(value, {**project.variables, "projectKey": project.key})
    if missing or find_variables(text):
        raise _Refusal(f"variable non résolue dans {what} Dataiku ({value})")
    return text


def _csv_options(dataset: Dataset) -> dict[str, str]:
    fp = dataset.format_params
    style = fp.get("style")
    if style not in CSV_STYLES:
        raise _Refusal(f"style CSV {style!r} non pris en charge")
    for key in ("skipRowsBeforeHeader", "skipRowsAfterHeader"):
        if fp.get(key, 0):
            raise _Refusal(f"option CSV {key} = {fp[key]} non prise en charge")
    charset = str(fp.get("charset", "utf8")).lower()
    if charset not in CHARSETS:
        raise _Refusal(f"encodage {charset!r} non pris en charge")
    dates = fp.get("dateSerializationFormat", "ISO")
    if dates not in DATE_FORMATS:
        raise _Refusal(f"format de date {dates!r} non pris en charge")
    quote = str(fp.get("quoteChar", '"'))
    options = {
        "sep": str(fp.get("separator", ",")),
        "header": "true" if fp.get("parseHeaderRow") else "false",
        "encoding": CHARSETS[charset],
        "mode": "FAILFAST",  # a malformed line fails the copy instead of becoming NULLs
        "quote": "" if style == "escape_only_no_quote" else quote,
        "escape": quote if style == "excel" else str(fp.get("escapeChar", "\\")),
    }
    if DATE_FORMATS[dates]:
        options["timestampFormat"] = str(DATE_FORMATS[dates])
    return options


def _file_read(dataset: Dataset, path: str) -> dict[str, Any]:
    fmt = dataset.format_type
    columns = _columns(dataset)
    schema = ", ".join(f"`{name}` {kind.upper()}" for name, kind in columns)
    if fmt == "csv":
        return {
            "kind": "files",
            "format": "csv",
            "path": path,
            "options": _csv_options(dataset),
            "schema": schema,
        }
    if fmt in SIMPLE_FORMATS:
        return {"kind": "files", "format": fmt, "path": path, "options": {}, "schema": schema}
    raise _Refusal(f"format de fichier {fmt!r} non pris en charge")


def _join(root: str, path: str) -> str:
    return root.rstrip("/") + "/" + path.lstrip("/")


def federation_status(dataset: Dataset, config: DataConfig) -> Federation:
    if dataset.type not in FEDERATION_TYPES:
        if dataset.type in OTHER_SQL_TYPES:
            return Federation.INCOMPATIBLE
        return Federation.NOT_APPLICABLE
    declared = config.connections.get(dataset.connection or "")
    if declared is None or declared.federation is None:
        return Federation.TO_CONFIRM
    if declared.federation is FederationDeclaration.UNAVAILABLE:
        return Federation.UNAVAILABLE
    return Federation.COMPATIBLE


_FEDERATION_REASONS = {
    Federation.TO_CONFIRM: "type compatible avec Lakehouse Federation, mais connexion {c} non "
    "déclarée dans data.yaml (federation: available + foreign_catalog) : à confirmer",
    Federation.UNAVAILABLE: "connexion {c} déclarée indisponible pour Lakehouse Federation",
    Federation.INCOMPATIBLE: "base {t} sans connecteur Lakehouse Federation : à brancher "
    "(phase 7) ou à extraire autrement",
}


def _sql_source(
    project: Project, dataset: Dataset, config: DataConfig
) -> tuple[Strategy, Federation, str | None, dict[str, Any] | None, str]:
    federation = federation_status(dataset, config)
    if federation is not Federation.COMPATIBLE:
        reason = _FEDERATION_REASONS[federation].format(c=dataset.connection, t=dataset.type)
        raise _Refusal(reason, federation)
    if dataset.mode not in (None, "table"):
        raise _Refusal(f"dataset défini en mode {dataset.mode!r} (requête) : non pris en charge")
    declared = config.connections[dataset.connection or ""]
    try:  # resolved by the reader, instance variables included (phase 7)
        schema = _resolved(dataset.db_schema_resolved, "le schéma", project)
        table = _resolved(dataset.table_resolved, "la table", project)
        foreign = _declared(str(declared.foreign_catalog), "foreign_catalog")
    except _Refusal as exc:
        raise _Refusal(exc.reason, federation) from exc
    name = ".".join(f"`{p}`" for p in (foreign, schema, table))
    if declared.access is Access.REFERENCE:
        return (
            Strategy.REFERENCE,
            federation,
            f"{foreign}.{schema}.{table}",
            None,
            "lu sur place par Lakehouse Federation : ajouter le branchement dans "
            "connections.local.yaml",
        )
    read = {"kind": "table", "name": name}
    return Strategy.COPY, federation, None, read, "copié une fois depuis Lakehouse Federation"


def _read_plan(
    project: Project, dataset: Dataset, config: DataConfig
) -> tuple[Strategy, Federation, str | None, dict[str, Any] | None, str]:
    """(strategy, federation, reference, read spec, reason) of data stored by Dataiku."""
    if dataset.type in FEDERATION_TYPES or dataset.type in OTHER_SQL_TYPES:
        return _sql_source(project, dataset, config)
    if dataset.type == UPLOADED:
        if not config.uploads_root:
            raise _Refusal(
                "fichiers déposés dans Dataiku : uploads_root non déclaré dans data.yaml"
            )
        root = _declared(config.uploads_root, "uploads_root")
        read = _file_read(dataset, _join(root, dataset.name))
        return (
            Strategy.COPY,
            Federation.NOT_APPLICABLE,
            None,
            read,
            (f"fichiers déposés dans Dataiku, à placer dans {read['path']} puis copiés une fois"),
        )
    if dataset.type in FILE_TYPES:
        declared = config.connections.get(dataset.connection or "")
        if declared is None or declared.root is None:
            raise _Refusal(
                f"emplacement des fichiers de la connexion {dataset.connection} non déclaré "
                "(root dans data.yaml)"
            )
        raw = dataset.params.get("path")
        path = _resolved(raw if isinstance(raw, str) else None, "le chemin", project)
        read = _file_read(dataset, _join(_declared(declared.root, "root"), path))
        return (
            Strategy.COPY,
            Federation.NOT_APPLICABLE,
            None,
            read,
            (f"fichiers copiés une fois depuis {read['path']}"),
        )
    raise _Refusal(f"type de dataset {dataset.type} non pris en charge")


def _history_reason(project: Project, dataset: str) -> str | None:
    """Why the existing data of a produced dataset cannot be rebuilt, None if it can."""
    for rcp in project.recipes:
        for out in rcp.outputs:
            if out.ref != dataset:
                continue
            if out.append_mode:
                return f"sortie en mode append de {rcp.name} : l'historique ne se recalcule pas"
            if rcp.never_recompute_existing_partitions:
                return f"partitions existantes jamais recalculées par {rcp.name}"
    return None


def _decide(
    project: Project, dataset: Dataset, config: DataConfig, branching: Branching | None
) -> DatasetDecision:
    base: dict[str, Any] = {
        "dataset": dataset.name,
        "uc_name": dataset.name,
        "properties": dataiku_properties(project, dataset),
    }
    produced = producer(project, dataset.name) is not None
    base["role"] = "produit" if produced else "source"
    if branching is not None and dataset.name in branching.resolved:
        return DatasetDecision(
            **base,
            strategy=Strategy.BRANCHED,
            federation=federation_status(dataset, config),
            reference=branching.resolved[dataset.name],
            reason="déjà branché sur une table Unity Catalog (phase 7)",
        )
    history = _history_reason(project, dataset.name) if produced else None
    if produced and history is None:
        return DatasetDecision(
            **base, strategy=Strategy.RECOMPUTE, reason="recalculé par le job à partir des sources"
        )
    try:
        strategy, federation, reference, read, reason = _read_plan(project, dataset, config)
        if read is not None:
            columns = _columns(dataset)
            part = _partition(dataset, files=read["kind"] == "files")
        else:
            columns, part = [], None
    except _Refusal as exc:
        why = f"historique à copier ({history}) ; {exc.reason}" if history else exc.reason
        return DatasetDecision(
            **base, strategy=Strategy.REFUSED, federation=exc.federation, reason=why
        )
    if history is not None and strategy is Strategy.REFERENCE:
        reason, read = f"{history} ; référencer l'historique par Federation", None
    elif history is not None:
        strategy, reason = Strategy.HISTORY, f"{history} : {reason}"
    return DatasetDecision(
        **base,
        strategy=strategy,
        federation=federation,
        reference=reference,
        reason=reason,
        read=read,
        columns=columns,
        partition_column=part,
        column_mapping=any(DELTA_FORBIDDEN.search(name) for name, _ in columns),
    )


def _shared(name: str, branching: Branching | None) -> DatasetDecision:
    """A PROJECT.dataset read from another project: migrated with its own project."""
    if branching is not None and name in branching.resolved:
        return DatasetDecision(
            dataset=name,
            role="partagé",
            strategy=Strategy.BRANCHED,
            uc_name=name.rpartition(".")[2],
            reference=branching.resolved[name],
            reason="dataset partagé, branché via shared_projects (phase 7)",
        )
    return DatasetDecision(
        dataset=name,
        role="partagé",
        strategy=Strategy.REFUSED,
        uc_name=name.rpartition(".")[2],
        reason="dataset partagé par un autre projet : à migrer avec lui, puis à brancher "
        "(shared_projects)",
    )


def _check_name(decision: DatasetDecision, collisions: dict[str, list[str]]) -> DatasetDecision:
    """Refuse a dataset whose name cannot be used as is; the original stays in metadata."""
    problem = name_problem(decision.dataset)
    if problem is None and decision.dataset in collisions:
        others = ", ".join(collisions[decision.dataset])
        problem = f"même nom que {others} à la casse près (Unity Catalog ignore la casse)"
    if problem is None or decision.role == "partagé":
        return decision
    proposal = proposed_name(decision.dataset)
    return decision.model_copy(
        update={
            "strategy": Strategy.REFUSED,
            "uc_name": proposal,
            "name_note": problem,
            "reason": f"nom incompatible avec Unity Catalog ({problem}) : renommage à décider, "
            f"proposition {proposal}",
            "read": None,
        }
    )


def plan_data(
    project: Project, config: DataConfig, source: str, branching: Branching | None = None
) -> DataPlan:
    """One decision per dataset of the project, and per shared dataset it reads."""
    collisions = case_collisions([d.name for d in project.datasets])
    decisions = [
        _check_name(_decide(project, d, config, branching), collisions)
        for d in sorted(project.datasets, key=lambda d: d.name)
    ]
    own = {d.name for d in project.datasets}
    decisions += [_shared(name, branching) for name in sources(project) if name not in own]
    return DataPlan(project=project.key, source=source, decisions=decisions)
