"""``config/data.yaml``: where the data of each Dataiku connection can be read (phase 11).

Decision of 2026-11-25: nothing is assumed. A connection is usable through Lakehouse
Federation only when it is declared ``federation: available`` with its foreign catalog;
the files of a file connection are copied only from a declared ``root`` (a Unity Catalog
Volume or an external location). The real values go in ``config/data.local.yaml``
(ignored by Git, read first).
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from dku2dbx.errors import Dku2dbxError

DEFAULT_PATH = Path("config/data.yaml")
DEFAULT_LOCAL = Path("config/data.local.yaml")


class DataConfigError(Dku2dbxError):
    """The dataset migration configuration file is invalid."""


class FederationDeclaration(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class Access(StrEnum):
    REFERENCE = "reference"
    """Read in place through the foreign catalog (no copy)."""
    COPY = "copy"
    """Copy once into the target schema."""


class ConnectionData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    federation: FederationDeclaration | None = None
    """Absent: not declared, the connection is never assumed federated."""
    foreign_catalog: str | None = None
    access: Access = Access.REFERENCE
    root: str | None = None
    """Files of a file connection: <root>/<Dataiku path of the dataset>."""

    @model_validator(mode="after")
    def _check(self) -> ConnectionData:
        if self.federation is FederationDeclaration.AVAILABLE and not self.foreign_catalog:
            raise ValueError("federation: available requires foreign_catalog")
        if self.root is not None and not self.root.strip("/"):
            raise ValueError("root must not be empty")
        return self


class DataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connections: dict[str, ConnectionData] = Field(default_factory=dict)
    uploads_root: str | None = None
    """Files uploaded in Dataiku (UploadedFiles), dropped by hand in <uploads_root>/<dataset>/."""


def load_data_config(path: Path | None = None) -> tuple[DataConfig, str]:
    """(configuration, source); no file at all: no connection declared."""
    if path is not None and not path.is_file():
        raise DataConfigError(f"{path}: not found")
    candidate = path or next((p for p in (DEFAULT_LOCAL, DEFAULT_PATH) if p.is_file()), None)
    if candidate is None:
        return DataConfig(), "(aucun fichier : aucune connexion déclarée)"
    try:
        data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        return DataConfig.model_validate(data), str(candidate)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise DataConfigError(f"{candidate}: invalid data configuration\n{exc}") from exc
