"""``config/reconcile.yaml``: business keys and tolerances per table (phase 9).

Decision of 2026-11-24: keys, tolerances and ignored columns are declared per Dataiku
dataset in ``config/reconcile.local.yaml`` (ignored by Git, read first) or
``config/reconcile.yaml``. A table without a key is compared row by row with
``EXCEPT ALL`` (no tolerance is possible then).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dku2dbx.errors import Dku2dbxError

DEFAULT_PATH = Path("config/reconcile.yaml")
DEFAULT_LOCAL = Path("config/reconcile.local.yaml")


class ReconcileConfigError(Dku2dbxError):
    """The reconciliation configuration file is invalid."""


class TableRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: list[str] | None = None
    abs_tolerance: float | None = Field(default=None, ge=0.0)
    rel_tolerance: float | None = Field(default=None, ge=0.0)
    ignore_columns: list[str] | None = None
    partition_columns: list[str] | None = None
    """None: the partition dimensions of the dataset (a column per dimension, H-53)."""
    reference: str | None = None
    """Dataiku output in Unity Catalog; None: <ref_catalog>.<ref_schema>.<dataset>."""
    skip: bool = False


class ReconcileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    defaults: TableRule = Field(default_factory=TableRule)
    tables: dict[str, TableRule] = Field(default_factory=dict)

    def rule(self, dataset: str) -> dict[str, Any]:
        """The merged rule of a dataset (table values override the defaults)."""
        merged = self.defaults.model_dump(exclude_none=True)
        merged.update(self.tables.get(dataset, TableRule()).model_dump(exclude_none=True))
        return merged


def load_reconcile_config(path: Path | None = None) -> tuple[ReconcileConfig, str]:
    """(configuration, source); no file at all: no key, no tolerance."""
    if path is not None and not path.is_file():
        raise ReconcileConfigError(f"{path}: not found")
    candidate = path or next((p for p in (DEFAULT_LOCAL, DEFAULT_PATH) if p.is_file()), None)
    if candidate is None:
        return ReconcileConfig(), "(aucun fichier : ni clé ni tolérance)"
    try:
        data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        return ReconcileConfig.model_validate(data), str(candidate)
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ReconcileConfigError(
            f"{candidate}: invalid reconciliation configuration\n{exc}"
        ) from exc
