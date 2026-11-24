"""``config/llm.yaml``: the security-validated Model Serving endpoint (CLAUDE.md, rule 5).

Without an endpoint the LLM step is disabled: only its input files are prepared.
No token here: authentication goes through a Databricks CLI profile or the
environment variables of the Databricks SDK.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from dku2dbx.errors import Dku2dbxError

DEFAULT_PATH = Path("config/llm.yaml")


class LLMConfigError(Dku2dbxError):
    """The LLM configuration file is invalid."""


class LLMConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint: str | None = None
    """Name of the Model Serving endpoint validated by security; None: LLM disabled."""
    profile: str | None = None
    max_tokens: int = Field(default=4096, gt=0)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    _source: str = PrivateAttr(default="(configuration en mémoire)")

    @property
    def source(self) -> str:
        """The file it was read from (for the reports)."""
        return self._source

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)


def load_llm_config(path: Path | None = None) -> LLMConfig:
    """The configuration (absent default file: LLM disabled); ``source`` says where from."""
    candidate = path or DEFAULT_PATH
    config = LLMConfig()
    if not candidate.is_file():
        if path is not None:
            raise LLMConfigError(f"{path}: not found")
        config._source = "(aucun fichier config/llm.yaml)"
        return config
    try:
        data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        config = LLMConfig.model_validate(data)
        config._source = str(candidate)
        return config
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise LLMConfigError(f"{candidate}: invalid LLM configuration\n{exc}") from exc
