"""``config/llm.yaml``: the security-validated LLM endpoint (CLAUDE.md, rule 5).

Two providers (decision of 2026-11-24):
- ``databricks``: a Databricks Model Serving endpoint (``endpoint``, CLI ``profile``);
- ``openai``: an internal La Poste endpoint with an OpenAI-compatible API, such as
  vLLM (``base_url``, ``model``, optional ``ca_bundle``).

Without an endpoint the LLM step is disabled: only its input files are prepared.
No token here: Databricks authenticates through a CLI profile or the SDK environment
variables; the OpenAI-compatible key is read from the environment variable named by
``api_key_env``. The real values go in ``config/llm.local.yaml`` (ignored by Git),
read first, or in the environment (``.env``): ``VLLM_BASE_URL`` and ``VLLM_MODEL``,
with ``SSL_CA_BUNDLE`` for the certificate.

Order: ``--llm-config``, ``config/llm.local.yaml``, ``VLLM_*`` variables,
``config/llm.yaml``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, model_validator

from dku2dbx.errors import Dku2dbxError

DEFAULT_PATH = Path("config/llm.yaml")
DEFAULT_LOCAL = Path("config/llm.local.yaml")
ENV_BASE_URL, ENV_MODEL, ENV_CA_BUNDLE = "VLLM_BASE_URL", "VLLM_MODEL", "SSL_CA_BUNDLE"


class LLMConfigError(Dku2dbxError):
    """The LLM configuration file is invalid."""


class LLMConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Literal["databricks", "openai"] = "databricks"
    endpoint: str | None = None
    """databricks: name of the Model Serving endpoint validated by security."""
    profile: str | None = None
    base_url: str | None = None
    """openai: base URL of the internal API, ending with /v1 (HTTPS only)."""
    model: str | None = None
    """openai: name of the served model."""
    ca_bundle: str | None = None
    """openai: CA certificate of the internal authority; TLS is always verified."""
    api_key_env: str = "OPENAI_API_KEY"
    """openai: name of the environment variable holding the key (never the key)."""
    use_env_proxy: bool = False
    """openai: go through HTTP(S)_PROXY; off by default for an internal endpoint."""
    timeout_s: int = Field(default=300, gt=0)
    max_tokens: int = Field(default=4096, gt=0)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    _source: str = PrivateAttr(default="(configuration en mémoire)")

    @model_validator(mode="after")
    def _check_provider(self) -> LLMConfig:
        if self.provider == "openai":
            if self.endpoint or self.profile:
                raise ValueError("provider openai: use base_url and model, not endpoint/profile")
            if bool(self.base_url) != bool(self.model):
                raise ValueError("provider openai: base_url and model go together")
            if self.base_url and not self.base_url.startswith("https://"):
                raise ValueError("provider openai: base_url must use https://")
        elif self.base_url or self.model or self.ca_bundle:
            raise ValueError("provider databricks: base_url/model/ca_bundle need provider openai")
        return self

    @property
    def source(self) -> str:
        """The file it was read from (for the reports)."""
        return self._source

    @property
    def enabled(self) -> bool:
        return bool(self.base_url if self.provider == "openai" else self.endpoint)

    @property
    def target(self) -> str:
        """What answers, for the headers and the reports."""
        if self.provider == "openai":
            return f"{self.model} (API compatible OpenAI, {self.base_url})"
        return f"{self.endpoint} (Databricks Model Serving)"


def _from_environment() -> LLMConfig | None:
    """The internal vLLM declared in the environment (``.env``), if both variables are set."""
    base_url, model = os.environ.get(ENV_BASE_URL), os.environ.get(ENV_MODEL)
    if not (base_url and model):
        return None
    source = f"variables d'environnement {ENV_BASE_URL} et {ENV_MODEL}"
    try:
        config = LLMConfig(
            provider="openai",
            base_url=base_url,
            model=model,
            ca_bundle=os.environ.get(ENV_CA_BUNDLE) or None,
        )
    except ValidationError as exc:
        raise LLMConfigError(f"{source}: invalid LLM configuration\n{exc}") from exc
    config._source = source
    return config


def load_llm_config(path: Path | None = None) -> LLMConfig:
    """The configuration (nothing declared: LLM disabled); ``source`` says where from."""
    if path is not None and not path.is_file():
        raise LLMConfigError(f"{path}: not found")
    if path is None and not DEFAULT_LOCAL.is_file():
        environment = _from_environment()
        if environment is not None:
            return environment
    candidate = path or next((p for p in (DEFAULT_LOCAL, DEFAULT_PATH) if p.is_file()), None)
    config = LLMConfig()
    if candidate is None:
        config._source = "(aucun fichier config/llm.local.yaml ni config/llm.yaml)"
        return config
    try:
        data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        config = LLMConfig.model_validate(data)
        config._source = str(candidate)
        return config
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise LLMConfigError(f"{candidate}: invalid LLM configuration\n{exc}") from exc
