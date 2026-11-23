"""Complexity score and effort estimate, driven by ``config/scoring.yaml``.

A level is reached as soon as ONE of its conditions is met (measure >= threshold);
the highest level reached wins, otherwise LOW. Every value of the file is a
starting point "to calibrate on the pilot". An unknown level, measure or class in
the file is an error: a typo must never silently disable a condition.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from dku2dbx.analyze.model import Level, TargetClass, Trigger
from dku2dbx.errors import ScoringConfigError

MEASURES = frozenset(
    {
        "recipes",
        "code_share",
        "formula_prepares",
        "partitioned",
        "python_scenarios",
        "plugins",
        "saved_models",
    }
)
DEFAULT_CONFIG = Path("config/scoring.yaml")
# Copy of config/scoring.yaml shipped in the wheel (pyproject, force-include).
PACKAGED_CONFIG = "default_scoring.yaml"


class EffortDays(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    auto: float = Field(alias="AUTO", ge=0)
    llm_review: float = Field(alias="LLM_REVIEW", ge=0)
    manual: float = Field(alias="MANUAL", ge=0)
    per_scenario: float = Field(ge=0)

    def per_class(self, target: TargetClass) -> float:
        return {
            TargetClass.AUTO: self.auto,
            TargetClass.LLM_REVIEW: self.llm_review,
            TargetClass.MANUAL: self.manual,
        }[target]


class ScoringConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    levels: dict[Level, dict[str, float]]
    formula_rich_min_steps: int = Field(ge=1)
    effort_days: EffortDays

    @field_validator("levels")
    @classmethod
    def _check_levels(cls, levels: dict[Level, dict[str, float]]) -> dict[Level, dict[str, float]]:
        if Level.LOW in levels:
            raise ValueError("LOW is the default level: it has no condition")
        for level, conditions in levels.items():
            unknown = sorted(set(conditions) - MEASURES)
            if unknown:
                known = sorted(MEASURES)
                raise ValueError(f"{level}: unknown measure(s) {unknown}; known: {known}")
        return levels


def load_scoring(path: Path | None = None) -> ScoringConfig:
    """Load ``path``, else ``config/scoring.yaml``, else the copy shipped in the package."""
    if path is not None:
        text, origin = _read(path), str(path)
    elif DEFAULT_CONFIG.is_file():
        text, origin = _read(DEFAULT_CONFIG), str(DEFAULT_CONFIG)
    else:
        resource = resources.files("dku2dbx").joinpath(PACKAGED_CONFIG)
        if not resource.is_file():
            raise ScoringConfigError(
                f"no scoring configuration: {DEFAULT_CONFIG} not found (use --scoring)"
            )
        text, origin = resource.read_text(encoding="utf-8"), f"dku2dbx/{PACKAGED_CONFIG}"
    try:
        data: Any = yaml.safe_load(text)
        return ScoringConfig.model_validate(data)
    except (yaml.YAMLError, ValidationError) as exc:
        raise ScoringConfigError(f"{origin}: invalid scoring configuration\n{exc}") from exc


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ScoringConfigError(f"{path}: cannot read the scoring configuration ({exc})") from exc


_ORDER = [Level.VERY_HIGH, Level.HIGH, Level.MEDIUM]


def score(measures: dict[str, float], config: ScoringConfig) -> tuple[Level, list[Trigger]]:
    """Level reached and every condition met (all levels, for the report)."""
    triggers = [
        Trigger(level=level, measure=measure, value=measures[measure], threshold=threshold)
        for level in _ORDER
        for measure, threshold in sorted(config.levels.get(level, {}).items())
        if measures[measure] >= threshold
    ]
    level = triggers[0].level if triggers else Level.LOW
    return level, triggers


def effort(classes: list[TargetClass], scenarios: int, config: ScoringConfig) -> float:
    days = sum(config.effort_days.per_class(c) for c in classes)
    days += scenarios * config.effort_days.per_scenario
    return round(days, 2)
