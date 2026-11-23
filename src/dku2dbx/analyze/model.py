"""Result of the analysis of a project and of a portfolio (serialised to JSON)."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TargetClass(StrEnum):
    """How a recipe is planned to be migrated."""

    AUTO = "AUTO"
    """Deterministic translator (available now, or planned by the roadmap)."""
    LLM_REVIEW = "LLM_REVIEW"
    """Residue handled by the LLM, always reviewed by a human."""
    MANUAL = "MANUAL"
    """Out of scope of the automatic migration: done by hand."""


class Level(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"


class RecipeClassification(_Model):
    name: str
    type: str
    category: str
    target_class: TargetClass
    planned_phase: int | None
    """Roadmap phase that brings the translator (None: no translator planned)."""
    translator_available: bool
    """True once the translator of this recipe type exists in dku2dbx."""
    reasons: list[str] = Field(default_factory=list)


class Trigger(_Model):
    """A score condition that is met: ``measure >= threshold`` for ``level``."""

    level: Level
    measure: str
    value: float
    threshold: float


class CrossProjectLink(_Model):
    """``producer`` shares ``dataset`` with ``consumer``."""

    producer: str
    consumer: str
    dataset: str
    source: str
    """``input`` (consumer recipe input), ``foreign`` (manifest) or ``exposed`` (producer)."""


class ProjectAnalysis(_Model):
    key: str
    name: str | None = None
    source_file: str
    measures: dict[str, float] = Field(default_factory=dict)
    recipes_by_type: dict[str, int] = Field(default_factory=dict)
    recipes_by_category: dict[str, int] = Field(default_factory=dict)
    recipes_by_class: dict[str, int] = Field(default_factory=dict)
    datasets_by_type: dict[str, int] = Field(default_factory=dict)
    counts: dict[str, int] = Field(default_factory=dict)
    """datasets, zones, scenarios, saved_models, code_recipes, issues..."""
    plugins: list[str] = Field(default_factory=list)
    processors: dict[str, int] = Field(default_factory=dict)
    """Active Prepare processor occurrences by type."""
    disabled_steps: list[str] = Field(default_factory=list)
    """``<recipe>: <processor>`` of every disabled Prepare step (listed, never translated)."""
    issues_by_kind: dict[str, int] = Field(default_factory=dict)
    level: Level
    triggers: list[Trigger] = Field(default_factory=list)
    effort_days: float
    recipes: list[RecipeClassification] = Field(default_factory=list)
    links: list[CrossProjectLink] = Field(default_factory=list)


class ProcessorStat(_Model):
    type: str
    occurrences: int
    """Active steps of this type, all projects included."""
    disabled: int
    recipes: int
    projects: int
    target_class: TargetClass


class Wave(_Model):
    number: int
    projects: list[str]
    cycles: list[list[str]] = Field(default_factory=list)
    """Projects that depend on each other: to be migrated together."""


class PortfolioAnalysis(_Model):
    projects: list[ProjectAnalysis]
    processors: list[ProcessorStat]
    waves: list[Wave]
    external_links: list[CrossProjectLink] = Field(default_factory=list)
    """Links to projects outside the analysed portfolio."""
