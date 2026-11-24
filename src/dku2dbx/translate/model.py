"""Result of the translation of recipes into Spark SQL."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TranslatedOutput(_Model):
    """One SELECT producing one output dataset of a recipe."""

    dataset: str
    sql: str
    append: bool = False


class Translation(_Model):
    recipe: str
    type: str
    outputs: list[TranslatedOutput]
    warnings: list[str] = Field(default_factory=list)
    """Semantic differences the reviewer must know (never silent)."""
    review: bool = False
    """Deterministic rewrite of code (phase 8): to be reviewed before production."""


class CodeRewrite(_Model):
    """A code recipe whose dataiku idioms are rewritten (phase 8): always to review."""

    recipe: str
    type: str
    code: str
    reads: list[str] = Field(default_factory=list)
    writes: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class Unsupported(_Model):
    recipe: str
    type: str
    element: str
    """The Dataiku element not handled (payload key, option, mode...)."""
    detail: str


class ProjectTranslation(_Model):
    project: str
    translated: list[Translation] = Field(default_factory=list)
    rewritten: list[CodeRewrite] = Field(default_factory=list)
    """Python recipes rewritten into notebooks (phase 8)."""
    unsupported: list[Unsupported] = Field(default_factory=list)
    """Recipes with a translator that hit a construct it does not handle."""
    without_translator: dict[str, str] = Field(default_factory=dict)
    """Recipe name -> type, for recipes no translator covers yet."""
