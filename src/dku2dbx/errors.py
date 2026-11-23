"""Exceptions of dku2dbx."""

from __future__ import annotations


class Dku2dbxError(Exception):
    """Base class of all dku2dbx errors."""


class ExportReadError(Dku2dbxError):
    """The export cannot be read at all (not a ZIP, no manifest, invalid JSON...).

    Unknown object *types* are not errors: they are kept in the IR with
    ``status = UNKNOWN`` and reported as issues.
    """


class ScoringConfigError(Dku2dbxError):
    """``scoring.yaml`` is missing or invalid (unknown level, measure or class)."""


class UnsupportedConstruct(Dku2dbxError):  # noqa: N818 (name imposed by CLAUDE.md, rule 2)
    """A Dataiku construct has no deterministic translation (yet).

    Never guessed nor dropped: the recipe is reported with ``construct`` and
    ``detail``, and stays to be translated by a later phase or by hand.
    """

    def __init__(self, recipe: str, construct: str, detail: str) -> None:
        super().__init__(f"{recipe}: {construct} — {detail}")
        self.recipe = recipe
        self.construct = construct
        self.detail = detail
