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
