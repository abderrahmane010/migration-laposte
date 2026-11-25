"""The entries a project is built from: files of an export ZIP, or their API equivalent.

Both readers hand the same entries (``export-manifest.json``, ``project_config/…``) to
the same builder, so a project read from the API and from its export goes through the
same code (decision of 2026-11-25: one migration engine, two sources).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class EntrySource(Protocol):
    path: Path
    """Origin of the entries; its stem is the project key of last resort."""

    def exists(self, rel: str) -> bool: ...

    def list_dir(self, rel_dir: str) -> list[str]:
        """Relative paths of the entries directly under ``rel_dir``."""
        ...

    def read_text(self, rel: str) -> str: ...

    def read_json(self, rel: str) -> tuple[Any, str]:
        """Parsed JSON and SHA-256 of the raw bytes."""
        ...

    def unconsumed(self) -> list[str]:
        """Entries the builder did not read."""
        ...
