"""Unity Catalog names of the Dataiku datasets, and the metadata keeping the originals.

Decision of 2026-11-25: a table keeps the name of its Dataiku dataset. The original
identifiers are always stored in the table properties (and in donnees.json), even when
a Unity Catalog rule prevents using the name as is: then the dataset is refused with a
proposed name, never renamed silently.
"""

from __future__ import annotations

import re
from collections import defaultdict

from dku2dbx import __version__
from dku2dbx.model.ir import Dataset, Project

# ASSUMPTION (H-64): a Unity Catalog object name has at most 255 characters and no
# period, space, forward slash, ASCII control character or DEL; names are
# case-insensitive (stored in lower case).
MAX_LENGTH = 255
FORBIDDEN = re.compile(r"[. /\x00-\x1f\x7f]")
PROPERTY_PREFIX = "dku2dbx.dataiku."


def name_problem(name: str) -> str | None:
    """Why ``name`` cannot be a Unity Catalog table name, None when it can."""
    if not name:
        return "nom vide"
    if len(name) > MAX_LENGTH:
        return f"plus de {MAX_LENGTH} caractères"
    bad = sorted(set(FORBIDDEN.findall(name)))
    if bad:
        shown = ", ".join(repr(c) for c in bad)
        return f"caractères interdits par Unity Catalog ({shown})"
    return None


def proposed_name(name: str) -> str:
    """A valid name close to ``name``: forbidden characters become ``_``, lower case."""
    return (FORBIDDEN.sub("_", name).lower() or "_")[:MAX_LENGTH]


def case_collisions(names: list[str]) -> dict[str, list[str]]:
    """Dataset -> the other datasets with the same name in lower case."""
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for name in names:
        groups[name.lower()].append(name)
    return {
        name: sorted(n for n in group if n != name)
        for group in groups.values()
        if len(group) > 1
        for name in group
    }


def producer(project: Project, dataset: str) -> str | None:
    return next((r.name for r in project.recipes for o in r.outputs if o.ref == dataset), None)


def dataiku_properties(project: Project, dataset: Dataset) -> dict[str, str]:
    """Table properties tracing the table back to its Dataiku dataset (never empty values)."""
    values = {
        "project": project.key,
        "dataset": dataset.name,
        "type": dataset.type,
        "connection": dataset.connection,
        "schema": dataset.db_schema_resolved,
        "table": dataset.table_resolved,
        "path": dataset.params.get("path") if isinstance(dataset.params.get("path"), str) else None,
        "recipe": producer(project, dataset.name),
        "json_sha256": dataset.source_sha256,
    }
    props = {PROPERTY_PREFIX + k: str(v) for k, v in values.items() if v}
    props["dku2dbx.generator_version"] = __version__
    return dict(sorted(props.items()))
