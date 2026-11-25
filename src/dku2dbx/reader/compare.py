"""Compare the project read from the API with the project read from its export (phase 12).

Every field of the internal representation is compared (datasets, recipes, payloads,
Prepare steps, scenarios, variables, zones, partitions, schemas, exposed and foreign
objects, plugins, flow, issues). Each difference is listed field by field, either as
explained (provenance only, listed below) or as unexplained: the reader is reliable on
a project when there is no unexplained difference.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

from dku2dbx import __version__
from dku2dbx.model.ir import IssueKind, Project

EXPLANATIONS = (
    (
        re.compile(r"(^|\.)source_sha256$"),
        "provenance : hash des octets lus, et un export écrit son JSON dans sa propre mise "
        "en page (objets imbriqués compactés)",
    ),
    (
        re.compile(r"^unparsed_entries"),
        "provenance : entrées de l'export que le moteur n'interprète pas (discussions, "
        "tableaux de bord, analyses…), non lues par l'API",
    ),
)
KEYS = ("name", "id", "plugin_id")
COMPARISON = "comparaison_api_zip.md"


@dataclass(frozen=True)
class Difference:
    path: str
    zip_value: Any
    api_value: Any
    explanation: str | None = None


def _unique_by(items: list[Any], key: str) -> bool:
    if not all(isinstance(i, dict) and isinstance(i.get(key), str) for i in items):
        return False
    return len({i[key] for i in items}) == len(items)


def _key_of(zip_items: list[Any], api_items: list[Any]) -> str | None:
    """The field identifying the objects of both lists, if any (to align them)."""
    if not (zip_items or api_items):
        return None
    return next((k for k in KEYS if _unique_by(zip_items, k) and _unique_by(api_items, k)), None)


_MISSING = object()


def _walk(zip_value: Any, api_value: Any, path: str) -> list[tuple[str, Any, Any]]:
    if isinstance(zip_value, dict) and isinstance(api_value, dict):
        out = []
        for k in sorted(set(zip_value) | set(api_value)):
            sub = f"{path}.{k}" if path else k
            out += _walk(zip_value.get(k, _MISSING), api_value.get(k, _MISSING), sub)
        return out
    if isinstance(zip_value, list) and isinstance(api_value, list):
        key = _key_of(zip_value, api_value)
        if key is not None:
            z = {i[key]: i for i in zip_value}
            a = {i[key]: i for i in api_value}
            out = []
            for k in sorted(set(z) | set(a)):
                out += _walk(z.get(k, _MISSING), a.get(k, _MISSING), f"{path}[{key}={k}]")
            return out
        if len(zip_value) == len(api_value):
            out = []
            for i, (zv, av) in enumerate(zip(zip_value, api_value, strict=True)):
                out += _walk(zv, av, f"{path}[{i}]")
            return out
    if zip_value != api_value:
        return [(path, zip_value, api_value)]
    return []


def _explain(path: str) -> str | None:
    return next((why for pattern, why in EXPLANATIONS if pattern.search(path)), None)


def _without_unparsed_issue(data: dict[str, Any]) -> dict[str, Any]:
    """The issue counting the unparsed entries is provenance too (it is compared apart)."""
    out = dict(data)
    out["issues"] = [i for i in data["issues"] if i["kind"] != IssueKind.UNPARSED_ENTRY.value]
    return out


def compare_projects(from_zip: Project, from_api: Project) -> list[Difference]:
    zip_data = json.loads(from_zip.model_dump_json())
    api_data = json.loads(from_api.model_dump_json())
    differences = [
        Difference(path, z, a, _explain(path))
        for path, z, a in _walk(
            _without_unparsed_issue(zip_data), _without_unparsed_issue(api_data), ""
        )
    ]
    zip_issue = [i for i in zip_data["issues"] if i["kind"] == IssueKind.UNPARSED_ENTRY.value]
    api_issue = [i for i in api_data["issues"] if i["kind"] == IssueKind.UNPARSED_ENTRY.value]
    if zip_issue != api_issue:
        differences.append(
            Difference(
                "issues[kind=UNPARSED_ENTRY]", zip_issue, api_issue, _explain("unparsed_entries")
            )
        )
    return differences


def _shown(value: Any) -> str:
    if value is _MISSING:
        return "(absent)"
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = text if len(text) <= 120 else text[:117] + "…"
    return "`" + text.replace("|", "\\|").replace("`", "'") + "`"


def report_markdown(key: str, zip_name: str, differences: list[Difference]) -> str:
    unexplained = [d for d in differences if d.explanation is None]
    explained = Counter(d.explanation for d in differences if d.explanation is not None)
    verdict = (
        "**Aucune différence non expliquée** : l'API et l'export donnent la même "
        "représentation interne."
        if not unexplained
        else f"**{len(unexplained)} différence(s) non expliquée(s)** : à analyser avant "
        "d'utiliser la lecture par l'API sur ce projet."
    )
    lines = [
        f"# Comparaison API / export ZIP — {key}",
        "",
        f"Généré par dku2dbx {__version__}. Export : `{zip_name}`.",
        "",
        "Chaque champ de la représentation interne est comparé : datasets, recettes, "
        "payloads, steps Prepare, scénarios, variables, zones, partitions, schémas, "
        "objets exposés et étrangers, plugins, Flow et points à examiner.",
        "",
        verdict,
        "",
        "## Différences non expliquées",
        "",
    ]
    if unexplained:
        lines += ["| Champ | Export ZIP | API |", "|---|---|---|"]
        lines += [
            f"| `{d.path}` | {_shown(d.zip_value)} | {_shown(d.api_value)} |" for d in unexplained
        ]
    else:
        lines.append("(aucune)")
    lines += ["", "## Différences expliquées", ""]
    if explained:
        lines += [f"- {count} champ(s) — {why}" for why, count in sorted(explained.items())]
        lines += ["", "<details><summary>Détail</summary>", ""]
        lines += [f"- `{d.path}`" for d in differences if d.explanation is not None]
        lines += ["", "</details>"]
    else:
        lines.append("(aucune)")
    return "\n".join(lines) + "\n"
