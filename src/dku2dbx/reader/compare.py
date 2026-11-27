"""Compare the project read from the API with the project read from its export (phase 12).

Every field of the internal representation is compared (datasets, recipes, payloads,
Prepare steps, scenarios, variables, zones, partitions, schemas, exposed and foreign
objects, plugins, flow, issues). Each difference is listed field by field, either as
explained (provenance only, listed below), as unverifiable (a field the API key could
not read, reported as such) or as unexplained: the reader is reliable on a project when
there is no unexplained difference and the API read is complete.
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
UNVERIFIABLE_PLUGINS = re.compile(r"^plugins($|[.\[])|^recipes\[.*\]\.plugin_id$")
"""Fields that depend on the plugin list of the instance (H-74)."""
PLUGINS_UNKNOWN = (
    "liste des plugins de l'instance illisible avec la clé API : plugins, versions et "
    "plugin des recettes non vérifiables"
)
KEYS = ("name", "id", "plugin_id")
COMPARISON = "comparaison_api_zip.md"


@dataclass(frozen=True)
class Difference:
    path: str
    zip_value: Any
    api_value: Any
    explanation: str | None = None
    unverifiable: str | None = None
    """Why the API side of this field is unknown (the key could not read it)."""

    @property
    def unexplained(self) -> bool:
        return self.explanation is None and self.unverifiable is None


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


GAPS = (IssueKind.FOREIGN_OBJECTS_INCOMPLETE, IssueKind.PLUGINS_UNREADABLE)
"""Limits of the API read, reported by ``incompleteness``."""
APART = (IssueKind.UNPARSED_ENTRY.value, *(k.value for k in GAPS))


def _plugin_issue(issue: dict[str, Any]) -> bool:
    """A plugin recipe whose plugin is not in the (unknown) plugin list."""
    return issue["kind"] == IssueKind.MISSING_REFERENCE.value and "exportedWithPlugins" in str(
        issue.get("message")
    )


def _without_apart_issues(data: dict[str, Any], plugins_unknown: bool) -> dict[str, Any]:
    """Issues compared apart: the unparsed entries (provenance), the limits of the API
    read (reported by ``incompleteness``) and, when the plugin list is unknown, the
    undeclared plugins (unverifiable)."""
    out = dict(data)
    out["issues"] = [
        i
        for i in data["issues"]
        if i["kind"] not in APART and not (plugins_unknown and _plugin_issue(i))
    ]
    return out


def incompleteness(from_api: Project) -> str | None:
    """Why the comparison cannot be fully conclusive, if the API read was partial."""
    messages = [i.message for i in from_api.issues if i.kind in GAPS]
    return "; ".join(messages) or None


def compare_projects(from_zip: Project, from_api: Project) -> list[Difference]:
    zip_data = json.loads(from_zip.model_dump_json())
    api_data = json.loads(from_api.model_dump_json())
    plugins_unknown = any(i.kind is IssueKind.PLUGINS_UNREADABLE for i in from_api.issues)

    def unverifiable(path: str) -> str | None:
        return PLUGINS_UNKNOWN if plugins_unknown and UNVERIFIABLE_PLUGINS.search(path) else None

    differences = [
        Difference(path, z, a, _explain(path), unverifiable(path))
        for path, z, a in _walk(
            _without_apart_issues(zip_data, plugins_unknown),
            _without_apart_issues(api_data, plugins_unknown),
            "",
        )
    ]
    if plugins_unknown:
        zip_plugin = [i for i in zip_data["issues"] if _plugin_issue(i)]
        api_plugin = [i for i in api_data["issues"] if _plugin_issue(i)]
        if zip_plugin != api_plugin:
            differences.append(
                Difference(
                    "issues[plugin non déclaré]",
                    zip_plugin,
                    api_plugin,
                    unverifiable=PLUGINS_UNKNOWN,
                )
            )
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


def report_markdown(
    key: str, zip_name: str, differences: list[Difference], incomplete: str | None = None
) -> str:
    unexplained = [d for d in differences if d.unexplained]
    explained = Counter(d.explanation for d in differences if d.explanation is not None)
    unverifiable = Counter(d.unverifiable for d in differences if d.unverifiable is not None)
    if unexplained:
        verdict = (
            f"**{len(unexplained)} différence(s) non expliquée(s)** : à analyser avant "
            "d'utiliser la lecture par l'API sur ce projet."
        )
    elif incomplete:
        verdict = (
            "**Comparaison non concluante** : aucune différence non expliquée, mais la "
            "lecture par l'API est incomplète (voir l'avertissement ci-dessous)."
        )
    else:
        verdict = (
            "**Aucune différence non expliquée** : l'API et l'export donnent la même "
            "représentation interne."
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
    ]
    if incomplete:
        lines += [f"> **Attention — lecture par l'API incomplète** : {incomplete}.", ""]
    lines += [
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
    if unverifiable:
        lines += ["", "## Différences non vérifiables", ""]
        lines += [f"- {count} champ(s) — {why}" for why, count in sorted(unverifiable.items())]
        lines += ["", "<details><summary>Détail</summary>", ""]
        lines += [f"- `{d.path}`" for d in differences if d.unverifiable is not None]
        lines += ["", "</details>"]
    lines += ["", "## Différences expliquées", ""]
    if explained:
        lines += [f"- {count} champ(s) — {why}" for why, count in sorted(explained.items())]
        lines += ["", "<details><summary>Détail</summary>", ""]
        lines += [f"- `{d.path}`" for d in differences if d.explanation is not None]
        lines += ["", "</details>"]
    else:
        lines.append("(aucune)")
    return "\n".join(lines) + "\n"
