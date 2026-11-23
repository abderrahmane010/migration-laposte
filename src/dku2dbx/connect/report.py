"""Report « à brancher » (a_brancher.md, connect.json), with a Mermaid flow of the sources."""

from __future__ import annotations

import json
import re
from pathlib import Path

from dku2dbx import __version__
from dku2dbx.connect.resolve import Branching
from dku2dbx.model.ir import Project


def _node(name: str) -> str:
    return "n_" + re.sub(r"[^A-Za-z0-9_]", "_", name)


def mermaid(project: Project, branching: Branching) -> list[str]:
    """Sources and the recipes reading them; unbranched sources are dashed."""
    lines = ["```mermaid", "flowchart LR", "  classDef todo stroke-dasharray: 5 5"]
    sources = sorted({*branching.resolved, *branching.unresolved})
    for name in sources:
        label = branching.resolved.get(name, "à brancher")
        lines.append(f'  {_node(name)}["{name}<br/>{label}"]')
        if name in branching.unresolved:
            lines.append(f"  class {_node(name)} todo")
    for recipe in project.recipes:
        for inp in recipe.inputs:
            if inp.ref in sources:
                arrow = "-.->" if inp.ref in branching.unresolved else "-->"
                lines.append(f'  {_node(inp.ref)} {arrow} r_{_node(recipe.name)}("{recipe.name}")')
    return [*lines, "```"]


def markdown(project: Project, branching: Branching) -> str:
    lines = [
        f"# Sources à brancher — {project.key}",
        "",
        f"Généré par dku2dbx {__version__}. Mapping lu : `{branching.source_file}`.",
        "",
        f"- Sources branchées : **{len(branching.resolved)}**",
        f"- Sources **à brancher** : **{len(branching.unresolved)}**",
        "",
        "## À brancher",
        "",
        "| Source | Raison |",
        "|---|---|",
    ]
    lines += [f"| {n} | {why} |" for n, why in sorted(branching.unresolved.items())] or [
        "| (aucune) | |"
    ]
    lines += ["", "## Branchées", "", "| Source | Table Unity Catalog |", "|---|---|"]
    lines += [f"| {n} | `{fqn}` |" for n, fqn in sorted(branching.resolved.items())] or [
        "| (aucune) | |"
    ]
    if branching.partitioned:
        lines += [
            "",
            "Sources partitionnées dans Dataiku : leur table doit avoir la colonne de date de la "
            f"partition (H-53) : {', '.join(branching.partitioned)}.",
        ]
    lines += ["", "## Graphe (pointillés : à brancher)", "", *mermaid(project, branching)]
    return "\n".join(lines) + "\n"


def write_report(project: Project, branching: Branching, out: Path) -> list[Path]:
    base = out / project.key
    base.mkdir(parents=True, exist_ok=True)
    data = json.loads(branching.model_dump_json())
    files = {
        base / "connect.json": json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
        + "\n",
        base / "a_brancher.md": markdown(project, branching),
    }
    for path, text in files.items():
        path.write_bytes(text.encode("utf-8"))
    return sorted(files)
