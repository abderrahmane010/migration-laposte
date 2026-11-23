"""Simplification report: simplify.md and simplify.json (reproducible)."""

from __future__ import annotations

import json
from pathlib import Path

from dku2dbx import __version__
from dku2dbx.simplify.flow import SimplifiedFlow


def markdown(flow: SimplifiedFlow) -> str:
    lines = [
        f"# Simplification du Flow — {flow.project}",
        "",
        f"Généré par dku2dbx {__version__}."
        + ("" if flow.fusion else " Option `--no-fusion` : une étape par recette."),
        "",
        "## Synthèse",
        "",
        f"- Recettes avant : **{flow.recipes_before}**",
        f"- Étapes après : **{len(flow.steps)}**",
        f"- Tables matérialisées : {len(flow.kept)}",
        f"- Tables supprimées (devenues des CTE) : {len(flow.removed)}",
        "",
        "## Étapes",
        "",
        "| Étape | Recettes (ordre d'exécution) | Tables écrites | Tables en CTE |",
        "|---|---|---|---|",
    ]
    for step in flow.steps:
        lines.append(
            f"| {step.id} | {', '.join(step.recipes)} | {', '.join(step.outputs) or '—'} "
            f"| {', '.join(step.inlined) or '—'} |"
        )
    lines += ["", "## Tables conservées et justification", "", "| Table | Raisons |", "|---|---|"]
    lines += [f"| {name} | {'; '.join(why)} |" for name, why in sorted(flow.kept.items())]
    lines += ["", "## Tables supprimées", ""]
    lines += [f"- {name}" for name in flow.removed] or ["(aucune)"]
    return "\n".join(lines) + "\n"


def write_report(flow: SimplifiedFlow, out: Path) -> list[Path]:
    base = out / flow.project
    base.mkdir(parents=True, exist_ok=True)
    data = json.loads(flow.model_dump_json())
    paths = {
        base / "simplify.json": json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
        + "\n",
        base / "simplify.md": markdown(flow),
    }
    for path, text in paths.items():
        path.write_bytes(text.encode("utf-8"))
    return sorted(paths)
