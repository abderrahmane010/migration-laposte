"""RESIDU.md and residue.json: every file a human must review (ROADMAP phase 8)."""

from __future__ import annotations

import json
from pathlib import Path

from dku2dbx import __version__
from dku2dbx.translate.llm.residue import Residue, Status


def residue_markdown(residue: Residue) -> str:
    lines = [
        f"# Résidu à relire — {residue.project}",
        "",
        f"Généré par dku2dbx {__version__}. LLM : {residue.llm}.",
        "",
        "Rien de ce qui est listé ici n'est AUTO ni ajouté au bundle tout seul : un relecteur "
        "vérifie chaque fichier, puis le recopie dans le bundle à la place du notebook qui "
        "échoue.",
        "",
        "## Recettes confiées au LLM",
        "",
    ]
    if residue.items:
        lines += ["| Recette | Type | Statut | Fichier à relire | Raison du refus déterministe |"]
        lines += ["|---|---|---|---|---|"]
        for item in residue.items:
            target = f"`{item.output_file}`" if item.output_file else f"`{item.input_file}`"
            reason = item.reason.replace("|", "\\|")
            lines.append(f"| {item.recipe} | {item.type} | {item.status} | {target} | {reason} |")
        problems = [i for i in residue.items if i.status not in (Status.READY, Status.REVIEW)]
        for item in problems:
            lines += [
                "",
                f"- **{item.recipe}** ({item.status}) :",
                *(f"  - {c}" for c in item.checks),
            ]
    else:
        lines.append("(aucune)")
    lines += [
        "",
        "## Code réécrit par les règles déterministes, à relire",
        "",
        "Fichiers de `dku2dbx translate` (dossier `sql/` ou `python/`).",
        "",
    ]
    lines += [
        f"- {recipe} : `{path}`" for recipe, path in sorted(residue.deterministic_to_review.items())
    ] or ["(aucun)"]
    return "\n".join(lines) + "\n"


def write_residue_report(residue: Residue, out: Path) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    md = out / "RESIDU.md"
    md.write_bytes(residue_markdown(residue).encode("utf-8"))
    data = json.loads(residue.model_dump_json())
    js = out / "residue.json"
    js.write_bytes(
        (json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    )
    return [md, js]
