"""Analysis outputs: analyse.md/.json per project, portfolio CSV, waves, catalogue.

Every output is reproducible: same IR and same configuration give the same bytes
(sorted content, no timestamps, ``\\n`` line endings).
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from dku2dbx import __version__
from dku2dbx.analyze.model import PortfolioAnalysis, ProjectAnalysis, TargetClass

CLASS_LABELS = {
    TargetClass.AUTO: "AUTO : traducteur déterministe",
    TargetClass.LLM_REVIEW: "LLM_REVIEW : résidu traité par le LLM, relu",
    TargetClass.MANUAL: "MANUAL : hors périmètre, migration à la main",
}
PORTFOLIO_COLUMNS = [
    "projet",
    "nom",
    "vague",
    "niveau",
    "effort_jours",
    "recettes",
    "auto",
    "llm_review",
    "manual",
    "part_code",
    "datasets",
    "datasets_partitionnes",
    "scenarios",
    "plugins",
    "modeles",
    "anomalies",
]


def _header(title: str) -> list[str]:
    return [
        f"# {title}",
        "",
        f"Généré par dku2dbx {__version__}. Valeurs à calibrer sur le pilote.",
        "",
    ]


def _table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return ["(aucun)", ""]
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return [*lines, ""]


def _counts_table(title: str, counts: dict[str, int]) -> list[str]:
    return [
        f"### {title}",
        "",
        *_table(["Type", "Nombre"], [[k, str(v)] for k, v in counts.items()]),
    ]


def _num(value: float) -> str:
    return f"{value:g}"


def project_markdown(a: ProjectAnalysis, wave: int | None) -> str:
    lines = _header(f"Analyse du projet {a.key}")
    if a.name:
        lines += [f"Nom : {a.name}", ""]
    lines += [
        f"Source : `{a.source_file}`",
        "",
        "## Synthèse",
        "",
        f"- **Niveau de complexité : {a.level.value}**",
        f"- **Effort estimé : {_num(a.effort_days)} jours** (coefficients de `scoring.yaml`)",
        f"- Vague de migration : {wave if wave is not None else 'hors portefeuille'}",
        f"- Recettes : {a.counts['recipes']}, dont "
        + ", ".join(f"{k} {v}" for k, v in a.recipes_by_class.items()),
        f"- Anomalies du lecteur : {a.counts['issues']}",
        "",
        "## Conditions de score atteintes",
        "",
        *_table(
            ["Niveau", "Mesure", "Valeur", "Seuil"],
            [[t.level.value, t.measure, _num(t.value), _num(t.threshold)] for t in a.triggers],
        ),
        "## Mesures",
        "",
        *_table(["Mesure", "Valeur"], [[k, _num(v)] for k, v in sorted(a.measures.items())]),
        "## Inventaire",
        "",
        *_table(["Objet", "Nombre"], [[k, str(v)] for k, v in sorted(a.counts.items())]),
        *_counts_table("Recettes par type", a.recipes_by_type),
        *_counts_table("Recettes par catégorie", a.recipes_by_category),
        *_counts_table("Datasets par type", a.datasets_by_type),
        *_counts_table("Processeurs Prepare actifs", a.processors),
        "### Plugins utilisés",
        "",
        *([f"- {p}" for p in a.plugins] or ["(aucun)"]),
        "",
        "### Steps Prepare désactivées (ignorées, jamais traduites)",
        "",
        *([f"- {s}" for s in a.disabled_steps] or ["(aucune)"]),
        "",
        "## Classement des recettes",
        "",
    ]
    for target in TargetClass:
        rows = [
            [
                r.name,
                r.type,
                str(r.planned_phase) if r.planned_phase is not None else "—",
                "oui" if r.translator_available else "non",
                "; ".join(r.reasons) or "—",
            ]
            for r in a.recipes
            if r.target_class is target
        ]
        lines += [f"### {CLASS_LABELS[target]}", ""]
        lines += _table(["Recette", "Type", "Phase prévue", "Traducteur disponible", "Motif"], rows)
    lines += [
        "## Dépendances avec d'autres projets",
        "",
        *_table(
            ["Producteur", "Consommateur", "Dataset", "Vu par"],
            [[x.producer, x.consumer, x.dataset, x.source] for x in a.links],
        ),
        "## Anomalies du lecteur",
        "",
        *_table(
            ["Type (détail dans ir.json)", "Nombre"],
            [[k, str(v)] for k, v in a.issues_by_kind.items()],
        ),
    ]
    return "\n".join(lines).rstrip("\n") + "\n"


def portfolio_csv(p: PortfolioAnalysis) -> str:
    wave_of = {key: w.number for w in p.waves for key in w.projects}
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(PORTFOLIO_COLUMNS)
    for a in p.projects:
        classes = a.recipes_by_class
        writer.writerow(
            [
                a.key,
                a.name or "",
                wave_of.get(a.key, ""),
                a.level.value,
                _num(a.effort_days),
                a.counts["recipes"],
                classes.get(TargetClass.AUTO.value, 0),
                classes.get(TargetClass.LLM_REVIEW.value, 0),
                classes.get(TargetClass.MANUAL.value, 0),
                _num(a.measures["code_share"]),
                a.counts["datasets"],
                a.counts["partitioned_datasets"],
                a.counts["scenarios"],
                len(a.plugins),
                a.counts["saved_models"],
                a.counts["issues"],
            ]
        )
    return buffer.getvalue()


def waves_markdown(p: PortfolioAnalysis) -> str:
    effort = {a.key: a.effort_days for a in p.projects}
    lines = _header("Vagues de migration")
    lines += [
        "Un projet producteur passe avant ses consommateurs. Des projets qui dépendent",
        "les uns des autres (cycle) sont regroupés et migrés ensemble.",
        "",
    ]
    for wave in p.waves:
        total = round(sum(effort[k] for k in wave.projects), 2)
        lines += [f"## Vague {wave.number} ({_num(total)} jours)", ""]
        lines += [f"- {key}" for key in wave.projects]
        lines += [f"- Cycle, à migrer ensemble : {', '.join(c)}" for c in wave.cycles]
        lines.append("")
    lines += [
        "## Dépendances avec des projets hors portefeuille",
        "",
        *_table(
            ["Producteur", "Consommateur", "Dataset", "Vu par"],
            [[x.producer, x.consumer, x.dataset, x.source] for x in p.external_links],
        ),
    ]
    return "\n".join(lines).rstrip("\n") + "\n"


def catalogue_csv(p: PortfolioAnalysis) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["processeur", "occurrences", "desactivees", "recettes", "projets", "classe"])
    for s in p.processors:
        writer.writerow(
            [s.type, s.occurrences, s.disabled, s.recipes, s.projects, s.target_class.value]
        )
    return buffer.getvalue()


def catalogue_markdown(p: PortfolioAnalysis) -> str:
    total = sum(s.occurrences for s in p.processors)
    lines = _header("Catalogue des processeurs Prepare")
    lines += [
        "Occurrences actives sur tout le portefeuille, de la plus fréquente à la moins",
        "fréquente : cet ordre fixe l'ordre de développement de la phase 4.",
        "",
    ]
    cumulated = 0
    rows = []
    for s in p.processors:
        cumulated += s.occurrences
        share = f"{100 * cumulated / total:.0f} %" if total else "—"
        rows.append(
            [
                s.type,
                str(s.occurrences),
                str(s.disabled),
                str(s.recipes),
                str(s.projects),
                share,
                s.target_class.value,
            ]
        )
    lines += _table(
        ["Processeur", "Occurrences", "Désactivées", "Recettes", "Projets", "Cumul", "Classe"],
        rows,
    )
    return "\n".join(lines).rstrip("\n") + "\n"


def _json(model: ProjectAnalysis | PortfolioAnalysis) -> str:
    data = json.loads(model.model_dump_json())
    return json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def write_outputs(p: PortfolioAnalysis, out: Path) -> list[Path]:
    """Write every output under ``out`` and return the written paths."""
    wave_of = {key: w.number for w in p.waves for key in w.projects}
    files: dict[Path, str] = {}
    for a in p.projects:
        files[out / a.key / "analyse.json"] = _json(a)
        files[out / a.key / "analyse.md"] = project_markdown(a, wave_of.get(a.key))
    files[out / "portefeuille.csv"] = portfolio_csv(p)
    files[out / "vagues.md"] = waves_markdown(p)
    files[out / "catalogue_processeurs.csv"] = catalogue_csv(p)
    files[out / "catalogue_processeurs.md"] = catalogue_markdown(p)
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
    return sorted(files)
