"""Flow simplification (ROADMAP phase 5): merge recipe chains into steps.

A table stays materialized when (ROADMAP rules):
- it is a final output (no recipe reads it) or an object exposed to another project;
- several recipes read it;
- it is partitioned or written in append mode;
- it carries checks, or a scenario reads or builds it (ASSUMPTION H-49);
- it is an input or an output of a LLM_REVIEW or MANUAL recipe, or of a recipe its
  translator refused (decision of 2026-11-23: it cannot become a CTE).

Every other table is read by exactly one recipe: the recipe producing it joins the
step of that consumer, and the table becomes a CTE of the step. ``fusion=False``
keeps every table (one step per recipe), for fine reconciliation and debugging.
"""

from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel, ConfigDict, Field

from dku2dbx.analyze.classify import classify
from dku2dbx.analyze.model import TargetClass
from dku2dbx.errors import Dku2dbxError
from dku2dbx.model.flow import recipe_node_id
from dku2dbx.model.ir import Project
from dku2dbx.translate.engine import translate_project


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Step(_Model):
    id: str
    recipes: list[str]
    """In execution (topological) order."""
    outputs: list[str]
    """Materialized tables written by the step."""
    inlined: list[str] = Field(default_factory=list)
    """Intermediate tables that become CTEs of the step."""


class SimplifiedFlow(_Model):
    project: str
    fusion: bool
    recipes_before: int
    steps: list[Step]
    kept: dict[str, list[str]]
    """Materialized table -> reasons (French, for the report)."""
    removed: list[str]
    """Tables that become CTEs."""


class SimplifyError(Dku2dbxError):
    """The Flow cannot be simplified (cycle)."""


# ASSUMPTION (H-49): a scenario reads or builds a dataset through step params
# builds[].itemId (type DATASET) or a trigger params.datasetName.
def _scenario_datasets(project: Project) -> set[str]:
    names: set[str] = set()
    for scenario in project.scenarios:
        for step in scenario.steps:
            for build in step.params.get("builds") or []:
                if isinstance(build, dict) and build.get("type", "DATASET") == "DATASET":
                    names.add(str(build.get("itemId")))
        for trigger in scenario.triggers:
            if trigger.params.get("datasetName"):
                names.add(str(trigger.params["datasetName"]))
    return names


def _boundary_recipes(project: Project) -> dict[str, str]:
    """Recipes that cannot be merged, with the reason."""
    result = {
        c.name: f"recette {c.target_class.value} '{c.name}'"
        for c in classify(project)
        if c.target_class is not TargetClass.AUTO
    }
    for item in translate_project(project).unsupported:
        result.setdefault(item.recipe, f"recette non traduisible '{item.recipe}' ({item.element})")
    return result


def _structural(project: Project, name: str, readers: list[str]) -> list[str]:
    """Reasons that depend on the table itself (ROADMAP rules)."""
    why = []
    if not readers:
        why.append("sortie finale (aucune recette ne la lit)")
    if name in {e.local_name for e in project.exposed_objects}:
        why.append("exposée à d'autres projets")
    if len(readers) > 1:
        why.append(f"lue par {len(readers)} recettes")
    dataset = next((d for d in project.datasets if d.name == name), None)
    if dataset is not None and dataset.partitioning:
        why.append("partitionnée")
    if any(o.ref == name and o.append_mode for r in project.recipes for o in r.outputs):
        why.append("écrite en mode append")
    if dataset is not None and dataset.checks:
        why.append("porte des checks")
    if name in _scenario_datasets(project):
        why.append("lue ou construite par un scénario")
    return why


def _keep_reasons(project: Project, fusion: bool) -> dict[str, list[str]]:
    produced = {o.ref: r.name for r in project.recipes for o in r.outputs}
    readers: defaultdict[str, list[str]] = defaultdict(list)
    for rcp in project.recipes:
        for inp in rcp.inputs:
            readers[inp.ref].append(rcp.name)
    boundary = _boundary_recipes(project)
    reasons: dict[str, list[str]] = {}
    for name in sorted(produced):
        why = ["option --no-fusion"] if not fusion else []
        why += _structural(project, name, readers[name])
        for recipe in [produced[name], *readers[name]]:
            if recipe in boundary:
                side = "sortie" if recipe == produced[name] else "entrée"
                why.append(f"{side} d'une {boundary[recipe]}")
        reasons[name] = why
    return reasons


def simplify_flow(project: Project, *, fusion: bool = True) -> SimplifiedFlow:
    if project.recipes and not project.flow.topological_order:
        raise SimplifyError(f"{project.key}: the flow has a cycle, it cannot be simplified")
    order = {node: i for i, node in enumerate(project.flow.topological_order)}
    reasons = _keep_reasons(project, fusion)
    kept = {name: why for name, why in reasons.items() if why}
    removed = sorted(name for name, why in reasons.items() if not why)

    # Union-find: the producer of an inlined table joins the step of its only reader.
    parent: dict[str, str] = {r.name: r.name for r in project.recipes}

    def find(name: str) -> str:
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    producer = {o.ref: r.name for r in project.recipes for o in r.outputs}
    reader = {i.ref: r.name for r in project.recipes for i in r.inputs}
    for table in removed:
        parent[find(producer[table])] = find(reader[table])

    groups: defaultdict[str, list[str]] = defaultdict(list)
    for rcp in project.recipes:
        groups[find(rcp.name)].append(rcp.name)
    steps: list[dict[str, list[str]]] = []
    for members in groups.values():
        members.sort(key=lambda r: order[recipe_node_id(r)])
        outputs = sorted(
            o.ref for r in project.recipes if r.name in members for o in r.outputs if o.ref in kept
        )
        inlined = sorted(
            o.ref
            for r in project.recipes
            if r.name in members
            for o in r.outputs
            if o.ref not in kept
        )
        steps.append({"recipes": members, "outputs": outputs, "inlined": inlined})
    steps.sort(key=lambda s: order[recipe_node_id(s["recipes"][-1])])
    return SimplifiedFlow(
        project=project.key,
        fusion=fusion,
        recipes_before=len(project.recipes),
        steps=[Step(id=f"step_{i:02d}", **s) for i, s in enumerate(steps, start=1)],
        kept=kept,
        removed=removed,
    )
