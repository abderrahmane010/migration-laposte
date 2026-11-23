"""Portfolio analysis: Prepare processor catalogue and migration waves."""

from __future__ import annotations

from collections import Counter, defaultdict

import networkx as nx

from dku2dbx.analyze.classify import PREPARE_TYPE, processors
from dku2dbx.analyze.model import (
    CrossProjectLink,
    PortfolioAnalysis,
    ProcessorStat,
    ProjectAnalysis,
    TargetClass,
    Wave,
)
from dku2dbx.model.ir import Project
from dku2dbx.translate.prepare.processors import MANUAL_PROCESSORS


def processor_catalogue(projects: list[Project]) -> list[ProcessorStat]:
    """Every Prepare processor type, most frequent first (it orders phase 4)."""
    active: Counter[str] = Counter()
    disabled: Counter[str] = Counter()
    recipes: defaultdict[str, set[tuple[str, str]]] = defaultdict(set)
    for project in projects:
        for rcp in project.recipes:
            if rcp.type != PREPARE_TYPE:
                continue
            for state, counter in ((False, active), (True, disabled)):
                for ptype in processors(rcp, disabled=state):
                    counter[ptype] += 1
                    recipes[ptype].add((project.key, rcp.name))
    return sorted(
        (
            ProcessorStat(
                type=ptype,
                occurrences=active[ptype],
                disabled=disabled[ptype],
                recipes=len(where),
                projects=len({key for key, _ in where}),
                target_class=(
                    TargetClass.MANUAL if ptype in MANUAL_PROCESSORS else TargetClass.AUTO
                ),
            )
            for ptype, where in recipes.items()
        ),
        key=lambda s: (-s.occurrences, -s.disabled, s.type),
    )


def waves(analyses: list[ProjectAnalysis]) -> tuple[list[Wave], list[CrossProjectLink]]:
    """Producers before consumers; projects in a dependency cycle migrate together.

    Links come from both sides (consumer inputs and foreign objects, producer
    exposed objects). Links with a project outside the portfolio are returned
    apart: they do not order the waves, they are reported.
    """
    keys = {a.key for a in analyses}
    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_nodes_from(sorted(keys))
    internal: set[tuple[str, str]] = set()
    external: dict[tuple[str, str, str, str], CrossProjectLink] = {}
    for analysis in analyses:
        for link in analysis.links:
            if link.producer in keys and link.consumer in keys:
                internal.add((link.producer, link.consumer))
            else:
                external[(link.producer, link.consumer, link.dataset, link.source)] = link
    graph.add_edges_from(sorted(internal))

    condensed = nx.condensation(graph)
    members: dict[int, list[str]] = {
        node: sorted(data["members"]) for node, data in condensed.nodes(data=True)
    }
    result = []
    for number, generation in enumerate(nx.topological_generations(condensed), start=1):
        groups = sorted(members[node] for node in generation)
        result.append(
            Wave(
                number=number,
                projects=sorted(key for group in groups for key in group),
                cycles=[group for group in groups if len(group) > 1],
            )
        )
    return result, [external[k] for k in sorted(external)]


def analyze_portfolio(
    projects: list[Project], analyses: list[ProjectAnalysis]
) -> PortfolioAnalysis:
    wave_list, external = waves(analyses)
    return PortfolioAnalysis(
        projects=sorted(analyses, key=lambda a: a.key),
        processors=processor_catalogue(projects),
        waves=wave_list,
        external_links=external,
    )
