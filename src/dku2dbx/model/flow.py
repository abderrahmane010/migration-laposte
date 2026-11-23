"""Flow graph of a project: dataset or saved model -> recipe -> dataset or saved model.

``build_flow`` computes the serialisable ``FlowGraph`` stored in the IR, and
``to_networkx`` gives the networkx view used by the later phases.
"""

from __future__ import annotations

import networkx as nx

from dku2dbx.model.ir import (
    FlowEdge,
    FlowGraph,
    FlowNode,
    FlowNodeKind,
    Issue,
    IssueKind,
    ObjectKind,
    Project,
)


def dataset_node_id(name: str) -> str:
    return f"dataset:{name}"


def saved_model_node_id(model_id: str) -> str:
    return f"saved_model:{model_id}"


def recipe_node_id(name: str) -> str:
    return f"recipe:{name}"


def build_flow(project: Project) -> tuple[FlowGraph, list[Issue]]:
    """Build the flow graph and the issues it reveals (missing references, cycles)."""
    issues: list[Issue] = []
    nodes: dict[str, FlowNode] = {}
    edges: set[tuple[str, str]] = set()

    saved_models = {sm.id for sm in project.saved_models}
    foreign = {f"{fo.project_key}.{fo.id}" for fo in project.foreign_objects}

    for ds in project.datasets:
        nid = dataset_node_id(ds.name)
        nodes[nid] = FlowNode(id=nid, kind=FlowNodeKind.DATASET, name=ds.name)
    for sm in project.saved_models:
        nid = saved_model_node_id(sm.id)
        nodes[nid] = FlowNode(id=nid, kind=FlowNodeKind.SAVED_MODEL, name=sm.id)

    def ensure_ref(ref: str, recipe_name: str) -> str:
        nid = dataset_node_id(ref)
        if nid in nodes:
            return nid
        # ASSUMPTION (H-26): a recipe references a saved model by its id, like a dataset.
        if ref in saved_models:
            return saved_model_node_id(ref)
        if ref in foreign or "." in ref:
            kind = FlowNodeKind.FOREIGN_DATASET
            if ref not in foreign:
                issues.append(
                    Issue(
                        kind=IssueKind.MISSING_REFERENCE,
                        object_kind=ObjectKind.RECIPE,
                        object_name=recipe_name,
                        message=f"shared dataset '{ref}' is not declared in foreignObjects",
                    )
                )
        else:
            kind = FlowNodeKind.MISSING
            issues.append(
                Issue(
                    kind=IssueKind.MISSING_REFERENCE,
                    object_kind=ObjectKind.RECIPE,
                    object_name=recipe_name,
                    message=(
                        f"reference '{ref}' is neither a dataset nor a saved model of the export"
                    ),
                )
            )
        nodes[nid] = FlowNode(id=nid, kind=kind, name=ref)
        return nid

    for rcp in project.recipes:
        rid = recipe_node_id(rcp.name)
        nodes[rid] = FlowNode(id=rid, kind=FlowNodeKind.RECIPE, name=rcp.name)
        for inp in rcp.inputs:
            edges.add((ensure_ref(inp.ref, rcp.name), rid))
        for out in rcp.outputs:
            edges.add((rid, ensure_ref(out.ref, rcp.name)))

    graph = FlowGraph(
        nodes=sorted(nodes.values(), key=lambda n: n.id),
        edges=[FlowEdge(source=s, target=t) for s, t in sorted(edges)],
    )
    g = to_networkx(graph)
    if nx.is_directed_acyclic_graph(g):
        graph.topological_order = list(nx.lexicographical_topological_sort(g))
    else:
        cycle = nx.find_cycle(g)
        issues.append(
            Issue(
                kind=IssueKind.FLOW_CYCLE,
                object_kind=ObjectKind.PROJECT,
                object_name=project.key,
                message="flow contains a cycle: " + " -> ".join(str(e[0]) for e in cycle),
            )
        )
    return graph, issues


def to_networkx(flow: FlowGraph) -> nx.DiGraph[str]:
    """Directed graph whose nodes carry ``kind`` and ``name`` attributes."""
    g: nx.DiGraph[str] = nx.DiGraph()
    for node in flow.nodes:
        g.add_node(node.id, kind=node.kind, name=node.name)
    for edge in flow.edges:
        g.add_edge(edge.source, edge.target)
    return g
