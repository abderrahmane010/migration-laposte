"""Generate a Databricks bundle, target A: a Lakeflow Job of SQL notebooks (phase 6).

One notebook per step of the simplified Flow (phase 5). In a step, the tables that
are not materialized become CTEs of the statement writing a materialized table.
Every notebook starts with ``USE CATALOG`` / ``USE SCHEMA`` on the job parameters
(``catalog``, ``schema``): no real name is written (decision of 2026-11-23).

ASSUMPTION (H-50): SQL notebooks read job parameters as named parameter markers
(``:catalog``); a notebook task without cluster runs on serverless compute.

A step whose recipes are not all translated becomes a notebook that fails with
``raise_error``: the job stays complete, never silently empty.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp

from dku2dbx import __version__
from dku2dbx.errors import Dku2dbxError, UnsupportedConstruct
from dku2dbx.generate.templating import render
from dku2dbx.model.ir import Project
from dku2dbx.simplify.flow import SimplifiedFlow, Step, simplify_flow
from dku2dbx.translate.engine import payload_sha256, translate_project
from dku2dbx.translate.model import ProjectTranslation, TranslatedOutput
from dku2dbx.translate.sql import DIALECT, table
from dku2dbx.translate.visual.types import spark_type

NOTEBOOK_HEADER = "-- Databricks notebook source"
CELL = "\n\n-- COMMAND ----------\n\n"
SCHEMA_TASK = "prepare_schema"


class GenerateError(Dku2dbxError):
    """The bundle cannot be generated."""


@dataclass
class GeneratedStep:
    step: Step
    notebook: str
    translated: bool
    reasons: list[str] = field(default_factory=list)


@dataclass
class Bundle:
    project: str
    files: dict[str, str]
    """Path in the bundle -> content."""
    steps: list[GeneratedStep]


def _bundle_name(key: str) -> str:
    return "dku2dbx_" + re.sub(r"[^a-z0-9_]+", "_", key.lower()).strip("_")


def _comment(lines: list[str]) -> str:
    return "\n".join("-- " + line.replace("\n", " ") for line in lines)


def _parse(sql: str) -> exp.Query:
    query = sqlglot.parse_one(sql, read=DIALECT)
    if not isinstance(query, exp.Query):
        raise GenerateError(f"not a query: {sql[:60]}")
    return query


class _StepBuilder:
    def __init__(self, project: Project, result: ProjectTranslation, flow: SimplifiedFlow) -> None:
        self.project = project
        self.translated = {t.recipe: t for t in result.translated}
        self.unsupported = {u.recipe: u for u in result.unsupported}
        self.outputs: dict[str, tuple[str, TranslatedOutput]] = {
            o.dataset: (t.recipe, o) for t in result.translated for o in t.outputs
        }
        self.flow = flow

    def _reasons(self, step: Step) -> list[str]:
        reasons = []
        for name in step.recipes:
            if name in self.unsupported:
                u = self.unsupported[name]
                reasons.append(f"{name} ({u.type}) : {u.element} — {u.detail}")
            elif name not in self.translated:
                reasons.append(f"{name} ({self.project.recipe(name).type}) : pas de traducteur")
        return reasons

    def _query(self, dataset: str, inlined: set[str]) -> exp.Query:
        """The query writing ``dataset``, with the inlined tables it reads as CTEs."""
        recipe, output = self.outputs[dataset]
        query = _parse(output.sql)
        needed: list[str] = []

        def collect(name: str) -> None:
            producer = self.project.recipe(self.outputs[name][0])
            for inp in producer.inputs:
                if inp.ref in inlined and inp.ref not in needed:
                    collect(inp.ref)
                    needed.append(inp.ref)

        collect(dataset)
        if not needed:
            return query
        own = query.args.get("with_")
        own_names = {c.alias for c in own.expressions} if own else set()
        clash = own_names & set(needed)
        if clash:
            raise GenerateError(f"{recipe}: CTE names clash with inlined tables {sorted(clash)}")
        ctes = [
            exp.CTE(
                this=_parse(self.outputs[name][1].sql),
                alias=exp.TableAlias(this=exp.to_identifier(name, quoted=True)),
            )
            for name in needed
        ]
        query.set("with_", exp.With(expressions=[*ctes, *(own.expressions if own else [])]))
        return query

    def _statement(self, dataset: str, query: exp.Query, append: bool) -> list[str]:
        target = table(dataset)
        if not append:
            create = exp.Create(this=target, kind="TABLE", replace=True, expression=query)
            return [create.sql(dialect=DIALECT, pretty=True) + ";"]
        columns = [
            exp.ColumnDef(
                this=exp.to_identifier(c.name, quoted=True),
                kind=spark_type(c.type, self.outputs[dataset][0], c.name),
            )
            for c in self.project.dataset(dataset).columns
        ]
        schema = exp.Schema(this=target.copy(), expressions=columns)
        create = exp.Create(this=schema, kind="TABLE", exists=True)
        insert = exp.Insert(this=target.copy(), expression=query)
        return [
            create.sql(dialect=DIALECT, pretty=True) + ";",
            insert.sql(dialect=DIALECT, pretty=True) + ";",
        ]

    def header(self, step: Step) -> list[str]:
        lines = [
            f"Generated by dku2dbx {__version__} — do not edit by hand",
            f"Dataiku project: {self.project.key}",
            f"Step: {step.id} ({len(step.recipes)} recipe(s))",
        ]
        for name in step.recipes:
            rcp = self.project.recipe(name)
            lines.append(
                f"Recipe {name} ({rcp.type}) JSON SHA-256 {rcp.source_sha256}, "
                f"payload SHA-256 {payload_sha256(rcp)}"
            )
        lines += [f"Writes: {', '.join(step.outputs) or '—'}"]
        lines += [f"Inlined as CTE: {', '.join(step.inlined)}"] if step.inlined else []
        for name in step.recipes:
            if name in self.translated:
                lines += [f"WARNING ({name}): {w}" for w in self.translated[name].warnings]
        return lines

    def build(self, step: Step) -> GeneratedStep:
        reasons = self._reasons(step)
        cells = [NOTEBOOK_HEADER + "\n" + _comment(self.header(step))]
        cells += ["USE CATALOG IDENTIFIER(:catalog);", "USE SCHEMA IDENTIFIER(:schema);"]
        if reasons:
            return self._stub(step, cells, reasons)
        try:
            cells += self._writes(step)
        except UnsupportedConstruct as exc:  # e.g. a column type an append table needs
            return self._stub(step, cells, [f"{exc.recipe} : {exc.construct} — {exc.detail}"])
        return GeneratedStep(step, CELL.join(cells) + "\n", True, [])

    def _writes(self, step: Step) -> list[str]:
        appends = {
            o.ref for r in step.recipes for o in self.project.recipe(r).outputs if o.append_mode
        }
        cells: list[str] = []
        for dataset in step.outputs:
            query = self._query(dataset, set(step.inlined))
            cells += self._statement(dataset, query, dataset in appends)
        return cells

    def _stub(self, step: Step, cells: list[str], reasons: list[str]) -> GeneratedStep:
        """A notebook that fails explicitly: the step must be migrated by hand."""
        message = f"dku2dbx: step {step.id} must be migrated by hand: " + "; ".join(reasons)
        stub = [
            *cells,
            _comment(["NOT TRANSLATED:", *reasons]),
            f"SELECT raise_error({exp.Literal.string(message).sql(dialect=DIALECT)});",
        ]
        return GeneratedStep(step, CELL.join(stub) + "\n", False, reasons)


def _depends_on(flow: SimplifiedFlow, project: Project) -> dict[str, list[str]]:
    writer = {out: s.id for s in flow.steps for out in s.outputs}
    deps: dict[str, list[str]] = {}
    for step in flow.steps:
        reads = {i.ref for r in step.recipes for i in project.recipe(r).inputs}
        upstream = sorted({writer[t] for t in reads if t in writer and writer[t] != step.id})
        deps[step.id] = [SCHEMA_TASK, *upstream]
    return deps


def generate_job_bundle(project: Project, *, fusion: bool = True) -> Bundle:
    """Target A: every file of the bundle, in memory (written by ``write_bundle``)."""
    result = translate_project(project)
    flow = simplify_flow(project, fusion=fusion)
    builder = _StepBuilder(project, result, flow)
    steps = [builder.build(step) for step in flow.steps]
    header = f"Generated by dku2dbx {__version__} for the Dataiku project {project.key}"
    files: dict[str, str] = {
        "src/00_prepare_schema.sql": CELL.join(
            [
                NOTEBOOK_HEADER + "\n" + _comment([header, "Creates the target schema."]),
                "CREATE SCHEMA IF NOT EXISTS IDENTIFIER(CONCAT(:catalog, '.', :schema));",
            ]
        )
        + "\n",
    }
    for generated in steps:
        files[f"src/{generated.step.id}.sql"] = generated.notebook
    deps = _depends_on(flow, project)
    tasks = [{"key": SCHEMA_TASK, "depends_on": [], "notebook": "../src/00_prepare_schema.sql"}]
    tasks += [
        {"key": g.step.id, "depends_on": deps[g.step.id], "notebook": f"../src/{g.step.id}.sql"}
        for g in steps
    ]
    name = _bundle_name(project.key)
    files["databricks.yml"] = render("databricks.yml.j2", header=header, bundle_name=name)
    files["resources/job.yml"] = render(
        "job.yml.j2",
        header=header,
        job_key=f"{name}_job",
        job_name=f"[dku2dbx] {project.key}",
        description=f"Migration of the Dataiku project {project.key} (dku2dbx {__version__})",
        tasks=tasks,
    )
    return Bundle(project=project.key, files=files, steps=steps)


def write_bundle(bundle: Bundle, out: Path) -> list[Path]:
    """Write the bundle under ``out`` (stale generated files are removed first)."""
    for stale in sorted(out.glob("src/*.sql")) + sorted(out.glob("resources/*.yml")):
        stale.unlink()
    written = []
    for rel, text in sorted(bundle.files.items()):
        path = out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
        written.append(path)
    return written
