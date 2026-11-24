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

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from dku2dbx import __version__
from dku2dbx.connect.resolve import Branching
from dku2dbx.errors import Dku2dbxError, UnsupportedConstruct
from dku2dbx.generate.partitions import (
    DayPartition,
    day_partition,
    filter_partitioned_inputs,
    partition_date,
)
from dku2dbx.generate.scenarios import ScenarioJob, scenario_jobs
from dku2dbx.generate.templating import render
from dku2dbx.model.ir import Project
from dku2dbx.simplify.flow import SimplifiedFlow, Step, simplify_flow
from dku2dbx.translate.code.python_recipe import rewrite_python
from dku2dbx.translate.engine import TO_REVIEW, payload_sha256, translate_project
from dku2dbx.translate.model import ProjectTranslation, TranslatedOutput
from dku2dbx.translate.prepare.formula import JobParameter, parameter_name
from dku2dbx.translate.sql import DIALECT, column, table
from dku2dbx.translate.visual.types import spark_type

NOTEBOOK_HEADER = "-- Databricks notebook source"
CELL = "\n\n-- COMMAND ----------\n\n"
PY_NOTEBOOK_HEADER = "# Databricks notebook source"
PY_CELL = "\n\n# COMMAND ----------\n\n"
SCHEMA_TASK = "prepare_schema"
INGEST_PREFIX = "ingest_"


class GenerateError(Dku2dbxError):
    """The bundle cannot be generated."""


@dataclass
class GeneratedStep:
    step: Step
    notebook: str
    translated: bool
    reasons: list[str] = field(default_factory=list)
    unmapped_checks: list[str] = field(default_factory=list)
    path: str = ""
    """Notebook in the bundle: src/<step>.sql, or src/<step>.py for Python (phase 8)."""
    to_review: list[str] = field(default_factory=list)
    """Recipes whose code was rewritten deterministically: review before production."""

    def __post_init__(self) -> None:
        self.path = self.path or f"src/{self.step.id}.sql"


@dataclass
class Bundle:
    project: str
    files: dict[str, str]
    """Path in the bundle -> content."""
    steps: list[GeneratedStep]
    scenarios: list[ScenarioJob] = field(default_factory=list)
    target: str = "job"
    pipeline_steps: list[str] = field(default_factory=list)
    branching: Branching | None = None
    """Steps materialized as views of the pipeline (target B)."""


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
    def __init__(
        self,
        project: Project,
        result: ProjectTranslation,
        flow: SimplifiedFlow,
        branching: Branching | None = None,
    ) -> None:
        self.project = project
        self.branched = dict(branching.resolved) if branching else {}
        self.translated = {t.recipe: t for t in result.translated}
        self.unsupported = {u.recipe: u for u in result.unsupported}
        self.rewritten = {r.recipe for r in result.rewritten}
        self.outputs: dict[str, tuple[str, TranslatedOutput]] = {
            o.dataset: (t.recipe, o) for t in result.translated for o in t.outputs
        }
        self.flow = flow
        self.unmapped_checks: list[str] = []

    def _reasons(self, step: Step) -> list[str]:
        reasons = []
        for name in step.recipes:
            if name in self.unsupported:
                u = self.unsupported[name]
                reasons.append(f"{name} ({u.type}) : {u.element} — {u.detail}")
            elif name not in self.translated and name not in self.rewritten:
                reasons.append(f"{name} ({self.project.recipe(name).type}) : pas de traducteur")
        return reasons

    def _query(self, dataset: str, inlined: set[str]) -> exp.Query:
        """The query writing ``dataset``, with the inlined tables it reads as CTEs."""
        recipe = self.outputs[dataset][0]
        query = self._recipe_query(dataset)
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
                this=self._recipe_query(name),
                alias=exp.TableAlias(this=exp.to_identifier(name, quoted=True)),
            )
            for name in needed
        ]
        query.set("with_", exp.With(expressions=[*ctes, *(own.expressions if own else [])]))
        return query

    def _recipe_query(self, dataset: str) -> exp.Query:
        """The translated query of a recipe output, reading only the partition it builds."""
        recipe, output = self.outputs[dataset]
        query = filter_partitioned_inputs(
            _parse(output.sql), self.project.recipe(recipe), self.project
        )
        return _qualify_sources(query, self.branched)

    def _statement(self, dataset: str, query: exp.Query, append: bool) -> list[str]:
        recipe = self.outputs[dataset][0]
        part = day_partition(self.project.dataset(dataset), recipe)
        if part is not None:
            return self._partitioned(dataset, query, append, part)
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

    def _partitioned(
        self, dataset: str, query: exp.Query, append: bool, part: DayPartition
    ) -> list[str]:
        """CREATE TABLE IF NOT EXISTS, then INSERT INTO … REPLACE WHERE the partition (H-53)."""
        recipe = self.outputs[dataset][0]
        target = table(dataset)
        columns = [
            exp.ColumnDef(
                this=exp.to_identifier(c.name, quoted=True), kind=spark_type(c.type, recipe, c.name)
            )
            for c in self.project.dataset(dataset).columns
        ]
        rows: exp.Query = query
        if not part.in_schema:
            columns.append(
                exp.ColumnDef(
                    this=exp.to_identifier(part.column, quoted=True),
                    kind=exp.DataType.build("DATE"),
                )
            )
            rows = exp.select(
                exp.Star(), exp.alias_(partition_date(), part.column, quoted=True)
            ).from_(query.subquery("partition_rows"))
        schema = exp.Schema(this=target.copy(), expressions=columns)
        create = exp.Create(this=schema, kind="TABLE", exists=True)
        insert = exp.Insert(this=target.copy(), expression=rows)
        if not append:
            insert.set("where", exp.EQ(this=column(part.column), expression=partition_date()))
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
                if self.translated[name].review:
                    lines.append(f"{TO_REVIEW} ({name})")
                lines += [
                    f"WARNING ({name}): {w}"
                    for w in self.translated[name].warnings
                    if not w.startswith("dépendance de partition")  # applied here (H-53)
                ]
        return lines

    def build(self, step: Step) -> GeneratedStep:
        reasons = self._reasons(step)
        if not reasons and self.rewritten & set(step.recipes):
            return self._python(step)
        cells = [NOTEBOOK_HEADER + "\n" + _comment(self.header(step))]
        cells += ["USE CATALOG IDENTIFIER(:catalog);", "USE SCHEMA IDENTIFIER(:schema);"]
        if reasons:
            return self._stub(step, cells, reasons)
        before = len(self.unmapped_checks)
        try:
            cells += self._writes(step)
        except UnsupportedConstruct as exc:  # e.g. a column type an append table needs
            return self._stub(step, cells, [f"{exc.recipe} : {exc.construct} — {exc.detail}"])
        checks = self.unmapped_checks[before:]
        review = [r for r in step.recipes if r in self.translated and self.translated[r].review]
        return GeneratedStep(step, CELL.join(cells) + "\n", True, [], checks, to_review=review)

    def _python(self, step: Step) -> GeneratedStep:
        """A rewritten Python recipe (alone in its step: code recipes are boundaries)."""
        [name] = step.recipes
        recipe = self.project.recipe(name)
        try:
            rewrite = rewrite_python(self.project, recipe, self.branched)
        except UnsupportedConstruct as exc:
            cells = [NOTEBOOK_HEADER + "\n" + _comment(self.header(step))]
            return self._stub(step, cells, [f"{exc.recipe} : {exc.construct} — {exc.detail}"])
        header = [
            *self.header(step),
            TO_REVIEW,
            *(f"WARNING: {w}" for w in rewrite.warnings),
        ]
        cells = [
            PY_NOTEBOOK_HEADER + "\n" + "\n".join(f"# {line}" for line in header),
            "# Job parameters are notebook widgets (H-57): the target catalog and schema.\n"
            'spark.catalog.setCurrentCatalog(dbutils.widgets.get("catalog"))\n'
            'spark.catalog.setCurrentDatabase(dbutils.widgets.get("schema"))',
            rewrite.code.rstrip(),
        ]
        before = len(self.unmapped_checks)
        for dataset in step.outputs:
            for check in self._checks(dataset):
                sql = check.split("\n")[-1].removesuffix(";")
                cells.append(f"spark.sql({json.dumps(sql, ensure_ascii=False)}).collect()")
        checks = self.unmapped_checks[before:]
        return GeneratedStep(
            step,
            PY_CELL.join(cells) + "\n",
            True,
            [],
            checks,
            path=f"src/{step.id}.py",
            to_review=[name],
        )

    def _writes(self, step: Step) -> list[str]:
        appends = {
            o.ref for r in step.recipes for o in self.project.recipe(r).outputs if o.append_mode
        }
        cells: list[str] = []
        for dataset in step.outputs:
            query = self._query(dataset, set(step.inlined))
            cells += self._statement(dataset, query, dataset in appends)
            cells += self._checks(dataset)
        return cells

    def _checks(self, dataset: str) -> list[str]:
        """Dataset checks -> SQL assertions after the write (H-52)."""
        cells = []
        for check in self.project.dataset(dataset).checks:
            label = f"check {check.get('id') or check.get('type')} of {dataset}"
            if check.get("type") != "recordCountInRange":
                self.unmapped_checks.append(f"{label} : type {check.get('type')} non traduit")
                cells.append(_comment([f"NOT TRANSLATED: {label} ({check.get('type')})"]))
                continue
            count = exp.Count(this=exp.Star())
            bounds: list[exp.Expr] = []
            if check.get("minimum") is not None:
                bounds.append(exp.GTE(this=count, expression=exp.Literal.number(check["minimum"])))
            if check.get("maximum") is not None:
                bounds.append(
                    exp.LTE(this=count.copy(), expression=exp.Literal.number(check["maximum"]))
                )
            if not bounds:
                continue
            message = exp.Literal.string(f"dku2dbx: {label} failed (recordCountInRange)")
            assertion = exp.select(exp.func("assert_true", exp.and_(*bounds), message)).from_(
                table(dataset)
            )
            cells.append(_comment([label]) + "\n" + assertion.sql(dialect=DIALECT) + ";")
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


def _qualify_sources(query: exp.Query, branched: dict[str, str]) -> exp.Query:
    """Branched sources (phase 7) are read as catalog.schema.table."""
    if not branched:
        return query

    def qualify(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Table) and not node.args.get("db") and node.name in branched:
            catalog, schema, name = branched[node.name].split(".", 2)
            qualified = exp.table_(name, db=schema, catalog=catalog, quoted=True)
            qualified.set(
                "alias",
                node.args.get("alias")
                or exp.TableAlias(this=exp.to_identifier(node.name, quoted=True)),
            )
            return qualified
        return node

    result = query.transform(qualify)
    if not isinstance(result, exp.Query):
        raise TypeError("unexpected transform result")
    return result


def _ingest_tasks(project: Project, branching: Branching | None) -> dict[str, str]:
    """Unbranched source -> its failing ingestion task (decision of 2026-11-24)."""
    if branching is None:
        return {}
    return {name: f"ingest_{i:02d}" for i, name in enumerate(sorted(branching.unresolved), 1)}


def _ingest_files(
    ingest: dict[str, str], branching: Branching | None, header: str
) -> dict[str, str]:
    """Substitute ingestion notebooks: they fail until the source is branched (phase 7)."""
    files = {}
    for source, key in ingest.items():
        reason = branching.unresolved[source] if branching else ""
        message = f"dku2dbx: source {source} is not branched yet ({reason}): see a_brancher.md"
        files[f"src/{key}.sql"] = (
            CELL.join(
                [
                    NOTEBOOK_HEADER
                    + "\n"
                    + _comment(
                        [
                            header,
                            f"Substitute ingestion of the source {source} (phase 7).",
                            f"NOT BRANCHED: {reason}",
                        ]
                    ),
                    f"SELECT raise_error({exp.Literal.string(message).sql(dialect=DIALECT)});",
                ]
            )
            + "\n"
        )
    return files


def _depends_on(
    flow: SimplifiedFlow, project: Project, ingest: dict[str, str] | None = None
) -> dict[str, list[str]]:
    """Step -> tasks it waits for: schema, upstream steps, ingestion of unbranched sources."""
    writer = {out: s.id for s in flow.steps for out in s.outputs}
    ingest = ingest or {}
    deps: dict[str, list[str]] = {}
    for step in flow.steps:
        reads = {i.ref for r in step.recipes for i in project.recipe(r).inputs}
        upstream = sorted({writer[t] for t in reads if t in writer and writer[t] != step.id})
        sources = sorted({ingest[t] for t in reads if t in ingest})
        deps[step.id] = [SCHEMA_TASK, *sources, *upstream]
    return deps


def _has_partitions(project: Project, flow: SimplifiedFlow) -> bool:
    return any(project.dataset(t).partitioning for s in flow.steps for t in s.outputs)


def _variables(project: Project) -> dict[str, JobParameter]:
    """Project variables -> job parameters ``dku_<name>`` (scalars only)."""
    params: dict[str, JobParameter] = {}
    for name, value in sorted(project.variables.items()):
        if isinstance(value, (bool, int, float, str)):
            param = parameter_name(name)
            if param in {p.name for p in params.values()}:
                raise GenerateError(f"variables {name!r} and another give the parameter {param}")
            params[name] = JobParameter(param, value)
    return params


def _default(value: object) -> str:
    """A job parameter default is text (booleans as true/false, like Dataiku)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _pipeline_steps(project: Project, flow: SimplifiedFlow, steps: list[GeneratedStep]) -> set[str]:
    """Steps that can be materialized views of the pipeline (target B, H-54).

    Out of the pipeline: steps not translated, steps writing a partitioned or append
    table or reading a partition, and every step downstream of one of those (the
    pipeline runs first, the SQL tasks after it).
    """
    # Python notebooks (phase 8) are job tasks, never pipeline views.
    translated = {g.step.id for g in steps if g.translated and g.path.endswith(".sql")}
    writer = {out: s.id for s in flow.steps for out in s.outputs}
    out_of_pipeline: set[str] = set()
    for step in flow.steps:
        recipes = [project.recipe(r) for r in step.recipes]
        special = any(
            o.append_mode or project.dataset(o.ref).partitioning
            for r in recipes
            for o in r.outputs
            if o.ref in step.outputs
        ) or any(i.deps for r in recipes for i in r.inputs)
        upstream = {writer[i.ref] for r in recipes for i in r.inputs if i.ref in writer}
        if step.id not in translated or special or upstream & out_of_pipeline:
            out_of_pipeline.add(step.id)
    return {s.id for s in flow.steps} - out_of_pipeline


def _pipeline_query(query: exp.Query) -> exp.Query:
    """Pipelines read their configuration as ${key}: a job parameter :x becomes '${x}'."""

    def swap(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Placeholder) and node.name:
            return exp.Literal.string(f"${{{node.name}}}")
        return node

    result = query.transform(swap)
    if not isinstance(result, exp.Query):
        raise TypeError("unexpected transform result")
    return result


def generate_pipeline_files(
    builder: _StepBuilder, flow: SimplifiedFlow, pipeline_ids: set[str], header: str
) -> tuple[dict[str, str], list[str]]:
    """One SQL file of materialized views per pipeline step, and the checks cells."""
    files: dict[str, str] = {}
    checks: list[str] = []
    for step in flow.steps:
        if step.id not in pipeline_ids:
            continue
        statements = [_comment([*builder.header(step), header])]
        for dataset in step.outputs:
            query = _pipeline_query(builder._query(dataset, set(step.inlined)))
            view = exp.Create(
                this=table(dataset), kind="MATERIALIZED VIEW", expression=query, replace=False
            )
            sql = view.sql(dialect=DIALECT, pretty=True).replace(
                "CREATE MATERIALIZED VIEW", "CREATE OR REFRESH MATERIALIZED VIEW", 1
            )
            statements.append(sql + ";")
            checks += builder._checks(dataset)
        files[f"src/pipeline/{step.id}.sql"] = "\n\n".join(statements) + "\n"
    return files, checks


def _job_tasks(
    step_ids: list[str],
    deps: dict[str, list[str]],
    pipeline_ids: set[str],
    pipeline_key: str,
    paths: dict[str, str],
) -> list[dict[str, Any]]:
    """Tasks of a job running ``step_ids``: schema, then pipeline (B), then notebook tasks.

    ``paths``: notebook of each step, and of ``pipeline_checks`` when the views have checks.
    """
    has_checks = "pipeline_checks" in paths
    kept = set(step_ids)
    result_tasks: list[dict[str, Any]] = [
        {"key": SCHEMA_TASK, "depends_on": [], "notebook": "../src/00_prepare_schema.sql"}
    ]
    ingest = sorted({d for sid in step_ids for d in deps[sid] if d.startswith(INGEST_PREFIX)})
    result_tasks += [
        {"key": key, "depends_on": [SCHEMA_TASK], "notebook": f"../src/{key}.sql"} for key in ingest
    ]
    upstream_tasks = [SCHEMA_TASK]
    if kept & pipeline_ids:
        pipeline_ingest = sorted(
            {d for sid in kept & pipeline_ids for d in deps[sid] if d.startswith(INGEST_PREFIX)}
        )
        result_tasks.append(
            {
                "key": "pipeline",
                "depends_on": [SCHEMA_TASK, *pipeline_ingest],
                "pipeline": pipeline_key,
            }
        )
        upstream_tasks.append("pipeline")
        if has_checks:
            result_tasks.append(
                {
                    "key": "pipeline_checks",
                    "depends_on": ["pipeline"],
                    "notebook": "../src/pipeline_checks.sql",
                }
            )
    for sid in step_ids:
        if sid in pipeline_ids:
            continue
        # A SQL task runs after the pipeline (it may read its views) and after the
        # SQL tasks it reads; pipeline steps are covered by the "pipeline" task.
        notebook_deps = [
            d
            for d in deps[sid]
            if d.startswith(INGEST_PREFIX) or (d in kept and d not in pipeline_ids)
        ]
        result_tasks.append(
            {
                "key": sid,
                "depends_on": [*upstream_tasks, *notebook_deps],
                "notebook": f"../{paths[sid]}",
            }
        )
    return result_tasks


def _checks_notebook(header: str, checks: list[str]) -> str:
    cells = [
        NOTEBOOK_HEADER + "\n" + _comment([header, "Checks of the pipeline views."]),
        "USE CATALOG IDENTIFIER(:catalog);",
        "USE SCHEMA IDENTIFIER(:schema);",
        *checks,
    ]
    return CELL.join(cells) + "\n"


def generate_job_bundle(
    project: Project,
    *,
    fusion: bool = True,
    target: str = "job",
    branching: Branching | None = None,
) -> Bundle:
    """Every file of the bundle, in memory (written by ``write_bundle``).

    ``target="job"`` (A): a job of SQL notebooks. ``target="pipeline"`` (B): a Lakeflow
    pipeline of materialized views, run by the job before the SQL tasks it cannot hold.
    """
    if target not in ("job", "pipeline"):
        raise GenerateError(f"unknown target {target!r}")
    params = _variables(project)
    # Formulas read the variables as job parameters (:dku_<name>), typed by their value.
    as_parameters = project.model_copy(update={"variables": {**project.variables, **params}})
    result = translate_project(as_parameters)
    flow = simplify_flow(project, fusion=fusion)
    builder = _StepBuilder(project, result, flow, branching)
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
    name = _bundle_name(project.key)
    pipeline_key = f"{name}_pipeline"
    pipeline_ids = _pipeline_steps(project, flow, steps) if target == "pipeline" else set()
    if pipeline_ids:
        pipeline_files, pipeline_checks = generate_pipeline_files(
            builder, flow, pipeline_ids, header
        )
        files.update(pipeline_files)
        if pipeline_checks:
            files["src/pipeline_checks.sql"] = _checks_notebook(header, pipeline_checks)
    for generated in steps:
        if generated.step.id not in pipeline_ids:
            files[generated.path] = generated.notebook
    ingest = _ingest_tasks(project, branching)
    files.update(_ingest_files(ingest, branching, header))
    deps = _depends_on(flow, project, ingest)

    paths = {g.step.id: g.path for g in steps}
    if "src/pipeline_checks.sql" in files:
        paths["pipeline_checks"] = "src/pipeline_checks.sql"

    def tasks(step_ids: list[str]) -> list[dict[str, Any]]:
        return _job_tasks(step_ids, deps, pipeline_ids, pipeline_key, paths)

    scenarios = scenario_jobs(project, flow, name)
    jobs: list[dict[str, Any]] = [
        {
            "key": f"{name}_job",
            "name": f"[dku2dbx] {project.key}",
            "description": f"Full flow of the Dataiku project {project.key} "
            f"(dku2dbx {__version__}), not scheduled",
            "tasks": tasks([g.step.id for g in steps]),
            "schedule": None,
            "trigger": None,
        }
    ]
    jobs += [
        {
            "key": job.key,
            "name": f"[dku2dbx] {project.key} — {job.scenario.name or job.scenario.id}",
            "description": f"Dataiku scenario {job.scenario.id} (dku2dbx {__version__})",
            "tasks": tasks(job.steps),
            "schedule": job.schedule,
            "trigger": job.trigger,
        }
        for job in scenarios
        if not job.manual
    ]
    parameters = [
        {"name": "catalog", "default": "${var.catalog}"},
        {"name": "schema", "default": "${var.schema}"},
        *[{"name": p.name, "default": f"${{var.{p.name}}}"} for p in params.values()],
    ]
    partitioned = _has_partitions(project, flow)

    def job_parameters(offset: str = "0", zone: str = "${var.partition_timezone}") -> list[Any]:
        if not partitioned:
            return parameters
        return [
            *parameters,
            {"name": "date", "default": ""},  # empty: today in `timezone` minus `date_offset`
            {"name": "date_offset", "default": offset},
            {"name": "timezone", "default": zone},
        ]

    for job in jobs:
        scenario = next((j for j in scenarios if j.key == job["key"]), None)
        if scenario is None:
            job["parameters"] = job_parameters()
        else:
            zone = (scenario.schedule or {}).get("timezone_id", "${var.partition_timezone}")
            job["parameters"] = job_parameters(scenario.date_offset, zone)
    variables = [
        {"name": p.name, "description": f"Variable Dataiku ${{{v}}}", "default": _default(p.value)}
        for v, p in params.items()
    ]
    if partitioned:
        variables.append(
            {
                "name": "partition_timezone",
                "description": "Fuseau du calcul de la date de partition (à vérifier)",
                "default": "Europe/Paris",
            }
        )
    files["databricks.yml"] = render(
        "databricks.yml.j2", header=header, bundle_name=name, variables=variables
    )
    files["resources/jobs.yml"] = render("job.yml.j2", header=header, jobs=jobs)
    if pipeline_ids:
        files["resources/pipeline.yml"] = render(
            "pipeline.yml.j2",
            header=header,
            key=pipeline_key,
            name=f"[dku2dbx] {project.key}",
            configuration={p.name: f"${{var.{p.name}}}" for p in params.values()},
            libraries=[f"../src/pipeline/{s.id}.sql" for s in flow.steps if s.id in pipeline_ids],
        )
    bundle = Bundle(
        branching=branching,
        project=project.key,
        files=files,
        steps=steps,
        scenarios=scenarios,
        target=target,
        pipeline_steps=sorted(pipeline_ids),
    )
    files["GENERATION.md"] = generation_report(project, bundle, params, partitioned)
    return bundle


def write_bundle(bundle: Bundle, out: Path) -> list[Path]:
    """Write the bundle under ``out`` (stale generated files are removed first)."""
    stale_files = [
        *sorted(out.glob("src/*.sql")),
        *sorted(out.glob("src/*.py")),
        *sorted(out.glob("resources/*.yml")),
    ]
    for stale in stale_files:
        stale.unlink()
    written = []
    for rel, text in sorted(bundle.files.items()):
        path = out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
        written.append(path)
    return written


def generation_report(
    project: Project, bundle: Bundle, params: dict[str, JobParameter], partitioned: bool
) -> str:
    """GENERATION.md: what the bundle does, and everything left to a human."""
    manual_steps = [g for g in bundle.steps if not g.translated]
    lines = [
        f"# Bundle dku2dbx — {project.key}",
        "",
        f"Généré par dku2dbx {__version__}. "
        + (
            "Cible A : job de notebooks SQL."
            if bundle.target == "job"
            else f"Cible B : pipeline Lakeflow ({len(bundle.pipeline_steps)} étape(s) en vues "
            "matérialisées), puis tâches SQL pour les étapes hors pipeline."
        ),
        "",
        "## Synthèse",
        "",
        f"- Étapes : {len(bundle.steps)}, dont **{len(manual_steps)} à migrer à la main** "
        "(notebook qui échoue explicitement)"
        + f" et {sum(bool(g.to_review) for g in bundle.steps)} réécrite(s) à relire",
        f"- Jobs : 1 flux complet (sans planification) + "
        f"{sum(not j.manual for j in bundle.scenarios)} scénario(s)",
        f"- Paramètres issus des variables Dataiku : {len(params)}",
        "",
        "Avant de déployer : renseigner `catalog`, `schema` et `admin_group` dans "
        "`databricks.yml` (valeurs fictives entre crochets).",
        "",
    ]
    if partitioned:
        lines += [
            "Partitions (H-53) : la date construite est le paramètre `date`, ou, s'il est vide, "
            "aujourd'hui dans `timezone` moins `date_offset` jours. Chaque table partitionnée "
            "a une colonne de date ; une source partitionnée doit en avoir une une fois "
            "branchée (phase 7). Vérifier `partition_timezone` dans `databricks.yml`.",
            "",
        ]
    never = sorted(r.name for r in project.recipes if r.never_recompute_existing_partitions)
    if never:
        lines += [
            "Recettes « ne jamais recalculer les partitions existantes » : le job recalcule "
            f"la partition demandée à chaque exécution ({', '.join(never)}).",
            "",
        ]
    lines += [
        "## Étapes à migrer à la main",
        "",
    ]
    lines += [f"- {g.step.id} : {'; '.join(g.reasons)}" for g in manual_steps] or ["(aucune)"]
    review = [g for g in bundle.steps if g.to_review]
    lines += ["", "## Code réécrit à relire (phase 8)", ""]
    lines += [f"- {g.step.id} (`{g.path}`) : {', '.join(g.to_review)}" for g in review] or [
        "(aucun)"
    ]
    lines += ["", "## Sources (phase 7)", ""]
    if bundle.branching is None:
        lines.append("Branchement non demandé : les sources gardent leur nom Dataiku.")
    else:
        lines.append(
            f"{len(bundle.branching.resolved)} branchée(s), "
            f"**{len(bundle.branching.unresolved)} à brancher** (mapping "
            f"`{bundle.branching.source_file}`) ; chacune a une tâche d'ingestion qui échoue :"
        )
        lines += [f"- {n} : {why}" for n, why in sorted(bundle.branching.unresolved.items())]
    lines += ["", "## Scénarios", ""]
    for job in bundle.scenarios:
        status = (
            "MANUAL"
            if job.manual
            else (
                f"job `{job.key}`, {len(job.steps)} étape(s)"
                + (
                    f", planifié `{job.schedule['quartz_cron_expression']}` "
                    f"({job.schedule['timezone_id']})"
                    if job.schedule
                    else ""
                )
                + (", déclenché par une table" if job.trigger else "")
            )
        )
        lines.append(f"- {job.scenario.id} : {status}")
        lines += [f"  - non traduit : {r}" for r in job.not_translated]
        lines += [f"  - {w}" for w in job.warnings]
    if not bundle.scenarios:
        lines.append("(aucun)")
    lines += ["", "## Variables → paramètres de job", ""]
    lines += [
        f"- `${{{v}}}` → `:{p.name}` (défaut {_default(p.value)!r})" for v, p in params.items()
    ] or ["(aucune)"]
    unmapped = [c for g in bundle.steps for c in g.unmapped_checks]
    lines += ["", "## Checks non traduits", ""]
    lines += [f"- {c}" for c in unmapped] or ["(aucun)"]
    return "\n".join(lines) + "\n"
