"""Dataiku scenarios -> scheduled Lakeflow Jobs (ROADMAP phase 6, task 3).

ASSUMPTION (H-51): a step-based scenario made of ``build_flowitem`` steps with
``jobType: RECURSIVE_BUILD`` (observed in the fixtures) becomes one job whose tasks
are the steps writing the built datasets and every step upstream of them. Its
triggers map as follows:
- ``temporal`` Daily {hour, minute, timezone} or Hourly {minute} -> ``schedule``
  (quartz cron, Dataiku time zone kept: no UTC shift needed);
- ``ds_modified`` {datasetName} -> ``trigger.table_update`` on that table.
A Databricks job has one schedule or one trigger: further active triggers, inactive
triggers, Python scenarios and any other step type are listed, never dropped.
Dataiku rebuilds only out-of-date datasets; the job recomputes every step it holds.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from dku2dbx.generate.partitions import PARTITION_SPECS
from dku2dbx.model.ir import Project, Scenario
from dku2dbx.simplify.flow import SimplifiedFlow

MAPPED_STEP_TYPES = frozenset({"build_flowitem"})
MAPPED_JOB_TYPES = frozenset({"RECURSIVE_BUILD"})


@dataclass
class ScenarioJob:
    scenario: Scenario
    key: str
    steps: list[str] = field(default_factory=list)
    """Simplified steps (task keys) the job runs."""
    schedule: dict[str, Any] | None = None
    trigger: dict[str, Any] | None = None
    not_translated: list[str] = field(default_factory=list)
    """Why the scenario is MANUAL (then no job is generated)."""
    warnings: list[str] = field(default_factory=list)
    date_offset: str = "0"
    """Default of the date_offset job parameter (partition built, H-53)."""

    @property
    def manual(self) -> bool:
        return bool(self.not_translated)


def _cron(recipe_trigger: dict[str, Any]) -> tuple[str, str] | None:
    params = recipe_trigger
    frequency = params.get("frequency")
    minute = int(params.get("minute", 0))
    if frequency == "Daily":
        return f"0 {minute} {int(params.get('hour', 0))} * * ?", str(params.get("timezone", "UTC"))
    if frequency == "Hourly":
        return f"0 {minute} * * * ?", str(params.get("timezone", "UTC"))
    return None


def _upstream(flow: SimplifiedFlow, project: Project, targets: set[str]) -> list[str]:
    writer = {out: s.id for s in flow.steps for out in s.outputs}
    reads = {s.id: {i.ref for r in s.recipes for i in project.recipe(r).inputs} for s in flow.steps}
    needed: set[str] = set()
    todo = [writer[t] for t in targets if t in writer]
    while todo:
        step = todo.pop()
        if step not in needed:
            needed.add(step)
            todo += [writer[t] for t in reads[step] if t in writer]
    return [s.id for s in flow.steps if s.id in needed]


def _triggers(job: ScenarioJob, scenario: Scenario, tables_prefix: str) -> None:
    for trigger in scenario.triggers:
        label = f"déclencheur {trigger.id or trigger.type} ({trigger.type})"
        if not trigger.active:
            job.warnings.append(f"{label} inactif dans Dataiku : non repris")
            continue
        if job.schedule is not None or job.trigger is not None:
            job.warnings.append(f"{label} non traduit : un job n'a qu'un déclencheur")
            continue
        if trigger.type == "temporal" and (cron := _cron(trigger.params)) is not None:
            job.schedule = {"quartz_cron_expression": cron[0], "timezone_id": cron[1]}
        elif trigger.type == "ds_modified" and trigger.params.get("datasetName"):
            table = f"{tables_prefix}{trigger.params['datasetName']}"
            job.trigger = {"table_update": {"table_names": [table]}}
            job.warnings.append(f"{label} : la table source sera branchée en phase 7 ({table})")
        else:
            job.warnings.append(f"{label} non traduit (paramètres {sorted(trigger.params)})")


def _builds(job: ScenarioJob, scenario: Scenario) -> tuple[set[str], set[str]]:
    """Datasets the scenario builds, and the partitions it asks for."""
    targets: set[str] = set()
    specs: set[str] = set()
    for step in scenario.steps:
        if step.type not in MAPPED_STEP_TYPES:
            job.not_translated.append(f"step {step.id or step.type} de type {step.type}")
            continue
        job_type = step.params.get("jobType", "RECURSIVE_BUILD")
        if job_type not in MAPPED_JOB_TYPES:
            job.not_translated.append(f"step {step.id} : jobType {job_type}")
        for build in step.params.get("builds") or []:
            if build.get("type", "DATASET") != "DATASET":
                job.not_translated.append(f"step {step.id} : construit un {build.get('type')}")
                continue
            targets.add(str(build.get("itemId")))
            if build.get("partitionsSpec"):
                specs.add(str(build["partitionsSpec"]))
    return targets, specs


def _partition_offset(job: ScenarioJob, specs: set[str]) -> None:
    if sorted(specs - set(PARTITION_SPECS)) or len(specs) > 1:
        job.not_translated.append(f"partitions construites {sorted(specs)} : non traduites")
    elif specs:
        job.date_offset = PARTITION_SPECS[specs.pop()]
        job.warnings.append(
            f"partition construite : aujourd'hui moins {job.date_offset} jour(s) "
            "dans le fuseau du job (paramètres date, date_offset, timezone)"
        )


def scenario_jobs(project: Project, flow: SimplifiedFlow, bundle_name: str) -> list[ScenarioJob]:
    jobs = []
    for scenario in project.scenarios:
        key = f"{bundle_name}_scenario_{re.sub(r'[^a-z0-9_]+', '_', scenario.id.lower())}"
        job = ScenarioJob(scenario=scenario, key=key)
        if scenario.type != "step_based":
            job.not_translated.append(f"scénario de type {scenario.type} (Python) : MANUAL")
        targets, specs = _builds(job, scenario)
        _partition_offset(job, specs)
        if not scenario.active:
            job.warnings.append("scénario inactif dans Dataiku : job généré en pause")
        job.steps = _upstream(flow, project, targets)
        sources = sorted(t for t in targets if not any(t in s.outputs for s in flow.steps))
        if sources:
            job.warnings.append(f"datasets sources, rien à construire : {', '.join(sources)}")
        if not job.manual:
            _triggers(job, scenario, "${var.catalog}.${var.schema}.")
            if job.schedule is not None and not scenario.active:
                job.schedule["pause_status"] = "PAUSED"
        jobs.append(job)
    return jobs
