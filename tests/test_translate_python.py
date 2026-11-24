"""Phase 8: Python recipes, dataiku idioms rewritten (snapshot, PySpark, bundle, refusals)."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
import yaml
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.single_file import SingleFileSnapshotExtension, WriteMode

from dku2dbx.connect.resolve import Branching
from dku2dbx.generate.bundle import PY_CELL, generate_job_bundle
from dku2dbx.model.ir import Project
from dku2dbx.translate.code.python_recipe import rewrite_python
from dku2dbx.translate.engine import TO_REVIEW, translate_project, write_translation
from dku2dbx.translate.model import CodeRewrite, Unsupported
from dku2dbx.translate.prepare.formula import parameter_name
from tests.fixtures import make_fixture as mf
from tests.test_translate_sync import fixture, read_files

NAME = "python_scoring"


class PythonSnapshot(SingleFileSnapshotExtension):
    _write_mode = WriteMode.TEXT
    file_extension = "py.txt"  # not .py: neither collected by pytest nor linted


class FakeWidgets:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get(self, name: str) -> str:
        return self.values[name]


class FakeDbutils:
    def __init__(self, values: dict[str, str]) -> None:
        self.widgets = FakeWidgets(values)


def _result(project: Project, name: str = NAME) -> CodeRewrite | Unsupported:
    result = translate_project(project)
    for item in [*result.rewritten, *result.unsupported]:
        if item.recipe == name:
            return item
    raise AssertionError(name)


def _project(
    tmp_path: Path,
    code: str,
    *,
    append: bool = False,
    partitioned_output: bool = False,
    deps: list[Any] | None = None,
) -> Project:
    """DEMO_SIMPLE plus a Python recipe customers -> scored."""
    p = mf.build_demo_simple()
    p.variables.update({"threshold": "0.5", "limit": 3})
    p.add_dataset(
        mf.dataset(
            p.key,
            "scored",
            cols=mf.columns("customer_id:bigint", "score:double"),
            day_partitioned=partitioned_output,
        )
    )
    rcp = mf.recipe(
        p.key,
        NAME,
        "python",
        [mf.ref("customers", deps)],
        [mf.out_ref("scored", append=append)],
    )
    p.add_recipe(rcp, code)
    return read_files(tmp_path, p.files())


def _rewritten(project: Project) -> CodeRewrite:
    item = _result(project)
    assert isinstance(item, CodeRewrite), item
    return item


def _refused(project: Project) -> Unsupported:
    item = _result(project)
    assert isinstance(item, Unsupported), item
    return item


SCORE = """\
import dataiku
import pandas as pd

# Données clients : le libellé reste en commentaire
customers = dataiku.Dataset("customers").get_dataframe()  # lecture
variables = dataiku.get_custom_variables()
customers["score"] = customers["customer_id"] * float(variables["threshold"])
dataiku.Dataset("scored").write_with_schema(customers[["customer_id", "score"]])
"""


def test_fixture_rewrite_snapshot(snapshot: SnapshotAssertion) -> None:
    rewrite = rewrite_python(fixture("DEMO_COMPLEXE"), fixture("DEMO_COMPLEXE").recipe(NAME))
    assert rewrite.reads == ["delivered_enriched", "DEMO_SIMPLE.customer_stats"]
    assert rewrite.writes == ["customer_scores"]
    names = {n.id for n in ast.walk(ast.parse(rewrite.code)) if isinstance(n, ast.Name)}
    assert "dataiku" not in names
    assert rewrite.code == snapshot.use_extension(PythonSnapshot)


def test_comments_layout_and_accents_are_kept(tmp_path: Path) -> None:
    code = _rewritten(_project(tmp_path, SCORE)).code
    assert "# Données clients : le libellé reste en commentaire\n" in code
    assert 'customers = spark.table("`customers`").toPandas()  # lecture\n' in code
    assert "# dku2dbx: import dataiku (removed)\n" in code
    assert code.index("def _dku_custom_variables():") < code.index("variables = ")


@pytest.mark.spark
def test_rewritten_recipe_runs_on_spark(tmp_path: Path, spark: Any) -> None:
    project = _project(tmp_path, SCORE)
    code = _rewritten(project).code
    spark.createDataFrame(
        [(10, "Ann", "B2B"), (20, "Bob", "B2C")], "customer_id long, name string, segment string"
    ).createOrReplaceTempView("customers")
    dbutils = FakeDbutils({parameter_name(k): str(v) for k, v in project.variables.items()})
    try:
        exec(compile(code, NAME, "exec"), {"spark": spark, "dbutils": dbutils})
        got = sorted(tuple(r) for r in spark.table("scored").collect())
        # Computed by hand: customer_id * 0.5.
        assert got == [(10, 5.0), (20, 10.0)]
        exec(compile(code, NAME, "exec"), {"spark": spark, "dbutils": dbutils})
        assert spark.table("scored").count() == 2  # overwrite, not append
    finally:
        spark.sql("DROP TABLE IF EXISTS scored")


def test_append_output_and_write_dataframe(tmp_path: Path) -> None:
    code = SCORE.replace("write_with_schema", "write_dataframe")
    out = _rewritten(_project(tmp_path, code, append=True)).code
    assert '.write.mode("append").saveAsTable("`scored`")' in out
    out = _rewritten(_project(tmp_path, code)).code
    assert '.write.mode("overwrite").saveAsTable("`scored`")' in out  # schema kept


def test_bound_dataset_folder_and_project_key(tmp_path: Path) -> None:
    code = """\
import dataiku as dku
source = dku.Dataset("customers")
target = dku.Dataset("scored")
df = source.get_dataframe()
df["score"] = 1.0
path = dku.Folder("models").get_path()
key = dku.default_project_key()
target.write_with_schema(df[["customer_id", "score"]])
"""
    rewrite = _rewritten(_project(tmp_path, code))
    assert "# dku2dbx: source = dku.Dataset('customers') (rewritten where used)" in rewrite.code
    assert 'df = spark.table("`customers`").toPandas()' in rewrite.code
    assert (
        "path = f\"/Volumes/{dbutils.widgets.get('catalog')}/"
        "{dbutils.widgets.get('schema')}/models\"" in rewrite.code
    )
    assert 'key = "DEMO_SIMPLE"' in rewrite.code
    assert any("Volume du même nom" in w for w in rewrite.warnings)


def test_written_file_has_a_header_to_review(tmp_path: Path) -> None:
    project = fixture("DEMO_COMPLEXE")
    paths = write_translation(project, translate_project(project), tmp_path)
    [path] = [p for p in paths if p.suffix == ".py"]
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# Generated by dku2dbx ")
    assert f"# {TO_REVIEW}\n" in text
    assert "# Payload SHA-256: " in text


# --------------------------------------------------------------------------- #
# Bundle
# --------------------------------------------------------------------------- #
def test_bundle_runs_the_rewrite_as_a_python_notebook() -> None:
    bundle = generate_job_bundle(fixture("DEMO_COMPLEXE"))
    [step] = [g for g in bundle.steps if NAME in g.step.recipes]
    assert step.translated and step.to_review == [NAME]
    assert step.path == f"src/{step.step.id}.py"
    notebook = bundle.files[step.path]
    assert notebook.startswith("# Databricks notebook source\n")
    assert f"# {TO_REVIEW}" in notebook
    assert 'spark.catalog.setCurrentCatalog(dbutils.widgets.get("catalog"))' in notebook
    jobs = yaml.safe_load(bundle.files["resources/jobs.yml"])["resources"]["jobs"]
    paths = {
        t["task_key"]: t["notebook_task"]["notebook_path"]
        for job in jobs.values()
        for t in job["tasks"]
        if "notebook_task" in t
    }
    assert paths[step.step.id] == f"../src/{step.step.id}.py"
    assert "## Code réécrit à relire (phase 8)" in bundle.files["GENERATION.md"]
    assert f"`src/{step.step.id}.py`" in bundle.files["GENERATION.md"]


def test_branched_source_is_read_by_its_full_name() -> None:
    project = fixture("DEMO_COMPLEXE")
    branching = Branching(
        project=project.key,
        source_file="test",
        resolved={"DEMO_SIMPLE.customer_stats": "cat_x.sch_y.customer_stats"},
    )
    bundle = generate_job_bundle(project, branching=branching)
    [step] = [g for g in bundle.steps if NAME in g.step.recipes]
    assert 'spark.table("cat_x.sch_y.customer_stats").toPandas()' in bundle.files[step.path]


def test_python_step_stays_out_of_the_pipeline() -> None:
    bundle = generate_job_bundle(fixture("DEMO_COMPLEXE"), target="pipeline")
    [step] = [g for g in bundle.steps if NAME in g.step.recipes]
    assert step.step.id not in bundle.pipeline_steps
    assert step.path in bundle.files


def test_checks_of_a_python_output_run_after_the_write(tmp_path: Path) -> None:
    p = mf.build_demo_simple()
    checks = [{"type": "recordCountInRange", "id": "c1", "minimum": 1}]
    p.add_dataset(
        mf.dataset(
            p.key, "scored", cols=mf.columns("customer_id:bigint", "score:double"), checks=checks
        )
    )
    p.add_recipe(
        mf.recipe(p.key, NAME, "python", [mf.ref("customers")], [mf.out_ref("scored")]), SCORE
    )
    bundle = generate_job_bundle(read_files(tmp_path, p.files()))
    [step] = [g for g in bundle.steps if NAME in g.step.recipes]
    last = bundle.files[step.path].rstrip().split(PY_CELL)[-1]
    assert last.startswith('spark.sql("SELECT ASSERT_TRUE(COUNT(*) >= 1')
    assert last.endswith(").collect()")


# --------------------------------------------------------------------------- #
# Refusals: never a partial rewrite
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("code", "detail"),
    [
        (SCORE + "client = dataiku.api_client()\n", "dataiku use not rewritten"),
        (SCORE + "from dataiku import pandasutils\n", "from dataiku import"),
        ("import dataiku.spark as dkuspark\n" + SCORE, "import dataiku.spark"),
        ("import dataiku, os\n" + SCORE[15:], "import dataiku, os"),
        (SCORE.replace("get_dataframe()", "get_dataframe(limit=5)"), "with arguments"),
        (SCORE.replace('Dataset("customers")', 'Dataset("orders_raw")'), "neither an input"),
        (SCORE + 'dataiku.Dataset("customers").write_with_schema(x)\n', "not an output"),
        (SCORE + 'dataiku.Dataset("scored").write_with_schema(y)\n', "written twice"),
        (SCORE.replace('dataiku.Dataset("scored").write', "pass  #"), "never written"),
        (SCORE.replace("get_custom_variables()", "get_custom_variables(typed=True)"), "typed"),
        (SCORE.replace('"customers"', "name", 1), "single literal name"),
        (SCORE + 'print "python 2"\n', "not Python 3"),
        ("import pandas\n", "no import dataiku"),
        (SCORE + 'dataiku.Dataset("customers").get_config()\n', "not rewritten"),
        (SCORE + 'dataiku.Folder("f").list_paths_in_partition()\n', "not rewritten"),
        (
            'import dataiku\nds = dataiku.Dataset("customers")\nds = None\n' + SCORE[15:],
            "reassigned",
        ),
        ("", "empty"),
    ],
)
def test_refusals(tmp_path: Path, code: str, detail: str) -> None:
    refused = _refused(_project(tmp_path, code))
    assert detail in refused.detail


def test_partitions_are_refused(tmp_path: Path) -> None:
    assert "partitioned" in _refused(_project(tmp_path, SCORE, partitioned_output=True)).detail
    refused = _refused(_project(tmp_path, SCORE, deps=mf.SAME_DAY))
    assert "partition dependency" in refused.detail
