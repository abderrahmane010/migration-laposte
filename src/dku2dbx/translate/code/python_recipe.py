"""Dataiku Python recipes -> Databricks notebooks, by idiom rewriting (ROADMAP phase 8).

Decisions of 2026-11-24: the business code stays pandas (only the ``dataiku`` calls are
rewritten) and a fully rewritten recipe is still flagged "to review", never AUTO.

ASSUMPTION (H-57): the idioms below behave like their Databricks rewrite.

| Dataiku                                 | Databricks                                   |
|-----------------------------------------|----------------------------------------------|
| ``import dataiku``                      | removed                                      |
| ``dataiku.Dataset("x").get_dataframe()``| ``spark.table("x").toPandas()``              |
| ``….write_with_schema(df)``             | ``spark.createDataFrame(df).write`` overwrite |
|                                         | (or append) with the new schema, saveAsTable |
| ``….write_dataframe(df)``               | same, without changing the table schema      |
| ``dataiku.get_custom_variables()``      | the project variables read from the job      |
|                                         | parameters ``dku_<name>`` (text, as Dataiku) |
| ``dataiku.default_project_key()``       | the project key                              |
| ``dataiku.Folder("f").get_path()``      | ``/Volumes/<catalog>/<schema>/f``            |

The rewrite works on the syntax tree (``ast``) and splices the source text, so the
comments and the layout of the business code are kept. Anything else touching
``dataiku`` (``api_client()`` included: case by case), a dataset that is not an input
or output of the recipe, arguments of these calls, partitions, Python 2 code: the
recipe is not rewritten (UnsupportedConstruct, then the LLM residue).
"""

from __future__ import annotations

import ast
import json
from collections.abc import Mapping
from dataclasses import dataclass

from dku2dbx.errors import UnsupportedConstruct
from dku2dbx.model.ir import Project, Recipe
from dku2dbx.translate.model import CodeRewrite
from dku2dbx.translate.prepare.formula import parameter_name

READ_METHODS = frozenset({"get_dataframe"})
WRITE_METHODS = {"write_with_schema": True, "write_dataframe": False}
"""Write method -> whether it replaces the table schema."""
VARIABLES_HELPER = "_dku_custom_variables"


@dataclass
class _Edit:
    start: tuple[int, int]
    end: tuple[int, int]
    text: str


def _span(node: ast.expr | ast.stmt) -> tuple[tuple[int, int], tuple[int, int]]:
    if node.end_lineno is None or node.end_col_offset is None:  # pragma: no cover
        raise TypeError("node without end position")
    return (node.lineno, node.col_offset), (node.end_lineno, node.end_col_offset)


def _str(value: str) -> str:
    """A Python string literal in double quotes."""
    return json.dumps(value, ensure_ascii=False)


def _literal(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


class _Rewriter:
    def __init__(self, project: Project, recipe: Recipe, tables: Mapping[str, str]) -> None:
        self.project = project
        self.recipe = recipe
        self.tables = tables
        self.inputs = {i.ref: i for i in recipe.inputs}
        self.outputs = {o.ref: o for o in recipe.outputs}
        self.modules: set[str] = set()  # names bound to the dataiku module
        self.bound: dict[str, str] = {}  # variable -> dataset of dataiku.Dataset("...")
        self.edits: list[_Edit] = []
        self.handled: set[int] = set()  # id() of the nodes rewritten (with their children)
        self.result = CodeRewrite(recipe=recipe.name, type=recipe.type, code="")
        self.first_variables_use: ast.stmt | None = None
        self.source = ""
        self.parents: dict[int, ast.expr] = {}

    def fail(self, node: ast.AST | None, detail: str) -> UnsupportedConstruct:
        where = f" (line {node.lineno})" if node is not None and hasattr(node, "lineno") else ""
        return UnsupportedConstruct(self.recipe.name, "python", detail + where)

    # -- dataiku references ------------------------------------------------- #
    def _is_module(self, node: ast.expr) -> bool:
        return isinstance(node, ast.Name) and node.id in self.modules

    def _dataiku_call(self, node: ast.expr, name: str) -> ast.Call | None:
        """``dataiku.<name>(...)`` -> the call, else None."""
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == name
            and self._is_module(node.func.value)
        ):
            return node
        return None

    def _dataset(self, node: ast.expr) -> str | None:
        """The dataset a ``dataiku.Dataset("x")`` expression (or a variable of one) names."""
        if isinstance(node, ast.Name) and node.id in self.bound:
            return self.bound[node.id]
        call = self._dataiku_call(node, "Dataset")
        if call is None:
            return None
        name = _literal(call.args[0]) if len(call.args) == 1 and not call.keywords else None
        if name is None:
            raise self.fail(call, "dataiku.Dataset() without a single literal name")
        return name

    def table(self, dataset: str) -> str:
        return self.tables.get(dataset, f"`{dataset}`")

    # -- statements ---------------------------------------------------------- #
    def imports(self, module: ast.Module) -> None:
        for stmt in ast.walk(module):
            if isinstance(stmt, ast.Import):
                names = [a for a in stmt.names if a.name.split(".")[0] == "dataiku"]
                if not names:
                    continue
                if stmt not in module.body or len(stmt.names) != 1 or names[0].name != "dataiku":
                    raise self.fail(stmt, f"import {', '.join(a.name for a in stmt.names)}")
                self.modules.add(names[0].asname or "dataiku")
                self._replace(stmt, f"# dku2dbx: {ast.unparse(stmt)} (removed)")
            elif isinstance(stmt, ast.ImportFrom) and (stmt.module or "").startswith("dataiku"):
                raise self.fail(stmt, f"from {stmt.module} import …")

    def bindings(self, module: ast.Module) -> None:
        """Module-level ``x = dataiku.Dataset("name")``; ``x`` must not be rebound."""
        assigned: dict[str, int] = {}
        for node in ast.walk(module):
            targets = node.targets if isinstance(node, ast.Assign) else []
            for target in targets:
                for item in ast.walk(target):
                    if isinstance(item, ast.Name):
                        assigned[item.id] = assigned.get(item.id, 0) + 1
        for stmt in module.body:
            if not (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
                and self._dataiku_call(stmt.value, "Dataset") is not None
            ):
                continue
            name = stmt.targets[0].id
            if assigned[name] != 1:
                raise self.fail(stmt, f"variable {name} of a dataiku.Dataset is reassigned")
            dataset = self._dataset(stmt.value)
            if dataset is None:  # pragma: no cover - checked above
                continue
            self._known(dataset, stmt)
            self.bound[name] = dataset
            self._replace(stmt, f"# dku2dbx: {ast.unparse(stmt)} (rewritten where used)")

    def _known(self, dataset: str, node: ast.AST) -> None:
        if dataset not in self.inputs and dataset not in self.outputs:
            raise self.fail(node, f"dataset {dataset} is neither an input nor an output")

    # -- expressions --------------------------------------------------------- #
    def calls(self, module: ast.Module) -> None:
        for stmt in module.body:
            for node in ast.walk(stmt):
                if isinstance(node, ast.Call) and id(node) not in self.handled:
                    self._call(node, stmt)

    def _call(self, node: ast.Call, top: ast.stmt) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in READ_METHODS | set(WRITE_METHODS):
            dataset = self._dataset(func.value)
            if dataset is None:
                return
            self._known(dataset, node)
            if func.attr in READ_METHODS:
                self._read(node, dataset)
            else:
                self._write(node, dataset, WRITE_METHODS[func.attr])
            return
        if self._dataiku_call(node, "get_custom_variables") is not None:
            if node.args or node.keywords:
                raise self.fail(node, "get_custom_variables() with arguments (typed values)")
            self._mark(node)
            self.edits.append(_Edit(*_span(node), f"{VARIABLES_HELPER}()"))
            if self.first_variables_use is None:
                self.first_variables_use = top
            return
        if self._dataiku_call(node, "default_project_key") is not None and not node.args:
            self._mark(node)
            self.edits.append(_Edit(*_span(node), _str(self.project.key)))
            return
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "get_path"
            and not node.args
            and (folder := self._dataiku_call(func.value, "Folder")) is not None
        ):
            name = _literal(folder.args[0]) if len(folder.args) == 1 else None
            if name is None or folder.keywords:
                raise self.fail(folder, "dataiku.Folder() without a single literal name")
            self._mark(node)
            path = (
                "f\"/Volumes/{dbutils.widgets.get('catalog')}/"
                f"{{dbutils.widgets.get('schema')}}/{name}\""
            )
            self.edits.append(_Edit(*_span(node), path))
            self.result.warnings.append(
                f"dossier Dataiku « {name} » lu dans le Volume du même nom (à créer et remplir)"
            )

    def _read(self, node: ast.Call, dataset: str) -> None:
        if node.args or node.keywords:
            raise self.fail(node, f"get_dataframe() with arguments on {dataset}")
        if dataset not in self.inputs:
            raise self.fail(node, f"{dataset} is read but is not an input")
        if self.inputs[dataset].deps:
            raise self.fail(node, f"input {dataset} has a partition dependency")
        self._mark(node)
        self.edits.append(
            _Edit(*_span(node), f"spark.table({_str(self.table(dataset))}).toPandas()")
        )
        self.result.reads.append(dataset)

    def _write(self, node: ast.Call, dataset: str, new_schema: bool) -> None:
        if len(node.args) != 1 or node.keywords:
            raise self.fail(node, f"write to {dataset} without a single DataFrame argument")
        output = self.outputs.get(dataset)
        if output is None:
            raise self.fail(node, f"{dataset} is written but is not an output")
        if self.project.dataset(dataset).partitioning:
            raise self.fail(node, f"output {dataset} is partitioned")
        if dataset in self.result.writes:
            raise self.fail(node, f"{dataset} is written twice")
        self._mark(node)
        frame = ast.get_source_segment(self.source, node.args[0])
        mode = "append" if output.append_mode else "overwrite"
        option = '.option("overwriteSchema", "true")' if new_schema and mode == "overwrite" else ""
        text = (
            f"spark.createDataFrame({frame}).write.mode({_str(mode)}){option}"
            f".saveAsTable({_str(self.table(dataset))})"
        )
        self.edits.append(_Edit(*_span(node), text))
        self.result.writes.append(dataset)

    def _mark(self, node: ast.AST) -> None:
        self.handled.update(id(n) for n in ast.walk(node))

    def _replace(self, stmt: ast.stmt, text: str) -> None:
        self._mark(stmt)
        self.edits.append(_Edit(*_span(stmt), text))

    # -- leftovers ------------------------------------------------------------ #
    def leftovers(self, module: ast.Module) -> None:
        """Any dataiku reference not rewritten stops the rewrite (never left behind)."""
        for node in ast.walk(module):
            if id(node) in self.handled:
                continue
            if isinstance(node, ast.Name) and (node.id in self.modules or node.id in self.bound):
                parent = self.parents.get(id(node))
                what = ast.unparse(parent) if parent is not None else node.id
                raise self.fail(node, f"dataiku use not rewritten: {what[:80]}")

    def run(self, source: str) -> CodeRewrite:
        self.source = source
        try:
            module = ast.parse(source)
        except SyntaxError as exc:
            raise self.fail(None, f"not Python 3 ({exc.msg}, line {exc.lineno})") from None
        self.parents = {
            id(child): parent
            for parent in ast.walk(module)
            for child in ast.iter_child_nodes(parent)
            if isinstance(parent, ast.expr)
        }
        self.imports(module)
        if not self.modules:
            raise self.fail(None, "no import dataiku: nothing to rewrite")
        self.bindings(module)
        self.calls(module)
        self.leftovers(module)
        missing = sorted(set(self.outputs) - set(self.result.writes))
        if missing:
            raise self.fail(None, f"outputs never written: {missing}")
        if self.first_variables_use is not None:
            top = self.first_variables_use
            line = min([top.lineno, *(d.lineno for d in getattr(top, "decorator_list", []))])
            self.edits.append(_Edit((line, 0), (line, 0), self._helper()))
        code = _apply(source, self.edits)
        ast.parse(code)  # the rewrite must stay valid Python
        self.result.code = code
        if self.result.reads:
            self.result.warnings.append(
                "toPandas() charge les entrées en mémoire sur le driver (comme Dataiku en "
                "pandas) : vérifier la volumétrie"
            )
        self.result.warnings.append(
            "types des colonnes écrites déduits du DataFrame pandas (comme write_with_schema) : "
            "comparer au schéma Dataiku"
        )
        return self.result

    def _helper(self) -> str:
        names = sorted(n for n, v in self.project.variables.items() if _scalar(v))
        skipped = sorted(set(self.project.variables) - set(names))
        if skipped:
            self.result.warnings.append(f"variables non scalaires absentes : {skipped}")
        body = "".join(
            f"        {_str(n)}: dbutils.widgets.get({_str(parameter_name(n))}),\n" for n in names
        )
        return (
            f"def {VARIABLES_HELPER}():\n"
            '    """dataiku.get_custom_variables(): project variables, as text."""\n'
            f"    return {{\n{body}    }}\n\n\n"
        )


def _scalar(value: object) -> bool:
    return isinstance(value, (bool, int, float, str))


def _apply(source: str, edits: list[_Edit]) -> str:
    """Splice the edits; ast positions are (line, UTF-8 byte column)."""
    data = source.encode("utf-8")
    starts = [0, 0]
    for line in data.splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    ordered = sorted(edits, key=lambda e: (e.start, e.end), reverse=True)
    for previous, edit in zip(ordered[1:], ordered, strict=False):
        if previous.end > edit.start:  # pragma: no cover - nested edits are marked
            raise TypeError("overlapping rewrites")
    for edit in ordered:
        start = starts[edit.start[0]] + edit.start[1]
        end = starts[edit.end[0]] + edit.end[1]
        data = data[:start] + edit.text.encode("utf-8") + data[end:]
    return data.decode("utf-8")


def rewrite_python(
    project: Project, recipe: Recipe, tables: Mapping[str, str] | None = None
) -> CodeRewrite:
    """Rewrite the dataiku idioms of a Python recipe; ``tables``: branched sources (phase 7)."""
    if not recipe.payload_raw or not recipe.payload_raw.strip():
        raise UnsupportedConstruct(recipe.name, "payload", "empty Python script")
    return _Rewriter(project, recipe, tables or {}).run(recipe.payload_raw)
