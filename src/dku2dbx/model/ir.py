"""Internal representation (IR) of a Dataiku project, independent of the DSS export format.

The IR keeps everything the reader found, including what it does not understand:
unknown object types are stored with ``status = UNKNOWN`` and their raw JSON, and
are listed in ``Project.issues``. Nothing is dropped silently.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

JsonDict = dict[str, Any]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


class Status(StrEnum):
    KNOWN = "KNOWN"
    UNKNOWN = "UNKNOWN"


class RecipeCategory(StrEnum):
    VISUAL = "VISUAL"
    CODE = "CODE"
    PLUGIN = "PLUGIN"
    ML = "ML"
    UNKNOWN = "UNKNOWN"


class IssueKind(StrEnum):
    UNKNOWN_DATASET_TYPE = "UNKNOWN_DATASET_TYPE"
    UNKNOWN_RECIPE_TYPE = "UNKNOWN_RECIPE_TYPE"
    UNKNOWN_SCENARIO_TYPE = "UNKNOWN_SCENARIO_TYPE"
    UNKNOWN_STEP_META_TYPE = "UNKNOWN_STEP_META_TYPE"
    UNRESOLVED_VARIABLE = "UNRESOLVED_VARIABLE"
    MISSING_REFERENCE = "MISSING_REFERENCE"
    MISSING_PAYLOAD = "MISSING_PAYLOAD"
    MULTIPLE_PAYLOADS = "MULTIPLE_PAYLOADS"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    UNPARSED_ENTRY = "UNPARSED_ENTRY"
    FOREIGN_OBJECTS_INCOMPLETE = "FOREIGN_OBJECTS_INCOMPLETE"
    FLOW_CYCLE = "FLOW_CYCLE"


class ObjectKind(StrEnum):
    PROJECT = "PROJECT"
    DATASET = "DATASET"
    RECIPE = "RECIPE"
    SCENARIO = "SCENARIO"
    SAVED_MODEL = "SAVED_MODEL"
    ZONE = "ZONE"
    FILE = "FILE"


class Issue(_Model):
    """Something the reader could not fully interpret. Always reported, never fatal."""

    kind: IssueKind
    object_kind: ObjectKind
    object_name: str
    message: str


class UnresolvedVariable(_Model):
    object_kind: ObjectKind
    object_name: str
    field: str
    variable: str


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #
class Column(_Model):
    name: str
    type: str | None = None


class PartitionDimension(_Model):
    name: str
    type: str
    params: JsonDict = Field(default_factory=dict)


class Dataset(_Model):
    name: str
    type: str
    status: Status
    managed: bool
    connection: str | None = None
    catalog: str | None = None
    db_schema: str | None = None
    """``params.schema`` in DSS (renamed: ``schema`` is reserved by pydantic)."""
    table: str | None = None
    """Table name as written in the export, variables included."""
    table_resolved: str | None = None
    """Table name after variable substitution; may still contain ``${...}``."""
    db_schema_resolved: str | None = None
    mode: str | None = None
    partitioning: list[PartitionDimension] = Field(default_factory=list)
    columns: list[Column] = Field(default_factory=list)
    checks: list[JsonDict] = Field(default_factory=list)
    zone: str | None = None
    params: JsonDict = Field(default_factory=dict)
    """Full raw ``params`` block, for fields not modelled above."""
    format_type: str | None = None
    """``formatType`` of a file dataset (csv, parquet…), None for a SQL table."""
    format_params: JsonDict = Field(default_factory=dict)
    source_path: str
    source_sha256: str
    raw: JsonDict | None = None
    """Full raw definition, kept only when ``status`` is UNKNOWN."""

    @property
    def is_partitioned(self) -> bool:
        return bool(self.partitioning)


# --------------------------------------------------------------------------- #
# Recipes
# --------------------------------------------------------------------------- #
class RecipeInput(_Model):
    role: str = "main"
    ref: str
    """Reference as written: ``name`` or ``PROJECT.name`` for a shared dataset."""
    project_key: str | None = None
    """Set when the input comes from another project."""
    name: str
    deps: list[JsonDict] = Field(default_factory=list)


class RecipeOutput(_Model):
    role: str = "main"
    ref: str
    append_mode: bool = False


class PrepareStep(_Model):
    meta_type: str
    type: str | None = None
    name: str | None = None
    params: JsonDict = Field(default_factory=dict)
    disabled: bool = False
    steps: list[PrepareStep] = Field(default_factory=list)
    """Nested steps, for grouped steps (``meta_type != "PROCESSOR"``)."""


class Recipe(_Model):
    name: str
    type: str
    status: Status
    category: RecipeCategory
    engine: str | None = None
    inputs: list[RecipeInput] = Field(default_factory=list)
    outputs: list[RecipeOutput] = Field(default_factory=list)
    never_recompute_existing_partitions: bool = False
    zone: str | None = None
    plugin_id: str | None = None
    params: JsonDict = Field(default_factory=dict)
    payload_path: str | None = None
    payload: Any = None
    """Parsed payload, when the payload file is JSON (visual and plugin recipes)."""
    payload_raw: str | None = None
    """Payload file content as text, when it is not JSON (the script of a code recipe)."""
    prepare_steps: list[PrepareStep] | None = None
    """Parsed steps, for Prepare (``shaker``) recipes only."""
    source_path: str
    source_sha256: str
    raw: JsonDict | None = None
    """Full raw definition, kept only when ``status`` is UNKNOWN."""


# --------------------------------------------------------------------------- #
# Scenarios, zones, sharing, plugins
# --------------------------------------------------------------------------- #
class ScenarioTrigger(_Model):
    id: str | None = None
    type: str
    active: bool = True
    params: JsonDict = Field(default_factory=dict)


class ScenarioStep(_Model):
    id: str | None = None
    type: str
    name: str | None = None
    params: JsonDict = Field(default_factory=dict)


class Scenario(_Model):
    id: str
    name: str | None = None
    type: str
    status: Status
    active: bool = False
    triggers: list[ScenarioTrigger] = Field(default_factory=list)
    steps: list[ScenarioStep] = Field(default_factory=list)
    source_path: str
    source_sha256: str
    raw: JsonDict | None = None


class SavedModel(_Model):
    """A saved model of the Flow (output of a training recipe, input of a scoring one).

    Recipes reference it by ``id``. Only its definition is kept, never the model itself.
    """

    id: str
    name: str | None = None
    saved_model_type: str | None = None
    task_type: str | None = None
    prediction_type: str | None = None
    zone: str | None = None
    source_path: str
    source_sha256: str


class Zone(_Model):
    id: str
    name: str | None = None
    color: str | None = None


class ExposedObject(_Model):
    type: str
    local_name: str
    target_projects: list[str] = Field(default_factory=list)


class ForeignObject(_Model):
    project_key: str
    type: str
    id: str


class PluginUsage(_Model):
    plugin_id: str
    version: str | None = None
    used_by_recipes: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Flow graph (serialisable form; see model.flow for the networkx view)
# --------------------------------------------------------------------------- #
class FlowNodeKind(StrEnum):
    DATASET = "DATASET"
    FOREIGN_DATASET = "FOREIGN_DATASET"
    SAVED_MODEL = "SAVED_MODEL"
    MISSING = "MISSING"
    RECIPE = "RECIPE"


class FlowNode(_Model):
    id: str
    """``dataset:<name>``, ``saved_model:<id>`` or ``recipe:<name>``."""
    kind: FlowNodeKind
    name: str


class FlowEdge(_Model):
    source: str
    target: str


class FlowGraph(_Model):
    nodes: list[FlowNode] = Field(default_factory=list)
    edges: list[FlowEdge] = Field(default_factory=list)
    topological_order: list[str] = Field(default_factory=list)
    """Empty when the flow has a cycle (reported as an issue)."""


# --------------------------------------------------------------------------- #
# Project
# --------------------------------------------------------------------------- #
class Project(_Model):
    key: str
    name: str | None = None
    dss_version: str | None = None
    variables: JsonDict = Field(default_factory=dict)
    """Effective project variables (``local`` overrides ``standard``)."""
    zones: list[Zone] = Field(default_factory=list)
    datasets: list[Dataset] = Field(default_factory=list)
    recipes: list[Recipe] = Field(default_factory=list)
    scenarios: list[Scenario] = Field(default_factory=list)
    saved_models: list[SavedModel] = Field(default_factory=list)
    plugins: list[PluginUsage] = Field(default_factory=list)
    exposed_objects: list[ExposedObject] = Field(default_factory=list)
    foreign_objects: list[ForeignObject] = Field(default_factory=list)
    flow: FlowGraph = Field(default_factory=FlowGraph)
    unresolved_variables: list[UnresolvedVariable] = Field(default_factory=list)
    unparsed_entries: list[str] = Field(default_factory=list)
    """ZIP entries the reader does not interpret (yet): kept for the report."""
    issues: list[Issue] = Field(default_factory=list)

    def dataset(self, name: str) -> Dataset:
        for ds in self.datasets:
            if ds.name == name:
                return ds
        raise KeyError(name)

    def recipe(self, name: str) -> Recipe:
        for rcp in self.recipes:
            if rcp.name == name:
                return rcp
        raise KeyError(name)


class IrMeta(_Model):
    """Traceability header of a serialised IR (CLAUDE.md, rule 3)."""

    generator: str = "dku2dbx"
    generator_version: str
    source_file: str
    source_sha256: str


class IrDocument(_Model):
    meta: IrMeta
    project: Project

    def to_json(self) -> str:
        """Canonical, reproducible JSON form (same input -> same bytes)."""
        return self.model_dump_json(indent=2) + "\n"
