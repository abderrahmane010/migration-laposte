"""Reader of Dataiku DSS project export ZIPs ("configuration only").

The ZIP is read in memory, never extracted. One ZIP holds one project.
Unknown object types are kept with ``status = UNKNOWN``; ZIP entries the reader
does not interpret are listed in ``Project.unparsed_entries``. Only an unreadable
export (not a ZIP, no manifest, invalid JSON) raises ``ExportReadError``.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from dku2dbx import __version__
from dku2dbx.errors import ExportReadError
from dku2dbx.model.flow import build_flow
from dku2dbx.model.ir import (
    Column,
    Dataset,
    ExposedObject,
    ForeignObject,
    IrDocument,
    IrMeta,
    Issue,
    IssueKind,
    ObjectKind,
    PartitionDimension,
    PluginUsage,
    PrepareStep,
    Project,
    Recipe,
    RecipeCategory,
    RecipeInput,
    RecipeOutput,
    SavedModel,
    Scenario,
    ScenarioStep,
    ScenarioTrigger,
    Status,
    UnresolvedVariable,
    Zone,
)
from dku2dbx.reader.known_types import (
    CODE_RECIPE_TYPES,
    DATASET_TYPES,
    GROUP_META_TYPE,
    PAYLOAD_OPTIONAL_RECIPE_TYPES,
    PROCESSOR_META_TYPE,
    SCENARIO_TYPES,
    plugin_id_of,
    recipe_category,
)
from dku2dbx.reader.variables import effective_variables, resolve

JsonDict = dict[str, Any]

# ASSUMPTION (H-02): the manifest is at the ZIP root (or under a single top folder).
MANIFEST = "export-manifest.json"
CONFIG_DIR = "project_config/"

# Configuration-only exports are small: refuse oversized entries (ZIP bombs).
MAX_ENTRY_BYTES = 64 * 1024 * 1024


class _Archive:
    """ZIP access with bookkeeping of the entries actually interpreted."""

    def __init__(self, path: Path) -> None:
        self.path = path
        try:
            self._zip = zipfile.ZipFile(path)
        except (zipfile.BadZipFile, OSError) as exc:
            raise ExportReadError(f"{path}: not a readable ZIP file ({exc})") from exc
        self.names = sorted(n for n in self._zip.namelist() if not n.endswith("/"))
        self.prefix = self._find_prefix()
        self.consumed: set[str] = set()

    def _find_prefix(self) -> str:
        if MANIFEST in self.names:
            return ""
        candidates = [n for n in self.names if PurePosixPath(n).name == MANIFEST]
        if len(candidates) == 1 and candidates[0].count("/") == 1:
            return candidates[0][: -len(MANIFEST)]
        raise ExportReadError(f"{self.path}: '{MANIFEST}' not found at the root of the ZIP")

    def close(self) -> None:
        self._zip.close()

    def exists(self, rel: str) -> bool:
        return self.prefix + rel in self.names

    def list_dir(self, rel_dir: str) -> list[str]:
        """Relative paths of the entries directly under ``rel_dir``."""
        full = self.prefix + rel_dir
        return [
            n[len(self.prefix) :]
            for n in self.names
            if n.startswith(full) and "/" not in n[len(full) :]
        ]

    def read_bytes(self, rel: str) -> bytes:
        full = self.prefix + rel
        info = self._zip.getinfo(full)
        if info.file_size > MAX_ENTRY_BYTES:
            raise ExportReadError(f"{self.path}: entry '{full}' is too large ({info.file_size} B)")
        self.consumed.add(full)
        return self._zip.read(full)

    def read_text(self, rel: str) -> str:
        try:
            return self.read_bytes(rel).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ExportReadError(f"{self.path}: '{rel}' is not UTF-8 text ({exc})") from exc

    def read_json(self, rel: str) -> tuple[Any, str]:
        """Parsed JSON and SHA-256 of the raw bytes."""
        raw = self.read_bytes(rel)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExportReadError(f"{self.path}: '{rel}' is not valid JSON ({exc})") from exc
        return data, hashlib.sha256(raw).hexdigest()

    def unconsumed(self) -> list[str]:
        return [n[len(self.prefix) :] for n in self.names if n not in self.consumed]


def _as_dict(value: Any) -> JsonDict:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _stem(rel: str) -> str:
    return PurePosixPath(rel).name.removesuffix(".json")


class _ProjectBuilder:
    """Turns the entries of one export into a ``Project``."""

    def __init__(self, archive: _Archive, instance_variables: Mapping[str, Any] | None) -> None:
        self.ar = archive
        self.instance_variables = instance_variables
        self.issues: list[Issue] = []
        self.unresolved: list[UnresolvedVariable] = []
        self.variables: JsonDict = {}

    # -- helpers ------------------------------------------------------------ #
    def issue(self, kind: IssueKind, obj_kind: ObjectKind, name: str, message: str) -> None:
        self.issues.append(
            Issue(kind=kind, object_kind=obj_kind, object_name=name, message=message)
        )

    def resolve_field(
        self, value: str | None, obj_kind: ObjectKind, obj_name: str, field: str
    ) -> str | None:
        if value is None:
            return None
        resolved, missing = resolve(value, self.variables)
        for var in missing:
            self.unresolved.append(
                UnresolvedVariable(
                    object_kind=obj_kind, object_name=obj_name, field=field, variable=var
                )
            )
            self.issue(
                IssueKind.UNRESOLVED_VARIABLE,
                obj_kind,
                obj_name,
                f"variable '${{{var}}}' in {field} cannot be resolved from the export",
            )
        return resolved

    # -- build -------------------------------------------------------------- #
    def build(self) -> Project:
        manifest, _ = self.ar.read_json(MANIFEST)
        manifest = _as_dict(manifest)
        params: JsonDict = {}
        if self.ar.exists(CONFIG_DIR + "params.json"):
            params = _as_dict(self.ar.read_json(CONFIG_DIR + "params.json")[0])
        # ASSUMPTION (H-02): a DSS 15 export names the project originalProjectKey in its
        # manifest (params.json has no projectKey), seen on a Dataiku Cloud export.
        key = (
            _opt_str(params.get("projectKey"))
            or _opt_str(manifest.get("projectKey"))
            or _opt_str(manifest.get("originalProjectKey"))
            or self.ar.path.stem
        )
        self.variables = effective_variables(key, self._variables_json(), self.instance_variables)

        # ASSUMPTION (H-02): a plugin is {pluginId, version} in a DSS 15 export.
        declared_plugins = {
            str(p.get("pluginId")): _opt_str(p.get("pluginVersion")) or _opt_str(p.get("version"))
            for p in map(_as_dict, _as_list(manifest.get("exportedWithPlugins")))
            if p.get("pluginId")
        }

        zones = [self._zone(rel) for rel in self._json_entries("zones/")]
        datasets = [self._dataset(rel) for rel in self._json_entries("datasets/")]
        recipes = [
            self._recipe(rel, list(declared_plugins)) for rel in self._json_entries("recipes/")
        ]
        scenarios = [self._scenario(rel) for rel in self._json_entries("scenarios/")]
        saved_models = [self._saved_model(rel) for rel in self._json_entries("saved_models/")]

        project = Project(
            key=key,
            name=_opt_str(params.get("name")),
            dss_version=_opt_str(manifest.get("dssVersion"))
            or _opt_str(manifest.get("generatedWithDSSVersion")),
            variables={k: v for k, v in self.variables.items() if k != "projectKey"},
            zones=sorted(zones, key=lambda z: z.id),
            datasets=sorted(datasets, key=lambda d: d.name),
            recipes=sorted(recipes, key=lambda r: r.name),
            scenarios=sorted(scenarios, key=lambda s: s.id),
            saved_models=sorted(saved_models, key=lambda m: m.id),
            plugins=self._plugins(declared_plugins, recipes),
            exposed_objects=self._exposed(params),
            foreign_objects=self._foreign(manifest),
        )
        self._check_zones(project)

        flow, flow_issues = build_flow(project)
        project.flow = flow
        self.issues.extend(flow_issues)

        project.unparsed_entries = self.ar.unconsumed()
        if project.unparsed_entries:
            self.issue(
                IssueKind.UNPARSED_ENTRY,
                ObjectKind.PROJECT,
                key,
                f"{len(project.unparsed_entries)} ZIP entries not interpreted "
                "(listed in unparsed_entries)",
            )
        project.unresolved_variables = self.unresolved
        project.issues = sorted(
            self.issues,
            key=lambda i: (i.kind, i.object_kind, i.object_name, i.message),
        )
        return project

    def _variables_json(self) -> JsonDict | None:
        """Project variables as {"standard": {...}, "local": {...}}."""
        # ASSUMPTION (H-13): either variables.json holds both blocks, or (DSS 15 export,
        # seen on Dataiku Cloud) variables.json holds the standard variables and
        # localvariables.json the local ones.
        if not self.ar.exists(CONFIG_DIR + "variables.json"):
            return None
        data = _as_dict(self.ar.read_json(CONFIG_DIR + "variables.json")[0])
        if {"standard", "local"} & set(data):
            return data
        local: JsonDict = {}
        if self.ar.exists(CONFIG_DIR + "localvariables.json"):
            local = _as_dict(self.ar.read_json(CONFIG_DIR + "localvariables.json")[0])
        return {"standard": data, "local": local}

    def _json_entries(self, sub_dir: str) -> list[str]:
        return [rel for rel in self.ar.list_dir(CONFIG_DIR + sub_dir) if rel.endswith(".json")]

    # -- objects ------------------------------------------------------------ #
    def _zone(self, rel: str) -> Zone:
        # ASSUMPTION (H-12): zones in project_config/zones/<id>.json.
        data = _as_dict(self.ar.read_json(rel)[0])
        return Zone(
            id=_opt_str(data.get("id")) or _stem(rel),
            name=_opt_str(data.get("name")),
            color=_opt_str(data.get("color")),
        )

    def _dataset(self, rel: str) -> Dataset:
        data, sha = self.ar.read_json(rel)
        data = _as_dict(data)
        name = _opt_str(data.get("name")) or _stem(rel)
        ds_type = str(data.get("type", ""))
        status = Status.KNOWN if ds_type in DATASET_TYPES else Status.UNKNOWN
        if status is Status.UNKNOWN:
            self.issue(
                IssueKind.UNKNOWN_DATASET_TYPE,
                ObjectKind.DATASET,
                name,
                f"dataset type '{ds_type}' is unknown; definition kept as-is",
            )
        params = _as_dict(data.get("params"))
        table = _opt_str(params.get("table"))
        db_schema = _opt_str(params.get("schema"))
        partitioning = _as_dict(data.get("partitioning"))
        return Dataset(
            name=name,
            type=ds_type,
            status=status,
            managed=bool(data.get("managed", False)),
            connection=_opt_str(params.get("connection")),
            catalog=_opt_str(params.get("catalog")) or None,
            db_schema=db_schema,
            db_schema_resolved=self.resolve_field(
                db_schema, ObjectKind.DATASET, name, "params.schema"
            ),
            table=table,
            table_resolved=self.resolve_field(table, ObjectKind.DATASET, name, "params.table"),
            mode=_opt_str(params.get("mode")),
            partitioning=[
                PartitionDimension(
                    name=str(d.get("name", "")),
                    type=str(d.get("type", "")),
                    params=_as_dict(d.get("params")),
                )
                for d in map(_as_dict, _as_list(partitioning.get("dimensions")))
            ],
            columns=[
                Column(name=str(c.get("name", "")), type=_opt_str(c.get("type")))
                for c in map(_as_dict, _as_list(_as_dict(data.get("schema")).get("columns")))
            ],
            checks=[_as_dict(c) for c in _as_list(data.get("checks"))],
            zone=_opt_str(data.get("zone")),
            params=params,
            # ASSUMPTION (H-63): a file dataset has top-level formatType and formatParams
            # (seen on a Dataiku Cloud export: csv with separator, quoteChar, escapeChar,
            # style, charset, parseHeaderRow, dateSerializationFormat).
            format_type=_opt_str(data.get("formatType")),
            format_params=_as_dict(data.get("formatParams")),
            source_path=rel,
            source_sha256=sha,
            raw=data if status is Status.UNKNOWN else None,
        )

    def _recipe(self, rel: str, declared_plugins: list[str]) -> Recipe:
        data, sha = self.ar.read_json(rel)
        data = _as_dict(data)
        name = _opt_str(data.get("name")) or _stem(rel)
        rtype = str(data.get("type", ""))
        category = recipe_category(rtype)
        status = Status.UNKNOWN if category is RecipeCategory.UNKNOWN else Status.KNOWN
        if status is Status.UNKNOWN:
            self.issue(
                IssueKind.UNKNOWN_RECIPE_TYPE,
                ObjectKind.RECIPE,
                name,
                f"recipe type '{rtype}' is unknown; definition and payload kept as-is",
            )
        params = _as_dict(data.get("params"))
        # ASSUMPTION (H-22): selected engine in params.engineParams.engineType.
        engine = _opt_str(_as_dict(params.get("engineParams")).get("engineType"))

        payload_path, payload, payload_raw = self._recipe_payload(rel, name, rtype, category)
        steps = None
        if rtype == "shaker" and isinstance(payload, dict):
            steps = [self._step(s, name) for s in _as_list(payload.get("steps"))]

        return Recipe(
            name=name,
            type=rtype,
            status=status,
            category=category,
            engine=engine,
            inputs=self._inputs(data),
            outputs=self._outputs(data),
            never_recompute_existing_partitions=bool(
                data.get("neverRecomputeExistingPartitions", False)
            ),
            zone=_opt_str(data.get("zone")),
            plugin_id=plugin_id_of(rtype, declared_plugins),
            params=params,
            payload_path=payload_path,
            payload=payload,
            payload_raw=payload_raw,
            prepare_steps=steps,
            source_path=rel,
            source_sha256=sha,
            raw=data if status is Status.UNKNOWN else None,
        )

    @staticmethod
    def _roles(block: Any) -> list[tuple[str, JsonDict]]:
        # ASSUMPTION (H-06): inputs/outputs are {"<role>": {"items": [...]}}, role "main"
        # being the usual one. Role order of the file is kept.
        return [
            (str(role), item)
            for role, content in _as_dict(block).items()
            for item in map(_as_dict, _as_list(_as_dict(content).get("items")))
        ]

    def _inputs(self, data: JsonDict) -> list[RecipeInput]:
        inputs = []
        for role, item in self._roles(data.get("inputs")):
            ref = str(item.get("ref", ""))
            # ASSUMPTION (H-11): a shared dataset is referenced as "PROJECT.dataset".
            project_key, _, local_name = ref.rpartition(".")
            inputs.append(
                RecipeInput(
                    role=role,
                    ref=ref,
                    project_key=project_key or None,
                    name=local_name,
                    deps=[_as_dict(d) for d in _as_list(item.get("deps"))],
                )
            )
        return inputs

    def _outputs(self, data: JsonDict) -> list[RecipeOutput]:
        return [
            RecipeOutput(
                role=role,
                ref=str(item.get("ref", "")),
                append_mode=bool(item.get("appendMode", False)),
            )
            for role, item in self._roles(data.get("outputs"))
        ]

    def _recipe_payload(
        self, rel: str, name: str, rtype: str, category: RecipeCategory
    ) -> tuple[str | None, Any, str | None]:
        """(payload path, parsed JSON payload, raw text payload)."""
        # ASSUMPTION (H-09, H-10, H-23): payload is "recipes/<name>.<ext>", next to the
        # definition; recipe names contain no '.'.
        recipe_dir = str(PurePosixPath(rel).parent) + "/"
        candidates = [
            p
            for p in self.ar.list_dir(recipe_dir)
            if p != rel and PurePosixPath(p).name.startswith(name + ".")
        ]
        if not candidates:
            if (
                category in (RecipeCategory.VISUAL, RecipeCategory.CODE)
                and rtype not in PAYLOAD_OPTIONAL_RECIPE_TYPES
            ):
                self.issue(
                    IssueKind.MISSING_PAYLOAD,
                    ObjectKind.RECIPE,
                    name,
                    "no payload file found next to the recipe definition",
                )
            return None, None, None
        if len(candidates) > 1:
            preferred = [p for p in candidates if p.endswith("." + rtype)]
            self.issue(
                IssueKind.MULTIPLE_PAYLOADS,
                ObjectKind.RECIPE,
                name,
                f"several payload candidates: {', '.join(candidates)}",
            )
            if len(preferred) != 1:
                return None, None, None
            candidates = preferred
        path = candidates[0]
        text = self.ar.read_text(path)
        if not text.strip() and rtype in PAYLOAD_OPTIONAL_RECIPE_TYPES:
            return path, None, None
        if rtype in CODE_RECIPE_TYPES:
            return path, None, text
        try:
            return path, json.loads(text), None
        except json.JSONDecodeError:
            if category is RecipeCategory.VISUAL:
                self.issue(
                    IssueKind.INVALID_PAYLOAD,
                    ObjectKind.RECIPE,
                    name,
                    f"payload '{path}' of a visual recipe is not valid JSON; kept as text",
                )
            return path, None, text

    def _step(self, raw: Any, recipe_name: str) -> PrepareStep:
        data = _as_dict(raw)
        meta_type = str(data.get("metaType", ""))
        if meta_type not in (PROCESSOR_META_TYPE, GROUP_META_TYPE):
            self.issue(
                IssueKind.UNKNOWN_STEP_META_TYPE,
                ObjectKind.RECIPE,
                recipe_name,
                f"Prepare step with unknown metaType '{meta_type}' kept as-is",
            )
        return PrepareStep(
            meta_type=meta_type,
            type=_opt_str(data.get("type")),
            name=_opt_str(data.get("name")),
            params=_as_dict(data.get("params")),
            disabled=bool(data.get("disabled", False)),
            steps=[self._step(s, recipe_name) for s in _as_list(data.get("steps"))],
        )

    def _scenario(self, rel: str) -> Scenario:
        # ASSUMPTION (H-15): scenarios in project_config/scenarios/<id>.json,
        # steps in params.steps.
        data, sha = self.ar.read_json(rel)
        data = _as_dict(data)
        sid = _opt_str(data.get("id")) or _stem(rel)
        stype = str(data.get("type", ""))
        status = Status.KNOWN if stype in SCENARIO_TYPES else Status.UNKNOWN
        if status is Status.UNKNOWN:
            self.issue(
                IssueKind.UNKNOWN_SCENARIO_TYPE,
                ObjectKind.SCENARIO,
                sid,
                f"scenario type '{stype}' is unknown; definition kept as-is",
            )
        return Scenario(
            id=sid,
            name=_opt_str(data.get("name")),
            type=stype,
            status=status,
            active=bool(data.get("active", False)),
            triggers=[
                ScenarioTrigger(
                    id=_opt_str(t.get("id")),
                    type=str(t.get("type", "")),
                    active=bool(t.get("active", True)),
                    params=_as_dict(t.get("params")),
                )
                for t in map(_as_dict, _as_list(data.get("triggers")))
            ],
            steps=[
                ScenarioStep(
                    id=_opt_str(s.get("id")),
                    type=str(s.get("type", "")),
                    name=_opt_str(s.get("name")),
                    params=_as_dict(s.get("params")),
                )
                for s in map(_as_dict, _as_list(_as_dict(data.get("params")).get("steps")))
            ],
            source_path=rel,
            source_sha256=sha,
            raw=data if status is Status.UNKNOWN else None,
        )

    def _saved_model(self, rel: str) -> SavedModel:
        # ASSUMPTION (H-26): saved models in project_config/saved_models/<id>.json, with
        # savedModelType, miniTask.{taskType, predictionType} and an optional zone key.
        data, sha = self.ar.read_json(rel)
        data = _as_dict(data)
        mini_task = _as_dict(data.get("miniTask"))
        return SavedModel(
            id=_opt_str(data.get("id")) or _stem(rel),
            name=_opt_str(data.get("name")),
            saved_model_type=_opt_str(data.get("savedModelType")),
            task_type=_opt_str(mini_task.get("taskType")),
            prediction_type=_opt_str(mini_task.get("predictionType")),
            zone=_opt_str(data.get("zone")),
            source_path=rel,
            source_sha256=sha,
        )

    def _plugins(self, declared: dict[str, str | None], recipes: list[Recipe]) -> list[PluginUsage]:
        usages = {pid: PluginUsage(plugin_id=pid, version=v) for pid, v in declared.items()}
        for rcp in sorted(recipes, key=lambda r: r.name):
            if rcp.plugin_id is None:
                continue
            if rcp.plugin_id not in usages:
                self.issue(
                    IssueKind.MISSING_REFERENCE,
                    ObjectKind.RECIPE,
                    rcp.name,
                    f"plugin '{rcp.plugin_id}' is not declared in exportedWithPlugins",
                )
                usages[rcp.plugin_id] = PluginUsage(plugin_id=rcp.plugin_id)
            usages[rcp.plugin_id].used_by_recipes.append(rcp.name)
        return [usages[k] for k in sorted(usages)]

    @staticmethod
    def _exposed(params: JsonDict) -> list[ExposedObject]:
        # ASSUMPTION (H-11): exposedObjects.objects[] {type, localName, rules[].targetProject}.
        objects = _as_list(_as_dict(params.get("exposedObjects")).get("objects"))
        return [
            ExposedObject(
                type=str(o.get("type", "")),
                local_name=str(o.get("localName", "")),
                target_projects=[
                    str(r["targetProject"])
                    for r in map(_as_dict, _as_list(o.get("rules")))
                    if r.get("targetProject")
                ],
            )
            for o in map(_as_dict, objects)
        ]

    @staticmethod
    def _foreign(manifest: JsonDict) -> list[ForeignObject]:
        return [
            ForeignObject(
                project_key=str(o.get("projectKey", "")),
                type=str(o.get("type", "")),
                id=str(o.get("id", "")),
            )
            for o in map(_as_dict, _as_list(manifest.get("foreignObjects")))
        ]

    def _check_zones(self, project: Project) -> None:
        known = {z.id for z in project.zones}
        objects: list[tuple[ObjectKind, str, str | None]] = [
            (ObjectKind.DATASET, d.name, d.zone) for d in project.datasets
        ]
        objects += [(ObjectKind.RECIPE, r.name, r.zone) for r in project.recipes]
        objects += [(ObjectKind.SAVED_MODEL, m.id, m.zone) for m in project.saved_models]
        for kind, name, zone in objects:
            if zone is not None and zone not in known:
                self.issue(IssueKind.MISSING_REFERENCE, kind, name, f"zone '{zone}' is not defined")


class ZipExportReader:
    """``ProjectReader`` over one DSS project export ZIP."""

    def __init__(self, path: Path, instance_variables: Mapping[str, Any] | None = None) -> None:
        self.path = Path(path)
        self.instance_variables = dict(instance_variables or {})

    def list_projects(self) -> list[str]:
        return [self.read().key]

    def read_project(self, project_key: str) -> Project:
        project = self.read()
        if project.key != project_key:
            raise KeyError(project_key)
        return project

    def read(self) -> Project:
        """Read the (single) project of the export."""
        archive = _Archive(self.path)
        try:
            return _ProjectBuilder(archive, self.instance_variables).build()
        finally:
            archive.close()

    def read_document(self) -> IrDocument:
        """Project IR wrapped with its traceability header."""
        return IrDocument(
            meta=IrMeta(
                generator_version=__version__,
                source_file=self.path.name,
                source_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest(),
            ),
            project=self.read(),
        )


def read_export(path: Path, instance_variables: Mapping[str, Any] | None = None) -> Project:
    """Read a DSS project export ZIP into the IR."""
    return ZipExportReader(path, instance_variables).read()
