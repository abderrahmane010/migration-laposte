"""Reader over the Dataiku public REST API (ROADMAP phase 12).

The API reader rebuilds, in memory, the entries of a DSS 15 project export
(``export-manifest.json``, ``project_config/…``) from read-only API calls, then hands
them to the builder of the ZIP reader: a project read from the API and from its export
goes through the same code and gives the same internal representation (decision of
2026-11-25). Only the provenance differs: the SHA-256 of an object is computed over the
bytes it was read from, and an export writes its JSON in its own layout.

Security (CLAUDE.md, rule 4): the URL and the API key come from environment variables
(``DATAIKU_URL``, ``DATAIKU_API_KEY``), never from code or arguments; the key is never
printed. TLS is always verified (``DATAIKU_CA_BUNDLE`` for an internal certificate
authority); no proxy unless asked. The client only sends GET requests: it never
changes anything on the instance.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dku2dbx import __version__
from dku2dbx.errors import ExportReadError
from dku2dbx.model.ir import IrDocument, IrMeta, Issue, IssueKind, ObjectKind, Project
from dku2dbx.reader.known_types import CODE_RECIPE_TYPES
from dku2dbx.reader.zip_reader import CONFIG_DIR, MANIFEST, build_project

API_URL_ENV = "DATAIKU_URL"
API_KEY_ENV = "DATAIKU_API_KEY"  # name of an environment variable, not a secret
CA_BUNDLE_ENV = "DATAIKU_CA_BUNDLE"
TIMEOUT_S = 60

Transport = Callable[[str], bytes]
"""GET of a path under ``/public/api`` -> response body; raises ``ApiError``."""

# ASSUMPTION (H-70): payload file of a recipe in an export: "<name>.<type>" for a visual
# or plugin recipe (seen: .shaker, .sampling, .sync, .CustomCode_<plugin>-<recipe>), a
# language extension for code (seen: pyspark -> .py); written even when empty (seen:
# sync and plugin recipes of a real DSS 15 export).
CODE_EXTENSIONS = {
    "python": "py",
    "pyspark": "py",
    "sql_query": "sql",
    "sql_script": "sql",
    "spark_sql_query": "sql",
    "hive": "sql",
    "impala": "sql",
    "spark_scala": "scala",
    "sparkr": "R",
    "r": "R",
    "shell": "sh",
}

# ASSUMPTION (H-71): keys the API adds and an export leaves out, seen on Dataiku Cloud
# (DSS 15.2.2); removed so that both sources give the same objects. A nested key is
# only removed when it has the default value seen on the export. The default settings
# both sources write irregularly (containerSelection, selection.ascending) are removed
# by the common builder for both (zip_reader._without_default_settings).
API_ONLY_KEYS = {
    "datasets": ("name", "projectKey", "smartName", "versionTag"),
    "recipes": ("name", "projectKey", "versionTag"),
}
API_DEFAULTS: dict[str, tuple[tuple[tuple[str, ...], Any], ...]] = {
    "recipes": ((("hashPropagationBehavior",), "INHERIT"),),
}


class ApiError(ExportReadError):
    """The Dataiku API cannot be read (configuration, network, rights, answer)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status
        """HTTP status of the answer, when the instance answered."""


def http_transport(
    url: str, api_key: str, *, ca_bundle: str | None = None, use_env_proxy: bool = False
) -> Transport:
    """GET over HTTPS, the API key as basic-auth user (DSS convention)."""
    if not url.startswith("https://"):
        raise ApiError(f"{API_URL_ENV} must start with https:// (TLS is always verified)")
    if ca_bundle is not None and not Path(ca_bundle).is_file():
        raise ApiError(f"{CA_BUNDLE_ENV}: certificate file not found ({ca_bundle})")
    context = ssl.create_default_context(cafile=ca_bundle)
    handlers: list[Any] = [urllib.request.HTTPSHandler(context=context)]
    if not use_env_proxy:
        handlers.append(urllib.request.ProxyHandler({}))
    opener = urllib.request.build_opener(*handlers)
    auth = "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("ascii")
    base = url.rstrip("/") + "/public/api"

    def get(path: str) -> bytes:
        request = urllib.request.Request(
            base + path, headers={"Authorization": auth, "Accept": "application/json"}
        )
        try:
            with opener.open(request, timeout=TIMEOUT_S) as response:
                body: bytes = response.read()
                return body
        except urllib.error.HTTPError as exc:
            hint = {401: " (clé refusée)", 403: " (droits insuffisants)", 404: " (introuvable)"}
            raise ApiError(
                f"GET {path}: HTTP {exc.code}{hint.get(exc.code, '')}", status=exc.code
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ApiError(f"GET {path}: instance Dataiku injoignable ({exc})") from None

    return get


def transport_from_environment(environ: Mapping[str, str] | None = None) -> Transport:
    env = os.environ if environ is None else environ
    url, key = env.get(API_URL_ENV), env.get(API_KEY_ENV)
    if not url or not key:
        raise ApiError(
            f"{API_URL_ENV} et {API_KEY_ENV} doivent être définies (par exemple : "
            "set -a; . ~/.dataiku_cloud.env; set +a)"
        )
    return http_transport(url, key, ca_bundle=env.get(CA_BUNDLE_ENV) or None)


def _quote(name: str) -> str:
    return urllib.parse.quote(name, safe="")


class DataikuApi:
    """Read-only access to the objects of a project."""

    def __init__(self, transport: Transport) -> None:
        self._get = transport

    def json(self, path: str) -> Any:
        body = self._get(path)
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(f"GET {path}: answer is not JSON ({exc})") from exc

    def project_keys(self) -> list[str]:
        return sorted(str(p["projectKey"]) for p in self.json("/projects/"))

    def project(self, key: str) -> str:
        return "/projects/" + _quote(key)


def _drop(obj: dict[str, Any], kind: str) -> dict[str, Any]:
    """The object as an export writes it (H-71)."""
    out = {k: copy.deepcopy(v) for k, v in obj.items() if k not in API_ONLY_KEYS.get(kind, ())}
    for path, default in API_DEFAULTS.get(kind, ()):
        parent: Any = out
        for part in path[:-1]:
            parent = parent.get(part) if isinstance(parent, dict) else None
        if isinstance(parent, dict) and parent.get(path[-1]) == default:
            del parent[path[-1]]
    return out


def _dump(obj: Any) -> bytes:
    return json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")


def _payload_name(name: str, rtype: str) -> str:
    if rtype in CODE_RECIPE_TYPES or rtype in CODE_EXTENSIONS:
        return f"{name}.{CODE_EXTENSIONS.get(rtype, rtype)}"
    return f"{name}.{rtype}"


UNREADABLE_STATUSES = (403, 404)
"""Answers of a project the key may not read: its shares are then unknown."""


def _shared_with(
    api: DataikuApi, key: str, projects: list[str]
) -> tuple[set[tuple[str, str, str]], int]:
    """Objects other projects share with ``key`` on its Flow, and the unreadable projects.

    ASSUMPTION (H-72): the foreignObjects of an export are the objects other projects
    expose to it with ``appearOnFlow`` (seen on a real DSS 15 instance, 2026-11-26: a
    shared dataset read by no recipe is listed), read in the exposedObjects of their
    settings: {objects: [{type, localName, rules: [{targetProject, appearOnFlow}]}]}.
    """
    shared: set[tuple[str, str, str]] = set()
    unreadable = 0
    for other in projects:
        if other == key:
            continue
        try:
            settings = api.json(api.project(other) + "/settings")
        except ApiError as exc:
            if exc.status not in UNREADABLE_STATUSES:
                raise
            unreadable += 1
            continue
        for obj in (settings.get("exposedObjects") or {}).get("objects") or []:
            if any(
                rule.get("targetProject") == key and rule.get("appearOnFlow") is True
                for rule in obj.get("rules") or []
            ):
                shared.add((other, str(obj.get("type", "")), str(obj.get("localName", ""))))
    return shared, unreadable


def _foreign_objects(
    recipes: list[dict[str, Any]], shared: set[tuple[str, str, str]]
) -> list[dict[str, str]]:
    """Shared objects on the Flow (H-72), plus the datasets of other projects the
    recipes read ("PROJECT.dataset"), in case a share is not visible."""
    refs = {
        str(item.get("ref", ""))
        for rcp in recipes
        for role in (rcp.get("inputs") or {}).values()
        for item in (role or {}).get("items", [])
        if "." in str(item.get("ref", ""))
    }
    objects = shared | {(r.rpartition(".")[0], "DATASET", r.rpartition(".")[2]) for r in refs}
    return [{"projectKey": p, "type": k, "id": i} for p, k, i in sorted(objects)]


def _installed_plugins(api: DataikuApi) -> list[dict[str, Any]] | None:
    """The plugins of the instance as an export lists them, or None if the key may not
    list them.

    ASSUMPTION (H-74): listing the plugins needs administrator rights (HTTP 403 seen on
    a real production instance with a non-administrator key, 2026-11-27). The list is
    then unknown: it is reported, never guessed.
    """
    try:
        plugins = api.json("/plugins/")
    except ApiError as exc:
        if exc.status not in UNREADABLE_STATUSES:
            raise
        return None
    return [{"pluginId": p["id"], "version": p.get("version")} for p in plugins]


@dataclass(frozen=True)
class ReadGaps:
    """What the key could not read: a comparison with an export is then not conclusive."""

    unreadable_projects: int = 0
    """Other projects whose settings are unreadable: shared objects may be missing."""
    plugins_unreadable: bool = False
    """The plugin list of the instance is unreadable: plugins and versions are unknown."""


def export_entries(api: DataikuApi, key: str) -> dict[str, bytes]:
    """The entries of the export of ``key``, rebuilt from the API (H-70 to H-74)."""
    return export_entries_with_gaps(api, key)[0]


def export_entries_with_gaps(api: DataikuApi, key: str) -> tuple[dict[str, bytes], ReadGaps]:
    """The entries of the export of ``key``, and what could not be read."""
    base = api.project(key)
    projects = api.project_keys()
    if key not in projects:
        raise KeyError(key)
    entries: dict[str, bytes] = {}
    summary = api.json(base + "/")
    settings = dict(api.json(base + "/settings"))
    settings["name"] = summary.get("name")
    entries[CONFIG_DIR + "params.json"] = _dump(settings)
    variables = api.json(base + "/variables/")
    entries[CONFIG_DIR + "variables.json"] = _dump(variables.get("standard") or {})
    entries[CONFIG_DIR + "localvariables.json"] = _dump(variables.get("local") or {})
    for item in api.json(base + "/datasets/"):
        name = str(item["name"])
        data = api.json(f"{base}/datasets/{_quote(name)}")
        entries[f"{CONFIG_DIR}datasets/{name}.json"] = _dump(_drop(data, "datasets"))
    recipes = []
    for item in api.json(base + "/recipes/"):
        name = str(item["name"])
        answer = api.json(f"{base}/recipes/{_quote(name)}")
        recipe, payload = answer["recipe"], answer.get("payload")
        recipes.append(recipe)
        entries[f"{CONFIG_DIR}recipes/{name}.json"] = _dump(_drop(recipe, "recipes"))
        if isinstance(payload, str):
            rel = f"{CONFIG_DIR}recipes/{_payload_name(name, str(recipe.get('type', '')))}"
            entries[rel] = payload.encode("utf-8")
    # ASSUMPTION (H-73): scenarios, flow zones and saved models are read as
    # /scenarios/<id>/, /flow/zones and /savedmodels/<id>, and stored as in an export
    # (scenarios/, zones/, saved_models/). Zones seen on a real project (2026-11-26);
    # scenarios and saved models not seen yet.
    for item in api.json(base + "/scenarios/"):
        sid = str(item["id"])
        entries[f"{CONFIG_DIR}scenarios/{sid}.json"] = _dump(
            api.json(f"{base}/scenarios/{_quote(sid)}/")
        )
    for zone in api.json(base + "/flow/zones"):
        entries[f"{CONFIG_DIR}zones/{zone['id']}.json"] = _dump(zone)
    for item in api.json(base + "/savedmodels/"):
        mid = str(item["id"])
        entries[f"{CONFIG_DIR}saved_models/{mid}.json"] = _dump(
            api.json(f"{base}/savedmodels/{_quote(mid)}")
        )
    shared, unreadable = _shared_with(api, key, projects)
    info = api.json("/instance-info")
    plugins = _installed_plugins(api)
    manifest: dict[str, Any] = {
        "generatedWithDSSVersion": info.get("dssVersion"),
        "originalProjectKey": key,
    }
    if plugins is not None:
        manifest["exportedWithPlugins"] = plugins
    manifest["foreignObjects"] = _foreign_objects(recipes, shared)
    entries[MANIFEST] = _dump(manifest)
    return entries, ReadGaps(unreadable, plugins is None)


class ApiEntries:
    """``EntrySource`` over entries rebuilt from the API."""

    def __init__(self, key: str, entries: dict[str, bytes]) -> None:
        self.path = Path(f"{key}.api")
        self.entries = entries
        self.consumed: set[str] = set()

    def exists(self, rel: str) -> bool:
        return rel in self.entries

    def list_dir(self, rel_dir: str) -> list[str]:
        return sorted(
            n for n in self.entries if n.startswith(rel_dir) and "/" not in n[len(rel_dir) :]
        )

    def read_text(self, rel: str) -> str:
        self.consumed.add(rel)
        return self.entries[rel].decode("utf-8")

    def read_json(self, rel: str) -> tuple[Any, str]:
        raw = self.entries[rel]
        self.consumed.add(rel)
        return json.loads(raw.decode("utf-8")), hashlib.sha256(raw).hexdigest()

    def unconsumed(self) -> list[str]:
        return sorted(n for n in self.entries if n not in self.consumed)


class DataikuApiReader:
    """``ProjectReader`` backed by a live DSS instance, read only."""

    def __init__(
        self,
        api: DataikuApi | None = None,
        instance_variables: Mapping[str, Any] | None = None,
    ) -> None:
        self._api = api
        self.instance_variables = dict(instance_variables or {})

    @property
    def api(self) -> DataikuApi:
        if self._api is None:
            self._api = DataikuApi(transport_from_environment())
        return self._api

    def list_projects(self) -> list[str]:
        return self.api.project_keys()

    def read_project(self, project_key: str) -> Project:
        return self._read(project_key)[0]

    def _read(self, project_key: str) -> tuple[Project, str]:
        entries, gaps = export_entries_with_gaps(self.api, project_key)
        digest = hashlib.sha256()
        for name in sorted(entries):  # hash of the rebuilt entries, as a ZIP hash
            digest.update(name.encode("utf-8") + b"\0" + entries[name] + b"\0")
        project = build_project(ApiEntries(project_key, entries), self.instance_variables)
        if gaps.unreadable_projects:
            project.issues.append(
                Issue(
                    kind=IssueKind.FOREIGN_OBJECTS_INCOMPLETE,
                    object_kind=ObjectKind.PROJECT,
                    object_name=project_key,
                    message=(
                        f"{gaps.unreadable_projects} autre(s) projet(s) de l'instance "
                        "illisible(s) avec cette clé : les objets partagés avec ce projet "
                        "peuvent être incomplets"
                    ),
                )
            )
        if gaps.plugins_unreadable:
            project.issues.append(
                Issue(
                    kind=IssueKind.PLUGINS_UNREADABLE,
                    object_kind=ObjectKind.PROJECT,
                    object_name=project_key,
                    message=(
                        "liste des plugins de l'instance illisible avec cette clé (droits "
                        "d'administration requis) : plugins et versions inconnus"
                    ),
                )
            )
        return project, digest.hexdigest()

    def read_document(self, project_key: str) -> IrDocument:
        """Project IR with its traceability header (source: the API)."""
        project, sha = self._read(project_key)
        return IrDocument(
            meta=IrMeta(
                generator_version=__version__,
                source_file=f"api:{project_key}",
                source_sha256=sha,
            ),
            project=project,
        )
