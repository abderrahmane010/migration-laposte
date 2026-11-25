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
from pathlib import Path
from typing import Any

from dku2dbx import __version__
from dku2dbx.errors import ExportReadError
from dku2dbx.model.ir import IrDocument, IrMeta, Project
from dku2dbx.reader.known_types import CODE_RECIPE_TYPES, PLUGIN_RECIPE_PREFIX
from dku2dbx.reader.zip_reader import CONFIG_DIR, MANIFEST, build_project

API_URL_ENV = "DATAIKU_URL"
API_KEY_ENV = "DATAIKU_API_KEY"  # name of an environment variable, not a secret
CA_BUNDLE_ENV = "DATAIKU_CA_BUNDLE"
TIMEOUT_S = 60

Transport = Callable[[str], bytes]
"""GET of a path under ``/public/api`` -> response body; raises ``ApiError``."""

# ASSUMPTION (H-70): payload file of a recipe in an export: "<name>.<type>" for a visual
# recipe (seen: .shaker, .sampling), a language extension for code (seen: pyspark ->
# .py), "<name>.json.payload" for a plugin recipe (H-14); written only when not empty.
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
# only removed when it has the default value seen on the export.
API_ONLY_KEYS = {
    "datasets": ("name", "projectKey", "smartName", "versionTag"),
    "recipes": ("name", "projectKey", "versionTag"),
}
API_DEFAULTS = {
    "recipes": (
        (("hashPropagationBehavior",), "INHERIT"),
        (("params", "engineParams", "containerSelection"), {"containerMode": "INHERIT"}),
        (("params", "selection", "ascending"), True),
    ),
}


class ApiError(ExportReadError):
    """The Dataiku API cannot be read (configuration, network, rights, answer)."""


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
            raise ApiError(f"GET {path}: HTTP {exc.code}{hint.get(exc.code, '')}") from None
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
    if rtype.startswith(PLUGIN_RECIPE_PREFIX):
        return f"{name}.json.payload"
    return f"{name}.{rtype}"


def _foreign_objects(recipes: list[dict[str, Any]]) -> list[dict[str, str]]:
    # ASSUMPTION (H-72): the foreignObjects of an export are the datasets of other
    # projects its recipes read ("PROJECT.dataset").
    refs = {
        str(item.get("ref", ""))
        for rcp in recipes
        for role in (rcp.get("inputs") or {}).values()
        for item in (role or {}).get("items", [])
        if "." in str(item.get("ref", ""))
    }
    return [
        {"projectKey": ref.rpartition(".")[0], "type": "DATASET", "id": ref.rpartition(".")[2]}
        for ref in sorted(refs)
    ]


def export_entries(api: DataikuApi, key: str) -> dict[str, bytes]:
    """The entries of the export of ``key``, rebuilt from the API (H-70 to H-73)."""
    base = api.project(key)
    if key not in api.project_keys():
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
        recipe, payload = answer["recipe"], answer.get("payload") or ""
        recipes.append(recipe)
        entries[f"{CONFIG_DIR}recipes/{name}.json"] = _dump(_drop(recipe, "recipes"))
        if payload:
            rel = f"{CONFIG_DIR}recipes/{_payload_name(name, str(recipe.get('type', '')))}"
            entries[rel] = payload.encode("utf-8")
    # ASSUMPTION (H-73): scenarios, flow zones and saved models are read as
    # /scenarios/<id>/, /flow/zones and /savedmodels/<id>, and stored as in an export
    # (scenarios/, zones/, saved_models/). Not seen yet: the trial project has none.
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
    info = api.json("/instance-info")
    entries[MANIFEST] = _dump(
        {
            "generatedWithDSSVersion": info.get("dssVersion"),
            "originalProjectKey": key,
            "exportedWithPlugins": [
                {"pluginId": p["id"], "version": p.get("version")} for p in api.json("/plugins/")
            ],
            "foreignObjects": _foreign_objects(recipes),
        }
    )
    return entries


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
        entries = export_entries(self.api, project_key)
        digest = hashlib.sha256()
        for name in sorted(entries):  # hash of the rebuilt entries, as a ZIP hash
            digest.update(name.encode("utf-8") + b"\0" + entries[name] + b"\0")
        project = build_project(ApiEntries(project_key, entries), self.instance_variables)
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
