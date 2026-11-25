"""Phase 12: reader over the Dataiku API, same engine and same IR as the ZIP reader."""

from __future__ import annotations

import io
import json
import urllib.error
import zipfile
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from dku2dbx.cli import app
from dku2dbx.model.ir import Project
from dku2dbx.reader import api_reader
from dku2dbx.reader.api_reader import (
    ApiError,
    DataikuApi,
    DataikuApiReader,
    export_entries,
    http_transport,
    transport_from_environment,
)
from dku2dbx.reader.base import ProjectReader
from dku2dbx.reader.compare import compare_projects, report_markdown
from dku2dbx.reader.zip_reader import read_export
from tests.fixtures import make_fixture as mf

KEYS = ("DEMO_SIMPLE", "DEMO_COMPLEXE")


class FakeDataiku:
    """Serves the projects of export ZIPs the way the DSS public API does (H-70 to H-73).

    The API adds keys an export leaves out (H-71), answers {standard, local} for the
    variables, and its plugin list and instance version replace the manifest.
    """

    def __init__(self, *zips: Path) -> None:
        # The instance has the plugins of its projects: an export lists every plugin
        # installed on the instance (seen on Dataiku Cloud).
        self.projects: dict[str, dict[str, Any]] = {}
        self.calls: list[str] = []
        for path in zips:
            with zipfile.ZipFile(path) as zf:
                files = {n: zf.read(n) for n in zf.namelist() if not n.endswith("/")}
            self.projects[self._key(files)] = files
        self.overrides: dict[str, Any] = {}

    @staticmethod
    def _key(files: dict[str, bytes]) -> str:
        return str(json.loads(files["export-manifest.json"])["projectKey"])

    @staticmethod
    def _objects(files: dict[str, bytes], folder: str) -> dict[str, Any]:
        prefix = f"project_config/{folder}/"
        return {
            n[len(prefix) : -5]: json.loads(v)
            for n, v in files.items()
            if n.startswith(prefix) and n.endswith(".json") and "/" not in n[len(prefix) :]
        }

    @staticmethod
    def _payload(files: dict[str, bytes], name: str) -> str:
        prefix = f"project_config/recipes/{name}."
        found = [v for n, v in files.items() if n.startswith(prefix) and n != prefix + "json"]
        return found[0].decode("utf-8") if found else ""

    def _answer(self, path: str) -> Any:  # noqa: PLR0911, PLR0912 (one branch per endpoint)
        if path in self.overrides:
            return self.overrides[path]
        if path == "/projects/":
            return [{"projectKey": k} for k in sorted(self.projects)]
        if path == "/instance-info":
            return {"dssVersion": mf.DSS_VERSION}
        if path == "/plugins/":
            plugins = []
            for files in self.projects.values():
                manifest = json.loads(files["export-manifest.json"])
                plugins += [
                    {"id": p["pluginId"], "version": p.get("pluginVersion"), "meta": {}}
                    for p in manifest["exportedWithPlugins"]
                ]
            return plugins
        _, _, key, *rest = path.split("/")
        files = self.projects[key]
        params = json.loads(files["project_config/params.json"])
        sub = "/".join(rest)
        if sub == "":
            return {"projectKey": key, "name": params["name"], "ownerLogin": "x"}
        if sub == "settings":
            return {k: v for k, v in params.items() if k not in ("name", "projectKey")}
        if sub == "variables/":
            return json.loads(files["project_config/variables.json"])
        if sub == "datasets/":
            return [{"name": n} for n in self._objects(files, "datasets")]
        if sub.startswith("datasets/"):
            name = sub.removeprefix("datasets/")
            data = self._objects(files, "datasets")[name]
            return {**data, "name": name, "projectKey": key, "smartName": name, "versionTag": {}}
        if sub == "recipes/":
            return [{"name": n} for n in self._objects(files, "recipes")]
        if sub.startswith("recipes/"):
            name = sub.removeprefix("recipes/")
            recipe = json.loads(json.dumps(self._objects(files, "recipes")[name]))
            recipe.update(name=name, projectKey=key, hashPropagationBehavior="INHERIT")
            recipe.setdefault("params", {}).setdefault("engineParams", {})["containerSelection"] = {
                "containerMode": "INHERIT"
            }
            return {"recipe": recipe, "payload": self._payload(files, name)}
        if sub == "scenarios/":
            return [{"id": i} for i in self._objects(files, "scenarios")]
        if sub.startswith("scenarios/"):
            return self._objects(files, "scenarios")[sub.removeprefix("scenarios/").strip("/")]
        if sub == "flow/zones":
            return list(self._objects(files, "zones").values())
        if sub == "savedmodels/":
            return []
        raise ApiError(f"GET {path}: HTTP 404 (introuvable)")

    def __call__(self, path: str) -> bytes:
        self.calls.append(path)
        return json.dumps(self._answer(path)).encode("utf-8")


def _zip(key: str) -> Path:
    return mf.DEFAULT_OUT_DIR / f"{key}.zip"


def _fake() -> FakeDataiku:
    return FakeDataiku(*(_zip(k) for k in KEYS))


def _reader(fake: FakeDataiku | None = None) -> DataikuApiReader:
    return DataikuApiReader(DataikuApi(fake or _fake()))


@pytest.mark.parametrize("key", KEYS)
def test_api_and_zip_give_the_same_internal_representation(key: str) -> None:
    from_api = _reader(FakeDataiku(_zip(key))).read_project(key)  # one project per instance
    from_zip = read_export(_zip(key))
    differences = compare_projects(from_zip, from_api)
    assert [d.path for d in differences if d.explanation is None] == []
    assert {d.path.rsplit(".", 1)[-1] for d in differences} <= {"source_sha256"}
    # Everything but the provenance hashes is identical.
    strip = {"source_sha256"}

    def clean(project: Project) -> Any:
        def walk(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: walk(v) for k, v in value.items() if k not in strip}
            if isinstance(value, list):
                return [walk(v) for v in value]
            return value

        return walk(json.loads(project.model_dump_json()))

    assert clean(from_api) == clean(from_zip)


def test_the_complex_project_covers_every_kind_of_object() -> None:
    project = _reader().read_project("DEMO_COMPLEXE")
    assert project.scenarios and project.zones and project.plugins and project.foreign_objects
    assert project.dss_version == mf.DSS_VERSION
    assert any(r.prepare_steps for r in project.recipes)
    assert any(d.partitioning for d in project.datasets)
    assert project.exposed_objects


def test_the_reader_only_reads() -> None:
    fake = _fake()
    reader: ProjectReader = _reader(fake)
    assert reader.list_projects() == ["DEMO_COMPLEXE", "DEMO_SIMPLE"]
    reader.read_project("DEMO_SIMPLE")
    assert all(
        call.startswith(("/projects/", "/instance-info", "/plugins/")) for call in fake.calls
    )
    with pytest.raises(KeyError):
        reader.read_project("UNKNOWN")


def test_an_unexplained_difference_is_reported_field_by_field() -> None:
    fake = _fake()
    path = "/projects/DEMO_SIMPLE/datasets/orders_raw"
    changed = json.loads(fake(path))
    changed["params"]["table"] = "another_table"
    fake.overrides[path] = changed
    differences = compare_projects(
        read_export(_zip("DEMO_SIMPLE")), _reader(fake).read_project("DEMO_SIMPLE")
    )
    unexplained = [d for d in differences if d.explanation is None]
    paths = {d.path for d in unexplained}
    assert "datasets[name=orders_raw].params.table" in paths
    assert "datasets[name=orders_raw].table" in paths
    text = report_markdown("DEMO_SIMPLE", "DEMO_SIMPLE.zip", differences)
    assert "différence(s) non expliquée(s)" in text
    row = '| `datasets[name=orders_raw].params.table` | `"${projectKey}_orders_raw"` | '
    assert row + '`"another_table"` |' in text


def test_a_missing_object_is_reported() -> None:
    fake = _fake()
    fake.overrides["/projects/DEMO_SIMPLE/datasets/"] = [
        d for d in json.loads(fake("/projects/DEMO_SIMPLE/datasets/")) if d["name"] != "customers"
    ]
    differences = compare_projects(
        read_export(_zip("DEMO_SIMPLE")), _reader(fake).read_project("DEMO_SIMPLE")
    )
    assert any(d.path == "datasets[name=customers]" and d.explanation is None for d in differences)


def test_api_only_keys_are_removed_only_with_their_default_value() -> None:
    fake = _fake()
    path = "/projects/DEMO_SIMPLE/recipes/sort_orders"
    answer = json.loads(fake("/projects/DEMO_SIMPLE/recipes/window_orders"))
    answer["recipe"]["params"]["engineParams"]["containerSelection"] = {"containerMode": "X"}
    answer["recipe"]["params"]["selection"] = {"ascending": False}
    fake.overrides[path] = answer
    fake.overrides["/projects/DEMO_SIMPLE/recipes/"] = [{"name": "sort_orders"}]
    entries = export_entries(DataikuApi(fake), "DEMO_SIMPLE")
    recipe = json.loads(entries["project_config/recipes/sort_orders.json"])
    assert "name" not in recipe and "projectKey" not in recipe
    assert "hashPropagationBehavior" not in recipe
    assert recipe["params"]["engineParams"]["containerSelection"] == {"containerMode": "X"}
    assert recipe["params"]["selection"] == {"ascending": False}
    # The answer of the API is never modified in place.
    assert answer["recipe"]["name"] == "window_orders"


def test_payload_files_are_named_as_in_an_export() -> None:
    entries = export_entries(DataikuApi(_fake()), "DEMO_COMPLEXE")
    names = {n for n in entries if n.startswith("project_config/recipes/")}
    assert "project_config/recipes/python_scoring.py" in names
    assert "project_config/recipes/sql_anomaly_report.sql" in names
    assert "project_config/recipes/prepare_events.shaker" in names
    assert any(n.endswith(".json.payload") for n in names)  # plugin recipe (H-14)
    manifest = json.loads(entries["export-manifest.json"])
    assert manifest["originalProjectKey"] == "DEMO_COMPLEXE"
    assert manifest["foreignObjects"] == [
        {"projectKey": "DEMO_SIMPLE", "type": "DATASET", "id": "customer_stats"}
    ]


def test_api_document_has_a_reproducible_provenance() -> None:
    first = _reader().read_document("DEMO_SIMPLE")
    second = _reader().read_document("DEMO_SIMPLE")
    assert first.meta.source_file == "api:DEMO_SIMPLE"
    assert first.meta.source_sha256 == second.meta.source_sha256
    assert first.to_json() == second.to_json()


# --------------------------------------------------------------------------- #
# Transport: TLS, credentials, errors
# --------------------------------------------------------------------------- #
SECRET = "fake-key-for-tests-only"


def test_credentials_come_from_the_environment() -> None:
    with pytest.raises(ApiError, match="DATAIKU_URL et DATAIKU_API_KEY doivent"):
        transport_from_environment({})
    with pytest.raises(ApiError, match="https://"):
        transport_from_environment({"DATAIKU_URL": "http://dss", "DATAIKU_API_KEY": SECRET})
    with pytest.raises(ApiError, match="certificate file not found"):
        http_transport("https://dss", SECRET, ca_bundle="/nonexistent/ca.pem")


class _Opener:
    def __init__(self, error: Exception | None = None, body: bytes = b"[]") -> None:
        self.error, self.body = error, body
        self.requests: list[Any] = []

    def open(self, request: Any, timeout: int) -> Any:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return io.BytesIO(self.body)


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (urllib.error.HTTPError("u", 401, "x", {}, None), "HTTP 401 \\(clé refusée\\)"),  # type: ignore[arg-type]
        (urllib.error.HTTPError("u", 403, "x", {}, None), "HTTP 403 \\(droits insuffisants\\)"),  # type: ignore[arg-type]
        (urllib.error.URLError("no route"), "injoignable"),
    ],
)
def test_errors_are_clear_and_never_show_the_key(
    monkeypatch: pytest.MonkeyPatch, error: Exception, message: str
) -> None:
    opener = _Opener(error)
    monkeypatch.setattr(api_reader.urllib.request, "build_opener", lambda *h: opener)
    get = http_transport("https://dss.example", SECRET)
    with pytest.raises(ApiError, match=message) as info:
        get("/projects/")
    assert SECRET not in str(info.value)
    request = opener.requests[0]
    assert request.full_url == "https://dss.example/public/api/projects/"
    assert request.get_method() == "GET"


def test_no_proxy_unless_asked(monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: list[Any] = []

    def build(*h: Any) -> _Opener:
        handlers.extend(h)
        return _Opener()

    monkeypatch.setattr(api_reader.urllib.request, "build_opener", build)
    http_transport("https://dss.example", SECRET)
    assert any(
        isinstance(h, api_reader.urllib.request.ProxyHandler) and h.proxies == {} for h in handlers
    )


# --------------------------------------------------------------------------- #
# CLI: --api on every command, list, compare
# --------------------------------------------------------------------------- #
@pytest.fixture
def fake_env(monkeypatch: pytest.MonkeyPatch) -> FakeDataiku:
    fake = _fake()
    monkeypatch.setattr(api_reader, "transport_from_environment", lambda environ=None: fake)
    return fake


def test_cli_commands_read_through_the_api(fake_env: FakeDataiku, tmp_path: Path) -> None:
    runner = CliRunner()
    for command in ("simplify", "translate", "connect", "data", "reconcile", "generate"):
        via_api = tmp_path / "api" / command
        via_zip = tmp_path / "zip" / command
        result = runner.invoke(app, [command, "--api", "DEMO_SIMPLE", "--out", str(via_api)])
        assert result.exit_code == 0, (command, result.output)
        result = runner.invoke(app, [command, str(_zip("DEMO_SIMPLE")), "--out", str(via_zip)])
        assert result.exit_code == 0, (command, result.output)
        api_files = sorted(p.relative_to(via_api) for p in via_api.rglob("*") if p.is_file())
        zip_files = sorted(p.relative_to(via_zip) for p in via_zip.rglob("*") if p.is_file())
        assert api_files == zip_files, command
    result = runner.invoke(
        app,
        [
            "analyze",
            "--api",
            "DEMO_SIMPLE",
            "--api",
            "DEMO_COMPLEXE",
            "--out",
            str(tmp_path / "an"),
            "--dump-ir",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "2 projet(s) analysé(s)" in result.output
    ir = json.loads((tmp_path / "an" / "DEMO_SIMPLE" / "ir.json").read_text(encoding="utf-8"))
    assert ir["meta"]["source_file"] == "api:DEMO_SIMPLE"


def test_cli_needs_exactly_one_source(fake_env: FakeDataiku) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["simplify"])
    assert result.exit_code == 1
    assert "soit un export ZIP, soit --api" in result.output
    result = runner.invoke(app, ["simplify", str(_zip("DEMO_SIMPLE")), "--api", "DEMO_SIMPLE"])
    assert result.exit_code == 1
    result = runner.invoke(app, ["simplify", "--api", "NOPE"])
    assert result.exit_code == 1
    assert "absent de l'instance" in result.output
    assert runner.invoke(app, ["analyze"]).exit_code == 1


def test_cli_list_and_compare(fake_env: FakeDataiku, tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "- DEMO_COMPLEXE" in result.output
    args = ["compare", "DEMO_COMPLEXE", "--zip", str(_zip("DEMO_COMPLEXE")), "--out", str(tmp_path)]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    assert "dont 0 non expliquée(s)" in result.output
    report = (tmp_path / "DEMO_COMPLEXE" / "comparaison_api_zip.md").read_text(encoding="utf-8")
    assert "Aucune différence non expliquée" in report
    # The wrong export: refused.
    args = ["compare", "DEMO_COMPLEXE", "--zip", str(_zip("DEMO_SIMPLE")), "--out", str(tmp_path)]
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert "l'export est celui du projet DEMO_SIMPLE" in result.output


def test_cli_without_credentials_explains_how(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAIKU_URL", raising=False)
    monkeypatch.delenv("DATAIKU_API_KEY", raising=False)
    result = CliRunner().invoke(app, ["list"])
    assert result.exit_code == 1
    assert "DATAIKU_URL et DATAIKU_API_KEY" in result.output
