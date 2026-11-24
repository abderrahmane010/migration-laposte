"""Phase 8: LLM residue (inputs, fake client, static checks, REVIEW_REQUIRED, report)."""

from __future__ import annotations

import json
import ssl
import urllib.request
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dku2dbx.analyze.classify import classify
from dku2dbx.analyze.model import TargetClass
from dku2dbx.cli import app
from dku2dbx.connect.resolve import Branching
from dku2dbx.model.ir import Project
from dku2dbx.translate.engine import translate_project
from dku2dbx.translate.llm.config import LLMConfig, LLMConfigError, load_llm_config
from dku2dbx.translate.llm.report import residue_markdown, write_residue_report
from dku2dbx.translate.llm.residue import (
    REVIEW_REQUIRED,
    Status,
    extract_code,
    load_prompt,
    make_client,
    residual_recipes,
    run_residue,
    static_checks,
)
from tests.fixtures import make_fixture as mf
from tests.fixtures.make_fixture import DEFAULT_OUT_DIR
from tests.test_translate_sync import fixture, read_files

ENABLED = LLMConfig(endpoint="test-endpoint")
OPENAI_YAML = (
    "provider: openai\nbase_url: https://llm.example.test/v1\nmodel: qwen-test\n"
    "api_key_env: DKU2DBX_TEST_LLM_KEY\ntimeout_s: 7\n"
)

PY_API = """\
import dataiku
client = dataiku.api_client()
df = dataiku.Dataset("customers").get_dataframe()
dataiku.Dataset("py_out").write_with_schema(df)
"""
SQL_UDF = "SELECT my_udf(customer_id) AS customer_id FROM ${projectKey}_customers"


def _residue_project(tmp_path: Path) -> Project:
    """DEMO_SIMPLE plus a Python recipe (api_client), an SQL recipe (UDF), a fuzzy join."""
    p = mf.build_demo_simple()
    cols = mf.columns("customer_id:bigint")
    for name in ("py_out", "sql_out", "fuzzy_out"):
        p.add_dataset(mf.dataset(p.key, name, cols=cols))
    p.add_recipe(
        mf.recipe(p.key, "py_api", "python", [mf.ref("customers")], [mf.out_ref("py_out")]),
        PY_API,
    )
    p.add_recipe(
        mf.recipe(p.key, "sql_udf", "sql_query", [mf.ref("customers")], [mf.out_ref("sql_out")]),
        SQL_UDF,
    )
    p.add_recipe(
        mf.recipe(p.key, "fuzzy", "fuzzyjoin", [mf.ref("customers")], [mf.out_ref("fuzzy_out")]),
        {"joins": [], "virtualInputs": [{"index": 0}]},
    )
    return read_files(tmp_path, p.files())


class FakeClient:
    """Records the calls; answers by recipe name found in the input JSON."""

    def __init__(self, answers: dict[str, str] | None = None, fail: bool = False) -> None:
        self.answers = answers or {}
        self.fail = fail
        self.calls: list[tuple[str, str]] = []

    def complete(self, system: str, user: str) -> str:
        self.calls.append((system, user))
        if self.fail:
            raise ConnectionError("endpoint unreachable")
        return self.answers[json.loads(user)["recipe"]["name"]]


GOOD_PY = '```python\ndf = spark.table("customers").toPandas()\ndbutils.widgets.get("x")\n```'
GOOD_SQL = "Voici :\n```sql\nSELECT customer_id FROM customers\n```\n"


# --------------------------------------------------------------------------- #
# Configuration and prompts
# --------------------------------------------------------------------------- #
def test_repository_config_disables_the_llm() -> None:
    config = load_llm_config(Path("config/llm.yaml"))
    assert not config.enabled
    assert config.source == "config/llm.yaml"


def test_local_config_is_read_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("VLLM_BASE_URL", raising=False)
    assert not load_llm_config().enabled  # no file at all
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "llm.yaml").write_text("endpoint: null\n", encoding="utf-8")
    (tmp_path / "config" / "llm.local.yaml").write_text(OPENAI_YAML, encoding="utf-8")
    config = load_llm_config()
    assert config.source == "config/llm.local.yaml"
    assert config.enabled and config.provider == "openai"
    assert config.target == "qwen-test (API compatible OpenAI, https://llm.example.test/v1)"


def test_environment_declares_the_internal_vllm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "llm.yaml").write_text("endpoint: null\n", encoding="utf-8")
    monkeypatch.setenv("VLLM_BASE_URL", "https://llm.example.test/v1")
    monkeypatch.setenv("VLLM_MODEL", "qwen-test")
    monkeypatch.setenv("SSL_CA_BUNDLE", "./ca.crt")
    config = load_llm_config()  # before config/llm.yaml
    assert config.enabled and config.provider == "openai" and config.ca_bundle == "./ca.crt"
    assert config.source == "variables d'environnement VLLM_BASE_URL et VLLM_MODEL"
    assert config.api_key_env == "OPENAI_API_KEY"
    assert not load_llm_config(Path("config/llm.yaml")).enabled  # explicit file wins
    (tmp_path / "config" / "llm.local.yaml").write_text("endpoint: null\n", encoding="utf-8")
    assert not load_llm_config().enabled  # local file wins
    (tmp_path / "config" / "llm.local.yaml").unlink()
    monkeypatch.setenv("VLLM_BASE_URL", "http://llm.example.test/v1")
    with pytest.raises(LLMConfigError, match="VLLM_BASE_URL"):
        load_llm_config()  # TLS never disabled, even from the environment


@pytest.mark.parametrize(
    "bad",
    [
        "provider: openai\nbase_url: http://llm.example.test/v1\nmodel: m\n",  # no TLS
        "provider: openai\nbase_url: https://llm.example.test/v1\n",  # no model
        "provider: openai\nendpoint: e\n",  # Databricks field
        "base_url: https://llm.example.test/v1\nmodel: m\n",  # provider missing
        "provider: openai\nbase_url: https://x/v1\nmodel: m\napi_key: secret\n",  # no key
    ],
)
def test_openai_config_validation(tmp_path: Path, bad: str) -> None:
    path = tmp_path / "llm.yaml"
    path.write_text(bad, encoding="utf-8")
    with pytest.raises(LLMConfigError, match="invalid"):
        load_llm_config(path)


def test_config_validation(tmp_path: Path) -> None:
    path = tmp_path / "llm.yaml"
    path.write_text("endpoint: validated-endpoint\nprofile: p\n", encoding="utf-8")
    config = load_llm_config(path)
    assert config.enabled and config.temperature == 0.0 and config.source == str(path)
    path.write_text("endpoint: e\ntoken: secret\n", encoding="utf-8")
    with pytest.raises(LLMConfigError, match="invalid"):
        load_llm_config(path)
    with pytest.raises(LLMConfigError, match="not found"):
        load_llm_config(tmp_path / "absent.yaml")


def test_prompts_are_versioned_and_packaged() -> None:
    for language in ("python", "sql"):
        prompt = load_prompt(language)
        assert prompt.name == f"{language}_v1"
        assert "exactly one fenced code block" in prompt.text
        assert len(prompt.sha256) == 64


# --------------------------------------------------------------------------- #
# Residue and inputs
# --------------------------------------------------------------------------- #
def test_residue_is_the_llm_review_recipes_not_handled(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    residue = residual_recipes(project, translate_project(project))
    assert set(residue) == {"py_api", "sql_udf", "fuzzy"}
    assert "api_client" in residue["py_api"]
    assert "my_udf" in residue["sql_udf"]
    assert residue["fuzzy"] == "aucun traducteur déterministe prévu"
    classes = {c.name: c.target_class for c in classify(project)}
    assert {classes[n] for n in residue} == {TargetClass.LLM_REVIEW}  # never AUTO


def test_fixture_code_recipes_are_not_in_the_residue() -> None:
    project = fixture("DEMO_COMPLEXE")
    assert residual_recipes(project, translate_project(project)) == {}


def test_disabled_llm_only_writes_the_inputs(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    client = FakeClient()
    residue = run_residue(project, LLMConfig(), tmp_path / "r", client=client)
    assert client.calls == []
    assert {i.status for i in residue.items} == {Status.READY}
    assert sorted(p.name for p in (tmp_path / "r" / "inputs").iterdir()) == [
        "fuzzy.json",
        "py_api.json",
        "sql_udf.json",
    ]
    assert not (tmp_path / "r" / "outputs").exists()
    assert residue.llm.startswith("désactivé : aucun endpoint validé")


def test_input_json_describes_the_recipe_without_data(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    branching = Branching(project=project.key, source_file="t", resolved={"customers": "c.s.t"})
    run_residue(project, LLMConfig(), tmp_path / "a", branching=branching)
    run_residue(project, LLMConfig(), tmp_path / "b", branching=branching)
    a = (tmp_path / "a" / "inputs" / "py_api.json").read_bytes()
    assert a == (tmp_path / "b" / "inputs" / "py_api.json").read_bytes()  # reproducible
    data = json.loads(a)
    assert data["code"] == PY_API
    assert data["inputs"][0]["table"] == "c.s.t"
    assert data["inputs"][0]["columns"][0] == {"name": "customer_id", "type": "bigint"}
    assert data["outputs"][0] == {
        "dataset": "py_out",
        "table": "py_out",
        "append": False,
        "columns": [{"name": "customer_id", "type": "bigint"}],
        "partitioning": [],
    }
    assert data["job_parameters"] == {"min_amount": "dku_min_amount"}
    assert "api_client" in data["deterministic_refusal"]
    sql = json.loads((tmp_path / "a" / "inputs" / "sql_udf.json").read_bytes())
    assert sql["source_dialect"] == "postgres"
    fuzzy = json.loads((tmp_path / "a" / "inputs" / "fuzzy.json").read_bytes())
    assert fuzzy["payload"] == {"joins": [], "virtualInputs": [{"index": 0}]}


# --------------------------------------------------------------------------- #
# Calls through a fake client
# --------------------------------------------------------------------------- #
def test_answers_are_checked_and_flagged_review_required(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    client = FakeClient({"py_api": GOOD_PY, "sql_udf": GOOD_SQL, "fuzzy": GOOD_SQL})
    residue = run_residue(project, ENABLED, tmp_path / "r", client=client)
    assert {i.recipe: i.status for i in residue.items} == dict.fromkeys(
        ("fuzzy", "py_api", "sql_udf"), Status.REVIEW
    )
    system, user = next(c for c in client.calls if json.loads(c[1])["recipe"]["name"] == "py_api")
    assert system == load_prompt("python").text
    assert user == (tmp_path / "r" / "inputs" / "py_api.json").read_text(encoding="utf-8")
    py = (tmp_path / "r" / "outputs" / "py_api.py").read_text(encoding="utf-8")
    assert py.startswith(f"# {REVIEW_REQUIRED}\n")
    assert "# LLM: test-endpoint (Databricks Model Serving)" in py
    assert f"# Prompt: python_v1 (SHA-256 {load_prompt('python').sha256})" in py
    assert "# Static check: ruff : OK" in py  # spark and dbutils are notebook globals
    assert py.endswith('dbutils.widgets.get("x")\n')
    sql = (tmp_path / "r" / "outputs" / "sql_udf.sql").read_text(encoding="utf-8")
    assert sql.startswith(f"-- {REVIEW_REQUIRED}\n")
    assert "-- Static check: sqlglot : OK (1 instruction(s))" in sql


@pytest.mark.parametrize(
    ("answer", "status", "check"),
    [
        ("```python\nimport dataiku\n```", Status.CHECK_FAILED, "appels dataiku restants"),
        ("```python\ndef f(:\n```", Status.CHECK_FAILED, "erreur de syntaxe"),
        ("```python\nprint(undefined_name)\n```", Status.CHECK_FAILED, "F821"),
        ("no code at all", Status.UNUSABLE, "un seul bloc"),
        ("```python\na = 1\n```\n```python\nb = 2\n```", Status.UNUSABLE, "un seul bloc"),
        ("```sql\nSELECT 1\n```", Status.UNUSABLE, "un seul bloc"),  # wrong language
    ],
)
def test_bad_python_answers(tmp_path: Path, answer: str, status: str, check: str) -> None:
    project = _residue_project(tmp_path)
    client = FakeClient({"py_api": answer, "sql_udf": GOOD_SQL, "fuzzy": GOOD_SQL})
    residue = run_residue(project, ENABLED, tmp_path / "r", client=client)
    [item] = [i for i in residue.items if i.recipe == "py_api"]
    assert item.status == status
    assert any(check in c for c in item.checks), item.checks
    written = (tmp_path / "r" / "outputs" / "py_api.py").is_file()
    assert written == (status == Status.CHECK_FAILED)  # a failed check is still reviewed


def test_bad_sql_answer_and_unknown_functions(tmp_path: Path) -> None:
    passed, checks = static_checks("SELECT FROM WHERE (", "sql")
    assert not passed and checks[0].startswith("sqlglot : SQL illisible")
    passed, checks = static_checks("SELECT some_udf(a) FROM t", "sql")
    assert passed and checks[-1] == "fonctions inconnues de sqlglot, à vérifier : some_udf"


def test_call_errors_are_reported_not_fatal(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    residue = run_residue(project, ENABLED, tmp_path / "r", client=FakeClient(fail=True))
    assert {i.status for i in residue.items} == {Status.ERROR}
    assert residue.items[0].checks == ["ConnectionError: endpoint unreachable"]


def test_extract_code() -> None:
    assert extract_code("```py\nx = 1\n```", "python") == "x = 1\n"
    assert extract_code("```\nx = 1\n```", "python") is None  # untagged: never guessed
    assert extract_code("```sql\nSELECT 1;\n```", "sql") == "SELECT 1;\n"


# --------------------------------------------------------------------------- #
# Report and CLI
# --------------------------------------------------------------------------- #
def test_report_lists_every_file_to_review(tmp_path: Path) -> None:
    project = _residue_project(tmp_path)
    client = FakeClient({"py_api": "nothing", "sql_udf": GOOD_SQL, "fuzzy": GOOD_SQL})
    residue = run_residue(project, ENABLED, tmp_path / "r", client=client)
    paths = write_residue_report(residue, tmp_path / "r")
    assert [p.name for p in paths] == ["RESIDU.md", "residue.json"]
    md = residue_markdown(residue)
    assert "| sql_udf | sql_query | REVIEW_REQUIRED | `outputs/sql_udf.sql` |" in md
    assert "| py_api | python | RÉPONSE INEXPLOITABLE | `inputs/py_api.json` |" in md
    assert "- **py_api** (RÉPONSE INEXPLOITABLE) :" in md


def test_report_lists_the_deterministic_rewrites(tmp_path: Path) -> None:
    residue = run_residue(fixture("DEMO_COMPLEXE"), LLMConfig(), tmp_path)
    assert residue.deterministic_to_review == {
        "python_scoring": "python/python_scoring.py",
        "sql_anomaly_report": "sql/sql_anomaly_report.sql",
    }
    assert "- python_scoring : `python/python_scoring.py`" in residue_markdown(residue)


def test_cli_residue(tmp_path: Path) -> None:
    export = str(DEFAULT_OUT_DIR / "DEMO_COMPLEXE.zip")
    result = CliRunner().invoke(app, ["residue", export, "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "0 recette(s) dans le résidu LLM (aucune), 2 réécriture(s)" in result.output
    assert (tmp_path / "DEMO_COMPLEXE" / "residue" / "RESIDU.md").is_file()
    bad = tmp_path / "llm.yaml"
    bad.write_text("endpoint: [unclosed\n", encoding="utf-8")
    result = CliRunner().invoke(app, ["residue", export, "--llm-config", str(bad)])
    assert result.exit_code == 1


# --------------------------------------------------------------------------- #
# OpenAI-compatible client (internal vLLM): request shape, TLS, proxy, key
# --------------------------------------------------------------------------- #
class _FakeResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self._body = json.dumps(body).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def test_openai_client_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "llm.yaml"
    path.write_text(OPENAI_YAML, encoding="utf-8")
    config = load_llm_config(path)
    seen: dict[str, object] = {}

    class FakeOpener:
        def open(self, request: urllib.request.Request, timeout: float) -> _FakeResponse:
            seen["request"], seen["timeout"] = request, timeout
            return _FakeResponse({"choices": [{"message": {"content": "```python\nx = 1\n```"}}]})

    def fake_build_opener(*handlers: urllib.request.BaseHandler) -> FakeOpener:
        seen["handlers"] = handlers
        return FakeOpener()

    monkeypatch.setattr(urllib.request, "build_opener", fake_build_opener)
    monkeypatch.setenv("DKU2DBX_TEST_LLM_KEY", "k-test")
    client = make_client(config)
    assert client is not None
    assert client.complete("system prompt", "user json") == "```python\nx = 1\n```"
    request = seen["request"]
    assert isinstance(request, urllib.request.Request)
    assert request.full_url == "https://llm.example.test/v1/chat/completions"
    assert request.get_header("Authorization") == "Bearer k-test"
    assert isinstance(request.data, bytes)
    body = json.loads(request.data)
    assert body["model"] == "qwen-test" and body["temperature"] == 0.0
    assert [m["role"] for m in body["messages"]] == ["system", "user"]
    assert seen["timeout"] == 7
    handlers = seen["handlers"]
    assert isinstance(handlers, tuple)
    https = next(h for h in handlers if isinstance(h, urllib.request.HTTPSHandler))
    context = https._context  # type: ignore[attr-defined]
    assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname  # TLS verified
    proxy = next(h for h in handlers if isinstance(h, urllib.request.ProxyHandler))
    assert proxy.proxies == {}  # internal endpoint: direct
    monkeypatch.setenv("DKU2DBX_TEST_LLM_KEY", "EMPTY")  # vLLM convention: no key
    make_client(config).complete("s", "u")  # type: ignore[union-attr]
    request = seen["request"]
    assert isinstance(request, urllib.request.Request)
    assert request.get_header("Authorization") is None


def test_make_client_is_none_when_disabled() -> None:
    assert make_client(LLMConfig()) is None
