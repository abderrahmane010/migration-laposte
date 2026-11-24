"""Shared fixtures: one local SparkSession for the tests marked `spark`."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession  # importing does not start a JVM

HAS_JAVA = shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    if not HAS_JAVA:
        pytest.skip("Java (JVM 17+) absente : PySpark local indisponible")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dku2dbx-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        # saveAsTable (rewritten Python recipes) writes here, never in the repository.
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _no_llm_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A developer's .env (VLLM_*) must never make a test call a real LLM."""
    for name in ("VLLM_BASE_URL", "VLLM_MODEL", "SSL_CA_BUNDLE"):
        monkeypatch.delenv(name, raising=False)
