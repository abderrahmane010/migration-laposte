"""Shared fixtures: one local SparkSession for the tests marked `spark`."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession  # importing does not start a JVM

HAS_JAVA = shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    if not HAS_JAVA:
        pytest.skip("Java (JVM 17+) absente : PySpark local indisponible")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dku2dbx-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
