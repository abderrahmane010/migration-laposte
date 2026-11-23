"""Phase 0: the local execution toolchain used by later phases is available.

This is not a feature test: it only checks that sqlglot (databricks dialect)
works and that a local SparkSession can run SQL (requires Java 17+). The Spark
test is skipped, with an explicit reason, when no JVM is installed.
"""

from __future__ import annotations

import pytest
import sqlglot
from pyspark.sql import SparkSession  # importing does not start a JVM


def test_sqlglot_databricks_dialect() -> None:
    sql = sqlglot.transpile("select 1 as x", read="databricks", write="databricks")[0]
    assert sql == "SELECT 1 AS x"


@pytest.mark.spark
def test_local_spark_runs_sql(spark: SparkSession) -> None:
    """Uses the shared session (tests/conftest.py): stopping it here would stop it
    for every Spark test that runs afterwards. Skipped when no JVM is installed."""
    assert spark.sql("SELECT 1 + 1 AS two").collect()[0]["two"] == 2
