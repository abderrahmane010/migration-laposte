"""Dataiku column types -> Spark SQL types (for explicit casts)."""

from __future__ import annotations

from sqlglot import exp

from dku2dbx.errors import UnsupportedConstruct

# ASSUMPTION (H-32): Dataiku storage types. "date" is a timestamp in Dataiku (date and
# time), hence TIMESTAMP. A type absent from this table is never guessed.
SPARK_TYPES = {
    "string": "STRING",
    "boolean": "BOOLEAN",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "TIMESTAMP",
}


def spark_type(dku_type: str | None, recipe: str, column: str) -> exp.DataType:
    if dku_type not in SPARK_TYPES:
        raise UnsupportedConstruct(
            recipe, "column type", f"type '{dku_type}' of column '{column}' is not mapped"
        )
    return exp.DataType.build(SPARK_TYPES[dku_type], dialect="databricks")
