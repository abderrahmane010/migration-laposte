"""Runtime of the dataset copies (ROADMAP phase 11): read, write once, validate.

Self-contained (pyspark and the standard library only): this file is copied as is into
every copy notebook, so nothing has to be installed on the workspace.

Idempotence (decision of 2026-11-25): a copy can be run again without duplicates and
without overwriting by accident.
- mode ``skip`` (default): a table that already holds rows is left untouched; for a
  partitioned table, only the partitions it does not have yet are added;
- mode ``replace``: the table is fully replaced; only when asked explicitly.
Rows are never appended blindly.

A spec is a dict: ``dataset``, ``target`` (quoted full name), ``read`` (``{"kind":
"table", "name"}`` or ``{"kind": "files", "format", "path", "options", "schema"}``),
``columns`` (``[[name, spark type], ...]``, the Dataiku schema), ``partition_column``,
``properties`` (the original Dataiku identifiers) and ``column_mapping`` (a column name
Delta only accepts with column mapping: the table is created with it).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812 (PySpark convention)

SKIP = "skip"
REPLACE = "replace"
MODES = (SKIP, REPLACE)

LOADED = "CHARGÉ"
ALREADY_LOADED = "DÉJÀ CHARGÉ"
PARTITIONS_ADDED = "PARTITIONS AJOUTÉES"
REPLACED = "REMPLACÉ"


def _col(name: str) -> Column:
    return F.col("`" + name.replace("`", "``") + "`")


def _try_cast(name: str, kind: str) -> Column:
    return F.expr("try_cast(`" + name.replace("`", "``") + "` AS " + kind + ")")


def _literal(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def read_source(spark: SparkSession, spec: dict[str, Any]) -> DataFrame:
    """The source as a DataFrame; an unreadable source (permission, location) fails clearly."""
    read = spec["read"]
    try:
        if read["kind"] == "table":
            df = spark.table(read["name"])
        else:
            reader = spark.read.format(read["format"])
            if read.get("schema"):
                reader = reader.schema(read["schema"])
            for key, value in sorted(read.get("options", {}).items()):
                reader = reader.option(key, value)
            df = reader.load(read["path"])
        df.schema  # noqa: B018 (resolves the source now: missing table, rights, path)
    except Exception as exc:
        where = read.get("name") or read.get("path")
        raise RuntimeError(
            f"dku2dbx: source of {spec['dataset']} unreadable ({where}): check that it "
            f"exists and that the job identity may read it — {exc}"
        ) from exc
    return df


def conform(df: DataFrame, spec: dict[str, Any]) -> DataFrame:
    """The Dataiku columns, by name, cast to their declared types; no value lost silently."""
    present = {c.lower() for c in df.columns}
    missing = [name for name, _ in spec["columns"] if name.lower() not in present]
    if missing:
        raise ValueError(f"dku2dbx: {spec['dataset']}: columns missing in the source: {missing}")
    current = {f.name.lower(): f.dataType.simpleString() for f in df.schema.fields}
    changed = [(n, k) for n, k in spec["columns"] if current[n.lower()] != k]
    if changed:
        # try_cast: NULL instead of an error, whatever spark.sql.ansi.enabled says.
        checks = [
            F.sum(F.when(_col(n).isNotNull() & _try_cast(n, k).isNull(), 1).otherwise(0)).alias(n)
            for n, k in changed
        ]
        lost = df.agg(*checks).collect()[0].asDict()
        bad = {name: count for name, count in sorted(lost.items()) if count}
        if bad:
            raise ValueError(
                f"dku2dbx: {spec['dataset']}: values that do not convert to the Dataiku "
                f"type (count per column): {bad}"
            )
    return df.select([_col(name).cast(kind).alias(name) for name, kind in spec["columns"]])


def _exists(spark: SparkSession, target: str) -> bool:
    try:
        spark.table(target).schema  # noqa: B018
    except Exception:  # absent (an unreadable table then fails on write, never overwritten)
        return False
    return True


COLUMN_MAPPING = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}


def _create_with_column_mapping(spark: SparkSession, spec: dict[str, Any]) -> None:
    columns = ", ".join(
        "`" + name.replace("`", "``") + "` " + kind for name, kind in spec["columns"]
    )
    props = ", ".join(f"{_literal(k)} = {_literal(v)}" for k, v in COLUMN_MAPPING.items())
    spark.sql(f"CREATE TABLE {spec['target']} ({columns}) TBLPROPERTIES ({props})")


def load_table(spark: SparkSession, df: DataFrame, spec: dict[str, Any], mode: str) -> str:
    """Write ``df`` into the target table; returns the load status."""
    if mode not in MODES:
        raise ValueError(f"dku2dbx: unknown mode {mode!r} (skip or replace)")
    target, part = spec["target"], spec.get("partition_column")
    if mode == REPLACE:
        if spec.get("column_mapping") and not _exists(spark, target):
            _create_with_column_mapping(spark, spec)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        status = REPLACED
    elif not _exists(spark, target):
        if spec.get("column_mapping"):
            _create_with_column_mapping(spark, spec)
            df.write.mode("append").saveAsTable(target)
        else:
            df.write.mode("errorifexists").saveAsTable(target)
        status = LOADED
    elif part is None:
        if spark.table(target).limit(1).count():
            return ALREADY_LOADED
        df.write.mode("append").saveAsTable(target)
        status = LOADED
    else:
        loaded = spark.table(target).select(_col(part).alias("_dku2dbx_part")).distinct()
        new = df.join(loaded, _col(part).eqNullSafe(F.col("_dku2dbx_part")), "left_anti")
        if not new.limit(1).count():
            return ALREADY_LOADED
        new.write.mode("append").saveAsTable(target)
        status = PARTITIONS_ADDED
    props = ", ".join(
        f"{_literal(k)} = {_literal(v)}" for k, v in sorted(spec["properties"].items())
    )
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ({props})")
    return status


def _partitions(df: DataFrame, part: str) -> set[Any]:
    return {row[0] for row in df.select(_col(part)).distinct().collect()}


def validate(spark: SparkSession, source: DataFrame, spec: dict[str, Any]) -> list[str]:
    """Problems of the loaded table: schema, types, partitions, rows, metadata."""
    table = spark.table(spec["target"])
    problems = []
    actual = {f.name.lower(): (f.name, f.dataType.simpleString()) for f in table.schema.fields}
    expected = {name.lower(): (name, kind) for name, kind in spec["columns"]}
    for key in sorted(expected.keys() - actual.keys()):
        problems.append(f"colonne absente : {expected[key][0]}")
    for key in sorted(actual.keys() - expected.keys()):
        problems.append(f"colonne en trop : {actual[key][0]}")
    for key in sorted(expected.keys() & actual.keys()):
        if expected[key][1] != actual[key][1]:
            problems.append(
                f"type de {expected[key][0]} : {actual[key][1]} au lieu de {expected[key][1]}"
            )
    part = spec.get("partition_column")
    if part is not None and part.lower() in actual:
        missing = _partitions(source, part) - _partitions(table, part)
        if missing:
            shown = ", ".join(sorted(str(p) for p in missing)[:5])
            problems.append(f"{len(missing)} partition(s) de la source absente(s) : {shown}")
        nulls = table.where(_col(part).isNull()).count()
        if nulls:
            problems.append(f"{nulls} ligne(s) sans partition ({part} vide)")
    elif part is not None:
        problems.append(f"colonne de partition absente : {part}")
    rows, expected_rows = table.count(), source.count()
    if rows != expected_rows:
        problems.append(
            f"{rows} ligne(s) dans la table, {expected_rows} dans la source "
            "(source modifiée depuis la copie ? relancer avec mode=replace si c'est voulu)"
        )
    props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {spec['target']}").collect()}
    lost = sorted(k for k, v in spec["properties"].items() if props.get(k) != v)
    if lost:
        problems.append(f"métadonnées Dataiku absentes : {', '.join(lost)}")
    return problems


def copy_dataset(spark: SparkSession, spec: dict[str, Any], mode: str = SKIP) -> dict[str, Any]:
    """Read, write once, validate. ``problems`` empty: the table matches its Dataiku dataset."""
    source = conform(read_source(spark, spec), spec)
    status = load_table(spark, source, spec, mode)
    return {
        "dataset": spec["dataset"],
        "target": spec["target"],
        "mode": mode,
        "status": status,
        "problems": validate(spark, source, spec),
    }
