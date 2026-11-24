"""Compare a Dataiku output table with its Databricks counterpart (ROADMAP phase 9, task 1).

SELF-CONTAINED: this module imports only the standard library and pyspark, because
its source is copied into the generated reconciliation notebook (dku2dbx is not
installed on the workspace). Keep it that way.

Four layers, from the cheapest to the most precise:
1. schema: missing or extra columns, types, nullability;
2. row counts, in total and per partition column;
3. aggregates per column: null count and distinct count for every column, plus
   sum/min/max for numeric columns and min/max for the others;
4. rows: with a business key, a full outer join that counts missing rows on each
   side and the rows whose values differ; without a key, ``EXCEPT ALL`` both ways.

Every difference has a severity: ``tolerated`` (within the configured tolerance, or
a harmless type widening or nullability change) or ``diff``. The status of a table
is ``ÉCART`` when any difference is a ``diff``, ``ÉCART TOLÉRÉ`` when all are
tolerated, ``IDENTIQUE`` when there is none.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812 (PySpark convention)

IDENTICAL = "IDENTIQUE"
TOLERATED = "ÉCART TOLÉRÉ"
DIFFERENT = "ÉCART"
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
INTEGER_WIDENING = ("tinyint", "smallint", "int", "bigint")
FLOAT_WIDENING = ("float", "double")
COMPLEX_TYPES = ("array", "map", "struct")
MAX_EXAMPLES = 5


def _diff(layer: str, item: str, detail: str, severity: str = "diff") -> dict[str, str]:
    return {"layer": layer, "item": item, "detail": detail, "severity": severity}


def _numeric(dtype: str) -> bool:
    return dtype.split("(", maxsplit=1)[0] in NUMERIC_TYPES


def _widening(ref: str, new: str) -> bool:
    """A type change that loses nothing (int -> bigint, float -> double)."""
    for chain in (INTEGER_WIDENING, FLOAT_WIDENING):
        if ref in chain and new in chain:
            return chain.index(new) >= chain.index(ref)
    return False


def within(ref: Any, new: Any, abs_tolerance: float, rel_tolerance: float) -> bool:
    """|new - ref| <= abs + rel * |ref| (both None counts as equal)."""
    if ref is None or new is None:
        return ref is None and new is None
    return abs(float(new) - float(ref)) <= abs_tolerance + rel_tolerance * abs(float(ref))


# --------------------------------------------------------------------------- #
# 1. Schema
# --------------------------------------------------------------------------- #
def compare_schema(ref: DataFrame, new: DataFrame, ignore: list[str]) -> list[dict[str, str]]:
    ref_fields = {f.name: f for f in ref.schema.fields if f.name not in ignore}
    new_fields = {f.name: f for f in new.schema.fields if f.name not in ignore}
    diffs = [
        _diff("schéma", name, "colonne absente de la table Databricks")
        for name in ref_fields
        if name not in new_fields
    ]
    diffs += [
        _diff("schéma", name, "colonne en trop dans la table Databricks")
        for name in new_fields
        if name not in ref_fields
    ]
    for name, field in ref_fields.items():
        other = new_fields.get(name)
        if other is None:
            continue
        ref_type, new_type = field.dataType.simpleString(), other.dataType.simpleString()
        if ref_type != new_type:
            severity = "tolerated" if _widening(ref_type, new_type) else "diff"
            diffs.append(_diff("schéma", name, f"type {ref_type} -> {new_type}", severity))
        if field.nullable != other.nullable:
            detail = f"nullable {field.nullable} -> {other.nullable}"
            diffs.append(_diff("schéma", name, detail, "tolerated"))
    return diffs


# --------------------------------------------------------------------------- #
# 2. Row counts
# --------------------------------------------------------------------------- #
def compare_counts(
    ref: DataFrame, new: DataFrame, partition_columns: list[str]
) -> list[dict[str, str]]:
    ref_count, new_count = ref.count(), new.count()
    diffs = []
    if ref_count != new_count:
        diffs.append(_diff("comptes", "total", f"{ref_count} lignes -> {new_count} lignes"))
    usable = [c for c in partition_columns if c in ref.columns and c in new.columns]
    for column in partition_columns:
        if column not in usable:
            diffs.append(_diff("comptes", column, "colonne de partition absente d'un côté"))
    if usable:
        left = ref.groupBy(*usable).agg(F.count(F.lit(1)).alias("_ref"))
        right = new.groupBy(*usable).agg(F.count(F.lit(1)).alias("_new"))
        joined = left.join(right, on=usable, how="full_outer").fillna(0, ["_ref", "_new"])
        bad = joined.where(F.col("_ref") != F.col("_new"))
        total = bad.count()
        for row in bad.orderBy(*usable).limit(MAX_EXAMPLES).collect():
            where = ", ".join(f"{c}={row[c]}" for c in usable)
            diffs.append(_diff("comptes", where, f"{row['_ref']} lignes -> {row['_new']} lignes"))
        if total > MAX_EXAMPLES:
            diffs.append(_diff("comptes", "partitions", f"{total} partitions différentes"))
    return diffs


# --------------------------------------------------------------------------- #
# 3. Aggregates
# --------------------------------------------------------------------------- #
def _aggregates(df: DataFrame, columns: list[str], numeric: set[str]) -> dict[str, Any]:
    types = dict(df.dtypes)
    exprs: list[Column] = []
    for c in columns:
        col = F.col(f"`{c}`")
        exprs.append(F.count(F.when(col.isNull(), 1)).alias(f"{c}|nulls"))
        if types[c].startswith(COMPLEX_TYPES):  # not orderable: null count only
            continue
        exprs += [
            F.countDistinct(col).alias(f"{c}|distincts"),
            F.min(col).alias(f"{c}|min"),
            F.max(col).alias(f"{c}|max"),
        ]
        if c in numeric:
            exprs.append(F.sum(col.cast("double")).alias(f"{c}|sum"))
    return df.agg(*exprs).collect()[0].asDict() if exprs else {}


def compare_aggregates(
    ref: DataFrame,
    new: DataFrame,
    columns: list[str],
    tolerance: tuple[float, float] = (0.0, 0.0),
) -> list[dict[str, str]]:
    """``tolerance``: (absolute, relative), applied to sum/min/max of numeric columns."""
    abs_tolerance, rel_tolerance = tolerance
    types = dict(ref.dtypes)
    numeric = {c for c in columns if _numeric(types[c]) and _numeric(dict(new.dtypes)[c])}
    left = _aggregates(ref, columns, numeric)
    right = _aggregates(new, columns, numeric)
    diffs = []
    for name, expected in left.items():
        if name not in right:  # complex on one side only: the schema layer reports it
            continue
        actual = right[name]
        if expected == actual:
            continue
        column, metric = name.split("|")
        detail = f"{metric} {expected} -> {actual}"
        tolerable = column in numeric and metric in ("sum", "min", "max")
        if tolerable and within(expected, actual, abs_tolerance, rel_tolerance):
            diffs.append(_diff("agrégats", column, detail, "tolerated"))
        else:
            diffs.append(_diff("agrégats", column, detail))
    return diffs


# --------------------------------------------------------------------------- #
# 4. Rows
# --------------------------------------------------------------------------- #
def _rows_without_key(ref: DataFrame, new: DataFrame, columns: list[str]) -> list[dict[str, str]]:
    left, right = ref.select(*columns), new.select(*columns)
    only_ref, only_new = left.exceptAll(right).count(), right.exceptAll(left).count()
    diffs = []
    if only_ref:
        diffs.append(_diff("lignes", "EXCEPT ALL", f"{only_ref} ligne(s) seulement côté Dataiku"))
    if only_new:
        detail = f"{only_new} ligne(s) seulement côté Databricks"
        diffs.append(_diff("lignes", "EXCEPT ALL", detail))
    return diffs


def _duplicates(df: DataFrame, key: list[str], side: str) -> list[dict[str, str]]:
    count = df.groupBy(*key).count().where(F.col("count") > 1).count()
    detail = f"{count} valeur(s) de clé en double côté {side}"
    return [_diff("lignes", "clé", detail)] if count else []


def _rows_with_key(
    ref: DataFrame,
    new: DataFrame,
    key: list[str],
    columns: list[str],
    tolerance: tuple[float, float],
) -> list[dict[str, str]]:
    types, new_types = dict(ref.dtypes), dict(new.dtypes)
    numeric = {c for c in columns if _numeric(types[c]) and _numeric(new_types[c])}
    diffs = _duplicates(ref, key, "Dataiku") + _duplicates(new, key, "Databricks")
    values = [c for c in columns if c not in key]
    left = ref.select(*key, *[F.col(f"`{c}`").alias(f"r|{c}") for c in values]).withColumn(
        "_in_ref", F.lit(True)
    )
    right = new.select(*key, *[F.col(f"`{c}`").alias(f"n|{c}") for c in values]).withColumn(
        "_in_new", F.lit(True)
    )
    joined = left.join(right, on=key, how="full_outer").cache()
    only_ref = joined.where(F.col("_in_new").isNull()).count()
    only_new = joined.where(F.col("_in_ref").isNull()).count()
    if only_ref:
        diffs.append(_diff("lignes", "clé", f"{only_ref} clé(s) seulement côté Dataiku"))
    if only_new:
        diffs.append(_diff("lignes", "clé", f"{only_new} clé(s) seulement côté Databricks"))
    both = joined.where(F.col("_in_ref") & F.col("_in_new"))
    abs_tolerance, rel_tolerance = tolerance
    for c in values:
        r, n = F.col(f"`r|{c}`"), F.col(f"`n|{c}`")
        differs = ~r.eqNullSafe(n)
        if c in numeric:
            gap = F.abs(n.cast("double") - r.cast("double"))
            limit = F.lit(abs_tolerance) + F.lit(rel_tolerance) * F.abs(r.cast("double"))
            inside = differs & r.isNotNull() & n.isNotNull() & (gap <= limit)
            counts = both.agg(
                F.count(F.when(differs & ~inside, 1)).alias("bad"),
                F.count(F.when(inside, 1)).alias("near"),
            ).collect()[0]
            bad, near = counts["bad"], counts["near"]
        else:
            bad, near = both.where(differs).count(), 0
        if bad:
            example = both.where(differs).select(*key, r, n).limit(1).collect()[0]
            sample = ", ".join(f"{k}={example[k]}" for k in key)
            change = f"{example[f'r|{c}']} -> {example[f'n|{c}']}"
            diffs.append(
                _diff("lignes", c, f"{bad} ligne(s) différente(s), ex. {sample} : {change}")
            )
        if near:
            diffs.append(_diff("lignes", c, f"{near} ligne(s) dans la tolérance", "tolerated"))
    joined.unpersist()
    return diffs


def compare_rows(
    ref: DataFrame,
    new: DataFrame,
    key: list[str],
    columns: list[str],
    tolerance: tuple[float, float] = (0.0, 0.0),
) -> list[dict[str, str]]:
    """``tolerance``: (absolute, relative), applied to numeric columns with a key."""
    if not key:
        return _rows_without_key(ref, new, columns)
    missing = [k for k in key if k not in ref.columns or k not in new.columns]
    if missing:
        return [_diff("lignes", "clé", f"colonne(s) de clé absente(s) : {', '.join(missing)}")]
    return _rows_with_key(ref, new, key, columns, tolerance)


# --------------------------------------------------------------------------- #
# A table, then a report
# --------------------------------------------------------------------------- #
def status(diffs: list[dict[str, str]]) -> str:
    if any(d["severity"] == "diff" for d in diffs):
        return DIFFERENT
    return TOLERATED if diffs else IDENTICAL


def reconcile_table(ref: DataFrame, new: DataFrame, spec: dict[str, Any]) -> dict[str, Any]:
    """Every layer for one table; ``spec`` comes from config/reconcile.yaml."""
    ignore = list(spec.get("ignore_columns", []))
    tolerance = (float(spec.get("abs_tolerance", 0.0)), float(spec.get("rel_tolerance", 0.0)))
    key = list(spec.get("key", []))
    common = [c for c in ref.columns if c in new.columns and c not in ignore]
    diffs = compare_schema(ref, new, ignore)
    diffs += compare_counts(ref, new, list(spec.get("partition_columns", [])))
    diffs += compare_aggregates(ref, new, common, tolerance)
    diffs += compare_rows(ref, new, key, common, tolerance)
    return {
        "table": spec["dataset"],
        "status": status(diffs),
        "key": key,
        "differences": diffs,
    }


def missing_table(spec: dict[str, Any], side: str, error: str) -> dict[str, Any]:
    diff = _diff("table", side, f"table illisible : {error[:200]}")
    return {"table": spec["dataset"], "status": DIFFERENT, "key": [], "differences": [diff]}


def report_markdown(project: str, results: list[dict[str, Any]]) -> str:
    lines = [f"# Réconciliation — {project}", "", "| Table | Statut | Écarts |", "|---|---|---|"]
    for r in results:
        lines.append(f"| {r['table']} | {r['status']} | {len(r['differences'])} |")
    for r in results:
        if not r["differences"]:
            continue
        lines += ["", f"## {r['table']} ({r['status']})", ""]
        lines += ["| Couche | Élément | Détail | Gravité |", "|---|---|---|---|"]
        for d in r["differences"]:
            severity = "toléré" if d["severity"] == "tolerated" else "écart"
            lines.append(f"| {d['layer']} | {d['item']} | {d['detail']} | {severity} |")
    return "\n".join(lines) + "\n"


def results_json(results: list[dict[str, Any]]) -> str:
    return json.dumps(results, ensure_ascii=False, sort_keys=True)
