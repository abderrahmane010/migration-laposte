"""Synthetic Dataiku DSS project export generator.

Builds fake "configuration only" project export ZIPs that follow the format
described in CLAUDE.md. All names and values are synthetic: no real Dataiku
project, table, connection or business rule is used.

Usage:
    uv run python tests/fixtures/make_fixture.py [--out tests/fixtures/exports]

Two projects are produced, one ZIP each:
    - DEMO_SIMPLE: 5 recipes (Prepare, Join, Group, Window, Stack), no partition.
    - DEMO_COMPLEXE: 20 recipes, a day-partitioned dataset, a Python recipe,
      a SQL recipe, a step-based scenario and a plugin recipe.

Reproducibility: the output ZIP is byte-for-byte identical across runs and
platforms (sorted entries, fixed timestamp and permissions, no compression,
canonical JSON).

Every assumption about the (undocumented) DSS export format is tagged
`# ASSUMPTION:` and listed in docs/hypotheses.md.
"""

from __future__ import annotations

import argparse
import io
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_OUT_DIR = Path(__file__).parent / "exports"

# ASSUMPTION (H-01): DSS 15 export; version string format "15.x.y".
DSS_VERSION = "15.0.0"

# Fixed timestamp for every ZIP entry (earliest date allowed by the ZIP format).
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)

# ASSUMPTION (H-09): payload file extension of each visual recipe equals its type.
VISUAL_PAYLOAD_EXT = {
    "shaker": "shaker",
    "join": "join",
    "grouping": "grouping",
    "window": "window",
    "vstack": "vstack",
    "split": "split",
    "distinct": "distinct",
    "pivot": "pivot",
    "topn": "topn",
    "sort": "sort",
    "sync": "sync",
}
# ASSUMPTION (H-10): code recipe scripts are stored next to the recipe JSON,
# with a language extension. To be confirmed on real exports.
CODE_PAYLOAD_EXT = {"python": "py", "sql_query": "sql"}

Json = dict[str, Any]


# --------------------------------------------------------------------------- #
# Small builders
# --------------------------------------------------------------------------- #
def columns(*specs: str) -> list[Json]:
    """Build a schema column list from "name:type" strings."""
    out: list[Json] = []
    for spec in specs:
        name, typ = spec.split(":")
        out.append({"name": name, "type": typ})
    return out


def dataset(
    project_key: str,
    name: str,
    *,
    ds_type: str = "PostgreSQL",
    connection: str = "pg_managed",
    managed: bool = True,
    table: str | None = None,
    schema_name: str = "dku_${projectKey}",
    cols: list[Json],
    day_partitioned: bool = False,
    checks: list[Json] | None = None,
    zone: str | None = None,
) -> Json:
    # ASSUMPTION (H-03): dataset JSON keys (type, managed, params.connection/catalog/
    # schema/table/mode, partitioning.dimensions, schema.columns, checks).
    ds: Json = {
        "projectKey": project_key,
        "name": name,
        "type": ds_type,
        "managed": managed,
        "params": {
            "connection": connection,
            "mode": "table",
            "catalog": "",
            "schema": schema_name,
            "table": table if table is not None else "${projectKey}_" + name,
        },
        "partitioning": {
            "ignoreNonMatchingFile": False,
            "considerMissingRequestedPartitionsAsEmpty": False,
            "dimensions": [],
        },
        "schema": {"columns": cols, "userModified": False},
        "checks": checks or [],
        "tags": [],
    }
    if day_partitioned:
        # ASSUMPTION (H-04): time dimension shape {"name","type":"time","params":{"period"}}.
        ds["partitioning"]["dimensions"] = [
            {"name": "day", "type": "time", "params": {"period": "DAY"}}
        ]
    if zone is not None:
        # ASSUMPTION (H-12): flow zone membership stored as a "zone" key on the object.
        ds["zone"] = zone
    return ds


def ref(name: str, deps: list[Json] | None = None) -> Json:
    return {"ref": name, "deps": deps or []}


def out_ref(name: str, *, append: bool = False) -> Json:
    return {"ref": name, "appendMode": append}


# ASSUMPTION (H-05): partition dependency "same day" expressed as func "equals".
SAME_DAY = [{"func": "equals", "params": {}}]


def recipe(
    project_key: str,
    name: str,
    rtype: str,
    inputs: list[Json],
    outputs: list[Json],
    *,
    engine: str = "DSS",
    zone: str | None = None,
    never_recompute: bool = False,
) -> Json:
    # ASSUMPTION (H-06): recipe JSON keys (type, inputs.main.items[].ref/deps,
    # outputs.main.items[].ref/appendMode, params.engineParams,
    # neverRecomputeExistingPartitions).
    rcp: Json = {
        "projectKey": project_key,
        "name": name,
        "type": rtype,
        "inputs": {"main": {"items": inputs}},
        "outputs": {"main": {"items": outputs}},
        "params": {"engineParams": {"engineType": engine}},
        "neverRecomputeExistingPartitions": never_recompute,
        "tags": [],
    }
    if zone is not None:
        rcp["zone"] = zone
    return rcp


def step(proc_type: str, params: Json, *, disabled: bool = False) -> Json:
    # ASSUMPTION (H-07): Prepare step shape
    # {"metaType":"PROCESSOR","type":...,"params":...,"disabled":...}.
    return {"metaType": "PROCESSOR", "type": proc_type, "params": params, "disabled": disabled}


def step_group(name: str, steps: list[Json]) -> Json:
    # ASSUMPTION (H-08): grouped steps use metaType "GROUP" with nested "steps".
    return {"metaType": "GROUP", "name": name, "steps": steps, "disabled": False}


def shaker(steps: list[Json]) -> Json:
    return {"steps": steps, "columnsSelection": {"mode": "ALL"}, "origin": "PREPARE_RECIPE"}


# ASSUMPTION (H-09h): filter shape seen on a real project (pre/post filters, Sample/Filter,
# split): {enabled, distinct, uiData: {mode, conditions[]}}; plus "$status" (UI only).
def no_filter() -> Json:
    return {"enabled": False, "distinct": False}


def formula_filter(expr: str) -> Json:
    # ASSUMPTION (H-09h): a formula filter has uiData.mode "CUSTOM" and an "expression"
    # (mode values not observed yet).
    return {"enabled": True, "distinct": False, "uiData": {"mode": "CUSTOM"}, "expression": expr}


# Marker resolved in Project.files(): the column list is derived from the schemas.
AUTO_COLUMNS = "__auto_columns__"


def join_on(left: int, right: int, kind: str, pairs: list[tuple[str, str]]) -> Json:
    return {
        "table1": left,
        "table2": right,
        "type": kind,
        "conditionsMode": "AND",
        "outerJoinOnTheLeft": True,
        "rightLimit": {},
        "on": [
            {
                "column1": {"table": left, "name": c1},
                "column2": {"table": right, "name": c2},
                "type": "EQ",
                "caseInsensitive": False,
                "normalizeText": False,
                "strict": False,
                "maxDistance": 0,
                "maxMatches": 1,
                "dateDiffUnit": "DAY",
            }
            for c1, c2 in pairs
        ],
    }


def join_payload(n_inputs: int, joins: list[Json]) -> Json:
    # ASSUMPTION (H-09a), aligned on a real project: selectedColumns is a list of
    # {name, table, type}; virtualInputs carry their pre-filter and selection mode.
    return {
        "virtualInputs": [
            {
                "index": i,
                "preFilter": no_filter(),
                "autoSelectColumns": False,
                "outputColumnsSelectionMode": "MANUAL",
                "computedColumns": [],
            }
            for i in range(n_inputs)
        ],
        "joins": joins,
        "selectedColumns": AUTO_COLUMNS,
        "computedColumns": [],
        "postFilter": no_filter(),
        "enableAutoCastInJoinConditions": False,
    }


AGG_FLAGS = [
    "count",
    "countDistinct",
    "sum",
    "sum2",
    "avg",
    "stddev",
    "min",
    "max",
    "first",
    "last",
    "firstLastNotNull",
    "concat",
    "concatDistinct",
]


def agg(column: str, *aggs: str, col_type: str = "double") -> Json:
    return {"column": column, "type": col_type, **{a: (a in aggs) for a in AGG_FLAGS}}


def grouping_payload(keys: list[str], values: list[Json]) -> Json:
    # ASSUMPTION (H-09b), aligned on a real project.
    return {
        "keys": [{"column": k} for k in keys],
        "values": values,
        "globalCount": True,
        "computedColumns": [],
        "outputColumnNameOverrides": {},
        "enlargeYourBits": False,
        "preFilter": no_filter(),
        "postFilter": no_filter(),
    }


def window_value(column: str, *aggs: str, lag: str = "", lead: str = "") -> Json:
    value = agg(column, *aggs)
    value.update(
        {
            "lag": bool(lag),
            "lagValues": lag,
            "lagDiff": False,
            "lead": bool(lead),
            "leadValues": lead,
            "leadDiff": False,
            "dateDiffUnit": "DAY",
        }
    )
    return value


def window_payload(
    partition: list[str], order: list[tuple[str, bool]], values: list[Json], *, rank: bool = False
) -> Json:
    # ASSUMPTION (H-09c), aligned on a real project: rank flags at the root, explicit
    # frame bounds on each window.
    return {
        "windows": [
            {
                "enablePartitioning": bool(partition),
                "partitioningColumns": partition,
                "enableOrdering": bool(order),
                "orders": [{"column": c, "desc": d} for c, d in order],
                "enableLimits": False,
                "windowLimitMode": "ROWS",
                "limitPreceding": False,
                "precedingRows": 0,
                "limitFollowing": False,
                "followingRows": 0,
                "windowLowerBound": 0,
                "windowUpperBound": 0,
            }
        ],
        "values": values,
        "rank": rank,
        "rowNumber": False,
        "denseRank": False,
        "cumeDist": False,
        "ntile": False,
        "ntileValues": "",
        "computedColumns": [],
        "outputColumnNameOverrides": {},
        "preFilter": no_filter(),
        "postFilter": no_filter(),
    }


def vstack_payload(n_inputs: int, origin_labels: list[str]) -> Json:
    # ASSUMPTION (H-09d), aligned on a real project: selectedColumns is a list of names.
    return {
        "mode": "UNION",
        "virtualInputs": [
            {"index": i, "originLabel": origin_labels[i], "preFilter": no_filter()}
            for i in range(n_inputs)
        ],
        "selectedColumns": AUTO_COLUMNS,
        "addOriginColumn": True,
        "originColumnName": "original_dataset",
        "postFilter": no_filter(),
    }


# --------------------------------------------------------------------------- #
# Project container
# --------------------------------------------------------------------------- #
@dataclass
class Project:
    key: str
    name: str
    variables: Json = field(default_factory=dict)
    datasets: list[Json] = field(default_factory=list)
    recipes: list[tuple[Json, Json | str]] = field(default_factory=list)
    scenarios: list[Json] = field(default_factory=list)
    zones: list[Json] = field(default_factory=list)
    exposed_objects: list[Json] = field(default_factory=list)
    foreign_objects: list[Json] = field(default_factory=list)
    plugins: list[Json] = field(default_factory=list)

    def add_dataset(self, ds: Json) -> None:
        self.datasets.append(ds)

    def add_recipe(self, rcp: Json, payload: Json | str) -> None:
        self.recipes.append((rcp, payload))

    def _schema(self, name: str) -> list[Json]:
        return next(d["schema"]["columns"] for d in self.datasets if d["name"] == name)

    def _resolve_columns(self, rcp: Json, payload: Json | str) -> Json | str:
        """Fill an AUTO_COLUMNS selection from the output and input schemas."""
        if not isinstance(payload, dict) or payload.get("selectedColumns") != AUTO_COLUMNS:
            return payload
        inputs = [i["ref"] for i in rcp["inputs"]["main"]["items"]]
        output = rcp["outputs"]["main"]["items"][0]["ref"]
        origin = payload.get("originColumnName") if payload.get("addOriginColumn") else None
        selected: list[Any] = []
        for col in self._schema(output):
            if col["name"] == origin:
                continue
            table = next(
                (
                    t
                    for t, ref in enumerate(inputs)
                    if any(c["name"] == col["name"] for c in self._schema(ref))
                ),
                None,
            )
            if table is None:
                continue
            if rcp["type"] == "vstack":
                selected.append(col["name"])
            else:
                selected.append({"name": col["name"], "table": table, "type": col["type"]})
        return {**payload, "selectedColumns": selected}

    def files(self) -> dict[str, bytes]:
        """Return {path in ZIP: content} for this project."""
        files: dict[str, bytes] = {}
        # ASSUMPTION (H-02): manifest keys dssVersion, exportedWithPlugins, foreignObjects.
        files["export-manifest.json"] = _json(
            {
                "dssVersion": DSS_VERSION,
                "projectKey": self.key,
                "exportOptions": {"exportUploads": False, "exportManagedFS": False},
                "exportedWithPlugins": self.plugins,
                "foreignObjects": self.foreign_objects,
            }
        )
        files["project_config/params.json"] = _json(
            {
                "projectKey": self.key,
                "name": self.name,
                "projectStatus": "Sandbox",
                # ASSUMPTION (H-11): exposedObjects.objects[] {type, localName, rules[]}.
                "exposedObjects": {"objects": self.exposed_objects},
            }
        )
        # ASSUMPTION (H-13): project variables in project_config/variables.json.
        files["project_config/variables.json"] = _json({"standard": self.variables, "local": {}})
        for ds in self.datasets:
            files[f"project_config/datasets/{ds['name']}.json"] = _json(ds)
        for rcp, raw_payload in self.recipes:
            name, rtype = rcp["name"], rcp["type"]
            payload = self._resolve_columns(rcp, raw_payload)
            files[f"project_config/recipes/{name}.json"] = _json(rcp)
            if isinstance(payload, str):
                ext = CODE_PAYLOAD_EXT[rtype]
                files[f"project_config/recipes/{name}.{ext}"] = payload.encode("utf-8")
            elif rtype in VISUAL_PAYLOAD_EXT:
                files[f"project_config/recipes/{name}.{VISUAL_PAYLOAD_EXT[rtype]}"] = _json(payload)
            else:
                # ASSUMPTION (H-14): plugin recipes keep their settings in a .json payload.
                files[f"project_config/recipes/{name}.json.payload"] = _json(payload)
        for sc in self.scenarios:
            # ASSUMPTION (H-15): scenarios in project_config/scenarios/<id>.json.
            files[f"project_config/scenarios/{sc['id']}.json"] = _json(sc)
        for zone in self.zones:
            files[f"project_config/zones/{zone['id']}.json"] = _json(zone)
        return files


def _json(obj: Any) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def zip_bytes(files: dict[str, bytes]) -> bytes:
    """Build a byte-for-byte reproducible ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        for path in sorted(files):
            info = zipfile.ZipInfo(path, date_time=_ZIP_EPOCH)
            info.create_system = 3  # Unix, whatever the host OS
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_STORED
            zf.writestr(info, files[path])
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# DEMO_SIMPLE — 5 recipes, no partition
# --------------------------------------------------------------------------- #
def build_demo_simple() -> Project:
    k = "DEMO_SIMPLE"
    p = Project(key=k, name="Démo simple (synthétique)", variables={"min_amount": 0})

    order_cols = columns(
        "order_id:bigint",
        "customer_id:bigint",
        "order_date:date",
        "amount:double",
        "status:string",
    )
    p.add_dataset(
        dataset(
            k,
            "orders_raw",
            managed=False,
            connection="pg_sources",
            schema_name="sales",
            cols=order_cols,
        )
    )
    p.add_dataset(
        dataset(
            k,
            "orders_archive",
            managed=False,
            connection="pg_sources",
            schema_name="sales_archive",
            table="orders_2020",
            cols=order_cols,
        )
    )
    p.add_dataset(
        dataset(
            k,
            "customers",
            managed=False,
            connection="pg_sources",
            schema_name="crm",
            table="customers",
            cols=columns("customer_id:bigint", "name:string", "segment:string"),
        )
    )
    p.add_dataset(dataset(k, "orders_prepared", cols=order_cols + columns("amount_eur:double")))
    p.add_dataset(
        dataset(
            k,
            "orders_all",
            cols=order_cols + columns("amount_eur:double", "original_dataset:string"),
        )
    )
    p.add_dataset(
        dataset(
            k,
            "orders_enriched",
            cols=order_cols
            + columns(
                "amount_eur:double", "original_dataset:string", "name:string", "segment:string"
            ),
        )
    )
    p.add_dataset(
        dataset(
            k,
            "customer_stats",
            cols=columns(
                "customer_id:bigint",
                "count:bigint",
                "amount_eur_sum:double",
                "amount_eur_avg:double",
            ),
        )
    )
    p.add_dataset(
        dataset(
            k,
            "orders_ranked",
            cols=order_cols + columns("amount_eur:double", "rank:bigint", "amount_eur_sum:double"),
        )
    )

    p.add_recipe(
        recipe(k, "prepare_orders", "shaker", [ref("orders_raw")], [out_ref("orders_prepared")]),
        shaker(
            [
                step(
                    "FillEmptyWithValue",
                    {"columns": ["status"], "value": "UNKNOWN", "appliesTo": "SINGLE_COLUMN"},
                ),
                step(
                    "CreateColumnWithGREL",
                    {"column": "amount_eur", "expression": "round(amount * 1.0, 2)"},
                ),
                step(
                    "FilterOnValue",
                    {
                        "appliesTo": "SINGLE_COLUMN",
                        "columns": ["status"],
                        "values": ["CANCELLED"],
                        "action": "REMOVE_ROW",
                        "matchingMode": "FULL_STRING",
                        "normalizationMode": "EXACT",
                    },
                ),
                step(
                    "ColumnRenamer",
                    {"renamings": [{"from": "amount_eur", "to": "amount_eur"}]},
                    disabled=True,
                ),
            ]
        ),
    )
    p.add_recipe(
        recipe(
            k,
            "stack_orders",
            "vstack",
            [ref("orders_prepared"), ref("orders_archive")],
            [out_ref("orders_all")],
        ),
        vstack_payload(2, ["orders_prepared", "orders_archive"]),
    )
    p.add_recipe(
        recipe(
            k,
            "join_orders_customers",
            "join",
            [ref("orders_all"), ref("customers")],
            [out_ref("orders_enriched")],
            engine="SQL",
        ),
        join_payload(2, [join_on(0, 1, "LEFT", [("customer_id", "customer_id")])]),
    )
    p.add_recipe(
        recipe(
            k,
            "group_by_customer",
            "grouping",
            [ref("orders_enriched")],
            [out_ref("customer_stats")],
            engine="SQL",
        ),
        grouping_payload(["customer_id"], [agg("amount_eur", "sum", "avg")]),
    )
    p.add_recipe(
        recipe(
            k,
            "window_orders",
            "window",
            [ref("orders_enriched")],
            [out_ref("orders_ranked")],
            engine="SQL",
        ),
        window_payload(
            ["customer_id"], [("order_date", False)], [window_value("amount_eur", "sum")], rank=True
        ),
    )

    p.exposed_objects = [
        {
            "type": "DATASET",
            "localName": "customer_stats",
            "rules": [{"targetProject": "DEMO_COMPLEXE", "appearOnFlow": True}],
        }
    ]
    return p


# --------------------------------------------------------------------------- #
# DEMO_COMPLEXE — 20 recipes, partitions, code, scenario, plugin
# --------------------------------------------------------------------------- #
PYTHON_SCORING = """\
# -*- coding: utf-8 -*-
# Synthetic Python recipe (fixture). Exercises the usual dataiku idioms.
import dataiku
import pandas as pd

variables = dataiku.get_custom_variables()
threshold = float(variables.get("score_threshold", "0.5"))

deliveries = dataiku.Dataset("delivered_enriched").get_dataframe()
stats = dataiku.Dataset("DEMO_SIMPLE.customer_stats").get_dataframe()

df = deliveries.merge(stats, on="customer_id", how="left")
df["score"] = (df["amount_eur_sum"].fillna(0) / (df["delay_minutes"].fillna(0) + 1)).round(3)
df["is_good"] = df["score"] >= threshold

out = dataiku.Dataset("customer_scores")
out.write_with_schema(df[["customer_id", "score", "is_good"]])
"""

SQL_ANOMALY_REPORT = """\
-- Synthetic SQL recipe (fixture).
SELECT a.depot_id,
       d.depot_name,
       COUNT(*) AS anomalies,
       MAX(a.event_ts) AS last_anomaly_ts
FROM ${projectKey}_anomalies_clean a
LEFT JOIN ${projectKey}_depots_clean d ON a.depot_id = d.depot_id
WHERE a.event_ts >= '${start_date}'
GROUP BY a.depot_id, d.depot_name
"""


def build_demo_complexe() -> Project:
    k = "DEMO_COMPLEXE"
    p = Project(
        key=k,
        name="Démo complexe (synthétique)",
        variables={"start_date": "2026-01-01", "score_threshold": "0.5"},
    )
    p.zones = [
        {"id": "z_ingest", "name": "Ingestion", "color": "#2ab1ac"},
        {"id": "z_kpi", "name": "Indicateurs", "color": "#ff9900"},
    ]
    p.plugins = [{"pluginId": "geo-toolkit", "pluginVersion": "1.2.0"}]
    p.foreign_objects = [{"projectKey": "DEMO_SIMPLE", "type": "DATASET", "id": "customer_stats"}]

    ev = columns(
        "event_id:string",
        "parcel_id:string",
        "depot_id:int",
        "customer_id:bigint",
        "event_ts:timestamp",
        "status:string",
        "delay_minutes:int",
        "amount_eur:double",
    )
    depot = columns(
        "depot_id:int",
        "depot_name:string",
        "postcode:string",
        "latitude:double",
        "longitude:double",
    )

    def ds(name: str, cols: list[Json], **kw: Any) -> None:
        p.add_dataset(dataset(k, name, cols=cols, **kw))

    # Sources
    # ASSUMPTION (H-17): ${NODE_DC} is an instance variable, absent from the export.
    ds(
        "parcels_events",
        ev,
        managed=False,
        connection="hdfs_sources",
        ds_type="HDFS",
        table="${NODE_DC}_parcels_events",
        schema_name="logistics",
        day_partitioned=True,
        zone="z_ingest",
    )
    ds(
        "depots",
        depot,
        managed=False,
        connection="pg_sources",
        schema_name="ref",
        table="depots",
        zone="z_ingest",
    )
    ds(
        "routes_ref",
        columns("depot_id:int", "route_id:string", "route_km:double"),
        managed=False,
        connection="pg_sources",
        schema_name="ref",
        table="routes",
        zone="z_ingest",
    )
    ds(
        "customers_b2b",
        columns("customer_id:bigint", "siret:string", "company:string"),
        managed=False,
        connection="pg_sources",
        schema_name="crm",
        table="b2b_customers",
        zone="z_ingest",
    )

    # Intermediate and final datasets
    ds("events_sync", ev, day_partitioned=True, zone="z_ingest")
    ds("events_clean", ev + columns("status_norm:string"), day_partitioned=True, zone="z_ingest")
    for n in ("events_delivered", "events_failed", "events_other"):
        ds(n, ev + columns("status_norm:string"), day_partitioned=True)
    ds("depots_unique", depot)
    ds("depots_clean", depot + columns("department:string"))
    ds(
        "delivered_enriched",
        ev + columns("status_norm:string", "depot_name:string", "department:string"),
    )
    ds(
        "daily_depot_kpis",
        columns(
            "depot_id:int", "count:bigint", "delay_minutes_avg:double", "amount_eur_sum:double"
        ),
        day_partitioned=True,
        zone="z_kpi",
        # ASSUMPTION (H-16): dataset checks shape.
        checks=[{"type": "recordCountInRange", "minimum": 1, "maximum": None, "id": "chk_rows"}],
    )
    ds(
        "depot_trend",
        columns(
            "depot_id:int",
            "day:date",
            "delay_minutes_avg:double",
            "delay_minutes_avg_lag:double",
            "rank:bigint",
        ),
        zone="z_kpi",
    )
    ds("top_late_depots", columns("depot_id:int", "delay_minutes_avg:double"), zone="z_kpi")
    ds(
        "depot_trend_sorted",
        columns("depot_id:int", "day:date", "delay_minutes_avg:double"),
        zone="z_kpi",
    )
    ds(
        "status_by_depot",
        columns("depot_id:int", "DELIVERED_count:bigint", "FAILED_count:bigint"),
        zone="z_kpi",
    )
    ds("events_anomalies", ev + columns("status_norm:string", "original_dataset:string"))
    ds("anomalies_clean", ev + columns("status_norm:string"))
    ds(
        "anomaly_report",
        columns(
            "depot_id:int", "depot_name:string", "anomalies:bigint", "last_anomaly_ts:timestamp"
        ),
        zone="z_kpi",
    )
    ds("customer_scores", columns("customer_id:bigint", "score:double", "is_good:boolean"))
    ds("delivered_routes", ev + columns("route_id:string", "route_km:double"))
    ds("depots_geo", depot + columns("geo_point:string"))
    ds(
        "route_kpis",
        columns("route_id:string", "count:bigint", "route_km_sum:double"),
        zone="z_kpi",
    )
    ds("b2b_clean", columns("customer_id:bigint", "siret:string", "company:string"))
    ds(
        "b2b_scores",
        columns(
            "customer_id:bigint",
            "siret:string",
            "company:string",
            "score:double",
            "is_good:boolean",
        ),
        zone="z_kpi",
    )

    def add(
        name: str, rtype: str, ins: list[Json], outs: list[Json], payload: Json | str, **kw: Any
    ) -> None:
        p.add_recipe(recipe(k, name, rtype, ins, outs, **kw), payload)

    # 1 Sync (partitioned, same-day dependency)
    add(
        "sync_events",
        "sync",
        [ref("parcels_events", SAME_DAY)],
        [out_ref("events_sync")],
        {"schemaMode": "STRICT_SYNC"},
        zone="z_ingest",
        never_recompute=True,
    )
    # 2 Prepare with a disabled step and a group of steps (metaType GROUP)
    add(
        "prepare_events",
        "shaker",
        [ref("events_sync", SAME_DAY)],
        [out_ref("events_clean")],
        shaker(
            [
                step(
                    "ColumnsSelector",
                    {"keep": False, "columns": ["internal_flag"], "appliesTo": "COLUMNS"},
                ),
                step(
                    "StringTransformer",
                    {"columns": ["status"], "mode": "TO_UPPER", "appliesTo": "SINGLE_COLUMN"},
                ),
                step_group(
                    "normalisation",
                    [
                        step(
                            "CreateColumnWithGREL",
                            {
                                "column": "status_norm",
                                "expression": 'if(isBlank(status), "UNKNOWN", status)',
                            },
                        ),
                        step(
                            "FillEmptyWithValue",
                            {
                                "columns": ["delay_minutes"],
                                "value": "0",
                                "appliesTo": "SINGLE_COLUMN",
                            },
                        ),
                    ],
                ),
                step(
                    "DateParser",
                    {
                        "columns": ["event_ts"],
                        "formats": ["yyyy-MM-dd HH:mm:ss"],
                        "lang": "auto",
                        "timezone_id": "Europe/Paris",
                        "appliesTo": "SINGLE_COLUMN",
                    },
                    disabled=True,
                ),
            ]
        ),
        zone="z_ingest",
    )
    # 3 Split -> 2 outputs + default
    add(
        "split_status",
        "split",
        [ref("events_clean", SAME_DAY)],
        [out_ref("events_delivered"), out_ref("events_failed"), out_ref("events_other")],
        # ASSUMPTION (H-09e), aligned on a real project: valueSplits[], one list per mode.
        {
            "mode": "VALUES",
            "column": "status_norm",
            "valueSplits": [
                {"value": "DELIVERED", "caseSensitive": True, "outputIndex": 0},
                {"value": "FAILED", "caseSensitive": True, "outputIndex": 1},
            ],
            "defaultOutputIndex": 2,
            "filterSplits": [],
            "rangeSplits": [],
            "randomSplits": [],
            "randomColumns": [],
            "randomColumnsSplits": [],
            "centileSplits": [],
            "centileOrders": [],
            "computedColumns": [],
            "writeComputedColumnsInOutput": False,
            "seed": 1337,
            "preFilter": no_filter(),
        },
    )
    # 4 Distinct
    add(
        "distinct_depots",
        "distinct",
        [ref("depots")],
        [out_ref("depots_unique")],
        # Distinct on all columns (no key): the output keeps every column.
        {
            "keys": [],
            "globalCount": False,
            "outputColumnNameOverrides": {},
            "preFilter": no_filter(),
            "postFilter": no_filter(),
        },
    )
    # 5 Prepare depots (contains a reference-data processor -> MANUAL later)
    add(
        "prepare_depots",
        "shaker",
        [ref("depots_unique")],
        [out_ref("depots_clean")],
        shaker(
            [
                step(
                    "CreateColumnWithGREL",
                    {"column": "department", "expression": "substring(postcode, 0, 2)"},
                ),
                step("ZipCodeGeocoder", {"zipCodeColumn": "postcode", "countryCode": "FR"}),
            ]
        ),
    )
    # 6 Join delivered events with depots
    add(
        "join_delivered_depots",
        "join",
        [ref("events_delivered"), ref("depots_clean")],
        [out_ref("delivered_enriched")],
        join_payload(2, [join_on(0, 1, "LEFT", [("depot_id", "depot_id")])]),
        engine="SPARK",
    )
    # 7 Group daily depot KPIs (partitioned output)
    add(
        "group_daily_depot",
        "grouping",
        [ref("delivered_enriched")],
        [out_ref("daily_depot_kpis")],
        grouping_payload(["depot_id"], [agg("delay_minutes", "avg"), agg("amount_eur", "sum")]),
        zone="z_kpi",
    )
    # 8 Window trend with lag
    add(
        "window_depot_trend",
        "window",
        [ref("daily_depot_kpis")],
        [out_ref("depot_trend")],
        window_payload(
            ["depot_id"],
            [("day", False)],
            [window_value("delay_minutes_avg", lag="1")],
        ),
        zone="z_kpi",
    )
    # 9 Top N (ties possible)
    add(
        "topn_late_depots",
        "topn",
        [ref("depot_trend")],
        [out_ref("top_late_depots")],
        # ASSUMPTION (H-09f), aligned on a real project: firstRows/lastRows, retrievedColumns.
        {
            "firstRows": 10,
            "lastRows": 0,
            "orders": [{"column": "delay_minutes_avg", "desc": True}],
            "keys": [],
            "retrievedColumnsSelectionMode": "EXPLICIT",
            "retrievedColumns": ["depot_id", "day", "delay_minutes_avg"],
            "rank": False,
            "rowNumber": False,
            "denseRank": False,
            "duplicateCount": False,
            "computedColumns": [],
            "outputColumnNameOverrides": {},
            "preFilter": no_filter(),
        },
        zone="z_kpi",
    )
    # 10 Sort
    add(
        "sort_depot_trend",
        "sort",
        [ref("depot_trend")],
        [out_ref("depot_trend_sorted")],
        {
            "orders": [{"column": "depot_id", "desc": False}, {"column": "day", "desc": True}],
            "rank": False,
            "rowNumber": False,
            "denseRank": False,
            "computedColumns": [],
            "outputColumnNameOverrides": {},
            "preFilter": no_filter(),
        },
        zone="z_kpi",
    )
    # 11 Pivot with explicit values
    add(
        "pivot_status",
        "pivot",
        [ref("events_clean")],
        [out_ref("status_by_depot")],
        # ASSUMPTION (H-09g), aligned on a real project: pivots[] with keyColumns,
        # explicitValues and valueColumns; explicitIdentifiers for the rows.
        {
            "pivots": [
                {
                    "keyColumns": ["status_norm"],
                    # ASSUMPTION (H-09g): "EXPLICIT" for explicit values (only TOP_N seen).
                    "valueLimit": "EXPLICIT",
                    "explicitValues": ["DELIVERED", "FAILED"],
                    "topnLimit": 20,
                    "minOccLimit": 0,
                    "globalCount": True,
                    "valueColumns": [],
                }
            ],
            "identifierColumnsSelection": "EXPLICIT",
            "explicitIdentifiers": ["depot_id"],
            "otherColumns": [],
            "modalitySlugification": "NONE",
            "sortModalities": True,
            "customAggregates": [],
            "computedColumns": [],
            "schemaComputation": "ONLY_IF_NO_METADATA",
            "preFilter": no_filter(),
        },
        zone="z_kpi",
    )
    # 12 Stack failed + other, append mode output
    add(
        "stack_failed",
        "vstack",
        [ref("events_failed"), ref("events_other")],
        [out_ref("events_anomalies", append=True)],
        vstack_payload(2, ["events_failed", "events_other"]),
    )
    # 13 Prepare anomalies
    add(
        "prepare_anomalies",
        "shaker",
        [ref("events_anomalies")],
        [out_ref("anomalies_clean")],
        shaker(
            [
                step(
                    "ColumnsSelector",
                    {"keep": False, "columns": ["original_dataset"], "appliesTo": "COLUMNS"},
                ),
                step(
                    "FilterOnFormula", {"expression": "delay_minutes + 0 > 0", "action": "KEEP_ROW"}
                ),
            ]
        ),
    )
    # 14 SQL recipe
    add(
        "sql_anomaly_report",
        "sql_query",
        [ref("anomalies_clean"), ref("depots_clean")],
        [out_ref("anomaly_report")],
        SQL_ANOMALY_REPORT,
        engine="SQL",
        zone="z_kpi",
    )
    # 15 Python recipe, reads a foreign (shared) dataset
    add(
        "python_scoring",
        "python",
        [ref("delivered_enriched"), ref("DEMO_SIMPLE.customer_stats")],
        [out_ref("customer_scores")],
        PYTHON_SCORING,
    )
    # 16 Join with routes (inner)
    add(
        "join_routes",
        "join",
        [ref("delivered_enriched"), ref("routes_ref")],
        [out_ref("delivered_routes")],
        join_payload(2, [join_on(0, 1, "INNER", [("depot_id", "depot_id")])]),
    )
    # 17 Plugin recipe
    # ASSUMPTION (H-14): plugin recipe type "CustomCode_<pluginId>-<recipeId>".
    add(
        "plugin_geocode",
        "CustomCode_geo-toolkit-geocode",
        [ref("depots_clean")],
        [out_ref("depots_geo")],
        {
            "customConfig": {
                "lat_column": "latitude",
                "lon_column": "longitude",
                "output_column": "geo_point",
            }
        },
    )
    # 18 Group routes
    add(
        "group_routes",
        "grouping",
        [ref("delivered_routes")],
        [out_ref("route_kpis")],
        grouping_payload(["route_id"], [agg("route_km", "sum")]),
        zone="z_kpi",
    )
    # 19 Prepare B2B customers
    add(
        "prepare_b2b",
        "shaker",
        [ref("customers_b2b")],
        [out_ref("b2b_clean")],
        shaker(
            [
                step(
                    "StringTransformer",
                    {"columns": ["company"], "mode": "TRIM", "appliesTo": "SINGLE_COLUMN"},
                ),
                step(
                    "FillEmptyWithValue",
                    {"columns": ["siret"], "value": "N/A", "appliesTo": "SINGLE_COLUMN"},
                ),
            ]
        ),
    )
    # 20 Final join, exposed to other projects
    add(
        "join_b2b_scores",
        "join",
        [ref("customer_scores"), ref("b2b_clean")],
        [out_ref("b2b_scores")],
        join_payload(2, [join_on(0, 1, "INNER", [("customer_id", "customer_id")])]),
        zone="z_kpi",
    )

    p.exposed_objects = [{"type": "DATASET", "localName": "b2b_scores", "rules": []}]

    p.scenarios = [
        {
            "id": "DAILY_BUILD",
            "name": "Construction quotidienne",
            "projectKey": k,
            "type": "step_based",
            "active": True,
            # ASSUMPTION (H-15a): trigger shapes (temporal, ds_modified).
            "triggers": [
                {
                    "id": "t_daily",
                    "type": "temporal",
                    "active": True,
                    "params": {
                        "frequency": "Daily",
                        "hour": 5,
                        "minute": 0,
                        "timezone": "Europe/Paris",
                    },
                },
                {
                    "id": "t_src",
                    "type": "ds_modified",
                    "active": False,
                    "params": {"projectKey": k, "datasetName": "parcels_events"},
                },
            ],
            "params": {
                "steps": [
                    {
                        "id": "s_build_kpis",
                        "type": "build_flowitem",
                        "name": "KPIs du jour",
                        "params": {
                            "jobType": "RECURSIVE_BUILD",
                            "builds": [
                                {
                                    "type": "DATASET",
                                    "itemId": "daily_depot_kpis",
                                    "partitionsSpec": "PREVIOUS_DAY",
                                }
                            ],
                        },
                    },
                    {
                        "id": "s_build_final",
                        "type": "build_flowitem",
                        "name": "Sorties finales",
                        "params": {
                            "jobType": "RECURSIVE_BUILD",
                            "builds": [
                                {"type": "DATASET", "itemId": "b2b_scores"},
                                {"type": "DATASET", "itemId": "top_late_depots"},
                            ],
                        },
                    },
                ]
            },
        }
    ]
    return p


PROJECTS = {"DEMO_SIMPLE": build_demo_simple, "DEMO_COMPLEXE": build_demo_complexe}


def build_zip(project_key: str) -> bytes:
    """Return the reproducible ZIP export of one synthetic project."""
    return zip_bytes(PROJECTS[project_key]().files())


def write_fixtures(out_dir: Path = DEFAULT_OUT_DIR) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for key in PROJECTS:
        path = out_dir / f"{key}.zip"
        path.write_bytes(build_zip(key))
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    for path in write_fixtures(args.out):
        print(f"écrit : {path}")


if __name__ == "__main__":
    main()
