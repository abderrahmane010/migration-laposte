"""Object types the reader recognises.

"Known" means the reader understands the object's structure, not that it can be
translated: translation coverage is decided later (phase 2 classification).
Any other type is kept as-is with ``status = UNKNOWN`` and reported.
"""

from __future__ import annotations

from dku2dbx.model.ir import RecipeCategory

# ASSUMPTION (H-20): DSS recipe type identifiers. Lists to be completed from the
# real exports; an absent type is not an error, it is reported as UNKNOWN.
VISUAL_RECIPE_TYPES = frozenset(
    {
        "sync",
        "shaker",
        "sampling",
        "grouping",
        "distinct",
        "window",
        "join",
        "fuzzyjoin",
        "geojoin",
        "vstack",
        "split",
        "topn",
        "sort",
        "pivot",
        "upsert",
        "download",
        "export",
        "merge_folder",
    }
)
CODE_RECIPE_TYPES = frozenset(
    {
        "python",
        "sql_query",
        "sql_script",
        "pyspark",
        "spark_sql_query",
        "spark_scala",
        "sparkr",
        "r",
        "shell",
        "hive",
        "impala",
    }
)
ML_RECIPE_TYPES = frozenset(
    {
        "prediction_training",
        "clustering_training",
        "prediction_scoring",
        "clustering_scoring",
        "evaluation",
        "standalone_evaluation",
    }
)
# ASSUMPTION (H-25): these visual recipes may have no payload file (or an empty one):
# their whole configuration is in the recipe definition. Seen on a real project read
# through the API: the payload of every "sync" recipe is empty.
PAYLOAD_OPTIONAL_RECIPE_TYPES = frozenset({"sync"})

# ASSUMPTION (H-14): plugin recipe type is "CustomCode_<pluginId>-<recipeId>".
PLUGIN_RECIPE_PREFIX = "CustomCode_"

# ASSUMPTION (H-21): DSS dataset type identifiers.
DATASET_TYPES = frozenset(
    {
        "PostgreSQL",
        "MySQL",
        "Oracle",
        "SQLServer",
        "Synapse",
        "Teradata",
        "Snowflake",
        "BigQuery",
        "Redshift",
        "Databricks",
        "JDBC",
        "HDFS",
        "Filesystem",
        "S3",
        "Azure",
        "GCS",
        "FTP",
        "SFTP",
        "UploadedFiles",
        "Inline",
    }
)

# ASSUMPTION (H-15): scenario types.
SCENARIO_TYPES = frozenset({"step_based", "custom_python"})

PROCESSOR_META_TYPE = "PROCESSOR"
# ASSUMPTION (H-08): grouped Prepare steps use metaType "GROUP".
GROUP_META_TYPE = "GROUP"


def recipe_category(recipe_type: str) -> RecipeCategory:
    if recipe_type in VISUAL_RECIPE_TYPES:
        return RecipeCategory.VISUAL
    if recipe_type in CODE_RECIPE_TYPES:
        return RecipeCategory.CODE
    if recipe_type in ML_RECIPE_TYPES:
        return RecipeCategory.ML
    if recipe_type.startswith(PLUGIN_RECIPE_PREFIX):
        return RecipeCategory.PLUGIN
    return RecipeCategory.UNKNOWN


def plugin_id_of(recipe_type: str, declared_plugins: list[str]) -> str | None:
    """Plugin id of a plugin recipe type, or None if it is not a plugin recipe.

    Plugin and recipe ids may both contain '-', so the split relies on the plugin
    ids declared in the manifest (longest match). Without a match, the whole
    "<pluginId>-<recipeId>" part is returned.
    """
    if not recipe_type.startswith(PLUGIN_RECIPE_PREFIX):
        return None
    suffix = recipe_type[len(PLUGIN_RECIPE_PREFIX) :]
    for plugin_id in sorted(declared_plugins, key=len, reverse=True):
        if suffix == plugin_id or suffix.startswith(plugin_id + "-"):
            return plugin_id
    return suffix
