# Databricks notebook source
import os

dbutils.widgets.text("load_mode", "daily")
dbutils.widgets.text("source_config_path", "")
dbutils.widgets.text("target_config_path", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("excluded_entity_names", "")
dbutils.widgets.text("continue_on_error", "false")

# COMMAND ----------

# MAGIC %run ./utils/config_utils

# COMMAND ----------

def _resolve_widget_value(widget_name, fallback_env_var=None):
    value = dbutils.widgets.get(widget_name).strip()
    if value:
        return value
    if fallback_env_var:
        return os.getenv(fallback_env_var, "").strip()
    return ""


def _resolve_bool_widget_value(widget_name, default=False):
    value = dbutils.widgets.get(widget_name).strip()
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


def _resolve_source_config_path(load_mode):
    source_config_path = dbutils.widgets.get("source_config_path").strip()
    if source_config_path:
        return source_config_path

    source_config_env_by_mode = {
        "daily": "Daily_Source_Config_Path",
        "transactional": "Transactional_Source_Config_Path",
    }
    fallback_env_var = source_config_env_by_mode.get(load_mode.strip().lower(), "Daily_Source_Config_Path")
    return os.getenv(fallback_env_var, "").strip()


runtime_context = get_runtime_context()
load_mode = _resolve_widget_value("load_mode") or "daily"
source_config_path = _resolve_source_config_path(load_mode)
target_config_path = _resolve_widget_value("target_config_path", "Target_Config_Path")
requested_entity_name = _resolve_widget_value("entity_name")
excluded_entity_names = _resolve_widget_value("excluded_entity_names")
continue_on_error = _resolve_bool_widget_value("continue_on_error")

source_config = load_json_config(source_config_path)
entity_configs = get_entity_configs(
    source_config,
    requested_entity_name,
    excluded_entity_names=excluded_entity_names,
)
transform_results = []
transform_failures = []

for configured_entity_name, entity_config in entity_configs:
    entity_name = entity_config.get("entity_name", configured_entity_name)

    try:
        extract_config = entity_config.get("extract", {})
        transform_config = entity_config.get("transform", {})

        if not transform_config.get("enabled", True):
            print({"stage": "transform_skip", "entity_name": entity_name, "reason": "transform.disabled"})
            continue

        extract_temp_view_name = extract_config.get(
            "temp_view_name", f"azuresql_extract_{entity_name.lower()}"
        )
        extract_global_temp_view_name = extract_config.get(
            "global_temp_view_name", extract_temp_view_name
        )
        transform_temp_view_name = transform_config.get(
            "temp_view_name", f"azuresql_transform_{entity_name.lower()}"
        )
        transform_global_temp_view_name = transform_config.get(
            "global_temp_view_name", transform_temp_view_name
        )
        skip_row_count = bool(transform_config.get("skip_row_count", False))
        transform_query_by_env = transform_config.get("query_by_env")
        transform_query = (
            resolve_env_value(transform_query_by_env, runtime_context["databricks_env"], "transform.query_by_env")
            if transform_query_by_env
            else ""
        )

        print(
            {
                "stage": "transform",
                "entity_name": entity_name,
                "load_mode": load_mode,
                "source_global_temp_view_name": f"global_temp.{extract_global_temp_view_name}",
                "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
                "passthrough": not bool(transform_query),
            }
        )

        if transform_query:
            transformed_df = spark.sql(transform_query)
        else:
            # Legacy Azure SQL path had no business reshape; preserve columns/types via passthrough.
            transformed_df = spark.table(f"global_temp.{extract_global_temp_view_name}")

        transformed_df.createOrReplaceTempView(transform_temp_view_name)
        transformed_df.createOrReplaceGlobalTempView(transform_global_temp_view_name)
        row_count = None if skip_row_count else transformed_df.count()
        transform_results.append(
            {
                "entity_name": entity_name,
                "source_global_temp_view_name": f"global_temp.{extract_global_temp_view_name}",
                "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
                "row_count": row_count,
            }
        )
        print(
            f"Prepared {row_count} rows in temp view '{transform_temp_view_name}' and global temp view 'global_temp.{transform_global_temp_view_name}' for entity '{entity_name}'."
        )
    except Exception as exc:
        failure_details = {
            "stage": "transform_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc),
        }
        transform_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

print(
    {
        "stage": "transform_summary",
        "entity_count": len(transform_results),
        "failure_count": len(transform_failures),
        "entities": transform_results,
        "failures": transform_failures,
    }
)
