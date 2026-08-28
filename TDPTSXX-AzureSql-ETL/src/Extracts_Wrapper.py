# Databricks notebook source
import os

dbutils.widgets.text("load_mode", "daily")
dbutils.widgets.text("source_config_path", "")
dbutils.widgets.text("target_config_path", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("excluded_entity_names", "")
dbutils.widgets.text("continue_on_error", "false")

# COMMAND ----------

# MAGIC %run ./utils/config_utils.py

# COMMAND ----------

# MAGIC %run ./utils/jdbc_utils.py

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
target_config = load_target_config(target_config_path) if target_config_path else {}
databricks_env = runtime_context["databricks_env"]
normalized_load_mode = load_mode.strip().lower()
entity_configs = get_entity_configs(
    source_config,
    requested_entity_name,
    excluded_entity_names=excluded_entity_names,
)
extract_results = []
extract_failures = []

for configured_entity_name, entity_config in entity_configs:
    entity_name = entity_config.get("entity_name", configured_entity_name)

    try:
        extract_config = entity_config.get("extract", {})
        incremental_config = entity_config.get("incremental", {})
        if not extract_config.get("enabled", True):
            print({"stage": "extract_skip", "entity_name": entity_name, "reason": "extract.disabled"})
            continue

        query_by_env = extract_config.get("query_by_env")
        if not query_by_env:
            print({"stage": "extract_skip", "entity_name": entity_name, "reason": "extract.query_by_env_missing"})
            continue

        base_query = resolve_env_value(query_by_env, databricks_env, "extract.query_by_env")
        extract_query = base_query
        temp_view_name = extract_config.get("temp_view_name", f"azuresql_extract_{entity_name.lower()}")
        global_temp_view_name = extract_config.get("global_temp_view_name", temp_view_name)
        jdbc_options = build_jdbc_options(entity_config, databricks_env, target_config=target_config)
        tracking_entity_name = str(
            incremental_config.get("loadtracker_entity_name")
            or source_config.get("default_entity_name")
            or entity_name
        ).strip()
        watermark_column = str(incremental_config.get("watermark_column", "")).strip()
        uc_watermark_column = str(
            incremental_config.get("uc_watermark_column") or watermark_column
        ).strip()
        incremental_requested = bool(incremental_config.get("enabled", False))
        incremental_enabled = incremental_requested and normalized_load_mode == "transactional"
        last_load_timestamp = ""

        if incremental_requested and not incremental_enabled:
            print(
                {
                    "stage": "extract_incremental_skip",
                    "entity_name": entity_name,
                    "reason": f"incremental_not_supported_for_load_mode:{normalized_load_mode}",
                }
            )

        if incremental_enabled:
            fallback_paths = None
            try:
                fallback_paths = resolve_delta_target_paths(entity_config, databricks_env)
            except Exception:
                fallback_paths = None

            last_load_timestamp = get_last_successful_load_timestamp(
                tracking_entity_name,
                fallback_table_fqn=(fallback_paths or {}).get("primary_raw_table"),
                fallback_watermark_column=uc_watermark_column or None,
            )
            extract_query = build_incremental_extract_query(base_query, watermark_column, last_load_timestamp)

        print(
            {
                "stage": "extract",
                "entity_name": entity_name,
                "load_mode": load_mode,
                "databricks_env": databricks_env,
                "temp_view_name": temp_view_name,
                "global_temp_view_name": f"global_temp.{global_temp_view_name}",
                "incremental_enabled": incremental_enabled,
                "tracking_entity_name": tracking_entity_name,
                "watermark_column": watermark_column,
                "last_load_timestamp": last_load_timestamp,
                "jdbc_options": redact_jdbc_options(jdbc_options),
            }
        )

        extract_df = (
            spark.read.format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", f"({extract_query}) AZURE_SQL_SOURCE")
            .load()
        )

        skip_row_count = bool(extract_config.get("skip_row_count", False))
        row_count = None if skip_row_count else extract_df.count()
        extract_df.createOrReplaceTempView(temp_view_name)
        extract_df.createOrReplaceGlobalTempView(global_temp_view_name)
        extract_results.append(
            {
                "entity_name": entity_name,
                "temp_view_name": temp_view_name,
                "global_temp_view_name": f"global_temp.{global_temp_view_name}",
                "row_count": row_count,
                "incremental_enabled": incremental_enabled,
                "tracking_entity_name": tracking_entity_name,
                "last_load_timestamp": last_load_timestamp,
            }
        )
        print(
            f"Loaded {row_count} rows into temp view '{temp_view_name}' and global temp view 'global_temp.{global_temp_view_name}' for entity '{entity_name}'."
        )
    except Exception as exc:
        failure_details = {
            "stage": "extract_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc),
        }
        extract_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

print(
    {
        "stage": "extract_summary",
        "entity_count": len(extract_results),
        "failure_count": len(extract_failures),
        "entities": extract_results,
        "failures": extract_failures,
    }
)

if extract_failures and not continue_on_error:
    raise RuntimeError("Extract stage failed.")
