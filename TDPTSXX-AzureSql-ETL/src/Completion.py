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

# MAGIC %run ./utils/jdbc_utils

# COMMAND ----------

# MAGIC %run ./utils/delta_utils

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


def _read_full_extract_dataframe(entity_config, databricks_env, target_config):
    extract_config = entity_config.get("extract", {})
    query_by_env = extract_config.get("query_by_env")
    if not query_by_env:
        return None
    base_query = resolve_env_value(query_by_env, databricks_env, "extract.query_by_env")
    jdbc_options = build_jdbc_options(entity_config, databricks_env, target_config=target_config)
    return (
        spark.read.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", f"({base_query}) AZURE_SQL_SOURCE_FULL")
        .load()
    )


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
time_grain = str(source_config.get("time_grain", "")).strip().upper()
entity_configs = get_entity_configs(
    source_config,
    requested_entity_name,
    excluded_entity_names=excluded_entity_names,
)
completion_results = []
completion_failures = []

for configured_entity_name, entity_config in entity_configs:
    entity_name = entity_config.get("entity_name", configured_entity_name)

    try:
        completion_config = entity_config.get("completion", {})
        transform_config = entity_config.get("transform", {})
        if not completion_config.get("enabled", True):
            print({"stage": "completion_skip", "entity_name": entity_name, "reason": "completion.disabled"})
            continue
        if not transform_config.get("enabled", True):
            print({"stage": "completion_skip", "entity_name": entity_name, "reason": "transform.disabled"})
            continue

        transform_temp_view_name = transform_config.get(
            "temp_view_name", f"azuresql_transform_{entity_name.lower()}"
        )
        transform_global_temp_view_name = transform_config.get(
            "global_temp_view_name", transform_temp_view_name
        )
        completed_df = spark.table(f"global_temp.{transform_global_temp_view_name}")

        transform_bootstrap_dataframe = None
        entity_time_grain = time_grain or (
            "HOURLY" if load_mode.strip().lower() == "transactional" else "DAILY"
        )
        if entity_time_grain == "HOURLY":
            paths = resolve_delta_target_paths(entity_config, databricks_env)
            if not does_table_exist(paths["primary_transform_table"]):
                print(
                    {
                        "stage": "completion_transform_bootstrap_extract",
                        "entity_name": entity_name,
                        "reason": "transform_table_missing",
                    }
                )
                transform_bootstrap_dataframe = _read_full_extract_dataframe(
                    entity_config, databricks_env, target_config
                )

        write_result = complete_entity_to_delta(
            completed_df,
            entity_config,
            databricks_env,
            load_mode,
            time_grain=entity_time_grain,
            transform_bootstrap_dataframe=transform_bootstrap_dataframe,
        )
        completion_results.append(write_result)
        print({"stage": "completion_success", "result": write_result})
    except Exception as exc:
        failure_details = {
            "stage": "completion_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc),
        }
        completion_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

print(
    {
        "stage": "completion_summary",
        "entity_count": len(completion_results),
        "failure_count": len(completion_failures),
        "entities": completion_results,
        "failures": completion_failures,
    }
)

if completion_failures and not continue_on_error:
    raise RuntimeError("Completion stage failed.")
