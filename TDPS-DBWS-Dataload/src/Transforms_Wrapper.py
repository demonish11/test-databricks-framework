# Databricks notebook source
# Databricks notebook source
import os
dbutils.widgets.text("load_mode", "")
dbutils.widgets.text("source_config_path", "")
dbutils.widgets.text("target_config_path", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("excluded_entity_names", "")
dbutils.widgets.text("continue_on_error", "false")
dbutils.widgets.text("historical_cutoff_type", "")
dbutils.widgets.text("historical_cutoff_value", "")
dbutils.widgets.text("historical_cutoff_end_value", "")


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
        "historical": "Historical_Source_Config_Path"
    }

    fallback_env_var = source_config_env_by_mode.get(load_mode.strip().lower(), "Daily_Source_Config_Path")
    return os.getenv(fallback_env_var, "").strip()


def _display_entity_row_counts(stage_name, results):
    if not results:
        print(f"No entities processed during {stage_name}.")
        return

    summary_rows = [(result["entity_name"], result["row_count"]) for result in results]
    print(f"{stage_name.title()} row counts by entity:")
    display(spark.createDataFrame(summary_rows, "entity_name STRING, row_count BIGINT").orderBy("entity_name"))


def _get_validation_preview_df(dataframe, row_limit):
    if row_limit in (None, ""):
        return dataframe

    normalized_row_limit = int(row_limit)
    if normalized_row_limit <= 0:
        return None

    return dataframe.limit(normalized_row_limit)


runtime_context = get_runtime_context()
load_mode = _resolve_widget_value("load_mode")
source_config_path = _resolve_source_config_path(load_mode)
target_config_path = _resolve_widget_value("target_config_path", "Target_Config_Path")
requested_entity_name = _resolve_widget_value("entity_name")
excluded_entity_names = _resolve_widget_value("excluded_entity_names")
continue_on_error = _resolve_bool_widget_value("continue_on_error")
historical_cutoff_type = _resolve_widget_value("historical_cutoff_type")
historical_cutoff_value = _resolve_widget_value("historical_cutoff_value")
historical_cutoff_end_value = _resolve_widget_value("historical_cutoff_end_value")

source_config = load_json_config(source_config_path)
entity_configs = get_entity_configs(source_config, requested_entity_name, excluded_entity_names=excluded_entity_names)
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

        extract_temp_view_name = extract_config.get("temp_view_name", f"oracle_extract_{entity_name.lower()}_preview")
        extract_global_temp_view_name = extract_config.get("global_temp_view_name", extract_temp_view_name)
        transform_temp_view_name = transform_config.get("temp_view_name", f"oracle_transform_{entity_name.lower()}_preview")
        transform_global_temp_view_name = transform_config.get("global_temp_view_name", transform_temp_view_name)
        validation_row_limit = transform_config.get("validation_row_limit")
        skip_row_count = bool(transform_config.get("skip_row_count", False))
        transform_query_by_env = transform_config.get("query_by_env")
        transform_query = resolve_env_value(transform_query_by_env, runtime_context["databricks_env"], "transform.query_by_env") if transform_query_by_env else ""

        print({
            "stage": "transform",
            "entity_name": entity_name,
            "load_mode": load_mode,
            "databricks_env": runtime_context["databricks_env"],
            "source_config_path": source_config_path,
            "target_config_path": target_config_path,
            "excluded_entity_names": excluded_entity_names,
            "continue_on_error": continue_on_error,
            "source_temp_view_name": extract_temp_view_name,
            "source_global_temp_view_name": f"global_temp.{extract_global_temp_view_name}",
            "transform_temp_view_name": transform_temp_view_name,
            "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
            "historical_cutoff_type": historical_cutoff_type,
            "historical_cutoff_value": historical_cutoff_value,
            "historical_cutoff_end_value": historical_cutoff_end_value
        })

        if transform_query:
            transformed_df = spark.sql(transform_query)
        else:
            transformed_df = spark.table(f"global_temp.{extract_global_temp_view_name}")
        validation_preview_df = _get_validation_preview_df(transformed_df, validation_row_limit)
        transformed_df.createOrReplaceTempView(transform_temp_view_name)
        transformed_df.createOrReplaceGlobalTempView(transform_global_temp_view_name)
        row_count = None if skip_row_count else transformed_df.count()
        transform_results.append({
            "entity_name": entity_name,
            "source_temp_view_name": extract_temp_view_name,
            "source_global_temp_view_name": f"global_temp.{extract_global_temp_view_name}",
            "transform_temp_view_name": transform_temp_view_name,
            "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
            "row_count": row_count
        })

        if row_count is None:
            print(f"Prepared rows in temp view '{transform_temp_view_name}' and global temp view 'global_temp.{transform_global_temp_view_name}' for entity '{entity_name}'.")
        else:
            print(f"Prepared {row_count} rows in temp view '{transform_temp_view_name}' and global temp view 'global_temp.{transform_global_temp_view_name}' for entity '{entity_name}'.")
        if validation_preview_df is not None:
            display(validation_preview_df)
    except Exception as exc:
        failure_details = {
            "stage": "transform_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc)
        }
        transform_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

_display_entity_row_counts("transform", transform_results)

print({
    "stage": "transform_summary",
    "entity_count": len(transform_results),
    "failure_count": len(transform_failures),
    "entities": transform_results,
    "failures": transform_failures
})

# COMMAND ----------

