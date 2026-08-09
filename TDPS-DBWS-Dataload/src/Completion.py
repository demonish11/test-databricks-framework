# Databricks notebook source
# Databricks notebook source
import os
from pyspark.sql import functions as F
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


def _resolve_route_code_suffix_config_path(source_config_path):
    configured_path = os.getenv("Route_Code_Suffix_Config_Path", "").strip()
    if configured_path:
        return configured_path

    if source_config_path:
        return os.path.join(os.path.dirname(source_config_path), "route_code_suffix_route_ids.json")

    return ""


def _load_route_code_suffix_route_ids(source_config_path):
    config_path = _resolve_route_code_suffix_config_path(source_config_path)
    if not config_path or not os.path.exists(config_path):
        return set()

    config = load_json_config(config_path)
    route_ids = config.get("route_ids") or []
    return {int(route_id) for route_id in route_ids if str(route_id).strip()}


def _apply_route_code_suffix_rule(dataframe, entity_name, governed_route_ids):
    if not governed_route_ids:
        return dataframe

    entity_column_mapping = {
        "Route": ("RouteId", "RouteCode"),
        "RouteInitialLoad": ("RouteId", "RouteCode"),
        "Trip": ("RouteID", "TripCode")
    }
    column_mapping = entity_column_mapping.get(entity_name)
    if not column_mapping:
        return dataframe

    route_id_column_name, code_column_name = column_mapping
    completed_columns = set(dataframe.columns)
    if route_id_column_name not in completed_columns or code_column_name not in completed_columns:
        return dataframe

    suffix_condition = (
        F.col(route_id_column_name).isin(sorted(governed_route_ids))
        & F.col(code_column_name).isNotNull()
        & (~F.col(code_column_name).endswith("-D"))
    )
    return dataframe.withColumn(
        code_column_name,
        F.when(suffix_condition, F.concat(F.col(code_column_name), F.lit("-D"))).otherwise(F.col(code_column_name))
    )


def _get_validation_preview_df(dataframe, row_limit):
    if row_limit in (None, ""):
        return dataframe.limit(100)

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
target_config = load_target_config(target_config_path) if target_config_path else {}
route_code_suffix_route_ids = _load_route_code_suffix_route_ids(source_config_path)
databricks_env = runtime_context["databricks_env"]
normalized_load_mode = load_mode.strip().lower()
entity_configs = get_entity_configs(source_config, requested_entity_name, excluded_entity_names=excluded_entity_names)
completion_results = []
completion_failures = []

for configured_entity_name, entity_config in entity_configs:
    entity_name = entity_config.get("entity_name", configured_entity_name)

    try:
        completion_config = entity_config.get("completion", {})
        incremental_config = entity_config.get("incremental", {})
        transform_config = entity_config.get("transform", {})
        if not completion_config.get("enabled", True):
            print({"stage": "completion_skip", "entity_name": entity_name, "reason": "completion.disabled"})
            continue
        if not transform_config.get("enabled", True):
            print({"stage": "completion_skip", "entity_name": entity_name, "reason": "transform.disabled"})
            continue

        transform_temp_view_name = entity_config.get("transform", {}).get("temp_view_name", f"oracle_transform_{entity_name.lower()}_preview")
        transform_global_temp_view_name = entity_config.get("transform", {}).get("global_temp_view_name", transform_temp_view_name)
        tracking_entity_name = str(incremental_config.get("loadtracker_entity_name") or source_config.get("default_entity_name") or entity_name).strip()
        watermark_column = str(incremental_config.get("watermark_column", "")).strip()
        incremental_requested = bool(incremental_config.get("enabled", False))
        incremental_enabled = incremental_requested and normalized_load_mode == "transactional"
        new_watermark = ""
        loadtracker_table_name = ""
        validation_row_limit = completion_config.get("validation_row_limit")
        write_result = {
            "write_enabled": False,
            "write_mode": "",
            "table_name": "",
            "status": "skipped",
            "reason": "target_config_missing" if not target_config else "entity_write_config_missing"
        }

        if incremental_requested and not incremental_enabled:
            print({
                "stage": "completion_incremental_skip",
                "entity_name": entity_name,
                "reason": f"incremental_not_supported_for_load_mode:{normalized_load_mode}"
            })

        print({
            "stage": "completion",
            "entity_name": entity_name,
            "load_mode": load_mode,
            "source_config_path": source_config_path,
            "target_config_path": target_config_path,
            "excluded_entity_names": excluded_entity_names,
            "continue_on_error": continue_on_error,
            "transform_temp_view_name": transform_temp_view_name,
            "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
            "historical_cutoff_type": historical_cutoff_type,
            "historical_cutoff_value": historical_cutoff_value,
            "historical_cutoff_end_value": historical_cutoff_end_value,
            "incremental_enabled": incremental_enabled,
            "tracking_entity_name": tracking_entity_name,
            "watermark_column": watermark_column
        })

        completed_df = _apply_route_code_suffix_rule(
            spark.table(f"global_temp.{transform_global_temp_view_name}"),
            entity_name,
            route_code_suffix_route_ids
        ).persist()
        try:
            validation_preview_df = _get_validation_preview_df(completed_df, validation_row_limit)
            row_count = completed_df.count()

            if incremental_enabled:
                completed_columns = {column_name.lower() for column_name in completed_df.columns}
                if watermark_column.lower() not in completed_columns:
                    raise ValueError(f"Incremental watermark column '{watermark_column}' was not found in completion output for entity '{entity_name}'.")

                new_watermark = completed_df.selectExpr(
                    f"date_format(max(to_timestamp({watermark_column})), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS last_load_timestamp"
                ).first()[0] or ""

            entity_write_config = resolve_entity_write_config(entity_config, target_config, entity_name, required=False) if target_config else get_source_entity_write_config(entity_config)
            if entity_write_config:
                print({
                    "stage": "completion_target_write_request",
                    "entity_name": entity_name,
                    "entity_write_config": entity_write_config,
                    "completed_df_columns": list(completed_df.columns),
                    "completed_df_column_count": len(completed_df.columns),
                    "target_config_present": bool(target_config)
                })
                write_result = write_entity_to_target(completed_df, entity_name, entity_write_config, target_config, databricks_env, row_count=row_count)
                print({
                    "stage": "completion_target_write",
                    "entity_name": entity_name,
                    "write_result": write_result
                })
            else:
                print({
                    "stage": "completion_target_write_skip",
                    "entity_name": entity_name,
                    "reason": write_result["reason"]
                })

            if incremental_enabled:
                if new_watermark:
                    loadtracker_table_name = write_loadtracker_entry(tracking_entity_name, row_count, new_watermark)
                    print({
                        "stage": "completion_monitor_write",
                        "entity_name": entity_name,
                        "tracking_entity_name": tracking_entity_name,
                        "loadtracker_table_name": loadtracker_table_name,
                        "last_load_timestamp": new_watermark,
                        "row_count": row_count
                    })
                else:
                    print({
                        "stage": "completion_monitor_skip",
                        "entity_name": entity_name,
                        "reason": "empty_watermark",
                        "row_count": row_count
                    })

            print(f"Validated {row_count} rows from global temp view 'global_temp.{transform_global_temp_view_name}' for entity '{entity_name}'.")
            if validation_preview_df is not None:
                display(validation_preview_df)
        finally:
            completed_df.unpersist()

        completion_results.append({
            "entity_name": entity_name,
            "transform_temp_view_name": transform_temp_view_name,
            "transform_global_temp_view_name": f"global_temp.{transform_global_temp_view_name}",
            "row_count": row_count,
            "incremental_enabled": incremental_enabled,
            "tracking_entity_name": tracking_entity_name,
            "last_load_timestamp": new_watermark,
            "loadtracker_table_name": loadtracker_table_name,
            "write_enabled": write_result.get("write_enabled", False),
            "write_mode": write_result.get("write_mode", ""),
            "target_table_name": write_result.get("table_name", ""),
            "write_status": write_result.get("status", "skipped"),
            "write_reason": write_result.get("reason", "")
        })

    except Exception as exc:
        failure_details = {
            "stage": "completion_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc)
        }
        completion_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

_display_entity_row_counts("completion", completion_results)

print({
    "stage": "completion_summary",
    "load_mode": load_mode,
    "source_config_path": source_config_path,
    "target_config_path": target_config_path,
    "excluded_entity_names": excluded_entity_names,
    "continue_on_error": continue_on_error,
    "historical_cutoff_type": historical_cutoff_type,
    "historical_cutoff_value": historical_cutoff_value,
    "historical_cutoff_end_value": historical_cutoff_end_value,
    "failure_count": len(completion_failures),
    "entities": completion_results,
    "failures": completion_failures
})

# COMMAND ----------

