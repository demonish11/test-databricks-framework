# Databricks notebook source
import copy
import json
import os
import re

# COMMAND ----------

def load_json_config(json_string_or_file_path):
    if not json_string_or_file_path:
        raise ValueError("A config path or JSON string is required.")

    config_input = json_string_or_file_path.strip().lstrip("\ufeff")

    if os.path.exists(config_input) and config_input.lower().endswith(".json"):
        with open(config_input, "r", encoding="utf-8-sig") as config_file:
            return json.load(config_file)

    try:
        return json.loads(config_input)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Config input is neither a readable JSON file nor valid JSON text: {exc}") from exc


def resolve_env_value(value, environment, field_name):
    if isinstance(value, dict):
        if environment in value:
            return value[environment]
        raise KeyError(f"Missing environment '{environment}' for '{field_name}'.")
    return value


def normalize_entity_name_list(entity_names):
    if entity_names is None:
        return []
    if isinstance(entity_names, str):
        return [name.strip() for name in entity_names.split(",") if name.strip()]
    return [str(name).strip() for name in entity_names if str(name).strip()]


def _escape_sql_literal(value):
    return str(value).replace("'", "''")


def _require_value(value, name):
    if value is None or str(value).strip() == "":
        raise ValueError(f"Required value '{name}' was not provided.")
    return str(value).strip()


def get_loadtracker_table_name(required=False):
    catalog = os.getenv("Loadtracker_Target_Catalog", "").strip()
    schema = os.getenv("Loadtracker_Target_Schema", "").strip()
    table = os.getenv("Loadtracker_Target_Table", "").strip()

    if catalog and schema and table:
        return f"{catalog}.{schema}.{table}"

    if required:
        raise ValueError("Loadtracker target catalog, schema, and table must all be configured.")
    return ""


def ensure_loadtracker_table():
    loadtracker_table_name = get_loadtracker_table_name(required=True)
    catalog = os.getenv("Loadtracker_Target_Catalog", "").strip()
    schema = os.getenv("Loadtracker_Target_Schema", "").strip()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {loadtracker_table_name} (
            TABLE_NAME STRING,
            LOAD_DATE TIMESTAMP,
            LOAD_COMPLETE_FLAG STRING,
            ROW_COUNT INT,
            LAST_LOAD_TIMESTAMP TIMESTAMP
        )
        """
    )
    return loadtracker_table_name


def format_sql_server_watermark(timestamp_value):
    if timestamp_value is None:
        raise ValueError("Watermark timestamp is required for incremental Azure SQL extraction.")

    if hasattr(timestamp_value, "strftime"):
        return timestamp_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    text = str(timestamp_value).strip()
    if len(text) >= 26 and text[19] == ".":
        return text[:23]
    return text


def get_last_successful_load_timestamp(
    tracking_entity_name,
    default_watermark="1900-01-01 00:00:00.000",
    fallback_table_fqn=None,
    fallback_watermark_column=None,
):
    loadtracker_table_name = get_loadtracker_table_name(required=False)
    escaped_tracking_entity_name = _escape_sql_literal(tracking_entity_name)

    if loadtracker_table_name:
        try:
            tracker_select_sql = f"""
                SELECT MAX(LAST_LOAD_TIMESTAMP)
                FROM {loadtracker_table_name}
                WHERE LOAD_COMPLETE_FLAG = 'Y'
                  AND TABLE_NAME = '{escaped_tracking_entity_name}'
            """
            last_load_timestamp = spark.sql(tracker_select_sql).first()[0]
            if last_load_timestamp is not None:
                return format_sql_server_watermark(last_load_timestamp)
        except Exception as exc:
            print(
                {
                    "stage": "loadtracker_read_skip",
                    "tracking_entity_name": tracking_entity_name,
                    "reason": str(exc),
                }
            )

    if fallback_table_fqn and fallback_watermark_column:
        try:
            fallback_sql = f"""
                SELECT MAX(to_timestamp({fallback_watermark_column}))
                FROM {fallback_table_fqn}
            """
            fallback_timestamp = spark.sql(fallback_sql).first()[0]
            if fallback_timestamp is not None:
                return format_sql_server_watermark(fallback_timestamp)
        except Exception as exc:
            print(
                {
                    "stage": "loadtracker_fallback_skip",
                    "tracking_entity_name": tracking_entity_name,
                    "fallback_table_fqn": fallback_table_fqn,
                    "reason": str(exc),
                }
            )

    return default_watermark


def write_loadtracker_entry(tracking_entity_name, row_count, last_load_timestamp):
    if last_load_timestamp is None or str(last_load_timestamp).strip() == "":
        return ""

    loadtracker_table_name = ensure_loadtracker_table()
    escaped_tracking_entity_name = _escape_sql_literal(tracking_entity_name)
    watermark = format_sql_server_watermark(last_load_timestamp)
    if len(watermark) == 19:
        watermark = f"{watermark}.000"
    escaped_last_load_timestamp = _escape_sql_literal(watermark)
    row_count_value = int(row_count)

    sql_update = f"""
        INSERT INTO {loadtracker_table_name}
        SELECT
            '{escaped_tracking_entity_name}' AS TABLE_NAME,
            CURRENT_TIMESTAMP AS LOAD_DATE,
            'Y' AS LOAD_COMPLETE_FLAG,
            {row_count_value} AS ROW_COUNT,
            to_timestamp('{escaped_last_load_timestamp}', 'yyyy-MM-dd HH:mm:ss.SSS') AS LAST_LOAD_TIMESTAMP
    """
    spark.sql(sql_update)
    return loadtracker_table_name


def build_incremental_extract_query(base_query, watermark_column, last_load_timestamp):
    if not watermark_column:
        raise ValueError("Incremental loads require incremental.watermark_column.")

    if last_load_timestamp is None or str(last_load_timestamp).strip() == "":
        return base_query

    watermark = format_sql_server_watermark(last_load_timestamp)
    escaped_last_load_timestamp = _escape_sql_literal(watermark)
    return (
        f"SELECT * FROM ({base_query}) INCREMENTAL_SOURCE "
        f"WHERE {watermark_column} > CAST('{escaped_last_load_timestamp}' AS datetime2)"
    )


def load_target_config(target_config_reference):
    reference = _require_value(target_config_reference, "target_config_path")
    reference = reference.lstrip("\ufeff")
    if reference.startswith("{"):
        return json.loads(reference)
    with open(reference, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def get_runtime_context(databricks_env=None):
    runtime_env = databricks_env or os.getenv("Databricks_Env", "dev")
    runtime_env = runtime_env.strip().lower()

    if runtime_env not in {"dev", "tst", "stg", "prd"}:
        raise ValueError(f"Unsupported Databricks_Env '{runtime_env}'.")

    return {
        "databricks_env": runtime_env,
        "daily_source_config_path": os.getenv("Daily_Source_Config_Path", ""),
        "transactional_source_config_path": os.getenv("Transactional_Source_Config_Path", ""),
        "target_config_path": os.getenv("Target_Config_Path", ""),
        "loadtracker_target_catalog": os.getenv("Loadtracker_Target_Catalog", "").strip(),
        "loadtracker_target_schema": os.getenv("Loadtracker_Target_Schema", "").strip(),
        "loadtracker_target_table": os.getenv("Loadtracker_Target_Table", "").strip(),
    }


def _merge_entity_config_values(base_value, override_value):
    if isinstance(base_value, dict) and isinstance(override_value, dict):
        merged_value = copy.deepcopy(base_value)
        for key, value in override_value.items():
            if key in merged_value:
                merged_value[key] = _merge_entity_config_values(merged_value[key], value)
            else:
                merged_value[key] = copy.deepcopy(value)
        return merged_value
    return copy.deepcopy(override_value)


def resolve_inherited_entity_config(entity_configs, entity_name, resolution_stack=None):
    if entity_name not in entity_configs:
        available_entities = ", ".join(entity_configs.keys())
        raise KeyError(
            f"Entity '{entity_name}' was not found in source config. Available entities: {available_entities}"
        )

    entity_config = entity_configs[entity_name]
    if not isinstance(entity_config, dict):
        return entity_config

    inherited_entity_name = str(entity_config.get("inherits_from_entity_name", "")).strip()
    if not inherited_entity_name:
        resolved_config = copy.deepcopy(entity_config)
        resolved_config.setdefault("entity_name", entity_name)
        return resolved_config

    resolution_stack = list(resolution_stack or [])
    if entity_name in resolution_stack:
        cycle_path = " -> ".join(resolution_stack + [entity_name])
        raise ValueError(f"Circular entity inheritance detected: {cycle_path}")

    base_config = resolve_inherited_entity_config(
        entity_configs, inherited_entity_name, resolution_stack + [entity_name]
    )
    override_config = copy.deepcopy(entity_config)
    override_config.pop("inherits_from_entity_name", None)
    resolved_config = _merge_entity_config_values(base_config, override_config)
    resolved_config.setdefault("entity_name", entity_name)
    return resolved_config


def _get_entity_dependency_names(entity_configs, entity_name):
    entity_config = resolve_inherited_entity_config(entity_configs, entity_name)
    if not isinstance(entity_config, dict):
        return []
    return normalize_entity_name_list(entity_config.get("depends_on_entity_names"))


def _is_dependency_only_entity(entity_config):
    return bool((entity_config or {}).get("dependency_only", False))


def _resolve_requested_entity_names(entity_configs, requested_entity_name, excluded_entity_name_set):
    resolved_entity_names = []
    visited_entity_names = set()
    recursion_stack = set()

    def _visit(entity_name):
        if entity_name in excluded_entity_name_set:
            raise ValueError(
                f"Entity '{entity_name}' is excluded but required by requested entity '{requested_entity_name}'."
            )
        if entity_name not in entity_configs:
            available_entities = ", ".join(entity_configs.keys())
            raise KeyError(
                f"Entity '{entity_name}' was not found in source config. Available entities: {available_entities}"
            )
        if entity_name in recursion_stack:
            cycle_path = " -> ".join(list(recursion_stack) + [entity_name])
            raise ValueError(f"Circular entity dependency detected: {cycle_path}")
        if entity_name in visited_entity_names:
            return

        recursion_stack.add(entity_name)
        for dependency_entity_name in _get_entity_dependency_names(entity_configs, entity_name):
            _visit(dependency_entity_name)
        recursion_stack.remove(entity_name)
        visited_entity_names.add(entity_name)
        resolved_entity_names.append(entity_name)

    _visit(requested_entity_name)
    return resolved_entity_names


def parse_select_column_names(query):
    """Derive column names from a simple SELECT ... FROM query (no invented schema)."""
    if not query:
        return []
    match = re.search(r"SELECT\s+(?:TOP\s*\(\s*\d+\s*\)\s+)?(.+?)\s+FROM\s+", query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    select_list = match.group(1)
    columns = []
    for raw_item in select_list.split(","):
        item = raw_item.strip().strip("[]").strip()
        if not item or item == "*":
            continue
        if " as " in item.lower():
            item = re.split(r"\s+as\s+", item, flags=re.IGNORECASE)[-1].strip().strip("[]")
        else:
            item = item.split(".")[-1].strip().strip("[]")
        if item:
            columns.append(item)
    return columns


def validate_entity_schema(entity_config):
    """Ensure configured schema/PK match the extract SQL column list."""
    schema = entity_config.get("schema") or {}
    schema_columns = [str(col).strip() for col in schema.get("columns") or [] if str(col).strip()]
    primary_keys = [
        str(col).strip()
        for col in (entity_config.get("target") or {}).get("primary_key_columns") or []
        if str(col).strip()
    ]
    query_by_env = ((entity_config.get("extract") or {}).get("query_by_env") or {})
    sample_query = next(iter(query_by_env.values()), "")
    query_columns = parse_select_column_names(sample_query)

    if schema_columns and query_columns and schema_columns != query_columns:
        raise ValueError(
            f"Entity '{entity_config.get('entity_name')}' schema.columns do not match extract SQL columns."
        )
    missing_keys = [key for key in primary_keys if schema_columns and key not in schema_columns]
    if missing_keys:
        raise ValueError(
            f"Entity '{entity_config.get('entity_name')}' primary key {missing_keys} not present in schema.columns."
        )
    return {
        "schema_columns": schema_columns,
        "query_columns": query_columns,
        "primary_key_columns": primary_keys,
    }


def get_entity_configs(
    source_config,
    requested_entity_name="",
    excluded_entity_names=None,
    exclude_dependency_only=False,
):
    entity_configs = source_config.get("entities")
    normalized_entity_name = (requested_entity_name or "").strip()
    excluded_entity_name_set = set(normalize_entity_name_list(excluded_entity_names))

    if not entity_configs:
        entity_name = normalized_entity_name or source_config.get("entity_name", "azure_sql_extract")
        if entity_name in excluded_entity_name_set:
            return []
        return [(entity_name, source_config)]

    if normalized_entity_name:
        if normalized_entity_name in excluded_entity_name_set:
            return []
        resolved_entity_name_set = set(
            _resolve_requested_entity_names(entity_configs, normalized_entity_name, excluded_entity_name_set)
        )
        return [
            (entity_name, resolve_inherited_entity_config(entity_configs, entity_name))
            for entity_name in entity_configs.keys()
            if entity_name in resolved_entity_name_set and entity_name not in excluded_entity_name_set
        ]

    ordered_entity_configs = [
        (entity_name, resolve_inherited_entity_config(entity_configs, entity_name))
        for entity_name in entity_configs.keys()
        if entity_name not in excluded_entity_name_set
        and not (
            exclude_dependency_only
            and _is_dependency_only_entity(resolve_inherited_entity_config(entity_configs, entity_name))
        )
    ]
    default_entity_name = source_config.get("default_entity_name", "").strip()
    if default_entity_name in entity_configs:
        ordered_entity_configs.sort(key=lambda item: item[0] != default_entity_name)
    return ordered_entity_configs
