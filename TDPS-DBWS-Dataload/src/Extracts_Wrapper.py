# Databricks notebook source
# Databricks notebook source
import os

dbutils.widgets.text("load_mode", "daily")
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

# MAGIC %run ./utils/jdbc_utils

# COMMAND ----------

import re


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
        "initial": "Initial_Load_Source_Config_Path",
        "historical": "Historical_Source_Config_Path"
    }

    fallback_env_var = source_config_env_by_mode.get(load_mode.strip().lower(), "Daily_Source_Config_Path")
    return os.getenv(fallback_env_var, "").strip()


def _get_validation_preview_df(dataframe, row_limit):
    if row_limit in (None, ""):
        return dataframe

    normalized_row_limit = int(row_limit)
    if normalized_row_limit <= 0:
        return None

    return dataframe.limit(normalized_row_limit)


def _display_entity_row_counts(stage_name, results):
    if not results:
        print(f"No entities processed during {stage_name}.")
        return

    summary_rows = [(result["entity_name"], result["row_count"]) for result in results]
    print(f"{stage_name.title()} row counts by entity:")
    display(spark.createDataFrame(summary_rows, "entity_name STRING, row_count BIGINT").orderBy("entity_name"))


INITIAL_LOAD_48_MONTH_FILTER_PATTERN = r"(?:\b\w+\.)?CREATE_TS\s*>=\s*ADD_MONTHS\(TRUNC\(SYSDATE\),\s*-48\)"
SOURCE_ENTITY_FILTER_PUSH_DOWN_MAX_VALUES = 1000


def _should_preserve_initial_load_48_month_filter(entity_config):
    extract_config = entity_config.get("extract", {})
    return bool(extract_config.get("preserve_initial_load_48_month_filter", False))


def _remove_initial_load_48_month_filter(query, entity_name, entity_config, load_mode):
    if str(load_mode).strip().lower() != "initial":
        return query

    if _should_preserve_initial_load_48_month_filter(entity_config):
        return query

    updated_query = re.sub(rf"\s+WHERE\s+{INITIAL_LOAD_48_MONTH_FILTER_PATTERN}", "", query)
    updated_query = re.sub(rf"\s+AND\s+{INITIAL_LOAD_48_MONTH_FILTER_PATTERN}", "", updated_query)

    if updated_query != query:
        print({
            "stage": "extract_initial_load_date_filter_removed",
            "entity_name": entity_name
        })

    return updated_query


def _resolve_route_exclusion_config_path(source_config_path):
    if not source_config_path:
        return ""

    return os.path.join(os.path.dirname(source_config_path), "route_exclusion_route_ids.json")


def _load_route_exclusion_route_ids(source_config_path):
    config_path = _resolve_route_exclusion_config_path(source_config_path)
    if not config_path or not os.path.exists(config_path):
        return []

    config = load_json_config(config_path)
    route_ids = []
    for route_id in config.get("route_ids") or []:
        route_id_text = str(route_id).strip()
        if route_id_text:
            route_ids.append(int(route_id_text))

    return sorted(set(route_ids))


def _resolve_trip_exclusion_config_path(source_config_path):
    if not source_config_path:
        return ""

    return os.path.join(os.path.dirname(source_config_path), "trip_exclusion_trip_codes.json")


def _load_trip_exclusion_config(source_config_path):
    config_path = _resolve_trip_exclusion_config_path(source_config_path)
    if not config_path or not os.path.exists(config_path):
        return {'trip_codes': [], 'trip_ids': []}

    config = load_json_config(config_path)
    trip_codes = []
    for trip_code in config.get("trip_codes") or []:
        trip_code_text = str(trip_code)
        if trip_code_text != "":
            trip_codes.append(trip_code_text)

    trip_ids = []
    for trip_id in config.get("trip_ids") or []:
        trip_id_text = str(trip_id).strip()
        if trip_id_text:
            trip_ids.append(int(trip_id_text))

    return {'trip_codes': sorted(set(trip_codes)), 'trip_ids': sorted(set(trip_ids))}


def _resolve_entity_route_id_filter_config_path(source_config_path, extract_config):
    configured_path = str(extract_config.get("route_id_filter_config_path", "")).strip()
    if not configured_path:
        return ""

    if os.path.isabs(configured_path) or not source_config_path:
        return configured_path

    return os.path.join(os.path.dirname(source_config_path), configured_path)


def _resolve_entity_route_id_filter_source_temp_view(source_config, source_entity_name):
    entity_configs = source_config.get("entities", {})
    if source_entity_name not in entity_configs:
        available_entities = ", ".join(entity_configs.keys())
        raise KeyError(
            f"Route ID filter source entity '{source_entity_name}' was not found in source config. Available entities: {available_entities}"
        )

    source_entity_config = resolve_inherited_entity_config(entity_configs, source_entity_name)
    source_extract_config = source_entity_config.get("extract", {})
    return str(source_extract_config.get("temp_view_name", f"oracle_extract_{source_entity_name.lower()}_preview")).strip()


def _load_entity_route_id_filter_ids(config_path):
    if not config_path or not os.path.exists(config_path):
        raise ValueError(f"Route ID filter config '{config_path}' was not found.")

    if config_path.lower().endswith(".json"):
        config = load_json_config(config_path)
        configured_route_ids = config.get("route_ids") if isinstance(config, dict) else config
    else:
        with open(config_path, "r", encoding="utf-8") as config_file:
            configured_route_ids = config_file.readlines()

    route_ids = []
    for configured_route_id in configured_route_ids or []:
        route_id_text = str(configured_route_id).strip().rstrip(",")
        if route_id_text:
            route_ids.append(int(route_id_text))

    resolved_route_ids = sorted(set(route_ids))
    if not resolved_route_ids:
        raise ValueError(f"Route ID filter config '{config_path}' did not contain any Route IDs.")

    return resolved_route_ids


def _load_entity_route_id_filter_ids_from_source_entity(source_config, source_entity_name, source_column_name):
    temp_view_name = _resolve_entity_route_id_filter_source_temp_view(source_config, source_entity_name)
    source_df = spark.table(temp_view_name)
    resolved_column_name = next((column_name for column_name in source_df.columns if column_name.lower() == source_column_name.lower()), "")
    if not resolved_column_name:
        available_columns = ", ".join(source_df.columns)
        raise ValueError(
            f"Route ID filter source entity '{source_entity_name}' temp view '{temp_view_name}' does not contain column '{source_column_name}'. Available columns: {available_columns}"
        )

    route_id_count = source_df.select(resolved_column_name).where(f"{resolved_column_name} IS NOT NULL").dropDuplicates().count()
    return {
        "temp_view_name": temp_view_name,
        "column_name": resolved_column_name,
        "route_id_count": route_id_count
    }


def _build_entity_route_id_filter(entity_name, entity_config, source_config_path, source_config):
    extract_config = entity_config.get("extract", {})
    route_id_filter_mode = str(extract_config.get("route_id_filter_mode", "")).strip().lower()
    if not route_id_filter_mode:
        return None

    if route_id_filter_mode not in {"include", "exclude"}:
        raise ValueError(
            f"Entity '{entity_name}' has unsupported extract.route_id_filter_mode '{route_id_filter_mode}'. "
            "Supported values are 'include' and 'exclude'."
        )

    route_id_column_name = str(extract_config.get("route_id_filter_column_name", "")).strip()
    if not route_id_column_name:
        raise ValueError(f"Entity '{entity_name}' configured a Route ID filter but did not set extract.route_id_filter_column_name.")

    config_path = ""
    source_entity_name = str(extract_config.get("route_id_filter_source_entity_name", "")).strip()
    source_column_name = str(extract_config.get("route_id_filter_source_column_name", "")).strip()
    source_temp_view_name = ""
    resolved_source_column_name = ""
    route_id_count = 0
    apply_in_spark = False

    if source_entity_name:
        if not source_column_name:
            raise ValueError(
                f"Entity '{entity_name}' configured extract.route_id_filter_source_entity_name but did not set extract.route_id_filter_source_column_name."
            )

        source_filter_details = _load_entity_route_id_filter_ids_from_source_entity(source_config, source_entity_name, source_column_name)
        source_temp_view_name = source_filter_details["temp_view_name"]
        resolved_source_column_name = source_filter_details["column_name"]
        route_id_count = int(source_filter_details.get("route_id_count", 0))
        apply_in_spark = True
        route_id_clause = ""
    else:
        config_path = _resolve_entity_route_id_filter_config_path(source_config_path, extract_config)
        route_ids = _load_entity_route_id_filter_ids(config_path)
        route_id_operator = "IN" if route_id_filter_mode == "include" else "NOT IN"
        route_id_clause = _build_numeric_membership_clause(route_id_column_name, route_ids, route_id_operator)
        route_id_count = len(route_ids)

    return {
        "column_name": route_id_column_name,
        "mode": route_id_filter_mode,
        "config_path": config_path,
        "source_entity_name": source_entity_name,
        "source_temp_view_name": source_temp_view_name,
        "source_column_name": resolved_source_column_name,
        "route_id_count": route_id_count,
        "apply_in_spark": apply_in_spark,
        "clause": route_id_clause
    }


def _format_numeric_in_list(values):
    return ", ".join(str(int(value)) for value in values)


def _chunk_list(values, chunk_size):
    for start_index in range(0, len(values), chunk_size):
        yield values[start_index:start_index + chunk_size]


def _build_numeric_membership_clause(column_name, values, operator="IN", chunk_size=1000):
    normalized_values = [str(int(value)) for value in values]
    normalized_operator = str(operator).strip().upper()
    if normalized_operator not in {"IN", "NOT IN"}:
        raise ValueError(f"Unsupported membership operator '{operator}' for column '{column_name}'.")

    if not normalized_values:
        return "1 = 0" if normalized_operator == "IN" else "1 = 1"

    join_operator = " OR " if normalized_operator == "IN" else " AND "
    chunked_clauses = [
        f"{column_name} {normalized_operator} ({', '.join(value_chunk)})"
        for value_chunk in _chunk_list(normalized_values, chunk_size)
    ]

    if len(chunked_clauses) == 1:
        return chunked_clauses[0]

    return "(" + join_operator.join(chunked_clauses) + ")"


def _format_string_in_list(values):
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)


ROLE_ENTITY_NAMES = {
    "route": ["Route", "RouteInitialLoad", "RouteSource"],
    "route_stop": ["RouteStop", "RouteStopSource"],
    "route_stop_activity": ["RouteStopActivity", "RouteStopActivitySource"],
    "transportation_order": ["TransportationOrder", "TransportationOrderSource"],
    "transportation_order_line": ["TransportationOrderLine", "TransportationOrderLineSource"],
    "trip": ["Trip", "TripSource"],
    "trip_activity": ["TripActivity", "TripActivitySource"],
    "trip_stop": ["TripStop", "TripStopSource"],
    "trip_stop_activity": ["TripStopActivity", "TripStopActivitySource"],
    "trip_stop_delay": ["TripStopDelay", "TripStopDelaySource"]
}


def _find_entity_config(source_config, candidate_entity_names):
    for entity_config in source_config.get("entities", {}).values():
        entity_name = str(entity_config.get("entity_name", "")).strip()
        if entity_name in candidate_entity_names:
            return entity_config

    return None


def _get_extracted_entity_result(extracted_entities_by_name, candidate_entity_names):
    for entity_name in candidate_entity_names:
        entity_result = extracted_entities_by_name.get(entity_name)
        if entity_result:
            return entity_result

    return None


def _read_source_keyset(entity_config, databricks_env, target_config, query):
    jdbc_options = build_jdbc_options(entity_config, databricks_env, target_config=target_config)
    for option_name in ("partitionColumn", "lowerBound", "upperBound", "numPartitions"):
        jdbc_options.pop(option_name, None)

    return (
        spark.read
        .format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", f"({query}) ROUTE_EXCLUSION_KEYS")
        .load()
        .dropDuplicates()
    )


def _apply_entity_source_filter_in_spark(dataframe, route_id_filter):
    source_temp_view_name = route_id_filter.get("source_temp_view_name", "")
    source_column_name = route_id_filter.get("source_column_name", "")
    target_column_name = route_id_filter.get("column_name", "")
    filter_mode = str(route_id_filter.get("mode", "include")).strip().lower()

    source_df = spark.table(source_temp_view_name)
    source_join_column = next((column_name for column_name in source_df.columns if column_name.lower() == source_column_name.lower()), "")
    if not source_join_column:
        available_columns = ", ".join(source_df.columns)
        raise ValueError(f"Source filter temp view '{source_temp_view_name}' does not contain column '{source_column_name}'. Available columns: {available_columns}")

    target_join_column = next((column_name for column_name in dataframe.columns if column_name.lower() == target_column_name.lower()), "")
    if not target_join_column:
        available_columns = ", ".join(dataframe.columns)
        raise ValueError(f"Extract dataframe does not contain filter column '{target_column_name}'. Available columns: {available_columns}")

    filter_ids_df = source_df.selectExpr(f"{source_join_column} AS __filter_id").where("__filter_id IS NOT NULL").dropDuplicates()
    join_condition = dataframe[target_join_column] == filter_ids_df["__filter_id"]
    join_type = "left_semi" if filter_mode == "include" else "left_anti"
    return dataframe.join(filter_ids_df, join_condition, join_type)


def _update_extract_views(entity_result, dataframe, reason):
    temp_view_name = entity_result["temp_view_name"]
    global_temp_view_name = entity_result["global_temp_view_name"].split(".", 1)[1]
    previous_row_count = entity_result.get("row_count")
    filtered_row_count = None if previous_row_count is None else dataframe.count()

    dataframe.createOrReplaceTempView(temp_view_name)
    dataframe.createOrReplaceGlobalTempView(global_temp_view_name)
    entity_result["row_count"] = filtered_row_count

    print({
        "stage": "extract_route_exclusion",
        "entity_name": entity_result["entity_name"],
        "reason": reason,
        "previous_row_count": previous_row_count,
        "filtered_row_count": filtered_row_count
    })


def _apply_route_exclusion_filters(extract_results, source_config, databricks_env, target_config, excluded_route_ids):
    if not extract_results or not excluded_route_ids:
        return

    extracted_entities_by_name = {result["entity_name"]: result for result in extract_results}
    route_id_sql = _format_numeric_in_list(excluded_route_ids)
    route_id_membership_clause = _build_numeric_membership_clause("ROUTE_ID", excluded_route_ids, "IN")

    route_stop_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["route_stop"])
    route_stop_activity_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["route_stop_activity"])
    trip_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["trip"])
    trip_stop_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["trip_stop"])

    excluded_route_stop_ids_df = None
    excluded_transportation_order_ids_df = None
    excluded_trip_ids_df = None
    excluded_trip_stop_ids_df = None

    if route_stop_config:
        excluded_route_stop_ids_df = _read_source_keyset(
            route_stop_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(RS_ID AS DECIMAL(38,0)) AS RS_ID FROM DRVPAY.ROUTE_STOP WHERE {route_id_membership_clause}"
        )

    if route_stop_activity_config:
        excluded_transportation_order_ids_df = _read_source_keyset(
            route_stop_activity_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TO_ID AS DECIMAL(38,0)) AS TO_ID FROM DRVPAY.ROUTE_STOP_ACTVTY WHERE TO_ID IS NOT NULL AND RS_ID IN (SELECT RS_ID FROM DRVPAY.ROUTE_STOP WHERE {route_id_membership_clause})"
        )

    if trip_config:
        excluded_trip_ids_df = _read_source_keyset(
            trip_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TRIP_ID AS DECIMAL(38,0)) AS TRIP_ID FROM DRVPAY.TRIP START WITH {route_id_membership_clause} CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID"
        )

    if trip_stop_config:
        excluded_trip_stop_ids_df = _read_source_keyset(
            trip_stop_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TS_ID AS DECIMAL(38,0)) AS TS_ID FROM DRVPAY.TRIP_STOP WHERE TRIP_ID IN (SELECT TRIP_ID FROM DRVPAY.TRIP START WITH {route_id_membership_clause} CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID)"
        )

    route_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["route"])
    if route_result:
        route_df = spark.table(route_result["temp_view_name"]).where(f"ROUTE_ID NOT IN ({route_id_sql})")
        _update_extract_views(route_result, route_df, "route_id")

    route_stop_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["route_stop"])
    if route_stop_result:
        route_stop_df = spark.table(route_stop_result["temp_view_name"]).where(f"ROUTE_ID NOT IN ({route_id_sql})")
        _update_extract_views(route_stop_result, route_stop_df, "route_id")

    route_stop_activity_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["route_stop_activity"])
    if route_stop_activity_result and excluded_route_stop_ids_df is not None:
        route_stop_activity_df = spark.table(route_stop_activity_result["temp_view_name"]).join(excluded_route_stop_ids_df, "RS_ID", "left_anti")
        _update_extract_views(route_stop_activity_result, route_stop_activity_df, "route_stop_rs_id")

    transportation_order_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["transportation_order"])
    if transportation_order_result and excluded_transportation_order_ids_df is not None:
        transportation_order_df = spark.table(transportation_order_result["temp_view_name"]).join(excluded_transportation_order_ids_df, "TO_ID", "left_anti")
        _update_extract_views(transportation_order_result, transportation_order_df, "route_stop_activity_to_id")

    transportation_order_line_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["transportation_order_line"])
    if transportation_order_line_result and excluded_transportation_order_ids_df is not None:
        transportation_order_line_df = spark.table(transportation_order_line_result["temp_view_name"]).join(excluded_transportation_order_ids_df, "TO_ID", "left_anti")
        _update_extract_views(transportation_order_line_result, transportation_order_line_df, "transportation_order_to_id")

    trip_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip"])
    if trip_result and excluded_trip_ids_df is not None:
        trip_df = spark.table(trip_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_result, trip_df, "trip_id_hierarchy")

    trip_activity_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_activity"])
    if trip_activity_result and excluded_trip_ids_df is not None:
        trip_activity_df = spark.table(trip_activity_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_activity_result, trip_activity_df, "trip_id_hierarchy")

    trip_stop_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop"])
    if trip_stop_result and excluded_trip_ids_df is not None:
        trip_stop_df = spark.table(trip_stop_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_stop_result, trip_stop_df, "trip_id_hierarchy")

    trip_stop_activity_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop_activity"])
    if trip_stop_activity_result and excluded_trip_stop_ids_df is not None:
        trip_stop_activity_df = spark.table(trip_stop_activity_result["temp_view_name"]).join(excluded_trip_stop_ids_df, "TS_ID", "left_anti")
        _update_extract_views(trip_stop_activity_result, trip_stop_activity_df, "trip_stop_ts_id")

    trip_stop_delay_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop_delay"])
    if trip_stop_delay_result and excluded_trip_stop_ids_df is not None:
        trip_stop_delay_df = spark.table(trip_stop_delay_result["temp_view_name"]).join(excluded_trip_stop_ids_df, "TS_ID", "left_anti")
        _update_extract_views(trip_stop_delay_result, trip_stop_delay_df, "trip_stop_ts_id")


def _apply_trip_exclusion_filters(extract_results, source_config, databricks_env, target_config, trip_exclusion_config):
    excluded_trip_codes = trip_exclusion_config.get('trip_codes') or []
    excluded_trip_ids = trip_exclusion_config.get('trip_ids') or []
    if not extract_results or (not excluded_trip_codes and not excluded_trip_ids):
        return

    extracted_entities_by_name = {result["entity_name"]: result for result in extract_results}
    trip_code_sql = _format_string_in_list(excluded_trip_codes) if excluded_trip_codes else ''
    trip_id_membership_clause = _build_numeric_membership_clause('TRIP_ID', excluded_trip_ids, 'IN') if excluded_trip_ids else ''

    trip_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["trip"])
    trip_stop_config = _find_entity_config(source_config, ROLE_ENTITY_NAMES["trip_stop"])

    excluded_trip_ids_df = None
    excluded_trip_stop_ids_df = None

    if trip_config and excluded_trip_codes:
        excluded_trip_ids_df = _read_source_keyset(
            trip_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TRIP_ID AS DECIMAL(38,0)) AS TRIP_ID FROM DRVPAY.TRIP START WITH TRIP_CD IN ({trip_code_sql}) CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID"
        )
    elif trip_config and excluded_trip_ids:
        excluded_trip_ids_df = _read_source_keyset(
            trip_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TRIP_ID AS DECIMAL(38,0)) AS TRIP_ID FROM DRVPAY.TRIP START WITH {trip_id_membership_clause} CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID"
        )

    if trip_stop_config and excluded_trip_codes:
        excluded_trip_stop_ids_df = _read_source_keyset(
            trip_stop_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TS_ID AS DECIMAL(38,0)) AS TS_ID FROM DRVPAY.TRIP_STOP WHERE TRIP_ID IN (SELECT TRIP_ID FROM DRVPAY.TRIP START WITH TRIP_CD IN ({trip_code_sql}) CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID)"
        )
    elif trip_stop_config and excluded_trip_ids:
        excluded_trip_stop_ids_df = _read_source_keyset(
            trip_stop_config,
            databricks_env,
            target_config,
            f"SELECT DISTINCT CAST(TS_ID AS DECIMAL(38,0)) AS TS_ID FROM DRVPAY.TRIP_STOP WHERE TRIP_ID IN (SELECT TRIP_ID FROM DRVPAY.TRIP START WITH {trip_id_membership_clause} CONNECT BY NOCYCLE PRIOR TRIP_ID = PARENT_TRIP_ID)"
        )

    trip_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip"])
    if trip_result and excluded_trip_ids_df is not None:
        trip_df = spark.table(trip_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_result, trip_df, "tripcode_trip_id")

    trip_activity_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_activity"])
    if trip_activity_result and excluded_trip_ids_df is not None:
        trip_activity_df = spark.table(trip_activity_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_activity_result, trip_activity_df, "tripcode_trip_id")

    trip_stop_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop"])
    if trip_stop_result and excluded_trip_ids_df is not None:
        trip_stop_df = spark.table(trip_stop_result["temp_view_name"]).join(excluded_trip_ids_df, "TRIP_ID", "left_anti")
        _update_extract_views(trip_stop_result, trip_stop_df, "tripcode_trip_id")

    trip_stop_activity_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop_activity"])
    if trip_stop_activity_result and excluded_trip_stop_ids_df is not None:
        trip_stop_activity_df = spark.table(trip_stop_activity_result["temp_view_name"]).join(excluded_trip_stop_ids_df, "TS_ID", "left_anti")
        _update_extract_views(trip_stop_activity_result, trip_stop_activity_df, "tripcode_ts_id")

    trip_stop_delay_result = _get_extracted_entity_result(extracted_entities_by_name, ROLE_ENTITY_NAMES["trip_stop_delay"])
    if trip_stop_delay_result and excluded_trip_stop_ids_df is not None:
        trip_stop_delay_df = spark.table(trip_stop_delay_result["temp_view_name"]).join(excluded_trip_stop_ids_df, "TS_ID", "left_anti")
        _update_extract_views(trip_stop_delay_result, trip_stop_delay_df, "tripcode_ts_id")


runtime_context = get_runtime_context()
load_mode = _resolve_widget_value("load_mode") or "daily"
source_config_path = _resolve_source_config_path(load_mode)
target_config_path = _resolve_widget_value("target_config_path", "Target_Config_Path")
requested_entity_name = _resolve_widget_value("entity_name")
excluded_entity_names = _resolve_widget_value("excluded_entity_names")
continue_on_error = _resolve_bool_widget_value("continue_on_error")
historical_cutoff_type = _resolve_widget_value("historical_cutoff_type")
historical_cutoff_value = _resolve_widget_value("historical_cutoff_value")
historical_cutoff_end_value = _resolve_widget_value("historical_cutoff_end_value")

source_config = load_json_config(source_config_path)
route_exclusion_route_ids = _load_route_exclusion_route_ids(source_config_path)
trip_exclusion_config = _load_trip_exclusion_config(source_config_path)
target_config = load_target_config(target_config_path) if target_config_path else {}
databricks_env = runtime_context["databricks_env"]
normalized_load_mode = load_mode.strip().lower()
entity_configs = get_entity_configs(source_config, requested_entity_name, excluded_entity_names=excluded_entity_names)
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
        base_query = _remove_initial_load_48_month_filter(base_query, entity_name, entity_config, load_mode)
        validation_row_limit = extract_config.get("validation_row_limit")
        skip_row_count = bool(extract_config.get("skip_row_count", False))
        extract_query = base_query
        temp_view_name = extract_config.get("temp_view_name", f"oracle_extract_{entity_name.lower()}_preview")
        global_temp_view_name = extract_config.get("global_temp_view_name", temp_view_name)
        jdbc_options = build_jdbc_options(entity_config, databricks_env, target_config=target_config)
        tracking_entity_name = str(incremental_config.get("loadtracker_entity_name") or source_config.get("default_entity_name") or entity_name).strip()
        watermark_column = str(incremental_config.get("watermark_column", "")).strip()
        incremental_requested = bool(incremental_config.get("enabled", False))
        incremental_enabled = incremental_requested and normalized_load_mode == "transactional"
        last_load_timestamp = ""

        if incremental_requested and not incremental_enabled:
            print({
                "stage": "extract_incremental_skip",
                "entity_name": entity_name,
                "reason": f"incremental_not_supported_for_load_mode:{normalized_load_mode}"
            })

        if incremental_enabled:
            last_load_timestamp = get_last_successful_load_timestamp(tracking_entity_name)
            extract_query = build_incremental_extract_query(base_query, watermark_column, last_load_timestamp)

        route_id_filter = _build_entity_route_id_filter(entity_name, entity_config, source_config_path, source_config)
        if route_id_filter and not route_id_filter.get("apply_in_spark", False):
            extract_query = f"SELECT * FROM ({extract_query}) ENTITY_ROUTE_ID_FILTER WHERE {route_id_filter['clause']}"
            print({
                "stage": "extract_route_id_filter",
                "entity_name": entity_name,
                "mode": route_id_filter["mode"],
                "column_name": route_id_filter["column_name"],
                "config_path": route_id_filter["config_path"],
                "source_entity_name": route_id_filter["source_entity_name"],
                "source_temp_view_name": route_id_filter["source_temp_view_name"],
                "source_column_name": route_id_filter["source_column_name"],
                "route_id_count": route_id_filter["route_id_count"],
                "apply_in_spark": False
            })

        if route_id_filter and route_id_filter.get("apply_in_spark", False):
            print({
                "stage": "extract_route_id_filter",
                "entity_name": entity_name,
                "mode": route_id_filter["mode"],
                "column_name": route_id_filter["column_name"],
                "config_path": route_id_filter["config_path"],
                "source_entity_name": route_id_filter["source_entity_name"],
                "source_temp_view_name": route_id_filter["source_temp_view_name"],
                "source_column_name": route_id_filter["source_column_name"],
                "route_id_count": route_id_filter["route_id_count"],
                "apply_in_spark": True
            })

        print({
            "stage": "extract",
            "entity_name": entity_name,
            "load_mode": load_mode,
            "databricks_env": databricks_env,
            "source_config_path": source_config_path,
            "target_config_path": target_config_path,
            "excluded_entity_names": excluded_entity_names,
            "continue_on_error": continue_on_error,
            "temp_view_name": temp_view_name,
            "global_temp_view_name": f"global_temp.{global_temp_view_name}",
            "historical_cutoff_type": historical_cutoff_type,
            "historical_cutoff_value": historical_cutoff_value,
            "historical_cutoff_end_value": historical_cutoff_end_value,
            "incremental_enabled": incremental_enabled,
            "tracking_entity_name": tracking_entity_name,
            "watermark_column": watermark_column,
            "last_load_timestamp": last_load_timestamp
        })

        redacted_options = {
            key: ("<redacted>" if key.lower() in {"user", "password", "accesstoken"} else value)
            for key, value in jdbc_options.items()
        }
        print({
            "entity_name": entity_name,
            "jdbc_options": redacted_options,
            "validation_row_limit": validation_row_limit
        })

        extract_df = (
            spark.read
            .format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", f"({extract_query}) ORACLE_SOURCE")
            .load()
        )

        if route_id_filter and route_id_filter.get("apply_in_spark", False):
            extract_df = _apply_entity_source_filter_in_spark(extract_df, route_id_filter)

        row_count = None if skip_row_count else extract_df.count()
        validation_preview_df = _get_validation_preview_df(extract_df, validation_row_limit)
        extract_df.createOrReplaceTempView(temp_view_name)
        extract_df.createOrReplaceGlobalTempView(global_temp_view_name)
        extract_results.append({
            "entity_name": entity_name,
            "temp_view_name": temp_view_name,
            "global_temp_view_name": f"global_temp.{global_temp_view_name}",
            "row_count": row_count,
            "incremental_enabled": incremental_enabled,
            "tracking_entity_name": tracking_entity_name,
            "last_load_timestamp": last_load_timestamp
        })

        if row_count is None:
            print(f"Loaded rows into temp view '{temp_view_name}' and global temp view 'global_temp.{global_temp_view_name}' for entity '{entity_name}'.")
        else:
            print(f"Loaded {row_count} rows into temp view '{temp_view_name}' and global temp view 'global_temp.{global_temp_view_name}' for entity '{entity_name}'.")
        if validation_preview_df is not None:
            display(validation_preview_df)
    except Exception as exc:
        failure_details = {
            "stage": "extract_failure",
            "entity_name": entity_name,
            "continue_on_error": continue_on_error,
            "reason": str(exc)
        }
        extract_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise
        continue

if route_exclusion_route_ids:
    _apply_route_exclusion_filters(extract_results, source_config, databricks_env, target_config, route_exclusion_route_ids)
if trip_exclusion_config.get('trip_codes') or trip_exclusion_config.get('trip_ids'):
    _apply_trip_exclusion_filters(extract_results, source_config, databricks_env, target_config, trip_exclusion_config)

# COMMAND ----------

