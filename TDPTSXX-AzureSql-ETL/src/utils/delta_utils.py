# Databricks notebook source
try:
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window
except ImportError:  # pragma: no cover - local unit tests without Spark
    col = None
    row_number = None
    Window = None

try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover - available on Databricks runtime
    DeltaTable = None


def _require_value(value, name):
    if value is None or str(value).strip() == "":
        raise ValueError(f"Required value '{name}' was not provided.")
    return str(value).strip()


def _resolve_env_value(value, environment, field_name):
    if "resolve_env_value" in globals():
        return resolve_env_value(value, environment, field_name)
    if isinstance(value, dict):
        if environment in value:
            return value[environment]
        raise KeyError(f"Missing environment '{environment}' for '{field_name}'.")
    return value


def _split_columns(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


def resolve_delta_target_paths(entity_config, databricks_env):
    target = entity_config.get("target", {})
    if not target.get("write_enabled", True):
        raise ValueError(f"Entity '{entity_config.get('entity_name')}' has write_enabled=false.")

    target_entity = _require_value(target.get("target_entity"), "target.target_entity")
    raw_schema = _require_value(target.get("target_uc_schema"), "target.target_uc_schema")
    transform_schema = raw_schema.replace("raw", "transform")
    file_path = _require_value(target.get("target_file_path"), "target.target_file_path")
    file_format = _require_value(target.get("target_file_format", "Delta"), "target.target_file_format")

    primary_catalog = _resolve_env_value(
        target.get("target_unity_catalog_by_env"),
        databricks_env,
        "target.target_unity_catalog_by_env",
    )
    secondary_catalog = _resolve_env_value(
        target.get("secondary_unity_catalog_by_env"),
        databricks_env,
        "target.secondary_unity_catalog_by_env",
    )
    primary_datalake = _resolve_env_value(
        target.get("target_datalake_by_env"),
        databricks_env,
        "target.target_datalake_by_env",
    )
    secondary_datalake = _resolve_env_value(
        target.get("secondary_datalake_by_env"),
        databricks_env,
        "target.secondary_datalake_by_env",
    )

    primary_raw_path = f"{primary_datalake}{file_path}{target_entity}/{file_format}"
    primary_transform_path = primary_raw_path.replace("Raw", "Transform")
    secondary_raw_path = f"{secondary_datalake}{file_path}{target_entity}/{file_format}"
    secondary_transform_path = secondary_raw_path.replace("Raw", "Transform")

    return {
        "target_entity": target_entity,
        "raw_schema": raw_schema,
        "transform_schema": transform_schema,
        "primary_raw_table": f"{primary_catalog}.{raw_schema}.{target_entity}",
        "primary_transform_table": f"{primary_catalog}.{transform_schema}.{target_entity}",
        "secondary_raw_table": f"{secondary_catalog}.{raw_schema}.{target_entity}",
        "secondary_transform_table": f"{secondary_catalog}.{transform_schema}.{target_entity}",
        "primary_raw_path": primary_raw_path,
        "primary_transform_path": primary_transform_path,
        "secondary_raw_path": secondary_raw_path,
        "secondary_transform_path": secondary_transform_path,
        "primary_key_columns": _split_columns(target.get("primary_key_columns")),
        "dedupe_key_columns": _split_columns(target.get("dedupe_key_columns") or target.get("primary_key_columns")),
        "dedupe_order_columns": _split_columns(
            target.get("dedupe_order_columns") or ["UpdatedDateTime", "CreatedDateTime"]
        ),
        "raw_merge_on_primary_key": bool(target.get("raw_merge_on_primary_key", False)),
    }


def does_table_exist(table_fqn):
    try:
        return spark.catalog.tableExists(table_fqn)
    except Exception:
        parts = table_fqn.split(".")
        if len(parts) != 3:
            return False
        catalog, schema, table = parts
        rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'").collect()
        return len(rows) > 0


def ensure_schema(catalog_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")


def dedupe_dataframe(dataframe, dedupe_key_columns, dedupe_order_columns=None):
    if col is None or row_number is None or Window is None:
        raise RuntimeError("PySpark is required for dedupe_dataframe.")

    keys = _split_columns(dedupe_key_columns)
    if not keys:
        return dataframe

    order_columns = _split_columns(dedupe_order_columns) or keys
    order_expressions = [col(column_name).desc() for column_name in order_columns]
    window_spec = Window.partitionBy(*[col(column_name) for column_name in keys]).orderBy(*order_expressions)
    return (
        dataframe.withColumn("_dedupe_rank", row_number().over(window_spec))
        .filter(col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )


def build_merge_condition(primary_keys):
    predicates = [f"target.`{key}` = source.`{key}`" for key in _split_columns(primary_keys)]
    if not predicates:
        raise ValueError("At least one primary key is required for Delta merge.")
    return " AND ".join(predicates)


def merge_dataframe_into_delta(dataframe, delta_path, merge_condition):
    if DeltaTable is None:
        raise RuntimeError("delta.tables.DeltaTable is required for merge writes.")
    delta_table = DeltaTable.forPath(spark, delta_path)
    (
        delta_table.alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def deep_clone_table(source_table, target_table, target_path):
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {target_table}
        DEEP CLONE {source_table}
        LOCATION '{target_path}'
        """
    )


def write_delta_table(dataframe, table_fqn, table_path, mode):
    (
        dataframe.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn, path=table_path)
    )


def complete_entity_to_delta(
    dataframe,
    entity_config,
    databricks_env,
    load_mode,
    time_grain=None,
    transform_bootstrap_dataframe=None,
):
    paths = resolve_delta_target_paths(entity_config, databricks_env)
    entity_name = entity_config.get("entity_name", paths["target_entity"])
    incremental_config = entity_config.get("incremental", {})
    normalized_load_mode = str(load_mode or "").strip().lower()
    normalized_time_grain = str(time_grain or entity_config.get("time_grain") or "").strip().upper()
    if not normalized_time_grain:
        normalized_time_grain = "HOURLY" if normalized_load_mode == "transactional" else "DAILY"

    ensure_schema(f"{paths['primary_raw_table'].rsplit('.', 2)[0]}")
    ensure_schema(f"{paths['primary_transform_table'].rsplit('.', 2)[0]}")

    working_df = dedupe_dataframe(
        dataframe,
        paths["dedupe_key_columns"],
        paths["dedupe_order_columns"],
    )
    row_count = working_df.count()
    print(
        {
            "stage": "delta_completion_start",
            "entity_name": entity_name,
            "load_mode": normalized_load_mode,
            "time_grain": normalized_time_grain,
            "row_count": row_count,
            "primary_raw_table": paths["primary_raw_table"],
            "primary_transform_table": paths["primary_transform_table"],
        }
    )

    if row_count == 0:
        return {
            "entity_name": entity_name,
            "row_count": 0,
            "status": "skipped_empty",
            "paths": paths,
        }

    raw_existed_before = does_table_exist(paths["primary_raw_table"])
    if not raw_existed_before:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {paths['primary_raw_table']} OPTIONS (path '{paths['primary_raw_path']}')"
        )

    incremental_enabled = bool(incremental_config.get("enabled", False)) and normalized_load_mode == "transactional"
    use_raw_merge = (
        paths["raw_merge_on_primary_key"]
        and incremental_enabled
        and raw_existed_before
        and normalized_time_grain == "HOURLY"
    )
    # Preserve legacy semantics: incremental raw uses append unless merge-on-PK applies.
    raw_write_mode = "append" if incremental_enabled else "overwrite"

    if use_raw_merge:
        merge_condition = build_merge_condition(paths["primary_key_columns"])
        print({"stage": "delta_raw_merge", "entity_name": entity_name, "merge_condition": merge_condition})
        merge_dataframe_into_delta(working_df, paths["primary_raw_path"], merge_condition)
        raw_write_mode = "merge"
    else:
        print({"stage": "delta_raw_write", "entity_name": entity_name, "mode": raw_write_mode})
        write_delta_table(working_df, paths["primary_raw_table"], paths["primary_raw_path"], raw_write_mode)

    deep_clone_table(paths["primary_raw_table"], paths["secondary_raw_table"], paths["secondary_raw_path"])

    transform_existed_before = does_table_exist(paths["primary_transform_table"])
    if not transform_existed_before:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {paths['primary_transform_table']}
            LIKE {paths['primary_raw_table']}
            LOCATION '{paths['primary_transform_path']}'
            USING DELTA
            """
        )

    transform_write_mode = "overwrite"
    if normalized_time_grain == "DAILY":
        write_delta_table(
            working_df,
            paths["primary_transform_table"],
            paths["primary_transform_path"],
            "overwrite",
        )
    elif normalized_time_grain == "HOURLY":
        # Legacy: first transform load uses a full (non-incremental) extract when provided.
        if not transform_existed_before:
            bootstrap_df = transform_bootstrap_dataframe if transform_bootstrap_dataframe is not None else working_df
            bootstrap_df = dedupe_dataframe(
                bootstrap_df,
                paths["dedupe_key_columns"],
                paths["dedupe_order_columns"],
            )
            write_delta_table(
                bootstrap_df,
                paths["primary_transform_table"],
                paths["primary_transform_path"],
                "overwrite",
            )
            transform_write_mode = "overwrite"
        else:
            merge_condition = build_merge_condition(paths["primary_key_columns"])
            print(
                {
                    "stage": "delta_transform_merge",
                    "entity_name": entity_name,
                    "merge_condition": merge_condition,
                }
            )
            merge_dataframe_into_delta(working_df, paths["primary_transform_path"], merge_condition)
            transform_write_mode = "merge"
    else:
        raise ValueError(f"Unsupported time_grain '{normalized_time_grain}'. Expected HOURLY or DAILY.")

    deep_clone_table(
        paths["primary_transform_table"],
        paths["secondary_transform_table"],
        paths["secondary_transform_path"],
    )

    new_watermark = None
    if incremental_enabled:
        uc_watermark_column = str(
            incremental_config.get("uc_watermark_column")
            or incremental_config.get("watermark_column")
            or ""
        ).strip()
        if uc_watermark_column:
            watermark_sql = f"""
                SELECT date_format(MAX(to_timestamp({uc_watermark_column})), 'yyyy-MM-dd HH:mm:ss.SSS')
                FROM {paths['primary_raw_table']}
            """
            new_watermark = spark.sql(watermark_sql).first()[0]
            tracking_entity_name = str(
                incremental_config.get("loadtracker_entity_name") or entity_name
            ).strip()
            write_loadtracker_entry(tracking_entity_name, row_count, new_watermark)

    return {
        "entity_name": entity_name,
        "row_count": row_count,
        "status": "success",
        "raw_write_mode": raw_write_mode,
        "transform_write_mode": transform_write_mode,
        "new_watermark": new_watermark,
        "paths": paths,
    }

