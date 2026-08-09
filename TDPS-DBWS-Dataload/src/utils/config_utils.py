# Databricks notebook source
# Databricks notebook source
import copy
import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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

# COMMAND ----------

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
        return [name.strip() for name in entity_names.split(',') if name.strip()]

    return [str(name).strip() for name in entity_names if str(name).strip()]


def _escape_sql_literal(value):
    return str(value).replace("'", "''")


SQL_TOKEN_MIN_VALIDITY_SECONDS = 300


def _parse_token_expiry_seconds(token_payload):
    expires_in_value = token_payload.get("expires_in", 3600)
    try:
        return int(expires_in_value)
    except (TypeError, ValueError):
        return 3600


def _is_expired_sql_token_error(exception):
    error_text = str(exception).lower()
    return "token is expired" in error_text or "expired" in error_text and "token" in error_text


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


def get_last_successful_load_timestamp(tracking_entity_name, default_watermark="1900-01-01 00:00:00.000000"):
    loadtracker_table_name = get_loadtracker_table_name(required=False)
    if not loadtracker_table_name:
        return default_watermark

    escaped_tracking_entity_name = _escape_sql_literal(tracking_entity_name)

    try:
        tracker_select_sql = f"""
            SELECT MAX(LAST_LOAD_TIMESTAMP)
            FROM {loadtracker_table_name}
            WHERE LOAD_COMPLETE_FLAG = 'Y'
              AND TABLE_NAME = '{escaped_tracking_entity_name}'
        """
        last_load_timestamp = spark.sql(tracker_select_sql).first()[0]

        if last_load_timestamp is None:
            return default_watermark

        if hasattr(last_load_timestamp, "strftime"):
            return last_load_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

        return str(last_load_timestamp)
    except Exception as exc:
        print({
            "stage": "loadtracker_read_skip",
            "tracking_entity_name": tracking_entity_name,
            "reason": str(exc)
        })
        return default_watermark


def write_loadtracker_entry(tracking_entity_name, row_count, last_load_timestamp):
    if last_load_timestamp is None or str(last_load_timestamp).strip() == "":
        return ""

    loadtracker_table_name = ensure_loadtracker_table()
    escaped_tracking_entity_name = _escape_sql_literal(tracking_entity_name)
    escaped_last_load_timestamp = _escape_sql_literal(last_load_timestamp)
    row_count_value = int(row_count)

    sql_update = f"""
        INSERT INTO {loadtracker_table_name}
        SELECT
            '{escaped_tracking_entity_name}' AS TABLE_NAME,
            CURRENT_TIMESTAMP AS LOAD_DATE,
            'Y' AS LOAD_COMPLETE_FLAG,
            {row_count_value} AS ROW_COUNT,
            to_timestamp('{escaped_last_load_timestamp}', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS LAST_LOAD_TIMESTAMP
    """
    spark.sql(sql_update)
    return loadtracker_table_name


def build_incremental_extract_query(base_query, watermark_column, last_load_timestamp):
    if not watermark_column:
        raise ValueError("Incremental loads require incremental.watermark_column.")

    if last_load_timestamp is None or str(last_load_timestamp).strip() == "":
        return base_query

    escaped_last_load_timestamp = _escape_sql_literal(last_load_timestamp)
    return (
        f"SELECT * FROM ({base_query}) INCREMENTAL_SOURCE "
        f"WHERE {watermark_column} > '{escaped_last_load_timestamp}'"
    )


def _require_value(value, name):
    if value is None or str(value).strip() == "":
        raise ValueError(f"Required value '{name}' was not provided.")

    return str(value).strip()


def _split_sql_table_name(table_name):
    normalized_name = _require_value(table_name, "table_name")
    name_parts = [part.strip().strip("[]") for part in normalized_name.split(".") if part.strip()]

    if len(name_parts) != 2:
        raise ValueError(f"Table name '{table_name}' must use the format <schema>.<table>.")

    return name_parts[0], name_parts[1]


def _quote_sql_server_identifier(identifier):
    return f"[{str(identifier).replace(']', ']]')}]"


def _get_qualified_sql_server_table_name(table_name):
    schema_name, base_table_name = _split_sql_table_name(table_name)
    return f"{_quote_sql_server_identifier(schema_name)}.{_quote_sql_server_identifier(base_table_name)}"


def load_target_config(target_config_reference):
    reference = _require_value(target_config_reference, "target_config_path")
    reference = reference.lstrip("\ufeff")

    if reference.startswith("{"):
        return json.loads(reference)

    with open(reference, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def _resolve_required_env_value(value, environment, field_name):
    return _require_value(resolve_env_value(value, environment, field_name), field_name)


def get_target_write_entities(target_config):
    write_targets = target_config.get("write_targets")
    if isinstance(write_targets, dict):
        return write_targets

    planned_entities = target_config.get("planned_entities")
    if isinstance(planned_entities, dict):
        return planned_entities

    return {}


def get_target_entity_write_config(target_config, entity_name, required=False):
    entity_write_configs = get_target_write_entities(target_config)
    entity_write_config = entity_write_configs.get(entity_name)

    if entity_write_config is None and required:
        available_entities = sorted(entity_write_configs.keys())
        raise ValueError(
            f"Entity '{entity_name}' is not defined in target write config. Available values: {available_entities}"
        )

    return entity_write_config or {}


def get_source_entity_write_config(entity_config):
    if not isinstance(entity_config, dict):
        return {}

    source_write_config = entity_config.get("target") or entity_config.get("write_target") or {}
    if not isinstance(source_write_config, dict):
        raise ValueError("Entity target/write_target config must be an object when provided.")

    return source_write_config


def resolve_entity_write_config(entity_config, target_config, entity_name, required=False):
    source_write_config = get_source_entity_write_config(entity_config)
    if source_write_config:
        return source_write_config

    return get_target_entity_write_config(target_config, entity_name, required=required)


def build_sql_jdbc_url(target_config, databricks_env):
    connection_config = target_config.get("connection", {})
    sql_server = _resolve_required_env_value(connection_config.get("server_by_env", connection_config.get("server")), databricks_env, "connection.server_by_env")
    sql_database = _resolve_required_env_value(connection_config.get("database_by_env", connection_config.get("database")), databricks_env, "connection.database_by_env")
    sql_port = _require_value(connection_config.get("port", 1433), "connection.port")

    return (
        f"jdbc:sqlserver://{sql_server}:{sql_port};"
        f"database={sql_database};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;"
    )


def get_sql_access_token(target_config, databricks_env, min_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    auth_config = target_config.get("authentication", {})
    tenant_id = _require_value(auth_config.get("tenant_id"), "authentication.tenant_id")
    client_id = _require_value(_resolve_required_env_value(auth_config.get("client_id_by_env", auth_config.get("client_id")), databricks_env, "authentication.client_id_by_env"), "authentication.client_id_by_env")
    secret_scope = _resolve_required_env_value(auth_config.get("secret_scope_by_env", auth_config.get("secret_scope")), databricks_env, "authentication.secret_scope_by_env")
    secret_key = _require_value(auth_config.get("secret_key"), "authentication.secret_key")
    token_scope = _require_value(auth_config.get("token_scope"), "authentication.token_scope")

    client_secret = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    request_body = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": token_scope,
        }
    ).encode("utf-8")
    request_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    token_request = Request(token_endpoint, data=request_body, headers=request_headers, method="POST")

    for token_attempt in range(2):
        try:
            with urlopen(token_request, timeout=30) as response:
                token_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Service principal token request failed via {token_endpoint}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Service principal token endpoint was unreachable via {token_endpoint}: {exc.reason!r}") from exc

        access_token = token_payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"Token response did not contain an access token: {token_payload}")

        expires_in_seconds = _parse_token_expiry_seconds(token_payload)
        if expires_in_seconds > int(min_validity_seconds):
            return access_token

        if token_attempt == 0:
            print({
                "stage": "sql_access_token_low_validity_retry",
                "expires_in_seconds": expires_in_seconds,
                "min_validity_seconds": int(min_validity_seconds)
            })

    raise RuntimeError(
        f"SQL access token lifetime ({expires_in_seconds}s) was below the required minimum ({int(min_validity_seconds)}s)."
    )


def build_sql_jdbc_options(target_config, databricks_env, table_name, min_token_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    connection_config = target_config.get("connection", {})
    jdbc_driver = _require_value(connection_config.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), "connection.driver")
    return {
        "url": build_sql_jdbc_url(target_config, databricks_env),
        "driver": jdbc_driver,
        "dbtable": _get_qualified_sql_server_table_name(table_name),
        "accessToken": get_sql_access_token(target_config, databricks_env, min_validity_seconds=min_token_validity_seconds),
        "encrypt": "true",
        "trustServerCertificate": "false",
        "hostNameInCertificate": "*.database.windows.net"
    }


def get_sql_write_diagnostics(target_config, databricks_env, table_name, write_mode, extra_options=None, dataframe=None, jdbc_options=None):
    if jdbc_options is None:
        connection_config = target_config.get("connection", {})
        jdbc_driver = _require_value(connection_config.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), "connection.driver")
        jdbc_options = {
            "url": build_sql_jdbc_url(target_config, databricks_env),
            "driver": jdbc_driver,
            "dbtable": _get_qualified_sql_server_table_name(table_name),
            "accessToken": "<not-requested>",
            "encrypt": "true",
            "trustServerCertificate": "false",
            "hostNameInCertificate": "*.database.windows.net"
        }
    schema_name, base_table_name = _split_sql_table_name(table_name)
    redacted_jdbc_options = {
        option_name: ("<redacted>" if option_name.lower() == "accesstoken" and option_value != "<not-requested>" else option_value)
        for option_name, option_value in jdbc_options.items()
    }
    diagnostics = {
        "raw_table_name": table_name,
        "schema_name": schema_name,
        "base_table_name": base_table_name,
        "qualified_table_name": _get_qualified_sql_server_table_name(table_name),
        "write_mode": write_mode,
        "extra_options": extra_options or {},
        "jdbc_options": redacted_jdbc_options
    }
    if dataframe is not None:
        diagnostics["dataframe_columns"] = list(dataframe.columns)
        diagnostics["dataframe_column_count"] = len(dataframe.columns)
    return diagnostics


def _apply_write_column_filters(dataframe, entity_name, entity_write_config):
    excluded_columns = entity_write_config.get("excluded_columns") or []
    normalized_excluded_columns = [str(column_name).strip() for column_name in excluded_columns if str(column_name).strip()]
    if not normalized_excluded_columns:
        return dataframe, []

    dataframe_columns_by_lower_name = {column_name.lower(): column_name for column_name in dataframe.columns}
    missing_columns = [column_name for column_name in normalized_excluded_columns if column_name.lower() not in dataframe_columns_by_lower_name]
    if missing_columns:
        raise ValueError(f"Entity '{entity_name}' excludes columns {missing_columns} but they were not found in dataframe columns {list(dataframe.columns)}.")

    excluded_dataframe_columns = [dataframe_columns_by_lower_name[column_name.lower()] for column_name in normalized_excluded_columns]
    selected_columns = [column_name for column_name in dataframe.columns if column_name not in excluded_dataframe_columns]
    return dataframe.select(*selected_columns), excluded_dataframe_columns


def _get_sql_server_connection(target_config, databricks_env):
    jdbc_options = build_sql_jdbc_options(target_config, databricks_env, "dbo.__connection_probe__")
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        spark_context = getattr(spark, "sparkContext", None) or getattr(spark, "_sc", None)
        if spark_context is not None and hasattr(spark_context, "_gateway"):
            jvm = spark_context._gateway.jvm
    if jvm is None:
        raise RuntimeError("Unable to access the Spark JVM for SQL Server write operations.")
    properties = jvm.java.util.Properties()
    properties.setProperty("driver", jdbc_options["driver"])
    properties.setProperty("accessToken", jdbc_options["accessToken"])
    properties.setProperty("encrypt", jdbc_options["encrypt"])
    properties.setProperty("trustServerCertificate", jdbc_options["trustServerCertificate"])
    properties.setProperty("hostNameInCertificate", jdbc_options["hostNameInCertificate"])
    return jvm.java.sql.DriverManager.getConnection(jdbc_options["url"], properties)


def _execute_sql_with_token_retry(operation_name, operation, retry_on_token_expiry=True, retry_details=None):
    try:
        return operation(1)
    except Exception as sql_error:
        if not retry_on_token_expiry or not _is_expired_sql_token_error(sql_error):
            raise

        retry_payload = {
            "stage": f"{operation_name}_retry_token_expired",
            "error": str(sql_error)
        }
        if retry_details:
            retry_payload.update(retry_details)
        print(retry_payload)
        return operation(2)


def execute_sql_server_non_query(target_config, databricks_env, statement_text, retry_on_token_expiry=True):
    def _execute_once(attempt_number):
        _ = attempt_number
        connection = _get_sql_server_connection(target_config, databricks_env)
        statement = None
        try:
            statement = connection.createStatement()
            statement.execute(statement_text)
        finally:
            if statement is not None:
                statement.close()
            connection.close()

    return _execute_sql_with_token_retry(
        "execute_sql_server_non_query",
        _execute_once,
        retry_on_token_expiry=retry_on_token_expiry,
        retry_details={"statement_preview": statement_text[:120]}
    )


def _clear_sql_server_table(target_config, databricks_env, table_name):
    qualified_table_name = _get_qualified_sql_server_table_name(table_name)
    truncate_statement = f"TRUNCATE TABLE {qualified_table_name}"
    delete_statement = f"DELETE FROM {qualified_table_name}"

    try:
        execute_sql_server_non_query(target_config, databricks_env, truncate_statement)
        return {
            "clear_method": "truncate",
            "statement": truncate_statement
        }
    except Exception as truncate_error:
        print({
            "stage": "write_entity_to_target_clear_fallback",
            "table_name": table_name,
            "qualified_table_name": qualified_table_name,
            "truncate_error": str(truncate_error)
        })
        execute_sql_server_non_query(target_config, databricks_env, delete_statement)
        return {
            "clear_method": "delete",
            "statement": delete_statement,
            "truncate_error": str(truncate_error)
        }


SQL_WRITE_TUNING_VERSION = "2026-07-01-1"


def write_dataframe_to_target(dataframe, target_config, databricks_env, table_name, write_mode="append", extra_options=None):
    def _write_once(attempt_number):
        jdbc_options = build_sql_jdbc_options(target_config, databricks_env, table_name)
        print({
            "stage": "write_dataframe_to_target_start",
            "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
            "attempt": attempt_number,
            "diagnostics": get_sql_write_diagnostics(target_config, databricks_env, table_name, write_mode, extra_options=extra_options, dataframe=dataframe, jdbc_options=jdbc_options)
        })
        writer = dataframe.write.format("jdbc").mode(write_mode)
        for option_name, option_value in jdbc_options.items():
            writer = writer.option(option_name, option_value)
        for option_name, option_value in (extra_options or {}).items():
            writer = writer.option(option_name, option_value)
        writer.save()

    return _execute_sql_with_token_retry(
        "write_dataframe_to_target",
        _write_once,
        retry_on_token_expiry=True,
        retry_details={"table_name": table_name, "write_mode": write_mode}
    )


def _coerce_positive_int(value, default_value=None):
    if value in (None, ""):
        return default_value
    coerced_value = int(value)
    if coerced_value <= 0:
        raise ValueError(f"Expected a positive integer but received {value!r}.")
    return coerced_value


def _resolve_configured_dataframe_columns(dataframe, entity_name, configured_columns, config_name):
    normalized_configured_columns = [str(column_name).strip() for column_name in configured_columns if str(column_name).strip()]
    if not normalized_configured_columns:
        return []

    dataframe_columns_by_lower_name = {column_name.lower(): column_name for column_name in dataframe.columns}
    missing_columns = [column_name for column_name in normalized_configured_columns if column_name.lower() not in dataframe_columns_by_lower_name]
    if missing_columns:
        raise ValueError(f"Entity '{entity_name}' configured {config_name} columns {missing_columns} but they were not found in dataframe columns {list(dataframe.columns)}.")

    return [dataframe_columns_by_lower_name[column_name.lower()] for column_name in normalized_configured_columns]


def _prepare_dataframe_for_sql_write(dataframe, entity_name, entity_write_config, row_count=None):
    write_options = dict(entity_write_config.get("write_options") or {})
    desired_partition_count = _coerce_positive_int(write_options.get("numPartitions"), None)
    normalized_row_count = int(row_count) if row_count is not None else None

    if desired_partition_count is None and normalized_row_count is not None:
        if normalized_row_count >= 5000000:
            desired_partition_count = 64
        elif normalized_row_count >= 2000000:
            desired_partition_count = 48
        elif normalized_row_count >= 1000000:
            desired_partition_count = 32
        elif normalized_row_count >= 250000:
            desired_partition_count = 16
        elif normalized_row_count >= 50000:
            desired_partition_count = 8

    dataframe_to_write = dataframe
    current_partition_count = None
    if desired_partition_count is not None:
        dataframe_to_write = dataframe.repartition(desired_partition_count)
        write_options["numPartitions"] = str(desired_partition_count)

    if "batchsize" not in write_options:
        if normalized_row_count is not None and normalized_row_count >= 1000000:
            write_options["batchsize"] = "20000"
        else:
            write_options["batchsize"] = "10000"
    write_options.setdefault("isolationLevel", "NONE")

    final_partition_count = desired_partition_count if desired_partition_count is not None else None
    print({
        "stage": "write_entity_to_target_partitioning",
        "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
        "entity_name": entity_name,
        "row_count": normalized_row_count,
        "initial_partition_count": current_partition_count,
        "final_partition_count": final_partition_count,
        "write_options": write_options
    })
    return dataframe_to_write, write_options, final_partition_count


def _resolve_write_chunking_config(dataframe, entity_name, entity_write_config):
    chunking_config = entity_write_config.get("write_chunking") or {}
    if not chunking_config:
        return {}
    if not isinstance(chunking_config, dict):
        raise ValueError(f"Entity '{entity_name}' write_chunking config must be an object when provided.")

    bucket_count = _coerce_positive_int(chunking_config.get("bucket_count"), None)
    if bucket_count is None or bucket_count <= 1:
        return {}

    configured_column_name = str(chunking_config.get("column_name", "")).strip()
    if not configured_column_name:
        raise ValueError(f"Entity '{entity_name}' write_chunking.bucket_count requires write_chunking.column_name.")

    resolved_column_name = _resolve_configured_dataframe_columns(
        dataframe,
        entity_name,
        [configured_column_name],
        "write_chunking"
    )[0]

    return {
        "column_name": resolved_column_name,
        "bucket_count": bucket_count
    }


def _resolve_write_dedupe_columns(dataframe, entity_name, entity_write_config):
    dedupe_columns = entity_write_config.get("dedupe_key_columns") or []
    return _resolve_configured_dataframe_columns(dataframe, entity_name, dedupe_columns, "dedupe_key_columns")


def _load_existing_target_keys(target_config, databricks_env, table_name, dedupe_columns):
    jdbc_options = build_sql_jdbc_options(target_config, databricks_env, table_name)
    qualified_column_names = ", ".join(f"[{column_name}]" for column_name in dedupe_columns)
    key_query = f"(SELECT DISTINCT {qualified_column_names} FROM {_get_qualified_sql_server_table_name(table_name)}) existing_target_keys"
    return (
        spark.read
        .format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", key_query)
        .load()
    )


def _apply_append_dedupe(dataframe, entity_name, entity_write_config, target_config, databricks_env, table_name):
    dedupe_columns = _resolve_write_dedupe_columns(dataframe, entity_name, entity_write_config)
    if not dedupe_columns:
        return dataframe, {"dedupe_enabled": False}

    deduped_dataframe = dataframe.dropDuplicates(dedupe_columns)
    existing_keys_df = _load_existing_target_keys(target_config, databricks_env, table_name, dedupe_columns)
    anti_joined_dataframe = deduped_dataframe.join(existing_keys_df, dedupe_columns, "left_anti")
    return anti_joined_dataframe, {
        "dedupe_enabled": True,
        "dedupe_key_columns": dedupe_columns
    }


def _write_dataframe_to_target_in_chunks(dataframe, entity_name, target_config, databricks_env, table_name, write_mode, extra_options, write_chunking_config):
    chunk_column_name = write_chunking_config["column_name"]
    bucket_count = int(write_chunking_config["bucket_count"])
    chunked_dataframe = dataframe.selectExpr("*", f"pmod(hash(`{chunk_column_name}`), {bucket_count}) AS __write_chunk_id").persist()

    try:
        for chunk_index in range(bucket_count):
            print({
                "stage": "write_entity_to_target_chunk_start",
                "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
                "entity_name": entity_name,
                "table_name": table_name,
                "write_mode": write_mode,
                "chunk_column_name": chunk_column_name,
                "chunk_index": chunk_index,
                "chunk_count": bucket_count
            })
            chunk_dataframe = chunked_dataframe.where(f"__write_chunk_id = {chunk_index}").drop("__write_chunk_id")
            write_dataframe_to_target(chunk_dataframe, target_config, databricks_env, table_name, write_mode=write_mode, extra_options=extra_options)
    finally:
        chunked_dataframe.unpersist()


def write_entity_to_target(dataframe, entity_name, entity_write_config, target_config, databricks_env, row_count=None):
    write_enabled = bool(entity_write_config.get("write_enabled", False))
    write_mode = str(entity_write_config.get("write_mode", "")).strip().lower()
    table_name = str(entity_write_config.get("table_name", "")).strip()

    if not write_enabled:
        return {
            "write_enabled": False,
            "write_mode": write_mode,
            "table_name": table_name,
            "status": "skipped",
            "reason": "write_disabled"
        }

    if not write_mode:
        raise ValueError(f"Entity '{entity_name}' has write_enabled=true but no write_mode configured.")
    if not table_name:
        raise ValueError(f"Entity '{entity_name}' has write_enabled=true but no table_name configured.")
    if write_mode not in {"append", "truncate_insert"}:
        raise ValueError(
            f"Unsupported write_mode '{write_mode}' for entity '{entity_name}'. "
            "Supported write modes are 'append' and 'truncate_insert'."
        )

    dataframe_to_write, excluded_dataframe_columns = _apply_write_column_filters(dataframe, entity_name, entity_write_config)
    dedupe_result = {"dedupe_enabled": False}
    if write_mode == "append":
        dataframe_to_write, dedupe_result = _apply_append_dedupe(
            dataframe_to_write,
            entity_name,
            entity_write_config,
            target_config,
            databricks_env,
            table_name
        )
    dataframe_to_write, write_options, write_partition_count = _prepare_dataframe_for_sql_write(
        dataframe_to_write,
        entity_name,
        entity_write_config,
        row_count=row_count
    )
    write_chunking_config = _resolve_write_chunking_config(dataframe_to_write, entity_name, entity_write_config)
    clear_result = None

    if write_mode == "truncate_insert":
        clear_result = _clear_sql_server_table(target_config, databricks_env, table_name)
        print({
            "stage": "write_entity_to_target_resolved",
            "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
            "entity_name": entity_name,
            "excluded_dataframe_columns": excluded_dataframe_columns,
            "write_partition_count": write_partition_count,
            "write_chunking_config": write_chunking_config,
            "dedupe_result": dedupe_result,
            "clear_result": clear_result,
            "diagnostics": get_sql_write_diagnostics(target_config, databricks_env, table_name, "append", extra_options=write_options, dataframe=dataframe_to_write)
        })
        if write_chunking_config:
            _write_dataframe_to_target_in_chunks(dataframe_to_write, entity_name, target_config, databricks_env, table_name, "append", write_options, write_chunking_config)
        else:
            write_dataframe_to_target(dataframe_to_write, target_config, databricks_env, table_name, write_mode="append", extra_options=write_options)
    else:
        print({
            "stage": "write_entity_to_target_resolved",
            "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
            "entity_name": entity_name,
            "excluded_dataframe_columns": excluded_dataframe_columns,
            "write_partition_count": write_partition_count,
            "write_chunking_config": write_chunking_config,
            "dedupe_result": dedupe_result,
            "diagnostics": get_sql_write_diagnostics(target_config, databricks_env, table_name, "append", extra_options=write_options, dataframe=dataframe_to_write)
        })
        if write_chunking_config:
            _write_dataframe_to_target_in_chunks(dataframe_to_write, entity_name, target_config, databricks_env, table_name, "append", write_options, write_chunking_config)
        else:
            write_dataframe_to_target(dataframe_to_write, target_config, databricks_env, table_name, write_mode="append", extra_options=write_options)

    write_row_count = int(row_count) if row_count is not None and not dedupe_result.get("dedupe_enabled", False) else None

    return {
        "write_enabled": True,
        "write_mode": write_mode,
        "table_name": table_name,
        "status": "written_in_chunks" if write_chunking_config else "written",
        "row_count": write_row_count,
        "clear_method": (clear_result or {}).get("clear_method", ""),
        "write_chunk_count": int(write_chunking_config.get("bucket_count", 0)) if write_chunking_config else 0,
        "dedupe_key_columns": dedupe_result.get("dedupe_key_columns", [])
    }


def get_runtime_context(databricks_env=None):
    runtime_env = databricks_env or os.getenv("Databricks_Env", "dev")
    runtime_env = runtime_env.strip().lower()

    if runtime_env not in {"dev", "tst", "stg", "prd"}:
        raise ValueError(f"Unsupported Databricks_Env '{runtime_env}'.")

    return {
        "databricks_env": runtime_env,
        "daily_source_config_path": os.getenv("Daily_Source_Config_Path", ""),
        "transactional_source_config_path": os.getenv("Transactional_Source_Config_Path", ""),
        "historical_source_config_path": os.getenv("Historical_Source_Config_Path", ""),
        "target_config_path": os.getenv("Target_Config_Path", ""),
        "loadtracker_target_catalog": os.getenv("Loadtracker_Target_Catalog", "").strip(),
        "loadtracker_target_schema": os.getenv("Loadtracker_Target_Schema", "").strip(),
        "loadtracker_target_table": os.getenv("Loadtracker_Target_Table", "").strip()
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
        raise KeyError(f"Entity '{entity_name}' was not found in source config. Available entities: {available_entities}")

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

    base_config = resolve_inherited_entity_config(entity_configs, inherited_entity_name, resolution_stack + [entity_name])
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
            raise ValueError(f"Entity '{entity_name}' is excluded but required by requested entity '{requested_entity_name}'.")

        if entity_name not in entity_configs:
            available_entities = ", ".join(entity_configs.keys())
            raise KeyError(f"Entity '{entity_name}' was not found in source config. Available entities: {available_entities}")

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


def get_entity_configs(source_config, requested_entity_name="", excluded_entity_names=None, exclude_dependency_only=False):
    entity_configs = source_config.get("entities")
    normalized_entity_name = (requested_entity_name or "").strip()
    excluded_entity_name_set = set(normalize_entity_name_list(excluded_entity_names))

    if not entity_configs:
        entity_name = normalized_entity_name or source_config.get("entity_name", "oracle_extract_preview")
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
        and not (exclude_dependency_only and _is_dependency_only_entity(resolve_inherited_entity_config(entity_configs, entity_name)))
    ]
    default_entity_name = source_config.get("default_entity_name", "").strip()

    if default_entity_name in entity_configs:
        ordered_entity_configs.sort(key=lambda item: item[0] != default_entity_name)

    return ordered_entity_configs


# COMMAND ----------

import base64
import time


SQL_WRITE_TOKEN_MIN_VALIDITY_SECONDS = 1800


def _decode_access_token_expiry_seconds(access_token):
    token_parts = str(access_token or "").split(".")
    if len(token_parts) < 2:
        return None

    payload_segment = token_parts[1]
    payload_segment += "=" * (-len(payload_segment) % 4)

    try:
        payload_bytes = base64.urlsafe_b64decode(payload_segment.encode("ascii"))
        payload = json.loads(payload_bytes.decode("utf-8"))
        expiry_epoch = int(payload.get("exp") or 0)
    except Exception:
        return None

    if expiry_epoch <= 0:
        return None

    return max(0, expiry_epoch - int(time.time()))


def get_sql_access_token_details(target_config, databricks_env, min_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    auth_config = target_config.get("authentication", {})
    tenant_id = _require_value(auth_config.get("tenant_id"), "authentication.tenant_id")
    client_id = _require_value(_resolve_required_env_value(auth_config.get("client_id_by_env", auth_config.get("client_id")), databricks_env, "authentication.client_id_by_env"), "authentication.client_id_by_env")
    secret_scope = _resolve_required_env_value(auth_config.get("secret_scope_by_env", auth_config.get("secret_scope")), databricks_env, "authentication.secret_scope_by_env")
    secret_key = _require_value(auth_config.get("secret_key"), "authentication.secret_key")
    token_scope = _require_value(auth_config.get("token_scope"), "authentication.token_scope")

    client_secret = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    request_body = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": token_scope,
        }
    ).encode("utf-8")
    request_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    token_request = Request(token_endpoint, data=request_body, headers=request_headers, method="POST")

    last_remaining_seconds = 0
    for token_attempt in range(2):
        try:
            with urlopen(token_request, timeout=30) as response:
                token_payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Service principal token request failed via {token_endpoint}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Service principal token endpoint was unreachable via {token_endpoint}: {exc.reason!r}") from exc

        access_token = token_payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"Token response did not contain an access token: {token_payload}")

        response_expires_in_seconds = _parse_token_expiry_seconds(token_payload)
        jwt_remaining_seconds = _decode_access_token_expiry_seconds(access_token)
        remaining_seconds = jwt_remaining_seconds if jwt_remaining_seconds is not None else response_expires_in_seconds
        last_remaining_seconds = int(remaining_seconds)

        if last_remaining_seconds > int(min_validity_seconds):
            return {
                "access_token": access_token,
                "remaining_seconds": last_remaining_seconds,
                "response_expires_in_seconds": int(response_expires_in_seconds),
                "token_attempt": token_attempt + 1,
            }

        if token_attempt == 0:
            print({
                "stage": "sql_access_token_low_validity_retry",
                "remaining_seconds": last_remaining_seconds,
                "response_expires_in_seconds": int(response_expires_in_seconds),
                "min_validity_seconds": int(min_validity_seconds),
            })

    raise RuntimeError(
        f"SQL access token lifetime ({last_remaining_seconds}s) was below the required minimum ({int(min_validity_seconds)}s)."
    )


def get_sql_access_token(target_config, databricks_env, min_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    return get_sql_access_token_details(
        target_config,
        databricks_env,
        min_validity_seconds=min_validity_seconds
    )["access_token"]


def build_sql_jdbc_options_with_metadata(target_config, databricks_env, table_name, min_token_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    connection_config = target_config.get("connection", {})
    jdbc_driver = _require_value(connection_config.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"), "connection.driver")
    token_details = get_sql_access_token_details(
        target_config,
        databricks_env,
        min_validity_seconds=min_token_validity_seconds
    )
    return ({
        "url": build_sql_jdbc_url(target_config, databricks_env),
        "driver": jdbc_driver,
        "dbtable": _get_qualified_sql_server_table_name(table_name),
        "accessToken": token_details["access_token"],
        "encrypt": "true",
        "trustServerCertificate": "false",
        "hostNameInCertificate": "*.database.windows.net"
    }, token_details)


def build_sql_jdbc_options(target_config, databricks_env, table_name, min_token_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    jdbc_options, _ = build_sql_jdbc_options_with_metadata(
        target_config,
        databricks_env,
        table_name,
        min_token_validity_seconds=min_token_validity_seconds
    )
    return jdbc_options


def write_dataframe_to_target(dataframe, target_config, databricks_env, table_name, write_mode="append", extra_options=None):
    def _write_once(attempt_number):
        jdbc_options, token_details = build_sql_jdbc_options_with_metadata(
            target_config,
            databricks_env,
            table_name,
            min_token_validity_seconds=SQL_WRITE_TOKEN_MIN_VALIDITY_SECONDS
        )
        print({
            "stage": "write_dataframe_to_target_start",
            "sql_write_tuning_version": SQL_WRITE_TUNING_VERSION,
            "attempt": attempt_number,
            "token_remaining_seconds": int(token_details["remaining_seconds"]),
            "token_response_expires_in_seconds": int(token_details["response_expires_in_seconds"]),
            "token_request_attempt": int(token_details["token_attempt"]),
            "min_token_validity_seconds": int(SQL_WRITE_TOKEN_MIN_VALIDITY_SECONDS),
            "diagnostics": get_sql_write_diagnostics(target_config, databricks_env, table_name, write_mode, extra_options=extra_options, dataframe=dataframe, jdbc_options=jdbc_options)
        })
        writer = dataframe.write.format("jdbc").mode(write_mode)
        for option_name, option_value in jdbc_options.items():
            writer = writer.option(option_name, option_value)
        for option_name, option_value in (extra_options or {}).items():
            writer = writer.option(option_name, option_value)
        writer.save()

    return _execute_sql_with_token_retry(
        "write_dataframe_to_target",
        _write_once,
        retry_on_token_expiry=True,
        retry_details={"table_name": table_name, "write_mode": write_mode}
    )

# COMMAND ----------

