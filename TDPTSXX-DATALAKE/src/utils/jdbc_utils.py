# Databricks notebook source
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from delta.tables import DeltaTable

SQL_TOKEN_MIN_VALIDITY_SECONDS = 300
AZURE_SQL_DB_TYPES = {"AZURESQL", "SQLSERVER", "SQL", "AZURE_SQL", "MSSQL"}


def _require_value(value, field_name):
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"Missing required value for '{field_name}'.")

    return value


def _resolve_env_setting(settings, environment, field_name):
    if isinstance(settings, dict):
        if environment in settings:
            return settings[environment]
        raise KeyError(f"Missing environment '{environment}' for '{field_name}'.")

    return settings


def _normalize_jdbc_option_value(value):
    return str(value) if isinstance(value, (int, float, bool)) else value


def _normalize_db_type(db_type):
    return str(db_type or "").strip().upper()


def is_azure_sql_db_type(db_type):
    return _normalize_db_type(db_type) in AZURE_SQL_DB_TYPES


def load_json_config(config_path):
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def _build_sql_server_jdbc_url(jdbc_config, environment):
    sql_server = _resolve_env_setting(
        jdbc_config.get("server_by_env", jdbc_config.get("server")),
        environment,
        "jdbc.server_by_env",
    )
    sql_database = _resolve_env_setting(
        jdbc_config.get("database_by_env", jdbc_config.get("database")),
        environment,
        "jdbc.database_by_env",
    )
    sql_port = _require_value(jdbc_config.get("port", 1433), "jdbc.port")

    return (
        f"jdbc:sqlserver://{_require_value(sql_server, 'jdbc.server_by_env')}:{sql_port};"
        f"database={_require_value(sql_database, 'jdbc.database_by_env')};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;"
    )


def _get_sql_server_access_token(source_config, environment, min_validity_seconds=SQL_TOKEN_MIN_VALIDITY_SECONDS):
    auth_config = source_config.get("authentication", {})
    tenant_id = _require_value(auth_config.get("tenant_id"), "authentication.tenant_id")
    client_id = _require_value(
        _resolve_env_setting(
            auth_config.get("client_id_by_env", auth_config.get("client_id")),
            environment,
            "authentication.client_id_by_env",
        ),
        "authentication.client_id_by_env",
    )
    secret_scope = _resolve_env_setting(
        auth_config.get("secret_scope_by_env", auth_config.get("secret_scope")),
        environment,
        "authentication.secret_scope_by_env",
    )
    secret_key = _require_value(auth_config.get("secret_key"), "authentication.secret_key")
    token_scope = _require_value(auth_config.get("token_scope"), "authentication.token_scope")

    client_secret = dbutils.secrets.get(
        scope=_require_value(secret_scope, "authentication.secret_scope_by_env"),
        key=secret_key,
    )
    token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    request_body = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": token_scope,
        }
    ).encode("utf-8")
    token_request = Request(
        token_endpoint,
        data=request_body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

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

    expires_in_seconds = int(token_payload.get("expires_in") or 0)
    if expires_in_seconds and expires_in_seconds < int(min_validity_seconds):
        raise RuntimeError(
            f"SQL access token lifetime ({expires_in_seconds}s) was below the required minimum ({int(min_validity_seconds)}s)."
        )

    return access_token


def build_sql_server_jdbc_options(source_config, environment):
    jdbc_config = source_config.get("jdbc", {})
    auth_config = source_config.get("authentication", {})
    auth_mode = str(auth_config.get("mode", "username_password")).strip().lower()
    driver = jdbc_config.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    user_options = dict(jdbc_config.get("options", {}))

    options = {
        "url": _build_sql_server_jdbc_url(jdbc_config, environment),
        "driver": driver,
        "encrypt": "true",
        "trustServerCertificate": "false",
        "hostNameInCertificate": "*.database.windows.net",
    }

    if auth_mode == "service_principal":
        options["accessToken"] = _get_sql_server_access_token(source_config, environment)
    else:
        secret_scope = _resolve_env_setting(
            jdbc_config.get("secret_scope_by_env"),
            environment,
            "jdbc.secret_scope_by_env",
        )
        password_key = _resolve_env_setting(
            jdbc_config.get("password_key_by_env"),
            environment,
            "jdbc.password_key_by_env",
        )
        username = _resolve_env_setting(
            jdbc_config.get("username_by_env"),
            environment,
            "jdbc.username_by_env",
        )
        options["user"] = _require_value(username, "jdbc.username_by_env")
        options["password"] = dbutils.secrets.get(scope=secret_scope, key=password_key)

    for key, value in user_options.items():
        options[key] = _normalize_jdbc_option_value(value)

    return options


def read_jdbc_dataframe(spark, args, sqlstmt):
    db_type = _normalize_db_type(args.get("dbtype"))

    if is_azure_sql_db_type(db_type):
        source_config = args.get("source_connection_config")
        if not isinstance(source_config, dict):
            raise ValueError("Azure SQL entities require source_connection_config to be loaded in args.")

        options = build_sql_server_jdbc_options(source_config, args["DatabricksEnv"])
        jdbc_url = options.pop("url")
        driver = options.pop("driver")
        properties = {"driver": driver, **options}
        fetch_size = properties.pop("fetchsize", "10000")
        return spark.read.option("fetchsize", fetch_size).jdbc(
            url=jdbc_url,
            table=sqlstmt,
            properties=properties,
        )

    if db_type in {"ORACLE", "DB2"}:
        connection_details = {
            "user": args["user"],
            "password": dbutils.secrets.get(scope=args["keyvault"], key=args["password"]),
            "driver": args["driver"],
        }
        return spark.read.option("fetchsize", "10000").jdbc(
            url=args["dburl"],
            table=sqlstmt,
            properties=connection_details,
        )

    raise ValueError(f"Unsupported DBType '{args.get('dbtype')}' for JDBC extraction.")


def dedupe_dataframe(dataframe, args):
    dedupe_keys = [key.strip() for key in args.get("dedupe_key_columns", "").split(",") if key.strip()]
    if not dedupe_keys:
        return dataframe

    order_columns = [column.strip() for column in args.get("dedupe_order_columns", "").split(",") if column.strip()]
    if not order_columns:
        order_columns = dedupe_keys

    order_expressions = [col(column_name).desc() for column_name in order_columns]
    window_spec = Window.partitionBy(*[col(column_name) for column_name in dedupe_keys]).orderBy(*order_expressions)
    return (
        dataframe.withColumn("_dedupe_rank", row_number().over(window_spec))
        .filter(col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )


def build_merge_condition(primary_keys):
    merge_condition = ""
    for key in primary_keys:
        predicate = f"target.{key} = source.{key}"
        merge_condition = predicate if not merge_condition else f"{merge_condition} and {predicate}"
    return merge_condition


def merge_dataframe_into_delta(spark, dataframe, delta_path, merge_condition):
    delta_table = DeltaTable.forPath(spark, delta_path)
    (
        delta_table.alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def format_sql_server_watermark(timestamp_value):
    if timestamp_value is None:
        raise ValueError("Watermark timestamp is required for incremental Azure SQL extraction.")

    if hasattr(timestamp_value, "strftime"):
        return timestamp_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return str(timestamp_value)
