# Databricks notebook source
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SQL_TOKEN_MIN_VALIDITY_SECONDS = 300


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


def get_jdbc_username(source_config, environment):
    jdbc_config = source_config.get("jdbc", {})
    username = _resolve_env_setting(jdbc_config.get("username_by_env"), environment, "jdbc.username_by_env")
    return _require_value(username, "jdbc.username_by_env")


def get_jdbc_password(source_config, environment):
    jdbc_config = source_config.get("jdbc", {})
    secret_scope = _resolve_env_setting(jdbc_config.get("secret_scope_by_env"), environment, "jdbc.secret_scope_by_env")
    password_key = _resolve_env_setting(jdbc_config.get("password_key_by_env"), environment, "jdbc.password_key_by_env")
    _require_value(secret_scope, "jdbc.secret_scope_by_env")
    _require_value(password_key, "jdbc.password_key_by_env")
    return dbutils.secrets.get(scope=secret_scope, key=password_key)


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
        raise RuntimeError(
            f"Service principal token endpoint was unreachable via {token_endpoint}: {exc.reason!r}"
        ) from exc

    access_token = token_payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Token response did not contain an access token: {token_payload}")

    expires_in_seconds = int(token_payload.get("expires_in") or 0)
    if expires_in_seconds and expires_in_seconds < int(min_validity_seconds):
        raise RuntimeError(
            f"SQL access token lifetime ({expires_in_seconds}s) was below the required minimum ({int(min_validity_seconds)}s)."
        )

    return access_token


def _get_sql_server_source_config(source_config, target_config):
    jdbc_config = source_config.get("jdbc", {})
    connection_source = str(jdbc_config.get("connection_source", "")).strip().lower()
    if connection_source != "target_config":
        return source_config

    if not target_config:
        raise ValueError(
            "SQL source requested jdbc.connection_source='target_config' but target_config_path was not provided or could not be loaded."
        )

    target_connection = dict(target_config.get("connection", {}))
    target_authentication = dict(target_config.get("authentication", {}))
    merged_jdbc_config = {
        "server_by_env": target_connection.get("server_by_env", target_connection.get("server")),
        "database_by_env": target_connection.get("database_by_env", target_connection.get("database")),
        "port": target_connection.get("port", 1433),
        "driver": jdbc_config.get(
            "driver",
            target_connection.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
        ),
        "options": dict(jdbc_config.get("options", {})),
    }
    return {"jdbc": merged_jdbc_config, "authentication": target_authentication}


def _build_sql_server_options(source_config, environment, target_config=None):
    effective_source_config = _get_sql_server_source_config(source_config, target_config)
    jdbc_config = effective_source_config.get("jdbc", {})
    auth_config = effective_source_config.get("authentication", {})
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
        options["accessToken"] = _get_sql_server_access_token(effective_source_config, environment)
    else:
        options["user"] = get_jdbc_username(effective_source_config, environment)
        options["password"] = get_jdbc_password(effective_source_config, environment)

    for key, value in user_options.items():
        options[key] = _normalize_jdbc_option_value(value)

    return options


def build_jdbc_options(source_config, environment, target_config=None):
    jdbc_config = source_config.get("jdbc", {})
    database_type = str(jdbc_config.get("database", "")).strip().lower()

    if database_type in {"sqlserver", "sql_server", "azure_sql", "mssql"}:
        return _build_sql_server_options(source_config, environment, target_config=target_config)

    raise ValueError(
        f"Unsupported database type '{database_type}'. Azure SQL ETL supports azure_sql/sqlserver only."
    )


def redact_jdbc_options(jdbc_options):
    return {
        key: ("<redacted>" if key.lower() in {"user", "password", "accesstoken"} else value)
        for key, value in (jdbc_options or {}).items()
    }
