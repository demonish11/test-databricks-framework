# Databricks notebook source
"""MySQL JDBC helpers for MATM → Unity Catalog ingestion."""


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
    auth_config = source_config.get("authentication", {})

    username_by_env = jdbc_config.get("username_by_env") or auth_config.get("username_by_env")
    if username_by_env is not None:
        return _require_value(
            _resolve_env_setting(username_by_env, environment, "jdbc.username_by_env"),
            "jdbc.username_by_env",
        )

    secret_scope = _resolve_env_setting(
        auth_config.get("secret_scope_by_env", auth_config.get("secret_scope")),
        environment,
        "authentication.secret_scope_by_env",
    )
    username_key = _require_value(
        auth_config.get("username_key", "username"),
        "authentication.username_key",
    )
    return dbutils.secrets.get(
        scope=_require_value(secret_scope, "authentication.secret_scope_by_env"),
        key=username_key,
    )


def get_jdbc_password(source_config, environment):
    jdbc_config = source_config.get("jdbc", {})
    auth_config = source_config.get("authentication", {})

    secret_scope = _resolve_env_setting(
        jdbc_config.get("secret_scope_by_env")
        or auth_config.get("secret_scope_by_env", auth_config.get("secret_scope")),
        environment,
        "jdbc.secret_scope_by_env",
    )
    password_key = _resolve_env_setting(
        jdbc_config.get("password_key_by_env")
        or auth_config.get("password_key", "password"),
        environment,
        "jdbc.password_key_by_env",
    )
    _require_value(secret_scope, "jdbc.secret_scope_by_env")
    _require_value(password_key, "jdbc.password_key_by_env")
    return dbutils.secrets.get(scope=secret_scope, key=password_key)


def _build_mysql_jdbc_url(jdbc_config, environment):
    mysql_host = _resolve_env_setting(
        jdbc_config.get("server_by_env", jdbc_config.get("server", jdbc_config.get("host"))),
        environment,
        "jdbc.server_by_env",
    )
    mysql_database = _resolve_env_setting(
        jdbc_config.get("database_by_env", jdbc_config.get("schema", jdbc_config.get("database"))),
        environment,
        "jdbc.database_by_env",
    )
    mysql_port = _require_value(jdbc_config.get("port", 3306), "jdbc.port")
    query_params = jdbc_config.get(
        "url_params",
        "zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8",
    )
    return (
        f"jdbc:mysql://{_require_value(mysql_host, 'jdbc.server_by_env')}:{mysql_port}/"
        f"{_require_value(mysql_database, 'jdbc.database_by_env')}"
        f"?{query_params}"
    )


def _get_mysql_source_config(source_config, target_config):
    jdbc_config = source_config.get("jdbc", {})
    connection_source = str(jdbc_config.get("connection_source", "")).strip().lower()
    if connection_source != "target_config":
        return source_config

    if not target_config:
        raise ValueError(
            "MySQL source requested jdbc.connection_source='target_config' but target_config_path "
            "was not provided or could not be loaded."
        )

    target_connection = dict(target_config.get("connection", {}))
    target_authentication = dict(target_config.get("authentication", {}))

    # Entity-level database/schema overrides the shared connection stub so one host
    # can serve many MATM microservice databases.
    entity_database = jdbc_config.get("database_by_env") or jdbc_config.get("database")
    merged_jdbc_config = {
        "server_by_env": target_connection.get(
            "server_by_env", target_connection.get("server", target_connection.get("host"))
        ),
        "database_by_env": entity_database
        or target_connection.get("database_by_env", target_connection.get("database")),
        "port": target_connection.get("port", 3306),
        "driver": jdbc_config.get(
            "driver",
            target_connection.get("driver", "com.mysql.cj.jdbc.Driver"),
        ),
        "url_params": jdbc_config.get("url_params", target_connection.get("url_params")),
        "options": dict(jdbc_config.get("options", {})),
        "username_by_env": jdbc_config.get("username_by_env")
        or target_connection.get("username_by_env"),
        "secret_scope_by_env": jdbc_config.get("secret_scope_by_env"),
        "password_key_by_env": jdbc_config.get("password_key_by_env"),
    }
    return {"jdbc": merged_jdbc_config, "authentication": target_authentication}


def _build_mysql_options(source_config, environment, target_config=None):
    effective_source_config = _get_mysql_source_config(source_config, target_config)
    jdbc_config = effective_source_config.get("jdbc", {})
    auth_config = effective_source_config.get("authentication", {})
    auth_mode = str(auth_config.get("mode", "username_password")).strip().lower()
    driver = jdbc_config.get("driver", "com.mysql.cj.jdbc.Driver")
    user_options = dict(jdbc_config.get("options", {}) or {})

    options = {
        "url": _build_mysql_jdbc_url(jdbc_config, environment),
        "driver": driver,
    }

    if auth_mode != "username_password":
        raise ValueError(
            f"Unsupported MySQL authentication mode '{auth_mode}'. Use 'username_password'."
        )

    options["user"] = get_jdbc_username(effective_source_config, environment)
    options["password"] = get_jdbc_password(effective_source_config, environment)

    for key, value in user_options.items():
        options[key] = _normalize_jdbc_option_value(value)

    return options


def build_jdbc_options(source_config, environment, target_config=None):
    jdbc_config = source_config.get("jdbc", {})
    database_type = str(jdbc_config.get("database", "")).strip().lower()

    if database_type in {"mysql", "mariadb", "matm_mysql"}:
        return _build_mysql_options(source_config, environment, target_config=target_config)

    raise ValueError(
        f"Unsupported database type '{database_type}'. MATM MySQL ETL supports mysql/mariadb only."
    )


def redact_jdbc_options(jdbc_options):
    return {
        key: ("<redacted>" if key.lower() in {"user", "password", "accesstoken"} else value)
        for key, value in (jdbc_options or {}).items()
    }
