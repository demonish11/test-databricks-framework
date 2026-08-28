# Databricks notebook source
import json
import os

dbutils.widgets.text("load_mode", "daily")
dbutils.widgets.text("source_config_path", "")
dbutils.widgets.text("target_config_path", "")
dbutils.widgets.text("entity_name", "")

# COMMAND ----------

# MAGIC %run ./utils/config_utils.py

# COMMAND ----------

def _resolve_widget_value(widget_name, fallback_env_var=None):
    value = dbutils.widgets.get(widget_name).strip()
    if value:
        return value
    if fallback_env_var:
        return os.getenv(fallback_env_var, "").strip()
    return ""


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


def _get_current_notebook_path():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


def _get_sibling_notebook_path(notebook_name):
    notebook_directory = _get_current_notebook_path().rsplit("/", 1)[0]
    return f"{notebook_directory}/{notebook_name}"


load_mode = _resolve_widget_value("load_mode") or "daily"
source_config_path = _resolve_source_config_path(load_mode)
target_config_path = _resolve_widget_value("target_config_path", "Target_Config_Path")
configured_entity_name = _resolve_widget_value("entity_name")

if not configured_entity_name:
    raise ValueError("Entity_Pipeline_Worker requires a specific entity_name.")
if not source_config_path:
    raise ValueError("source_config_path is required.")
if not target_config_path:
    raise ValueError("target_config_path is required.")

source_config = load_json_config(source_config_path)
entity_configs = source_config.get("entities") or {}

if entity_configs:
    if configured_entity_name not in entity_configs:
        available_entities = ", ".join(entity_configs.keys())
        raise KeyError(
            f"Entity '{configured_entity_name}' was not found in source config. Available entities: {available_entities}"
        )
else:
    configured_root_entity_name = str(source_config.get("entity_name", "")).strip()
    if configured_root_entity_name and configured_root_entity_name != configured_entity_name:
        raise KeyError(
            f"Entity '{configured_entity_name}' does not match root config entity '{configured_root_entity_name}'."
        )

source_config_json = json.dumps(source_config)
base_parameters = {
    "load_mode": load_mode,
    "source_config_path": source_config_json,
    "target_config_path": target_config_path,
    "entity_name": configured_entity_name,
    "excluded_entity_names": "",
    "continue_on_error": "false",
}

extract_notebook_path = _get_sibling_notebook_path("Extracts_Wrapper")
transform_notebook_path = _get_sibling_notebook_path("Transforms_Wrapper")
completion_notebook_path = _get_sibling_notebook_path("Completion")

print(
    {
        "stage": "entity_pipeline_worker_start",
        "entity_name": configured_entity_name,
        "load_mode": load_mode,
        "extract_notebook_path": extract_notebook_path,
        "transform_notebook_path": transform_notebook_path,
        "completion_notebook_path": completion_notebook_path,
    }
)

dbutils.notebook.run(extract_notebook_path, 0, base_parameters)
dbutils.notebook.run(transform_notebook_path, 0, base_parameters)
dbutils.notebook.run(completion_notebook_path, 0, base_parameters)

result = {
    "stage": "entity_pipeline_worker_summary",
    "entity_name": configured_entity_name,
    "load_mode": load_mode,
    "status": "success",
}
print(result)
dbutils.notebook.exit(json.dumps(result))
