# Databricks notebook source
import json
import os

dbutils.widgets.text("load_mode", "daily")
dbutils.widgets.text("source_config_path", "")
dbutils.widgets.text("target_config_path", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("excluded_entity_names", "")
dbutils.widgets.text("continue_on_error", "true")

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
requested_entity_name = _resolve_widget_value("entity_name")
excluded_entity_names = _resolve_widget_value("excluded_entity_names")
continue_on_error = _resolve_bool_widget_value("continue_on_error", default=True)

if not source_config_path:
    raise ValueError("source_config_path / Daily_Source_Config_Path / Transactional_Source_Config_Path is required.")
if not target_config_path:
    raise ValueError("target_config_path / Target_Config_Path is required (Azure SQL source connection stub).")

ensure_loadtracker_table()
source_config = load_json_config(source_config_path)
entity_configs = get_entity_configs(
    source_config,
    requested_entity_name,
    excluded_entity_names=excluded_entity_names,
    exclude_dependency_only=not bool(requested_entity_name.strip()),
)
worker_notebook_path = _get_sibling_notebook_path("Entity_Pipeline_Worker")
worker_results = []
worker_failures = []

for configured_entity_name, entity_config in entity_configs:
    entity_name = entity_config.get("entity_name", configured_entity_name)
    worker_arguments = {
        "load_mode": load_mode,
        "source_config_path": source_config_path,
        "target_config_path": target_config_path,
        "entity_name": configured_entity_name,
    }

    print(
        {
            "stage": "sequential_worker_start",
            "entity_name": entity_name,
            "configured_entity_name": configured_entity_name,
            "load_mode": load_mode,
            "worker_notebook_path": worker_notebook_path,
        }
    )

    try:
        worker_result_raw = dbutils.notebook.run(worker_notebook_path, 0, worker_arguments)
        worker_result = (
            json.loads(worker_result_raw)
            if worker_result_raw
            else {"entity_name": entity_name, "status": "success"}
        )
        worker_results.append(worker_result)
        print({"stage": "sequential_worker_success", "entity_name": entity_name, "result": worker_result})
    except Exception as exc:
        failure_details = {
            "stage": "sequential_worker_failure",
            "entity_name": entity_name,
            "configured_entity_name": configured_entity_name,
            "reason": str(exc),
        }
        worker_failures.append(failure_details)
        print(failure_details)
        if not continue_on_error:
            raise

summary = {
    "stage": "sequential_runner_summary",
    "load_mode": load_mode,
    "source_config_path": source_config_path,
    "target_config_path": target_config_path,
    "requested_entity_name": requested_entity_name,
    "excluded_entity_names": excluded_entity_names,
    "continue_on_error": continue_on_error,
    "success_count": len(worker_results),
    "failure_count": len(worker_failures),
    "results": worker_results,
    "failures": worker_failures,
}
print(summary)

if worker_failures:
    failed_entity_names = ", ".join(failure["entity_name"] for failure in worker_failures)
    raise RuntimeError(f"Sequential runner completed with failures for: {failed_entity_names}")

dbutils.notebook.exit(json.dumps(summary))
