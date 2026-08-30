"""Unit tests for azure_sql_etl configuration and pure helpers.

These tests run without Databricks/Spark. JDBC token retrieval and Delta writes
require a Databricks runtime and are validated statically / by config contract.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]  # .../TDPTSXX-AzureSql-ETL/src
PROJECT_ROOT = ROOT.parent  # .../TDPTSXX-AzureSql-ETL
CONFIGS = ROOT / "configs"
UTILS = ROOT / "utils"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Strip Databricks notebook markers for local import.
    source = path.read_text(encoding="utf-8")
    source = re.sub(r"(?m)^# MAGIC.*\n?", "", source)
    source = re.sub(r"(?m)^# COMMAND -+\n?", "", source)
    source = source.replace("# Databricks notebook source\n", "")
    code = compile(source, str(path), "exec")
    sys.modules[module_name] = module
    exec(code, module.__dict__)
    return module


class ConfigLoadingTests(unittest.TestCase):
    def test_hourly_and_daily_configs_load(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        daily = json.loads((CONFIGS / "azure_sql_extract_daily.json").read_text())
        stub = json.loads((CONFIGS / "azure_sql_source_stub.json").read_text())

        self.assertEqual(hourly["time_grain"], "HOURLY")
        self.assertEqual(daily["time_grain"], "DAILY")
        self.assertEqual(set(hourly["entities"]), {"DispatchNonDrivingStandard"})
        self.assertEqual(set(daily["entities"]), {"DispatchActivityStandard"})
        self.assertEqual(stub["authentication"]["mode"], "service_principal")
        self.assertEqual(stub["authentication"]["secret_key"], "database-sp-sec")
        self.assertIn("connection", stub)
        self.assertNotIn("client_secret", json.dumps(stub))

    def test_entities_are_not_duplicated_across_frequencies(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        daily = json.loads((CONFIGS / "azure_sql_extract_daily.json").read_text())
        overlap = set(hourly["entities"]) & set(daily["entities"])
        self.assertEqual(overlap, set(), msg=f"Entities appear in both hourly and daily: {overlap}")

    def test_azure_sql_source_recognized(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        for entity in hourly["entities"].values():
            self.assertEqual(entity["jdbc"]["database"], "azure_sql")
            self.assertEqual(entity["jdbc"]["connection_source"], "target_config")

    def test_hourly_incremental_and_merge_flags(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        for entity in hourly["entities"].values():
            self.assertTrue(entity["incremental"]["enabled"])
            self.assertEqual(entity["incremental"]["watermark_column"], "UpdatedDateTime")
            self.assertTrue(entity["target"]["raw_merge_on_primary_key"])
            self.assertEqual(entity["target"]["dedupe_order_columns"], ["UpdatedDateTime", "CreatedDateTime"])
            self.assertEqual(entity["target"]["primary_key_columns"], ["DispatchAlias"])

    def test_daily_full_load_flags(self):
        daily = json.loads((CONFIGS / "azure_sql_extract_daily.json").read_text())
        for entity in daily["entities"].values():
            self.assertFalse(entity["incremental"]["enabled"])
            self.assertFalse(entity["target"]["raw_merge_on_primary_key"])
            self.assertEqual(entity["target"]["primary_key_columns"], ["DispatchAlias"])

    def test_schema_derived_from_source_queries(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        daily = json.loads((CONFIGS / "azure_sql_extract_daily.json").read_text())
        config_utils = _load_module("azure_sql_etl_config_utils_schema", UTILS / "config_utils.py")
        for entity in list(hourly["entities"].values()) + list(daily["entities"].values()):
            validated = config_utils.validate_entity_schema(entity)
            self.assertTrue(validated["schema_columns"])
            self.assertEqual(validated["schema_columns"], validated["query_columns"])
            self.assertIn("DispatchAlias", validated["schema_columns"])
            self.assertEqual(validated["primary_key_columns"], ["DispatchAlias"])
            query = next(iter(entity["extract"]["query_by_env"].values())).upper()
            self.assertNotRegex(query, r"\bTOP\s*\(")

    def test_unity_catalog_conventions_preserved(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        daily = json.loads((CONFIGS / "azure_sql_extract_daily.json").read_text())
        nondriving = hourly["entities"]["DispatchNonDrivingStandard"]["target"]
        activity = daily["entities"]["DispatchActivityStandard"]["target"]

        self.assertEqual(nondriving["target_entity"], "TDPS_DispatchNonDrivingStandard")
        self.assertEqual(activity["target_entity"], "TDPS_DispatchActivityStandard")
        self.assertEqual(nondriving["target_uc_schema"], "transportation_dispatchsite_raw")
        self.assertEqual(activity["target_uc_schema"], "transportation_dispatchsite_raw")
        self.assertEqual(nondriving["target_unity_catalog_by_env"]["dev"], "ent_dtlk_dev")
        self.assertEqual(nondriving["secondary_unity_catalog_by_env"]["dev"], "entc_dtlk_dev")
        self.assertTrue(nondriving["target_file_path"].endswith("/DispatchSite/Raw/"))
        self.assertEqual(nondriving["target_file_format"], "Delta")


class HelperLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_utils = _load_module("azure_sql_etl_config_utils", UTILS / "config_utils.py")
        cls.jdbc_utils = _load_module("azure_sql_etl_jdbc_utils", UTILS / "jdbc_utils.py")
        cls.delta_utils = _load_module("azure_sql_etl_delta_utils", UTILS / "delta_utils.py")

    def test_entity_configs_resolved(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        entities = self.config_utils.get_entity_configs(hourly, exclude_dependency_only=True)
        names = [name for name, _ in entities]
        self.assertEqual(names, ["DispatchNonDrivingStandard"])

    def test_incremental_query_uses_datetime2(self):
        query = self.config_utils.build_incremental_extract_query(
            "SELECT 1 AS UpdatedDateTime FROM dbo.T",
            "UpdatedDateTime",
            "2024-01-02 03:04:05.678",
        )
        self.assertIn("CAST('2024-01-02 03:04:05.678' AS datetime2)", query)
        self.assertIn("INCREMENTAL_SOURCE", query)

    def test_jdbc_options_use_secret_scope_not_hardcoded_secret(self):
        stub = json.loads((CONFIGS / "azure_sql_source_stub.json").read_text())
        entity = {
            "jdbc": {
                "database": "azure_sql",
                "connection_source": "target_config",
                "options": {"fetchsize": "10000"},
            }
        }

        fake_dbutils = mock.Mock()
        fake_dbutils.secrets.get.return_value = "secret-value-not-logged"
        token_payload = json.dumps({"access_token": "token-value", "expires_in": 3600}).encode("utf-8")

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return token_payload

        with mock.patch.object(self.jdbc_utils, "dbutils", fake_dbutils, create=True), mock.patch.object(
            self.jdbc_utils, "urlopen", return_value=FakeResponse()
        ):
            options = self.jdbc_utils.build_jdbc_options(entity, "dev", target_config=stub)

        self.assertIn("accessToken", options)
        self.assertTrue(options["url"].startswith("jdbc:sqlserver://cutdasqltdps01"))
        fake_dbutils.secrets.get.assert_called_once_with(scope="cutdkyvtdbwss0tdpsxx01", key="database-sp-sec")
        redacted = self.jdbc_utils.redact_jdbc_options(options)
        self.assertEqual(redacted["accessToken"], "<redacted>")

    def test_delta_paths_for_dev(self):
        hourly = json.loads((CONFIGS / "azure_sql_extract_hourly.json").read_text())
        paths = self.delta_utils.resolve_delta_target_paths(
            hourly["entities"]["DispatchNonDrivingStandard"],
            "dev",
        )
        self.assertEqual(
            paths["primary_raw_table"],
            "ent_dtlk_dev.transportation_dispatchsite_raw.TDPS_DispatchNonDrivingStandard",
        )
        self.assertEqual(
            paths["primary_transform_table"],
            "ent_dtlk_dev.transportation_dispatchsite_transform.TDPS_DispatchNonDrivingStandard",
        )
        self.assertEqual(
            paths["secondary_raw_table"],
            "entc_dtlk_dev.transportation_dispatchsite_raw.TDPS_DispatchNonDrivingStandard",
        )
        self.assertIn("/DispatchSite/Raw/TDPS_DispatchNonDrivingStandard/Delta", paths["primary_raw_path"])
        self.assertIn("/DispatchSite/Transform/TDPS_DispatchNonDrivingStandard/Delta", paths["primary_transform_path"])
        self.assertTrue(paths["raw_merge_on_primary_key"])
        self.assertEqual(paths["primary_key_columns"], ["DispatchAlias"])

    def test_merge_condition(self):
        condition = self.delta_utils.build_merge_condition(["DispatchAlias"])
        self.assertEqual(
            condition,
            "target.`DispatchAlias` = source.`DispatchAlias`",
        )


class IndependenceTests(unittest.TestCase):
    FORBIDDEN = [
        "Wrapper.ipynb",
        "Runner.ipynb",
        "Wrapper_Backfill",
        "datamapping_AzureSql",
        "datamapping_Hourly",
        "datamapping_Daily",
        "../utils/jdbc_utils",
        "src/utils/jdbc_utils",
        "%run ../",
        "%run ../../",
    ]

    def test_no_legacy_orchestration_references(self):
        offenders = []
        code_suffixes = {".py", ".json", ".yml", ".yaml"}
        for path in ROOT.rglob("*"):
            if path.is_dir() or path.suffix not in code_suffixes:
                continue
            if "tests" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            for token in self.FORBIDDEN:
                if token in text:
                    offenders.append(f"{path.relative_to(ROOT)} -> {token}")
        self.assertEqual(offenders, [], msg="Legacy references found:\n" + "\n".join(offenders))

    def test_project_layout_matches_dataload_style(self):
        self.assertTrue((PROJECT_ROOT / "databricks.yml").exists())
        self.assertTrue((PROJECT_ROOT / "configs" / "vars_base.yml").exists())
        self.assertTrue((PROJECT_ROOT / "resources" / "jobs" / "azuresql_etl_jobs.yml").exists())
        self.assertTrue((PROJECT_ROOT / "targets" / "dev.yml").exists())
        self.assertTrue((PROJECT_ROOT / "azure-pipelines" / "azure-pipelines.yml").exists())
        self.assertTrue((ROOT / "Entity_Sequential_Runner.py").exists())
        self.assertTrue((CONFIGS / "azure_sql_source_stub.json").exists())
        self.assertTrue((CONFIGS / "azure_sql_extract_hourly.json").exists())
        self.assertTrue((CONFIGS / "azure_sql_extract_daily.json").exists())


if __name__ == "__main__":
    unittest.main()
