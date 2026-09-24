# Databricks notebook source
# MAGIC %md
# MAGIC # MATM table sample export
# MAGIC Reads the table list from `Matm_tables_format.xlsx`, queries each MySQL table for
# MAGIC the first 50 rows, and writes one CSV per table under the output folder:
# MAGIC
# MAGIC ```
# MAGIC {output_folder}/{database}/{table}.csv
# MAGIC ```
# MAGIC
# MAGIC **Requirements**
# MAGIC - Network access from Databricks compute to `10.219.252.18:3306`
# MAGIC - Secret scope `mysql-replica` with keys `username` and `password`
# MAGIC - Place `Matm_tables_format.xlsx` in the same workspace folder as this notebook,
# MAGIC   or set the **Excel path** widget (DBFS or Unity Catalog Volume path)
# MAGIC
# MAGIC **Path examples**
# MAGIC - Workspace folder (default): leave widgets blank
# MAGIC - DBFS input: `/dbfs/FileStore/shared_uploads/your_folder/Matm_tables_format.xlsx`
# MAGIC - DBFS output: `/dbfs/FileStore/shared_uploads/your_folder/table_samples`
# MAGIC - Volume output: `/Volumes/catalog/schema/volume/table_samples`

# COMMAND ----------

# MAGIC %pip install pymysql openpyxl -q

# COMMAND ----------

dbutils.widgets.text("template_excel_path", "", "Excel path (optional)")
dbutils.widgets.text("output_folder_path", "", "Output folder path (optional)")
dbutils.widgets.text("table_limit", "", "Table limit for testing (optional)")
dbutils.widgets.text("row_limit", "50", "Rows per table (default 50)")

import os
import re
from pathlib import PurePosixPath

import pandas as pd
import pymysql

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

TEMPLATE_EXCEL = "Matm_tables_format.xlsx"
MAIN_SHEET = "Matm_tables_format"
DEFAULT_OUTPUT_FOLDER = "table_samples"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

TEMPLATE_EXCEL_PATH = dbutils.widgets.get("template_excel_path").strip() or None
OUTPUT_FOLDER_PATH = dbutils.widgets.get("output_folder_path").strip() or None
_table_limit_raw = dbutils.widgets.get("table_limit").strip()
TABLE_LIMIT = int(_table_limit_raw) if _table_limit_raw else None
_row_limit_raw = dbutils.widgets.get("row_limit").strip()
ROW_LIMIT = int(_row_limit_raw) if _row_limit_raw else 50


def get_connection(database=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PWD,
        database=database,
        connect_timeout=10,
        read_timeout=120,
        charset="utf8mb4",
    )


def get_notebook_directory():
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    workspace_notebook_path = (
        notebook_path
        if notebook_path.startswith("/Workspace/")
        else f"/Workspace{notebook_path}"
    )
    return str(PurePosixPath(workspace_notebook_path).parent)


def resolve_local_path(path):
    if path.startswith("/dbfs/"):
        return path
    if path.startswith("dbfs:/"):
        return path.replace("dbfs:", "/dbfs", 1)
    return path


def ensure_directory(path):
    local_path = resolve_local_path(path)
    os.makedirs(local_path, exist_ok=True)


def fetch_table_rows(database, table, limit=ROW_LIMIT):
    if not IDENTIFIER_PATTERN.fullmatch(database) or not IDENTIFIER_PATTERN.fullmatch(table):
        raise ValueError(f"Invalid database or table identifier: {database}.{table}")

    with get_connection(database) as connection:
        query = f"SELECT * FROM `{database}`.`{table}` LIMIT {int(limit)}"
        return pd.read_sql(query, connection)


def export_table_csv(database, table, output_folder):
    rows_df = fetch_table_rows(database, table)
    table_dir = str(PurePosixPath(output_folder) / database)
    ensure_directory(table_dir)

    csv_path = str(PurePosixPath(table_dir) / f"{table}.csv")
    rows_df.to_csv(resolve_local_path(csv_path), index=False)
    return csv_path, len(rows_df)


# COMMAND ----------

template_path = TEMPLATE_EXCEL_PATH or f"{get_notebook_directory()}/{TEMPLATE_EXCEL}"
output_folder = OUTPUT_FOLDER_PATH or f"{get_notebook_directory()}/{DEFAULT_OUTPUT_FOLDER}"

sheet_df = pd.read_excel(template_path, sheet_name=MAIN_SHEET, dtype=str)
sheet_df.columns = [str(col).strip() for col in sheet_df.columns]

required_columns = {"Database", "Table"}
missing_columns = required_columns - set(sheet_df.columns)
if missing_columns:
    raise ValueError(f"Missing required columns in '{MAIN_SHEET}': {sorted(missing_columns)}")

if TABLE_LIMIT:
    sheet_df = sheet_df.head(TABLE_LIMIT)

ensure_directory(output_folder)

print(f"Loaded {len(sheet_df):,} tables from '{MAIN_SHEET}' in {template_path}")
print(f"Exporting up to {ROW_LIMIT} rows per table to {output_folder}")

# COMMAND ----------

results = []
failures = []

for index, row in sheet_df.iterrows():
    database = str(row["Database"]).strip()
    table_name = str(row["Table"]).strip()

    if not database or not table_name:
        failures.append(
            {
                "database": database,
                "table": table_name,
                "error": "Missing database or table name",
            }
        )
        continue

    try:
        csv_path, row_count = export_table_csv(database, table_name, output_folder)
        results.append(
            {
                "database": database,
                "table": table_name,
                "csv_path": csv_path,
                "row_count": row_count,
                "status": "success",
            }
        )
        print(f"[{len(results):,}/{len(sheet_df):,}] Wrote {row_count:,} rows to {csv_path}")
    except Exception as exc:
        failures.append(
            {
                "database": database,
                "table": table_name,
                "error": str(exc),
            }
        )
        print(f"Failed {database}.{table_name}: {exc}")

# COMMAND ----------

summary_df = pd.DataFrame(results + [
    {
        "database": item["database"],
        "table": item["table"],
        "csv_path": None,
        "row_count": None,
        "status": f"failed: {item['error']}",
    }
    for item in failures
])

print(
    f"Finished: {len(results):,} succeeded, {len(failures):,} failed, "
    f"{len(sheet_df):,} total"
)

display(summary_df)
