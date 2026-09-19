# Databricks notebook source
# MAGIC %md
# MAGIC # MATM tables metadata enrichment
# MAGIC Reads table list from `Matm_tables_format.csv`, pulls schema metadata from MySQL,
# MAGIC generates **Table Description** via the Databricks model, and stores results in a DataFrame.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Network access from Databricks compute to `10.219.252.18:3306`
# MAGIC - Secret scope `mysql-replica` with keys `username` and `password`
# MAGIC - Place `Matm_tables_format.csv` in the same workspace folder as this notebook,
# MAGIC   or set the **CSV path** widget (DBFS or Unity Catalog Volume path)
# MAGIC
# MAGIC **Path examples**
# MAGIC - Workspace folder (default): leave widget blank
# MAGIC - DBFS: `/dbfs/FileStore/shared_uploads/your_folder/Matm_tables_format.csv`
# MAGIC - Volume: `/Volumes/catalog/schema/volume/Matm_tables_format.csv`

# COMMAND ----------

# MAGIC %pip install pymysql openai -q

# COMMAND ----------

dbutils.widgets.text("template_csv_path", "", "CSV path (optional)")
dbutils.widgets.text("table_limit", "", "Table limit for testing (optional)")

from collections import defaultdict
from datetime import datetime
import json
import re
from pathlib import PurePosixPath

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

TEMPLATE_CSV = "Matm_tables_format.csv"
AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"

TEMPLATE_CSV_PATH = dbutils.widgets.get("template_csv_path").strip() or None
_table_limit_raw = dbutils.widgets.get("table_limit").strip()
TABLE_LIMIT = int(_table_limit_raw) if _table_limit_raw else None

DATE_TYPE_PATTERN = re.compile(r"(date|time|timestamp|year)", re.IGNORECASE)
SENSITIVE_COLUMN_PATTERN = re.compile(
    r"(email|e_mail|phone|mobile|ssn|social.?sec|password|passwd|pwd|"
    r"token|secret|credit.?card|card.?num|bank.?acct|account.?num|"
    r"date.?of.?birth|dob|birth.?date|address|license|passport|salary|"
    r"tax.?id|national.?id|ip.?addr)",
    re.IGNORECASE,
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def get_connection(database=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PWD,
        database=database,
        connect_timeout=10,
        read_timeout=60,
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


def normalize_mysql_type(column_type):
    return re.sub(r"\s+", " ", str(column_type or "").strip())


def is_date_column(column_type):
    return bool(DATE_TYPE_PATTERN.search(normalize_mysql_type(column_type)))


def format_column_list(columns):
    return ", ".join(
        f"{name} ({normalize_mysql_type(column_type)})"
        for name, column_type, *_rest in columns
    )


def format_date_columns(columns):
    return ", ".join(
        name for name, column_type, *_rest in columns if is_date_column(column_type)
    )


def detect_primary_key(columns):
    return [name for name, _t, _n, key, _c in columns if key == "PRI"]


def detect_sensitive_flag(columns):
    for name, *_rest in columns:
        if SENSITIVE_COLUMN_PATTERN.search(name):
            return "Yes"
    return "No"


def format_sample_row(row, columns):
    if not row:
        return None
    payload = {}
    for index, (name, *_rest) in enumerate(columns):
        value = row[index]
        if isinstance(value, datetime):
            value = value.isoformat(sep=" ", timespec="seconds")
        elif value is not None and not isinstance(value, (str, int, float, bool)):
            value = str(value)
        payload[name] = value
    return json.dumps(payload, ensure_ascii=False, default=str)


def fetch_sample_row(database, table, columns):
    if not IDENTIFIER_PATTERN.fullmatch(database) or not IDENTIFIER_PATTERN.fullmatch(table):
        return None

    with get_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{database}`.`{table}` LIMIT 1")
            row = cursor.fetchone()
    return format_sample_row(row, columns)


def fetch_database_metadata(database, table_names):
    table_name_set = set(table_names)
    metadata_by_table = {}

    with get_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                """,
                (database,),
            )
            tables = cursor.fetchall()

            cursor.execute(
                """
                SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                (database,),
            )
            column_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT TABLE_NAME, REFERENCED_TABLE_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
                """,
                (database,),
            )
            fk_rows = cursor.fetchall()

    columns_by_table = defaultdict(list)
    for table_name, column_name, column_type, nullable, key, comment in column_rows:
        if table_name in table_name_set:
            columns_by_table[table_name].append(
                (column_name, column_type, nullable, key, comment)
            )

    parent_tables = defaultdict(set)
    child_tables = defaultdict(set)
    for table_name, referenced_table_name in fk_rows:
        if table_name in table_name_set:
            parent_tables[table_name].add(referenced_table_name)
        if referenced_table_name in table_name_set:
            child_tables[referenced_table_name].add(table_name)

    table_info = {
        table_name: {
            "row_count": row_count,
            "table_comment": table_comment or "",
        }
        for table_name, _table_type, row_count, table_comment in tables
        if table_name in table_name_set
    }

    for table_name in table_names:
        columns = columns_by_table.get(table_name, [])
        pk_columns = detect_primary_key(columns)
        metadata_by_table[table_name] = {
            "row_count": table_info.get(table_name, {}).get("row_count"),
            "table_comment": table_info.get(table_name, {}).get("table_comment", ""),
            "columns": columns,
            "primary_key": ", ".join(pk_columns),
            "composite_pk": "Yes" if len(pk_columns) > 1 else ("No" if pk_columns else None),
            "parent_tables": ", ".join(sorted(parent_tables.get(table_name, []))) or None,
            "child_tables": ", ".join(sorted(child_tables.get(table_name, []))) or None,
            "date_columns": format_date_columns(columns) or None,
            "column_names_datatypes": format_column_list(columns) or None,
            "sensitive_data_flag": detect_sensitive_flag(columns),
            "sample_row": fetch_sample_row(database, table_name, columns),
        }

    return metadata_by_table

# COMMAND ----------

import openai
import pandas as pd
import pymysql

workspace_host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not workspace_host.startswith("https://"):
    workspace_host = f"https://{workspace_host}"

api_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .get()
)

ai_client = openai.OpenAI(
    api_key=api_token,
    base_url=f"{workspace_host}/serving-endpoints",
)


def generate_table_description(database, table_name, table_metadata):
    existing_comment = (table_metadata.get("table_comment") or "").strip()
    column_summary = "\n".join(
        f"- {name} ({normalize_mysql_type(column_type)}, key={key or 'none'})"
        for name, column_type, _nullable, key, _comment in table_metadata["columns"][:30]
    )
    prompt = f"""Describe the MySQL table {database}.{table_name} in 1-2 sentences.
Use only this schema metadata and do not invent details.
Existing table comment: {existing_comment or 'none'}
Columns:
{column_summary}"""

    try:
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        print(f"AI description failed for {database}.{table_name}: {exc}")
        return existing_comment or None

# COMMAND ----------

template_path = TEMPLATE_CSV_PATH or f"{get_notebook_directory()}/{TEMPLATE_CSV}"
sheet_df = pd.read_csv(template_path, dtype=str)

if TABLE_LIMIT:
    sheet_df = sheet_df.head(TABLE_LIMIT)

print(f"Loaded {len(sheet_df):,} tables from {template_path}")

tables_by_database = (
    sheet_df.groupby("Database")["Table"]
    .apply(lambda series: sorted(series.unique()))
    .to_dict()
)

metadata_cache = {}
for database, table_names in sorted(tables_by_database.items()):
    print(f"Fetching MySQL metadata for {database} ({len(table_names):,} tables)...")
    metadata_cache[database] = fetch_database_metadata(database, table_names)

# COMMAND ----------

enriched_rows = []

for _, row in sheet_df.iterrows():
    database = row["Database"]
    table_name = row["Table"]
    table_metadata = metadata_cache.get(database, {}).get(table_name)

    enriched_row = row.to_dict()

    if table_metadata is None:
        print(f"Warning: no MySQL metadata found for {database}.{table_name}")
        enriched_rows.append(enriched_row)
        continue

    enriched_row["Estimated Row Count"] = (
        None if table_metadata["row_count"] is None else str(table_metadata["row_count"])
    )
    enriched_row["Table Description"] = generate_table_description(
        database, table_name, table_metadata
    )
    enriched_row["Primary Key"] = table_metadata["primary_key"] or None
    enriched_row["Composite PK?"] = table_metadata["composite_pk"]
    enriched_row["Parent Table (comma delimit)"] = table_metadata["parent_tables"]
    enriched_row["Child Tables (comma delimit)"] = table_metadata["child_tables"]
    enriched_row["Date Columns (comma delimit)"] = table_metadata["date_columns"]
    enriched_row["Column Names/Datatypes (comma delimit)"] = table_metadata[
        "column_names_datatypes"
    ]
    enriched_row["Sample Row"] = table_metadata["sample_row"]
    enriched_row["Sensitive Data Flag"] = table_metadata["sensitive_data_flag"]

    enriched_rows.append(enriched_row)

matm_tables_pdf = pd.DataFrame(enriched_rows, columns=sheet_df.columns)
matm_tables_df = spark.createDataFrame(matm_tables_pdf)

print(f"Built DataFrame with {matm_tables_df.count():,} rows")
matm_tables_df.show(20, truncate=False)

# COMMAND ----------

display(matm_tables_df)
