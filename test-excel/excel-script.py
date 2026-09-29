# Databricks notebook source
# MAGIC %md
# MAGIC # MATM tables metadata enrichment
# MAGIC Reads the table list from `Matm_tables_format.xlsx`, pulls schema metadata and sample rows
# MAGIC from MySQL, and fills the workbook using:
# MAGIC - **SQL / information_schema** for factual columns (PK, FKs, datatypes, row counts, etc.)
# MAGIC - **Reference sheets** for controlled vocab columns (`Business Area`, `Functional Category`)
# MAGIC - **Model inference** for descriptive columns only (`Table Description`, `Grain`, etc.)
# MAGIC
# MAGIC Sample rows are passed to the model in **CSV format** (one table at a time), matching the
# MAGIC workflow that produced better results when a single-table CSV was analyzed manually.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Network access from Databricks compute to `10.219.252.18:3306`
# MAGIC - Secret scope `mysql-replica` with keys `username` and `password`
# MAGIC - Place `Matm_tables_format.xlsx` in the same workspace folder as this notebook,
# MAGIC   or set the **Excel path** widget (DBFS or Unity Catalog Volume path)

# COMMAND ----------

# MAGIC %pip install pymysql openai openpyxl -q

# COMMAND ----------

dbutils.widgets.text("template_excel_path", "", "Excel path (optional)")
dbutils.widgets.text("output_excel_path", "", "Output Excel path (optional)")
dbutils.widgets.text("table_limit", "", "Table limit for testing (optional)")
dbutils.widgets.text("sample_row_limit", "50", "Sample rows per table (default 50)")

from collections import defaultdict
from datetime import datetime
import json
import math
import re
from pathlib import PurePosixPath

import openai
import pandas as pd
import pymysql

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

TEMPLATE_EXCEL = "Matm_tables_format.xlsx"
MAIN_SHEET = "Matm_tables_format"
AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"
EMPTY_TABLE_NOTE = "Table is empty — no sample rows available for analysis."

TEMPLATE_EXCEL_PATH = dbutils.widgets.get("template_excel_path").strip() or None
OUTPUT_EXCEL_PATH = dbutils.widgets.get("output_excel_path").strip() or None
_table_limit_raw = dbutils.widgets.get("table_limit").strip()
TABLE_LIMIT = int(_table_limit_raw) if _table_limit_raw else None
_sample_row_limit_raw = dbutils.widgets.get("sample_row_limit").strip()
SAMPLE_ROW_LIMIT = int(_sample_row_limit_raw) if _sample_row_limit_raw else 50

PLACEHOLDER_PATTERN = re.compile(
    r"^(refer\s+sheets?|from\s+gpt|from\s+model|from\s+mysql)$",
    re.IGNORECASE,
)
DATE_TYPE_PATTERN = re.compile(r"(date|time|timestamp|year)", re.IGNORECASE)
SENSITIVE_COLUMN_PATTERN = re.compile(
    r"(email|e_mail|phone|mobile|ssn|social.?sec|password|passwd|pwd|"
    r"token|secret|credit.?card|card.?num|bank.?acct|account.?num|"
    r"date.?of.?birth|dob|birth.?date|address|license|passport|salary|"
    r"tax.?id|national.?id|ip.?addr)",
    re.IGNORECASE,
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

REFERENCE_SHEET_COLUMNS = {
    "Business Area": {
        "sheet": "Buisness Areas",
        "value_column": "Business Areas",
        "stop_values": {"Data Category"},
    },
    "Functional Category": {
        "sheet": "Functional Categories",
        "value_column": "Functional Categories",
    },
}

# Filled directly from MySQL metadata — never sent to the model.
SQL_FILLED_COLUMNS = [
    "Estimated Row Count",
    "Primary Key",
    "Composite PK?",
    "Parent Table (comma delimit)",
    "Child Tables (comma delimit)",
    "Date Columns (comma delimit)",
    "Column Names/Datatypes (comma delimit)",
    "Sensitive Data Flag",
    "Sample Row",
]

# Filled by the model from CSV-formatted sample data (+ table/column names).
AI_COLUMNS = [
    "Business Area",
    "Functional Category",
    "Table Description",
    "Grain ",
    "Notes/Observations",
]


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


def detect_sensitive_flag(columns):
    for name, *_rest in columns:
        if SENSITIVE_COLUMN_PATTERN.search(name):
            return "Yes"
    return "No"


def normalize_cell_value(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def is_placeholder(value):
    text = normalize_cell_value(value)
    return bool(text and PLACEHOLDER_PATTERN.match(text))


def should_fill(value):
    text = normalize_cell_value(value)
    return text is None or is_placeholder(text)


def serialize_cell_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if value is not None and not isinstance(value, (str, int, float, bool)):
        return str(value)
    return value


def format_sample_rows_csv(rows, columns):
    if not rows:
        return None

    column_names = [name for name, *_rest in columns]
    records = []
    for row in rows:
        record = {}
        for index, name in enumerate(column_names):
            record[name] = serialize_cell_value(row[index])
        records.append(record)

    sample_df = pd.DataFrame(records, columns=column_names)
    return sample_df.to_csv(index=False)


def format_sample_rows_json(rows, columns):
    if not rows:
        return None

    payload = []
    for row in rows:
        record = {}
        for index, (name, *_rest) in enumerate(columns):
            record[name] = serialize_cell_value(row[index])
        payload.append(record)
    return json.dumps(payload, ensure_ascii=False, default=str)


def fetch_sample_rows(database, table, columns, limit=SAMPLE_ROW_LIMIT):
    if not IDENTIFIER_PATTERN.fullmatch(database) or not IDENTIFIER_PATTERN.fullmatch(table):
        return []

    with get_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM `{database}`.`{table}` LIMIT {int(limit)}"
            )
            return cursor.fetchall()


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
                SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                (database,),
            )
            pk_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT TABLE_NAME, REFERENCED_TABLE_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
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

    pk_by_table = defaultdict(list)
    for table_name, column_name, _ordinal in pk_rows:
        if table_name in table_name_set:
            pk_by_table[table_name].append(column_name)

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
        pk_columns = pk_by_table.get(table_name, [])
        sample_rows = fetch_sample_rows(database, table_name, columns)
        metadata_by_table[table_name] = {
            "row_count": table_info.get(table_name, {}).get("row_count"),
            "table_comment": table_info.get(table_name, {}).get("table_comment", ""),
            "columns": columns,
            "primary_key": ", ".join(pk_columns) or None,
            "composite_pk": "Yes" if len(pk_columns) > 1 else ("No" if pk_columns else None),
            "parent_tables": ", ".join(sorted(parent_tables.get(table_name, []))) or None,
            "child_tables": ", ".join(sorted(child_tables.get(table_name, []))) or None,
            "date_columns": format_date_columns(columns) or None,
            "column_names_datatypes": format_column_list(columns) or None,
            "sensitive_data_flag": detect_sensitive_flag(columns),
            "sample_rows": sample_rows,
            "sample_row_csv": format_sample_rows_csv(sample_rows, columns),
            "sample_row": format_sample_rows_json(sample_rows, columns),
            "is_empty": len(sample_rows) == 0,
        }

    return metadata_by_table


def load_reference_values(excel_path):
    reference_values = {}

    for column_name, config in REFERENCE_SHEET_COLUMNS.items():
        sheet_name = config["sheet"]
        value_column = config["value_column"]
        stop_values = set(config.get("stop_values", set()))

        ref_df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
        ref_df.columns = [str(col).strip() for col in ref_df.columns]

        if value_column not in ref_df.columns:
            values = ref_df[ref_df.columns[0]]
        else:
            values = ref_df[value_column]

        allowed = []
        for raw_value in values:
            value = normalize_cell_value(raw_value)
            if value is None:
                continue
            if value in stop_values:
                break
            if value.lower() in {value_column.lower(), "description", "examples:"}:
                continue
            allowed.append(value)

        reference_values[column_name] = sorted(set(allowed))
        print(
            f"Loaded {len(reference_values[column_name]):,} allowed values for "
            f"'{column_name}' from sheet '{sheet_name}'"
        )

    return reference_values


def build_sql_filled_values(table_metadata):
    return {
        "Estimated Row Count": (
            None
            if table_metadata["row_count"] is None
            else str(table_metadata["row_count"])
        ),
        "Primary Key": table_metadata["primary_key"],
        "Composite PK?": table_metadata["composite_pk"],
        "Parent Table (comma delimit)": table_metadata["parent_tables"],
        "Child Tables (comma delimit)": table_metadata["child_tables"],
        "Date Columns (comma delimit)": table_metadata["date_columns"],
        "Column Names/Datatypes (comma delimit)": table_metadata["column_names_datatypes"],
        "Sensitive Data Flag": table_metadata["sensitive_data_flag"],
        "Sample Row": table_metadata["sample_row"],
    }


def apply_sql_filled_columns(enriched_row, table_metadata):
    for column_name, value in build_sql_filled_values(table_metadata).items():
        if column_name in enriched_row and should_fill(enriched_row.get(column_name)):
            if value is not None:
                enriched_row[column_name] = value


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


def build_ai_prompt(database, table_name, table_metadata, reference_values, columns_to_fill):
    sample_csv = table_metadata.get("sample_row_csv") or ""
    table_comment = (table_metadata.get("table_comment") or "").strip()
    column_names = ", ".join(name for name, *_rest in table_metadata["columns"])

    reference_instructions = []
    for column_name in sorted(columns_to_fill.intersection(REFERENCE_SHEET_COLUMNS)):
        allowed = reference_values.get(column_name, [])
        allowed_text = ", ".join(f'"{value}"' for value in allowed)
        reference_instructions.append(
            f'- "{column_name}": choose exactly one value from: {allowed_text}'
        )

    field_instructions = []
    if "Table Description" in columns_to_fill:
        field_instructions.append(
            '- "Table Description": 1-2 sentence business description of what this table stores.'
        )
    if "Grain " in columns_to_fill:
        field_instructions.append(
            '- "Grain ": one short phrase for what one row represents (row-level uniqueness).'
        )
    if "Notes/Observations" in columns_to_fill:
        field_instructions.append(
            '- "Notes/Observations": brief factual notes about patterns, nulls, or relationships '
            "visible in the sample data. Leave null if nothing notable."
        )

    response_schema = {
        column_name: "string or null"
        for column_name in sorted(columns_to_fill)
    }

    return f"""You are analyzing a single MySQL table. Below is the sample data exported as CSV
(the same format used for manual table analysis). Use the CSV rows plus table/column names to
fill the requested metadata columns.

Database: {database}
Table: {table_name}
Table comment from MySQL: {table_comment or "none"}
Column names: {column_names}

Sample data (up to {SAMPLE_ROW_LIMIT} rows, CSV format):
```csv
{sample_csv}
```

Return a JSON object with exactly these keys:
{json.dumps(response_schema, indent=2)}

Rules:
- Base answers on the CSV sample and column names only.
- Do not guess primary keys, parent tables, or datatypes — those are filled separately from SQL.
- For unknown values, use null.
- Be concise but complete; prefer filling a column over leaving it null when the CSV supports it.
{chr(10).join(reference_instructions)}
{chr(10).join(field_instructions)}"""


def generate_ai_enrichment(
    database,
    table_name,
    table_metadata,
    reference_values,
    columns_to_fill,
):
    if not columns_to_fill:
        return {}

    prompt = build_ai_prompt(
        database,
        table_name,
        table_metadata,
        reference_values,
        columns_to_fill,
    )

    try:
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1200,
            response_format={"type": "json_object"},
        )
        payload = json.loads(response.choices[0].message.content)
    except Exception as exc:
        print(f"AI enrichment failed for {database}.{table_name}: {exc}")
        return {}

    cleaned = {}
    for column_name in columns_to_fill:
        value = payload.get(column_name)
        if value is None and column_name == "Grain ":
            value = payload.get("Grain")
        value = normalize_cell_value(value)
        if value is None:
            continue

        if column_name in REFERENCE_SHEET_COLUMNS:
            allowed = reference_values.get(column_name, [])
            if value not in allowed:
                matched = next(
                    (candidate for candidate in allowed if candidate.lower() == value.lower()),
                    None,
                )
                if matched:
                    value = matched
                else:
                    print(
                        f"Warning: model returned invalid reference value "
                        f"'{value}' for {database}.{table_name}.{column_name}; skipping."
                    )
                    continue

        cleaned[column_name] = value

    return cleaned

# COMMAND ----------

template_path = TEMPLATE_EXCEL_PATH or f"{get_notebook_directory()}/{TEMPLATE_EXCEL}"
reference_values = load_reference_values(template_path)

sheet_df = pd.read_excel(template_path, sheet_name=MAIN_SHEET, dtype=str)
sheet_df.columns = [str(col).strip() for col in sheet_df.columns]

if TABLE_LIMIT:
    sheet_df = sheet_df.head(TABLE_LIMIT)

print(f"Loaded {len(sheet_df):,} tables from '{MAIN_SHEET}' in {template_path}")
print(f"Sample row limit: {SAMPLE_ROW_LIMIT}")

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
empty_table_count = 0
ai_enriched_count = 0

for index, row in sheet_df.iterrows():
    database = str(row["Database"]).strip()
    table_name = str(row["Table"]).strip()
    table_metadata = metadata_cache.get(database, {}).get(table_name)
    enriched_row = row.to_dict()

    if table_metadata is None:
        print(f"Warning: no MySQL metadata found for {database}.{table_name}")
        enriched_rows.append(enriched_row)
        continue

    apply_sql_filled_columns(enriched_row, table_metadata)

    if table_metadata["is_empty"]:
        empty_table_count += 1
        if should_fill(enriched_row.get("Notes/Observations")):
            enriched_row["Notes/Observations"] = EMPTY_TABLE_NOTE
        if should_fill(enriched_row.get("Sample Row")):
            enriched_row["Sample Row"] = "No rows"
        print(f"[{index + 1:,}/{len(sheet_df):,}] {database}.{table_name}: empty table, skipped AI")
        enriched_rows.append(enriched_row)
        continue

    columns_for_ai = {
        column_name
        for column_name in AI_COLUMNS
        if column_name in enriched_row and should_fill(enriched_row.get(column_name))
    }

    if columns_for_ai:
        ai_values = generate_ai_enrichment(
            database,
            table_name,
            table_metadata,
            reference_values,
            columns_for_ai,
        )
        for column_name, value in ai_values.items():
            enriched_row[column_name] = value
        ai_enriched_count += 1
        print(
            f"[{index + 1:,}/{len(sheet_df):,}] {database}.{table_name}: "
            f"AI filled {len(ai_values):,}/{len(columns_for_ai):,} columns"
        )
    else:
        print(f"[{index + 1:,}/{len(sheet_df):,}] {database}.{table_name}: SQL-only, no AI needed")

    enriched_rows.append(enriched_row)

matm_tables_pdf = pd.DataFrame(enriched_rows, columns=sheet_df.columns)
matm_tables_df = spark.createDataFrame(matm_tables_pdf)

print(
    f"Built DataFrame with {matm_tables_df.count():,} rows "
    f"({empty_table_count:,} empty, {ai_enriched_count:,} AI-enriched)"
)
matm_tables_df.show(20, truncate=False)

# COMMAND ----------

output_path = OUTPUT_EXCEL_PATH or template_path.replace(".xlsx", "_enriched.xlsx")

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    matm_tables_pdf.to_excel(writer, sheet_name=MAIN_SHEET, index=False)

    for sheet_name in pd.ExcelFile(template_path).sheet_names:
        if sheet_name == MAIN_SHEET:
            continue
        pd.read_excel(template_path, sheet_name=sheet_name, dtype=str).to_excel(
            writer,
            sheet_name=sheet_name,
            index=False,
        )

print(f"Wrote enriched workbook to {output_path}")
display(matm_tables_df)
