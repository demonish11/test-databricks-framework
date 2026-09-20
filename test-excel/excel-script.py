# Databricks notebook source
# MAGIC %md
# MAGIC # MATM tables metadata enrichment
# MAGIC Reads the table list from `Matm_tables_format.xlsx`, pulls schema metadata and sample rows
# MAGIC from MySQL, and fills the workbook using:
# MAGIC - **Reference sheets** for controlled vocab columns (`Business Area`, `Functional Category`)
# MAGIC - **Model inference** for descriptive / analytical columns
# MAGIC - **Direct MySQL metadata** for factual schema fields
# MAGIC
# MAGIC **Requirements**
# MAGIC - Network access from Databricks compute to `10.219.252.18:3306`
# MAGIC - Secret scope `mysql-replica` with keys `username` and `password`
# MAGIC - Place `Matm_tables_format.xlsx` in the same workspace folder as this notebook,
# MAGIC   or set the **Excel path** widget (DBFS or Unity Catalog Volume path)
# MAGIC
# MAGIC **Path examples**
# MAGIC - Workspace folder (default): leave widget blank
# MAGIC - DBFS: `/dbfs/FileStore/shared_uploads/your_folder/Matm_tables_format.xlsx`
# MAGIC - Volume: `/Volumes/catalog/schema/volume/Matm_tables_format.xlsx`

# COMMAND ----------

# MAGIC %pip install pymysql openai openpyxl -q

# COMMAND ----------

dbutils.widgets.text("template_excel_path", "", "Excel path (optional)")
dbutils.widgets.text("output_excel_path", "", "Output Excel path (optional)")
dbutils.widgets.text("table_limit", "", "Table limit for testing (optional)")

from collections import defaultdict
from datetime import datetime
import json
import math
import re
from pathlib import PurePosixPath

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

TEMPLATE_EXCEL = "Matm_tables_format.xlsx"
MAIN_SHEET = "Matm_tables_format"
AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"
SAMPLE_ROW_LIMIT = 10

TEMPLATE_EXCEL_PATH = dbutils.widgets.get("template_excel_path").strip() or None
OUTPUT_EXCEL_PATH = dbutils.widgets.get("output_excel_path").strip() or None
_table_limit_raw = dbutils.widgets.get("table_limit").strip()
TABLE_LIMIT = int(_table_limit_raw) if _table_limit_raw else None

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

# Columns filled by picking exactly one value from a reference sheet.
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

# Columns filled by the model from schema metadata and sample rows.
MODEL_COLUMNS = [
    "Table Description",
    "Grain ",
    "Primary Key",
    "Composite PK?",
    "Parent Table (comma delimit)",
    "Child Tables (comma delimit)",
    "Date Columns (comma delimit)",
    "Column Names/Datatypes (comma delimit)",
    "Sensitive Data Flag",
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


def format_sample_rows(rows, columns):
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
            rows = cursor.fetchall()
    return rows


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
        sample_rows = fetch_sample_rows(database, table_name, columns)
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
            "sample_rows": sample_rows,
            "sample_row": format_sample_rows(sample_rows, columns),
        }

    return metadata_by_table


def load_reference_values(excel_path):
    workbook = pd.ExcelFile(excel_path)
    reference_values = {}

    for column_name, config in REFERENCE_SHEET_COLUMNS.items():
        sheet_name = config["sheet"]
        value_column = config["value_column"]
        stop_values = set(config.get("stop_values", set()))

        ref_df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
        ref_df.columns = [str(col).strip() for col in ref_df.columns]

        if value_column not in ref_df.columns:
            first_col = ref_df.columns[0]
            values = ref_df[first_col]
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


def build_metadata_context(database, table_name, table_metadata):
    existing_comment = (table_metadata.get("table_comment") or "").strip()
    column_summary = "\n".join(
        f"- {name} ({normalize_mysql_type(column_type)}, key={key or 'none'}, "
        f"nullable={nullable}, comment={comment or 'none'})"
        for name, column_type, nullable, key, comment in table_metadata["columns"][:40]
    )
    sample_rows_json = table_metadata.get("sample_row") or "[]"
    return f"""Database: {database}
Table: {table_name}
Estimated row count: {table_metadata.get('row_count')}
Existing table comment: {existing_comment or 'none'}
Schema PK from information_schema: {table_metadata.get('primary_key') or 'none'}
Composite PK from schema: {table_metadata.get('composite_pk') or 'unknown'}
Parent tables from FK metadata: {table_metadata.get('parent_tables') or 'none'}
Child tables from FK metadata: {table_metadata.get('child_tables') or 'none'}
Date/time columns from schema: {table_metadata.get('date_columns') or 'none'}
Sensitive-data heuristic from column names: {table_metadata.get('sensitive_data_flag') or 'No'}
Columns:
{column_summary}
Sample rows ({SAMPLE_ROW_LIMIT} max):
{sample_rows_json}"""


def generate_ai_enrichment(
    database,
    table_name,
    table_metadata,
    reference_values,
    columns_to_fill,
):
    if not columns_to_fill:
        return {}

    reference_instructions = []
    for column_name in sorted(columns_to_fill.intersection(REFERENCE_SHEET_COLUMNS)):
        allowed = reference_values.get(column_name, [])
        allowed_text = ", ".join(f'"{value}"' for value in allowed)
        reference_instructions.append(
            f'- "{column_name}": choose exactly one value from this list only: {allowed_text}. '
            "Do not invent a new value."
        )

    model_instructions = []
    for column_name in MODEL_COLUMNS:
        if column_name not in columns_to_fill:
            continue
        if column_name == "Table Description":
            model_instructions.append(
                '- "Table Description": 1-2 sentence business description of the table.'
            )
        elif column_name.strip() == "Grain":
            model_instructions.append(
                '- "Grain ": one short phrase describing the row grain / uniqueness level.'
            )
        elif column_name == "Primary Key":
            model_instructions.append(
                '- "Primary Key": comma-separated PK column(s). Prefer schema PK when present; '
                "otherwise infer from sample rows and column names."
            )
        elif column_name == "Composite PK?":
            model_instructions.append(
                '- "Composite PK?": answer exactly "Yes" or "No".'
            )
        elif column_name == "Parent Table (comma delimit)":
            model_instructions.append(
                '- "Parent Table (comma delimit)": comma-separated parent tables if inferable; '
                "prefer FK metadata and leave blank if unknown."
            )
        elif column_name == "Child Tables (comma delimit)":
            model_instructions.append(
                '- "Child Tables (comma delimit)": comma-separated child tables if inferable; '
                "prefer FK metadata and leave blank if unknown."
            )
        elif column_name == "Date Columns (comma delimit)":
            model_instructions.append(
                '- "Date Columns (comma delimit)": comma-separated date/time/timestamp columns.'
            )
        elif column_name == "Column Names/Datatypes (comma delimit)":
            model_instructions.append(
                '- "Column Names/Datatypes (comma delimit)": comma-separated '
                "column (datatype) pairs."
            )
        elif column_name == "Sensitive Data Flag":
            model_instructions.append(
                '- "Sensitive Data Flag": answer exactly "Yes" or "No".'
            )
        elif column_name == "Notes/Observations":
            model_instructions.append(
                '- "Notes/Observations": short factual notes only when supported by the metadata.'
            )

    response_schema = {
        column_name: "string or null"
        for column_name in sorted(columns_to_fill)
    }

    prompt = f"""Analyze this MySQL table and fill the requested workbook columns.

{build_metadata_context(database, table_name, table_metadata)}

Return a JSON object with exactly these keys:
{json.dumps(response_schema, indent=2)}

Rules:
- Use only the metadata and sample rows above.
- Do not invent facts that are not supported by the metadata.
- For blank/unknown values, use null.
{chr(10).join(reference_instructions)}
{chr(10).join(model_instructions)}"""

    try:
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700,
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

        if column_name == "Composite PK?" and value not in {"Yes", "No"}:
            continue
        if column_name == "Sensitive Data Flag" and value not in {"Yes", "No"}:
            continue

        cleaned[column_name] = value

    return cleaned

# COMMAND ----------

template_path = TEMPLATE_EXCEL_PATH or f"{get_notebook_directory()}/{TEMPLATE_EXCEL}"
reference_values = load_reference_values(template_path)

sheet_df = pd.read_excel(template_path, sheet_name=MAIN_SHEET, dtype=str)
sheet_df.columns = [str(col) for col in sheet_df.columns]

if TABLE_LIMIT:
    sheet_df = sheet_df.head(TABLE_LIMIT)

print(f"Loaded {len(sheet_df):,} tables from '{MAIN_SHEET}' in {template_path}")

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

    if should_fill(enriched_row.get("Estimated Row Count")):
        enriched_row["Estimated Row Count"] = (
            None
            if table_metadata["row_count"] is None
            else str(table_metadata["row_count"])
        )

    if should_fill(enriched_row.get("Sample Row")):
        enriched_row["Sample Row"] = table_metadata["sample_row"]

    columns_for_ai = set()
    for column_name in list(REFERENCE_SHEET_COLUMNS.keys()) + MODEL_COLUMNS:
        if column_name in enriched_row and should_fill(enriched_row.get(column_name)):
            columns_for_ai.add(column_name)

    ai_values = generate_ai_enrichment(
        database,
        table_name,
        table_metadata,
        reference_values,
        columns_for_ai,
    )

    direct_defaults = {
        "Primary Key": table_metadata["primary_key"],
        "Composite PK?": table_metadata["composite_pk"],
        "Parent Table (comma delimit)": table_metadata["parent_tables"],
        "Child Tables (comma delimit)": table_metadata["child_tables"],
        "Date Columns (comma delimit)": table_metadata["date_columns"],
        "Column Names/Datatypes (comma delimit)": table_metadata["column_names_datatypes"],
        "Sensitive Data Flag": table_metadata["sensitive_data_flag"],
    }

    for column_name, default_value in direct_defaults.items():
        if column_name in columns_for_ai and column_name not in ai_values:
            if default_value is not None:
                ai_values[column_name] = default_value

    for column_name, value in ai_values.items():
        enriched_row[column_name] = value

    enriched_rows.append(enriched_row)

matm_tables_pdf = pd.DataFrame(enriched_rows, columns=sheet_df.columns)
matm_tables_df = spark.createDataFrame(matm_tables_pdf)

print(f"Built DataFrame with {matm_tables_df.count():,} rows")
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
