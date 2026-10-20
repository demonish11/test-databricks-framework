# Databricks notebook source
# MAGIC %md
# MAGIC # MATM databases metadata enrichment
# MAGIC Fills `Matm-databases_sheet_to_be_filled.csv` in two model passes:
# MAGIC 1. **Table level** — for every database in the sheet, connect to MySQL, list its tables,
# MAGIC    read column metadata + sample rows from each table, and ask the model for a description
# MAGIC    of that table. Results are collected in a table-level DataFrame.
# MAGIC 2. **Database level** — give the table-level DataFrame for each database back to the model
# MAGIC    so it can aggregate it into `Database Description` and `Business Area`.
# MAGIC
# MAGIC `Number of Tables` comes straight from `information_schema`.
# MAGIC `Business Area` is restricted to the values in the sheet's own `Business Areas` reference columns.
# MAGIC
# MAGIC **Quick preview** — set `preview_database` + `preview_table` to read that MySQL table into a
# MAGIC Spark DataFrame and `.show()` / `display()` it. Enrichment CSV widgets are not required in
# MAGIC that mode.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Network access from Databricks compute to `10.219.252.18:3306`
# MAGIC - Secret scope `mysql-replica` with keys `username` and `password`

# COMMAND ----------

# MAGIC %pip install pymysql openai -q

# COMMAND ----------

dbutils.widgets.text("preview_database", "", "MySQL database to preview (optional)")
dbutils.widgets.text("preview_table", "", "MySQL table to preview (optional)")
dbutils.widgets.text("preview_row_limit", "100", "Rows to show for preview (default 100)")
dbutils.widgets.text("input_csv_path", "", "Input CSV path")
dbutils.widgets.text("output_csv_path", "", "Output CSV path")
dbutils.widgets.text("table_output_csv_path", "", "Table-level output CSV path (optional)")
dbutils.widgets.text("database_limit", "", "Database limit for testing (optional)")
dbutils.widgets.text("max_tables_per_database", "", "Max tables per database (optional)")
dbutils.widgets.text("sample_row_limit", "10", "Sample rows per table (default 10)")

from datetime import datetime
import json
import math
import re

import openai
import pandas as pd
import pymysql

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")
MYSQL_JDBC_URL_TEMPLATE = (
    "jdbc:mysql://{host}:{port}/{database}"
    "?zeroDateTimeBehavior=convertToNull&autoReconnect=true"
    "&characterEncoding=UTF-8&characterSetResults=UTF-8"
)
MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"

DATABASE_COLUMN = "Database"
DESCRIPTION_COLUMN = "Database Description"
BUSINESS_AREA_COLUMN = "Business Area"
TABLE_COUNT_COLUMN = "Number of Tables"
REFERENCE_HEADER = "Business Areas"
REFERENCE_STOP_VALUE = "Data Category"

MAX_CELL_CHARS = 120
MASKED_VALUE = "<masked>"


def get_int_widget(name, default=None):
    raw_value = dbutils.widgets.get(name).strip()
    return int(raw_value) if raw_value else default


PREVIEW_DATABASE = dbutils.widgets.get("preview_database").strip()
PREVIEW_TABLE = dbutils.widgets.get("preview_table").strip()
PREVIEW_ROW_LIMIT = get_int_widget("preview_row_limit", 100)
INPUT_CSV_PATH = dbutils.widgets.get("input_csv_path").strip()
OUTPUT_CSV_PATH = dbutils.widgets.get("output_csv_path").strip()
TABLE_OUTPUT_CSV_PATH = dbutils.widgets.get("table_output_csv_path").strip() or None
DATABASE_LIMIT = get_int_widget("database_limit")
MAX_TABLES_PER_DATABASE = get_int_widget("max_tables_per_database")
SAMPLE_ROW_LIMIT = get_int_widget("sample_row_limit", 10)
PREVIEW_ONLY = bool(PREVIEW_DATABASE or PREVIEW_TABLE)

if PREVIEW_ONLY and not (PREVIEW_DATABASE and PREVIEW_TABLE):
    raise ValueError("Set both 'preview_database' and 'preview_table' to preview a table.")

if not PREVIEW_ONLY and (not INPUT_CSV_PATH or not OUTPUT_CSV_PATH):
    raise ValueError(
        "Set both 'input_csv_path' and 'output_csv_path', or use preview_database + preview_table."
    )

PLACEHOLDER_PATTERN = re.compile(
    r"^(refer\s+sheets?|from\s+gpt|from\s+model|from\s+mysql)$",
    re.IGNORECASE,
)
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
        read_timeout=120,
        charset="utf8mb4",
    )


def read_mysql_table_df(database, table_name, row_limit=None):
    """Load a MySQL table into a Spark DataFrame via JDBC."""
    if not IDENTIFIER_PATTERN.fullmatch(database):
        raise ValueError(f"Invalid database name '{database}'")
    if not IDENTIFIER_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table name '{table_name}'")

    jdbc_url = MYSQL_JDBC_URL_TEMPLATE.format(
        host=MYSQL_HOST, port=MYSQL_PORT, database=database
    )
    dbtable = f"`{database}`.`{table_name}`"
    if row_limit is not None:
        dbtable = f"(SELECT * FROM `{database}`.`{table_name}` LIMIT {int(row_limit)}) preview_src"

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PWD)
        .option("driver", MYSQL_JDBC_DRIVER)
        .option("fetchsize", "10000")
        .load()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Table preview
# MAGIC Fill `preview_database` and `preview_table`, then run this cell. Notebook stops here in preview mode.

# COMMAND ----------

if PREVIEW_ONLY:
    preview_df = read_mysql_table_df(
        PREVIEW_DATABASE, PREVIEW_TABLE, row_limit=PREVIEW_ROW_LIMIT
    )
    print(
        {
            "mode": "preview",
            "database": PREVIEW_DATABASE,
            "table": PREVIEW_TABLE,
            "row_limit": PREVIEW_ROW_LIMIT,
            "columns": preview_df.columns,
        }
    )
    preview_df.show(PREVIEW_ROW_LIMIT, truncate=False)
    display(preview_df)
    dbutils.notebook.exit(
        json.dumps(
            {
                "mode": "preview",
                "database": PREVIEW_DATABASE,
                "table": PREVIEW_TABLE,
                "row_limit": PREVIEW_ROW_LIMIT,
                "status": "success",
            }
        )
    )

# COMMAND ----------

def normalize_cell_value(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def should_fill(value):
    text = normalize_cell_value(value)
    return text is None or bool(PLACEHOLDER_PATTERN.match(text))


def serialize_cell_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    text = str(value)
    if len(text) > MAX_CELL_CHARS:
        text = text[:MAX_CELL_CHARS] + "..."
    return text


def format_sample_rows_csv(rows, column_names):
    if not rows:
        return None

    sensitive = {name for name in column_names if SENSITIVE_COLUMN_PATTERN.search(name)}
    records = [
        {
            name: MASKED_VALUE if name in sensitive else serialize_cell_value(row[index])
            for index, name in enumerate(column_names)
        }
        for row in rows
    ]
    return pd.DataFrame(records, columns=column_names).to_csv(index=False)


def fetch_tables(cursor, database):
    cursor.execute(
        """
        SELECT TABLE_NAME, TABLE_ROWS, TABLE_COMMENT
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (database,),
    )
    return [
        {"name": name, "row_count": row_count or 0, "comment": comment or ""}
        for name, row_count, comment in cursor.fetchall()
    ]


def fetch_columns_by_table(cursor, database):
    cursor.execute(
        """
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        (database,),
    )
    columns_by_table = {}
    for table_name, column_name, column_type, column_key in cursor.fetchall():
        columns_by_table.setdefault(table_name, []).append(
            (column_name, column_type, column_key)
        )
    return columns_by_table


def fetch_sample_csv(cursor, database, table_name):
    if not IDENTIFIER_PATTERN.fullmatch(table_name):
        return None
    cursor.execute(
        f"SELECT * FROM `{database}`.`{table_name}` LIMIT {int(SAMPLE_ROW_LIMIT)}"
    )
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return format_sample_rows_csv(rows, column_names)


def load_sheet(csv_path):
    raw_df = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)
    header = [str(value).strip() for value in raw_df.iloc[0].tolist()]
    body = raw_df.iloc[1:].reset_index(drop=True)
    return header, body


def find_column_index(header, column_name):
    try:
        return header.index(column_name)
    except ValueError as exc:
        raise ValueError(f"Column '{column_name}' not found in sheet header: {header}") from exc


def load_business_areas(header, body):
    reference_index = find_column_index(header, REFERENCE_HEADER)
    reference_values = body.iloc[:, reference_index].tolist()
    reference_descriptions = (
        body.iloc[:, reference_index + 1].tolist()
        if reference_index + 1 < len(header)
        else [""] * len(reference_values)
    )

    business_areas = {}
    for raw_value, raw_description in zip(reference_values, reference_descriptions):
        value = normalize_cell_value(raw_value)
        if value is None:
            if business_areas:
                break
            continue
        if value == REFERENCE_STOP_VALUE:
            break
        business_areas[value] = normalize_cell_value(raw_description) or ""

    print(f"Loaded {len(business_areas):,} allowed Business Area values: {list(business_areas)}")
    return business_areas


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


def call_model_json(prompt, max_tokens):
    response = ai_client.chat.completions.create(
        model=AI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content)


def build_table_prompt(database, table, columns, sample_csv):
    column_text = ", ".join(
        f"{name} ({column_type}{', PK' if column_key == 'PRI' else ''})"
        for name, column_type, column_key in columns
    )
    sample_text = (
        f"```csv\n{sample_csv}```" if sample_csv else "none (table is empty)"
    )

    return f"""You are analyzing a single MySQL table from a supply chain / logistics platform
(Manhattan Active style microservices).

Database: {database}
Table: {table['name']}
Approximate row count: {table['row_count']:,}
Table comment from MySQL: {table['comment'] or "none"}
Columns: {column_text or "none"}

Sample data (up to {SAMPLE_ROW_LIMIT} rows, CSV format):
{sample_text}

Return a JSON object with exactly this key:
{{"description": "string"}}

Rules:
- "description": 1-2 sentence business description of what this table stores and what one row represents.
- Base it on the table name, columns, and sample data. If the table is empty, infer from the name and columns."""


def describe_table(database, table, columns, sample_csv):
    prompt = build_table_prompt(database, table, columns, sample_csv)
    try:
        payload = call_model_json(prompt, max_tokens=300)
    except Exception as exc:
        print(f"  Table AI failed for {database}.{table['name']}: {exc}")
        return None
    return normalize_cell_value(payload.get("description"))


def build_database_prompt(database, table_df, business_areas, columns_to_fill):
    table_csv = table_df[["table_name", "row_count", "table_description"]].to_csv(index=False)

    field_instructions = []
    if DESCRIPTION_COLUMN in columns_to_fill:
        field_instructions.append(
            f'- "{DESCRIPTION_COLUMN}": 2-3 sentence business description of what this database '
            "is for, which business process/module it supports, and the main kinds of data it holds."
        )
    if BUSINESS_AREA_COLUMN in columns_to_fill:
        area_text = "\n".join(
            f'  - "{area}": {description}' for area, description in business_areas.items()
        )
        field_instructions.append(
            f'- "{BUSINESS_AREA_COLUMN}": choose exactly one value from:\n{area_text}'
        )

    response_schema = {column_name: "string or null" for column_name in sorted(columns_to_fill)}

    return f"""You are summarizing a MySQL database from a supply chain / logistics platform
(Manhattan Active style microservices). Below is a table-level summary of every table in the
database, in CSV format, with a description of each table. Aggregate it into a database-level view.

Database: {database}
Number of tables: {len(table_df):,}

Table-level details (CSV):
```csv
{table_csv}```

Return a JSON object with exactly these keys:
{json.dumps(response_schema, indent=2)}

Rules:
- Base answers on the database name and the table-level details.
- If the database has no tables, describe it from its name and say it currently has no tables.
- For unknown values, use null.
{chr(10).join(field_instructions)}"""


def describe_database(database, table_df, business_areas, columns_to_fill):
    if not columns_to_fill:
        return {}

    prompt = build_database_prompt(database, table_df, business_areas, columns_to_fill)
    try:
        payload = call_model_json(prompt, max_tokens=800)
    except Exception as exc:
        print(f"  Database AI failed for {database}: {exc}")
        return {}

    cleaned = {}
    for column_name in columns_to_fill:
        value = normalize_cell_value(payload.get(column_name))
        if value is None:
            continue

        if column_name == BUSINESS_AREA_COLUMN:
            matched = next(
                (area for area in business_areas if area.lower() == value.lower()),
                None,
            )
            if matched is None:
                print(f"  Warning: invalid Business Area '{value}' for {database}; skipping.")
                continue
            value = matched

        cleaned[column_name] = value

    return cleaned


def build_table_level_df(database):
    if not IDENTIFIER_PATTERN.fullmatch(database):
        raise ValueError(f"Invalid database name '{database}'")

    records = []
    with get_connection(database) as connection:
        with connection.cursor() as cursor:
            tables = fetch_tables(cursor, database)
            columns_by_table = fetch_columns_by_table(cursor, database)

            if MAX_TABLES_PER_DATABASE:
                tables = tables[:MAX_TABLES_PER_DATABASE]

            for position, table in enumerate(tables, start=1):
                columns = columns_by_table.get(table["name"], [])
                try:
                    sample_csv = fetch_sample_csv(cursor, database, table["name"])
                except Exception as exc:
                    print(f"  Could not sample {database}.{table['name']}: {exc}")
                    sample_csv = None

                description = describe_table(database, table, columns, sample_csv)
                records.append(
                    {
                        "database": database,
                        "table_name": table["name"],
                        "row_count": table["row_count"],
                        "column_count": len(columns),
                        "is_empty": sample_csv is None,
                        "table_description": description,
                    }
                )
                print(f"  [{position:,}/{len(tables):,}] {table['name']}: described")

    return pd.DataFrame(
        records,
        columns=[
            "database",
            "table_name",
            "row_count",
            "column_count",
            "is_empty",
            "table_description",
        ],
    )

# COMMAND ----------

header, body = load_sheet(INPUT_CSV_PATH)
business_areas = load_business_areas(header, body)

database_index = find_column_index(header, DATABASE_COLUMN)
description_index = find_column_index(header, DESCRIPTION_COLUMN)
business_area_index = find_column_index(header, BUSINESS_AREA_COLUMN)
table_count_index = find_column_index(header, TABLE_COUNT_COLUMN)

database_rows = [
    row_index
    for row_index in range(len(body))
    if normalize_cell_value(body.iat[row_index, database_index])
]
if DATABASE_LIMIT:
    database_rows = database_rows[:DATABASE_LIMIT]

print(f"Loaded {len(database_rows):,} databases from {INPUT_CSV_PATH}")

# COMMAND ----------

# Pass 1: table-level descriptions for every database.
table_level_frames = {}
failed_databases = []

for position, row_index in enumerate(database_rows, start=1):
    database = normalize_cell_value(body.iat[row_index, database_index])
    print(f"[{position:,}/{len(database_rows):,}] {database}: describing tables...")

    try:
        table_level_frames[database] = build_table_level_df(database)
    except Exception as exc:
        print(f"[{position:,}/{len(database_rows):,}] {database}: failed: {exc}")
        failed_databases.append(database)
        continue

    print(
        f"[{position:,}/{len(database_rows):,}] {database}: "
        f"{len(table_level_frames[database]):,} tables described"
    )

table_level_pdf = (
    pd.concat(table_level_frames.values(), ignore_index=True)
    if table_level_frames
    else pd.DataFrame()
)

if TABLE_OUTPUT_CSV_PATH and not table_level_pdf.empty:
    table_level_pdf.to_csv(TABLE_OUTPUT_CSV_PATH, index=False)
    print(f"Wrote table-level details to {TABLE_OUTPUT_CSV_PATH}")

display(table_level_pdf)

# COMMAND ----------

# Pass 2: aggregate each database's table-level DataFrame into database-level fields.
for position, row_index in enumerate(database_rows, start=1):
    database = normalize_cell_value(body.iat[row_index, database_index])
    table_df = table_level_frames.get(database)
    if table_df is None:
        continue

    if should_fill(body.iat[row_index, table_count_index]):
        body.iat[row_index, table_count_index] = str(len(table_df))

    columns_to_fill = {
        column_name
        for column_name, column_index in (
            (DESCRIPTION_COLUMN, description_index),
            (BUSINESS_AREA_COLUMN, business_area_index),
        )
        if should_fill(body.iat[row_index, column_index])
    }

    ai_values = describe_database(database, table_df, business_areas, columns_to_fill)
    if DESCRIPTION_COLUMN in ai_values:
        body.iat[row_index, description_index] = ai_values[DESCRIPTION_COLUMN]
    if BUSINESS_AREA_COLUMN in ai_values:
        body.iat[row_index, business_area_index] = ai_values[BUSINESS_AREA_COLUMN]

    print(
        f"[{position:,}/{len(database_rows):,}] {database}: "
        f"AI filled {len(ai_values):,}/{len(columns_to_fill):,} database fields"
    )

if failed_databases:
    print("Failed databases:", ", ".join(failed_databases))

# COMMAND ----------

output_df = pd.concat([pd.DataFrame([header]), body], ignore_index=True)
output_df.to_csv(OUTPUT_CSV_PATH, header=False, index=False)
print(f"Wrote filled sheet to {OUTPUT_CSV_PATH}")

display(
    pd.DataFrame(
        body.iloc[:, [database_index, description_index, business_area_index, table_count_index]].values,
        columns=[DATABASE_COLUMN, DESCRIPTION_COLUMN, BUSINESS_AREA_COLUMN, TABLE_COUNT_COLUMN],
    )
)
