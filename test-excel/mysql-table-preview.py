# Databricks notebook source
# Preview MySQL rows + discover PRIMARY KEY and candidate watermark columns
# for MATM ETL entity configs (mysql_extract_daily / hourly.json).

dbutils.widgets.text("database", "default_shipment", "MySQL database")
dbutils.widgets.text("table", "SHP_SHIPMENT", "MySQL table")
dbutils.widgets.text("row_limit", "100", "Row limit")

# COMMAND ----------

import re

MYSQL_HOST = "10.219.252.18"
MYSQL_PORT = 3306
SECRET_SCOPE = "mysql-replica"
MYSQL_USER = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
MYSQL_PWD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")
MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

# Name patterns that usually work as incremental watermarks (MATM / generic).
WATERMARK_NAME_HINTS = (
    "UPDATED_DTTM",
    "UPDATED_DATETIME",
    "UPDATEDATETIME",
    "UPDATE_TIME",
    "UPDATE_DTTM",
    "MODIFIED_DTTM",
    "MODIFIED_DATETIME",
    "MODIFIED_DATE",
    "LAST_UPDATED",
    "LAST_UPDATE",
    "CREATED_DTTM",
    "CREATED_DATETIME",
    "CREATEDATETIME",
    "CREATE_TIME",
    "CREATED_DATE",
    "CREATE_DTTM",
)

DATETIME_TYPES = {
    "timestamp",
    "datetime",
    "date",
    "time",
}


def _jdbc_read(sql_subquery: str, alias: str = "src"):
    dbtable = f"({sql_subquery}) {alias}"
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


def _rank_watermark(col_name: str, data_type: str) -> tuple:
    """Lower score = better watermark candidate."""
    name_u = col_name.upper()
    dtype = (data_type or "").lower()
    is_dt = any(t in dtype for t in DATETIME_TYPES)

    if name_u in WATERMARK_NAME_HINTS:
        # Prefer update/modified over create
        if "UPDATE" in name_u or "MODIF" in name_u or "LAST_" in name_u:
            return (0, name_u)
        return (1, name_u)
    if is_dt and ("UPDATE" in name_u or "MODIF" in name_u or "LAST_" in name_u):
        return (2, name_u)
    if is_dt and ("CREATE" in name_u):
        return (3, name_u)
    if is_dt:
        return (4, name_u)
    return (9, name_u)


database = dbutils.widgets.get("database").strip()
table = dbutils.widgets.get("table").strip()
row_limit_raw = dbutils.widgets.get("row_limit").strip()
row_limit = int(row_limit_raw) if row_limit_raw else 100

if not database or not table:
    raise ValueError("Set both 'database' and 'table'.")
if not IDENTIFIER_PATTERN.fullmatch(database):
    raise ValueError(f"Invalid database name '{database}'")
if not IDENTIFIER_PATTERN.fullmatch(table):
    raise ValueError(f"Invalid table name '{table}'")

jdbc_url = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    "?zeroDateTimeBehavior=convertToNull&autoReconnect=true"
    "&characterEncoding=UTF-8&characterSetResults=UTF-8"
)

# COMMAND ----------

# Magic: Primary key from information_schema
pk_sql = f"""
SELECT
  kcu.ORDINAL_POSITION AS pk_ordinal,
  kcu.COLUMN_NAME AS column_name,
  c.DATA_TYPE AS data_type,
  c.COLUMN_TYPE AS column_type,
  c.IS_NULLABLE AS is_nullable
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE kcu
  ON  tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
  AND tc.CONSTRAINT_NAME   = kcu.CONSTRAINT_NAME
  AND tc.TABLE_SCHEMA      = kcu.TABLE_SCHEMA
  AND tc.TABLE_NAME        = kcu.TABLE_NAME
JOIN information_schema.COLUMNS c
  ON  c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
  AND c.TABLE_NAME   = kcu.TABLE_NAME
  AND c.COLUMN_NAME  = kcu.COLUMN_NAME
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_SCHEMA = '{database}'
  AND tc.TABLE_NAME   = '{table}'
ORDER BY kcu.ORDINAL_POSITION
"""
pk_df = _jdbc_read(pk_sql, "pk_src")
print(f"\n=== PRIMARY KEY: `{database}`.`{table}` ===")
if pk_df.rdd.isEmpty():
    print("No PRIMARY KEY found in information_schema.")
    pk_cols = []
else:
    pk_df.show(truncate=False)
    display(pk_df)
    pk_cols = [r["column_name"] for r in pk_df.collect()]

# COMMAND ----------

# Magic: Unique keys (fallback if no PK) + indexes named like PK
uniq_sql = f"""
SELECT
  s.INDEX_NAME AS index_name,
  s.NON_UNIQUE AS non_unique,
  s.SEQ_IN_INDEX AS seq_in_index,
  s.COLUMN_NAME AS column_name,
  c.DATA_TYPE AS data_type
FROM information_schema.STATISTICS s
JOIN information_schema.COLUMNS c
  ON  c.TABLE_SCHEMA = s.TABLE_SCHEMA
  AND c.TABLE_NAME   = s.TABLE_NAME
  AND c.COLUMN_NAME  = s.COLUMN_NAME
WHERE s.TABLE_SCHEMA = '{database}'
  AND s.TABLE_NAME   = '{table}'
  AND s.NON_UNIQUE = 0
ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX
"""
uniq_df = _jdbc_read(uniq_sql, "uniq_src")
print(f"\n=== UNIQUE INDEXES (incl. PRIMARY): `{database}`.`{table}` ===")
display(uniq_df)

# COMMAND ----------

# Magic: All columns + watermark candidates
cols_sql = f"""
SELECT
  ORDINAL_POSITION AS ordinal_position,
  COLUMN_NAME AS column_name,
  DATA_TYPE AS data_type,
  COLUMN_TYPE AS column_type,
  IS_NULLABLE AS is_nullable,
  COLUMN_KEY AS column_key,
  EXTRA AS extra,
  COLUMN_DEFAULT AS column_default
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = '{database}'
  AND TABLE_NAME   = '{table}'
ORDER BY ORDINAL_POSITION
"""
cols_df = _jdbc_read(cols_sql, "cols_src")
print(f"\n=== COLUMNS: `{database}`.`{table}` ===")
display(cols_df)

col_rows = cols_df.collect()
all_col_names = [r["column_name"] for r in col_rows]

watermark_ranked = sorted(
    (
        (_rank_watermark(r["column_name"], r["data_type"]), r["column_name"], r["data_type"])
        for r in col_rows
    ),
    key=lambda x: x[0],
)
# Keep only plausible candidates (score < 9)
watermark_candidates = [
    {"column_name": name, "data_type": dtype, "rank": score[0]}
    for score, name, dtype in watermark_ranked
    if score[0] < 9
]

print("\n=== WATERMARK CANDIDATES (best first) ===")
if not watermark_candidates:
    print("No datetime/timestamp-like columns found. Pick a monotonic numeric/id carefully.")
else:
    for c in watermark_candidates:
        print(f"  rank={c['rank']}  {c['column_name']}  ({c['data_type']})")

recommended_wm = watermark_candidates[0]["column_name"] if watermark_candidates else None
# Prefer update-style over create when both exist
for c in watermark_candidates:
    n = c["column_name"].upper()
    if "UPDATE" in n or "MODIF" in n or "LAST_" in n:
        recommended_wm = c["column_name"]
        break

# COMMAND ----------

# Magic: Config snippet for MATM ETL JSON
print("\n" + "=" * 72)
print("MATM ETL CONFIG SUGGESTION")
print("=" * 72)
print(f"database: {database}")
print(f"table:    {table}")
print(f"primary_key_columns: {pk_cols if pk_cols else '<< NONE — use a UNIQUE index or confirm business key >>'}")
print(f"watermark_column:    {recommended_wm or '<< NONE — inspect columns manually >>'}")
print(f"schema.columns ({len(all_col_names)}):")
print("  " + ", ".join(all_col_names))
if all_col_names:
    select_list = ", ".join(f"`{c}`" for c in all_col_names)
    print("\nextract.query_by_env example:")
    print(f"  SELECT {select_list} FROM `{database}`.`{table}`")

# COMMAND ----------

# Magic: Row preview
preview_sql = f"SELECT * FROM `{database}`.`{table}` LIMIT {row_limit}"
df = _jdbc_read(preview_sql, "preview_src")
print(f"\n=== PREVIEW ({row_limit} rows): `{database}`.`{table}` ===")
df.show(row_limit, truncate=False)
display(df)
