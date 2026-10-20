# Databricks notebook source

dbutils.widgets.text("database", "", "MySQL database")
dbutils.widgets.text("table", "", "MySQL table")
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
dbtable = f"(SELECT * FROM `{database}`.`{table}` LIMIT {row_limit}) preview_src"

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PWD)
    .option("driver", MYSQL_JDBC_DRIVER)
    .option("fetchsize", "10000")
    .load()
)

df.show(row_limit, truncate=False)
display(df)
