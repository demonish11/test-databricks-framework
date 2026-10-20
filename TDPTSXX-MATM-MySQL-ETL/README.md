# TDPTSXX-MATM-MySQL-ETL

MySQL (MATM replica) → Delta / Unity Catalog ingestion bundle.

Same layout as `TDPTSXX-AzureSql-ETL` / DATALAKE-style entity pipeline:

`Entity_Sequential_Runner` → `Entity_Pipeline_Worker` → Extract → Transform → Completion

## Source

| Setting | Value |
|---------|-------|
| Host | `10.219.252.18:3306` (see `mysql_source_stub.json`) |
| Auth | Secret scope `mysql-replica` keys `username` / `password` |
| Driver | `com.mysql.cj.jdbc.Driver` |
| JDBC jar var | `MySql_Jdbc_Jar_Path` |

## Sample entities

Daily config ships two metadata entities (same catalog surface as `test-excel`):

- `InformationSchemaTables`
- `InformationSchemaColumns`

Targets land under UC schema `matm_mysql_raw` and lake path `/Publix/SupplyChain/MatmMysql/Raw/`.

Add business tables by copying an entity block and setting:

1. `jdbc.database_by_env` to the MATM microservice database
2. `extract.query_by_env` SELECT
3. `schema.columns` + `target.primary_key_columns`
4. `target.target_entity` / paths

## Deploy

```bash
databricks bundle validate -t DEV_East
databricks bundle deploy -t DEV_East
```

Jobs (paused by default):

- `S0TDPSXX_MatmMysql_ETL_Daily`
- `S0TDPSXX_MatmMysql_ETL_Hourly`

Azure DevOps: `azure-pipelines/azure-pipelines.yml` (same S0TDPSXX pattern as AzureSql-ETL).

## Related

- `TDPTSXX-DATALAKE` — Oracle → Delta (legacy Wrapper/Runner)
- `TDPTSXX-AzureSql-ETL` — Azure SQL → Delta (this package’s layout source)
- `test-excel/excel-script.py` — MATM metadata enrichment notebook that uses the same MySQL host/secrets
