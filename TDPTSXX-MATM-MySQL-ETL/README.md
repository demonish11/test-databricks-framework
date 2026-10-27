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

## Entities (default_shipment)

| Frequency | Entity | Source table | PK | Watermark |
|-----------|--------|--------------|----|-----------|
| Hourly | `ShpShipment` | `SHP_SHIPMENT` | `PK` | `UPDATED_TIMESTAMP` |
| Hourly | `ShpShipmentCost` | `SHP_SHIPMENT_COST` | `SHIPMENT_PK` | `UPDATED_TIMESTAMP` |
| Daily | `ShpImportReqOrder` | `SHP_IMPORT_REQ_ORDER` | `SHIPMENT_IMP_REQ_ORDER_PK` | `UPDATED_TIMESTAMP` |
| Daily | `ShpImportReqOrderDtl` | `SHP_IMPORT_REQ_ORDER_DTL` | `PK` | `UPDATED_TIMESTAMP` |

Targets land under UC schema `matm_mysql_raw` and lake path `/Publix/SupplyChain/MatmMysql/Raw/`.

Add more tables by copying an entity block and setting:

1. `jdbc.database_by_env` to the MATM microservice database
2. `extract.query_by_env` SELECT (prefer `SELECT *`)
3. `target.primary_key_columns` + incremental watermark
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
