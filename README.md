# test-databricks-framework

Databricks ETL framework repos for TDPTS / TDPS.

## Projects

| Folder | Purpose |
|--------|---------|
| `TDPTSXX-DATALAKE` | Oracle → Delta (legacy Wrapper / Runner) |
| `TDPS-DBWS-Dataload` | Oracle → Azure SQL (Extract / Transform / Complete) |
| `TDPTSXX-AzureSql-ETL` | Azure SQL → Delta / UC (same layout as Data Load) |
| `TDPTSXX-MATM-MySQL-ETL` | MATM MySQL → Delta / UC (same layout as AzureSql-ETL) |
| `test-excel` | MATM metadata enrichment notebook (MySQL + model) |

Each of `TDPS-DBWS-Dataload`, `TDPTSXX-AzureSql-ETL`, and `TDPTSXX-MATM-MySQL-ETL` is a standalone Databricks Asset Bundle with its own `databricks.yml`, `targets/`, `resources/`, and `azure-pipelines/`.
