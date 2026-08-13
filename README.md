# test-databricks-framework

Databricks ETL framework repos for TDPTS / TDPS.

## Projects

| Folder | Purpose |
|--------|---------|
| `TDPTSXX-DATALAKE` | Oracle → Delta (legacy Wrapper / Runner) |
| `TDPS-DBWS-Dataload` | Oracle → Azure SQL (Extract / Transform / Complete) |
| `TDPTSXX-AzureSql-ETL` | Azure SQL → Delta / UC (same layout as Data Load) |

Each of the last two is a standalone Databricks Asset Bundle with its own `databricks.yml`, `targets/`, `resources/`, and `azure-pipelines/`.
