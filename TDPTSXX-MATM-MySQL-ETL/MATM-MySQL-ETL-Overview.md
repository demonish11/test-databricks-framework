# MATM MySQL → Unity Catalog Ingestion Framework

**Project:** `TDPTSXX-MATM-MySQL-ETL`  
**Audience:** engineering / data platform walkthrough  
**Purpose:** config-driven ingestion of Manhattan Active (MATM) MySQL data into Databricks Delta + Unity Catalog

---

## 1. The idea (why this exists)

MATM data lives in a MySQL replica (microservice databases behind Manhattan Active). We need that data in the lakehouse on a schedule — not as one-off notebooks, but as a repeatable ingestion product.

**What we are implementing**

A **MySQL → Delta / Unity Catalog** ingestion framework where:

1. Connection details and credentials live in one shared stub.
2. Each table/entity is defined in JSON (SQL, keys, target paths).
3. One runner drives every entity through the same three stages: **Extract → Transform → Completion**.
4. Jobs support **daily full** and **hourly/transactional incremental** loads.
5. Writes land in Unity Catalog and ADLS Delta (primary + secondary region).

**Goals**

- Pull MATM MySQL databases into Delta / UC reliably.
- Keep business logic in **configs**, not hardcoded notebooks.
- Reuse the same notebooks for every new table.
- Track incremental watermarks via loadtracker.
- Deploy as a Databricks Asset Bundle (dev / tst / stg / prd targets).

---

## 2. High-level architecture

```text
┌─────────────────────┐
│  MATM MySQL replica │  10.219.252.18:3306
│  (microservice DBs) │  secrets: mysql-replica / username+password
└──────────┬──────────┘
           │ Spark JDBC (mysql-connector jar)
           ▼
┌──────────────────────────────────────────────────────────┐
│  Databricks Job (Asset Bundle)                           │
│  S0TDPSXX_MatmMysql_ETL_Daily  |  _Hourly                │
│                                                          │
│  Entity_Sequential_Runner                                │
│       │  for each entity in config                       │
│       ▼                                                  │
│  Entity_Pipeline_Worker                                  │
│       │                                                  │
│       ├── Extracts_Wrapper   (JDBC → temp / global views)│
│       ├── Transforms_Wrapper (SQL or passthrough)        │
│       └── Completion         (Delta + UC + loadtracker)  │
└──────────────────────────────┬───────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────┐
│  Targets                                                 │
│  • ADLS Raw / Transform Delta paths (primary + secondary)│
│  • Unity Catalog: matm_mysql_raw / matm_mysql_transform  │
│  • monitoring.load_tracking_* (watermarks)               │
└──────────────────────────────────────────────────────────┘
```

**Config-driven control plane**

| Config | Role |
|--------|------|
| `mysql_source_stub.json` | Shared host, port, driver, secret scope |
| `mysql_extract_daily.json` | Entity list + SQL for full daily loads |
| `mysql_extract_hourly.json` | Entity list + incremental flags for transactional loads |

Add a new MATM table ≈ add a JSON entity block. No new notebook required for a standard extract.

---

## 3. End-to-end runtime flow

### 3.1 Job start

1. Job cluster starts with env vars from the bundle target:
   - `Databricks_Env` (`dev` / `tst` / `stg` / `prd`)
   - `Daily_Source_Config_Path` / `Transactional_Source_Config_Path`
   - `Target_Config_Path` (MySQL connection stub)
   - Loadtracker catalog / schema / table
2. MySQL JDBC jar is attached via `MySql_Jdbc_Jar_Path`.
3. Entry notebook: **`Entity_Sequential_Runner`**.

### 3.2 Sequential runner

1. Loads source JSON (`entities` map).
2. Resolves which entities to run (`entity_name` widget empty = all; otherwise that entity + dependencies).
3. Ensures loadtracker table exists.
4. For each entity, calls **`Entity_Pipeline_Worker`** with the same paths + that entity name.
5. Aggregates successes / failures; fails the job if any entity failed.

### 3.3 Per-entity pipeline worker

Runs three notebooks in order:

```text
Extracts_Wrapper  →  Transforms_Wrapper  →  Completion
```

### 3.4 Extract

1. Read entity `extract.query_by_env` for current `Databricks_Env`.
2. Build JDBC options from entity `jdbc` + `mysql_source_stub` (`connection_source: target_config`).
3. Optional incremental: if `incremental.enabled` and `load_mode=transactional`, append watermark predicate using last successful load from loadtracker (or fallback MAX on UC table).
4. `spark.read.format("jdbc")` → create temp + global temp views (`mysql_extract_*`).

### 3.5 Transform

1. Read extract global temp view.
2. If `transform.query_by_env` is set → run that Spark SQL.
3. Else → **passthrough** (same columns/types as extract).
4. Publish `mysql_transform_*` views.

### 3.6 Completion

1. Resolve Delta / UC paths from entity `target.*_by_env`.
2. Dedupe on configured keys if needed.
3. Write Raw + Transform layers (primary + secondary region paths/catalogs).
4. Update loadtracker when incremental watermarks apply.
5. Hourly path can bootstrap a missing transform table with a full extract first.

---

## 4. Repository layout (what each file is for)

```text
TDPTSXX-MATM-MySQL-ETL/
├── databricks.yml                 # Asset Bundle root
├── configs/vars_base.yml          # Shared bundle variables
├── resources/jobs/mysql_etl_jobs.yml
├── targets/{dev,tst,stg,prd}.yml
├── azure-pipelines/               # CI/CD deploy
├── src/                           # Notebooks + configs synced to workspace
│   ├── Entity_Sequential_Runner.py
│   ├── Entity_Pipeline_Worker.py
│   ├── Extracts_Wrapper.py
│   ├── Transforms_Wrapper.py
│   ├── Completion.py
│   ├── configs/
│   │   ├── mysql_source_stub.json
│   │   ├── mysql_extract_daily.json
│   │   └── mysql_extract_hourly.json
│   └── utils/
│       ├── jdbc_utils.py
│       ├── config_utils.py
│       └── delta_utils.py
├── README.md                      # Short operator guide
└── MATM-MySQL-ETL-Overview.md     # This presentation doc
```

### 4.1 Bundle & deploy files

| File | What it contains / does |
|------|-------------------------|
| `databricks.yml` | Bundle name `TDPTSXX-MATM-MySQL-ETL`; includes vars, jobs, targets; syncs `src/` to workspace |
| `configs/vars_base.yml` | Defaults: `Databricks_Env`, `MySql_Jdbc_Jar_Path`, loadtracker names, cron, config path templates |
| `resources/jobs/mysql_etl_jobs.yml` | Two jobs: **Daily** (`load_mode=daily`) and **Hourly** (`load_mode=transactional`); cluster, env vars, jar library, notebook task |
| `targets/dev.yml` | DEV East (default) + DEV Central hosts, jar volume paths, permissions, config paths |
| `targets/tst.yml` | TST East + Central |
| `targets/stg.yml` / `prd.yml` | Placeholders until those stages are enabled |
| `azure-pipelines/azure-pipelines.yml` | Dev then Tst deploy stages (East + Central) |
| `azure-pipelines/templates/deploy_template.yml` | Key Vault PAT → `databricks bundle validate` → `bundle deploy` |

### 4.2 Orchestration notebooks

| File | Responsibility |
|------|----------------|
| `Entity_Sequential_Runner.py` | Job entrypoint. Loads entity list, loops entities, runs worker notebook per entity, prints summary, fails on worker errors |
| `Entity_Pipeline_Worker.py` | Single-entity orchestrator. Runs Extract → Transform → Completion sibling notebooks with shared widgets |

### 4.3 Stage notebooks

| File | Responsibility |
|------|----------------|
| `Extracts_Wrapper.py` | JDBC extract for selected entities; incremental SQL when transactional; creates `mysql_extract_*` views |
| `Transforms_Wrapper.py` | Optional Spark SQL reshape or passthrough; creates `mysql_transform_*` views |
| `Completion.py` | Writes Delta/UC via `delta_utils`; loadtracker updates; hourly transform bootstrap if table missing |

### 4.4 Utility modules (`%run` from notebooks)

| File | Responsibility |
|------|----------------|
| `utils/jdbc_utils.py` | Build `jdbc:mysql://...` URL; merge entity JDBC with connection stub; read username/password from `mysql-replica`; redact secrets in logs |
| `utils/config_utils.py` | Load JSON; resolve env maps; entity inheritance / depends_on; loadtracker ensure/read/write; MySQL incremental watermark (`DATETIME(3)`); schema vs SELECT column checks |
| `utils/delta_utils.py` | Resolve primary/secondary lake + UC paths; dedupe; merge/overwrite into Delta; register UC tables |

### 4.5 Runtime JSON configs

| File | Responsibility |
|------|----------------|
| `mysql_source_stub.json` | Shared connection: host `10.219.252.18`, port `3306`, driver, secret scope keys |
| `mysql_extract_daily.json` | Daily entities (full load). Ships sample **InformationSchemaTables** + **InformationSchemaColumns** |
| `mysql_extract_hourly.json` | Transactional sample with `incremental.enabled: true` |

---

## 5. Connection model (how MySQL auth works)

```text
Entity JSON                         Stub JSON
───────────                         ─────────
jdbc.database: "mysql"              connection.server_by_env
jdbc.connection_source:             connection.port / driver
  "target_config"                   authentication.secret_scope_by_env
jdbc.database_by_env:               authentication.username_key / password_key
  { "dev": "<matm_db>", ... }           │
        │                               │
        └──────────┬────────────────────┘
                   ▼
           jdbc_utils.build_jdbc_options()
                   │
                   ▼
     url = jdbc:mysql://host:3306/<database>?...
     user/password = dbutils.secrets.get(mysql-replica, ...)
```

**Why stub + entity split?**

- One MATM host / credential set for all microservice DBs.
- Each entity overrides **which MySQL database/schema** via `jdbc.database_by_env` (e.g. one entity per MATM service DB).

**Prerequisites on the workspace**

- Network path from job cluster to `10.219.252.18:3306`
- Secret scope `mysql-replica` with `username` and `password`
- MySQL connector JAR on the UC volume path referenced by `MySql_Jdbc_Jar_Path`

---

## 6. Entity config shape (what we implement per table)

Each key under `entities` is one ingestible object. Important fields:

```json
{
  "entity_name": "InformationSchemaTables",
  "jdbc": {
    "database": "mysql",
    "connection_source": "target_config",
    "database_by_env": { "dev": "information_schema", "...": "..." },
    "driver": "com.mysql.cj.jdbc.Driver",
    "options": { "fetchsize": "10000" }
  },
  "schema": {
    "source_table": "information_schema.TABLES",
    "columns": ["TABLE_SCHEMA", "TABLE_NAME", "..."]
  },
  "extract": {
    "enabled": true,
    "query_by_env": { "dev": "SELECT ... FROM ...", "tst": "...", "stg": "...", "prd": "..." },
    "temp_view_name": "mysql_extract_...",
    "global_temp_view_name": "mysql_extract_..."
  },
  "transform": {
    "enabled": true,
    "temp_view_name": "mysql_transform_...",
    "global_temp_view_name": "mysql_transform_..."
  },
  "completion": { "enabled": true },
  "incremental": {
    "enabled": false,
    "watermark_column": "UPDATE_TIME",
    "uc_watermark_column": "UPDATE_TIME",
    "loadtracker_entity_name": "InformationSchemaTables"
  },
  "target": {
    "write_enabled": true,
    "primary_key_columns": ["TABLE_SCHEMA", "TABLE_NAME"],
    "dedupe_key_columns": ["TABLE_SCHEMA", "TABLE_NAME"],
    "dedupe_order_columns": ["UPDATE_TIME", "CREATE_TIME"],
    "raw_merge_on_primary_key": false,
    "target_entity": "TDPS_MatmInformationSchemaTables",
    "target_uc_schema": "matm_mysql_raw",
    "target_file_path": "/Publix/SupplyChain/MatmMysql/Raw/",
    "target_file_format": "Delta",
    "target_datalake_by_env": { "dev": "abfss://...", "...": "..." },
    "secondary_datalake_by_env": { "...": "..." },
    "target_unity_catalog_by_env": { "dev": "ent_dtlk_dev", "...": "..." },
    "secondary_unity_catalog_by_env": { "dev": "entc_dtlk_dev", "...": "..." }
  }
}
```

**Daily vs hourly**

| Mode | Job | Typical incremental | Write behavior |
|------|-----|---------------------|----------------|
| `daily` | Daily job | Usually off (full extract) | Full / overwrite style per delta utils |
| `transactional` | Hourly job | On when `incremental.enabled` | Merge on PK when configured |

---

## 7. What ships today (sample implementation)

Starter entities prove the path end-to-end:

| Entity | Source | Target entity name | UC schema |
|--------|--------|--------------------|-----------|
| `InformationSchemaTables` | `information_schema.TABLES` (excl. system schemas) | `TDPS_MatmInformationSchemaTables` | `matm_mysql_raw` |
| `InformationSchemaColumns` | `information_schema.COLUMNS` (daily sample) | `TDPS_MatmInformationSchemaColumns` | `matm_mysql_raw` |

Lake root: `/Publix/SupplyChain/MatmMysql/Raw/` (Transform derived by replacing `Raw` → `Transform`).

These are **scaffolding / metadata** entities. Real MATM business tables are added the same way once owners pick databases and PKs.

---

## 8. Jobs & schedules

| Job name | Load mode | Config | Schedule (default, paused) |
|----------|-----------|--------|----------------------------|
| `S0TDPSXX_MatmMysql_ETL_Daily` | `daily` | `mysql_extract_daily.json` | `9 0 22 * * ?` America/New_York |
| `S0TDPSXX_MatmMysql_ETL_Hourly` | `transactional` | `mysql_extract_hourly.json` | `12 15 * * * ?` America/New_York |

**Job parameters**

- `entity_name` — empty = all entities; set to run one
- `continue_on_error` — default `true` on the job definition

Jobs are created **paused**; enable after jar + secrets + network are validated.

---

## 9. How to add a real MATM business table

1. Identify MySQL database name (MATM microservice) and table.
2. Copy an entity block in `mysql_extract_daily.json` (and hourly if needed).
3. Set:
   - `jdbc.database_by_env` → that microservice DB
   - `extract.query_by_env` → explicit column SELECT (avoid `SELECT *` for schema validation)
   - `schema.columns` matching the SELECT list
   - `target.primary_key_columns` / dedupe columns
   - `target.target_entity` naming convention (`TDPS_Matm...`)
   - Incremental watermark column if hourly
4. Deploy bundle; run job with `entity_name=<YourEntity>`.
5. Validate UC table + Delta path + loadtracker row.

No new Python notebooks required for a standard ingest.

---

## 10. Deploy & validate (operator checklist)

```bash
cd TDPTSXX-MATM-MySQL-ETL
databricks bundle validate -t DEV_East
databricks bundle deploy -t DEV_East
```

Then:

1. Confirm MySQL jar exists at the volume path in `targets/dev.yml`.
2. Confirm secret scope `mysql-replica` on the workspace.
3. Run Daily job with `entity_name=InformationSchemaTables`.
4. Check UC: `ent_dtlk_dev.matm_mysql_raw.TDPS_MatmInformationSchemaTables` (names per env).
5. Check loadtracker if using hourly/incremental.
6. Unpause schedules when ready.

Azure DevOps pipeline: Dev East/Central → Tst East/Central (`azure-pipelines/`).

---

## 11. Summary (one-liner)

> **MATM MySQL → Delta/UC ingestion as a Databricks Asset Bundle:** shared MySQL connection stub, JSON entity configs, sequential Extract → Transform → Completion, daily + hourly jobs, dual-region lake/UC writes, and loadtracker watermarks.

---

## 12. Open follow-ups

- Expand entity list beyond `information_schema` to prioritized MATM microservice tables.
- Confirm exact MySQL connector JAR version/path on each env volume.
- Fill `targets/stg.yml` / `prd.yml` and enable pipeline stages when ready.
- Align `target_uc_schema` / lake path naming with enterprise standards if `matm_mysql_raw` needs renaming.
- Optional unit tests under `src/tests/`.
