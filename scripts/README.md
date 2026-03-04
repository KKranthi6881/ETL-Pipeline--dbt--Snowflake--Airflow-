# Sample Scripts (dbt + Airflow)

This folder contains **sample helper scripts** to run the dbt project(s) and the local Airflow (Astro Runtime) environment in this repo.

> Note: This repo contains **two dbt project roots**:
> - `data_pipeline_snowflake/`
> - `dbt_dag/dags/data_pipeline_snowflake/` (a copy embedded for Airflow/Cosmos)

## dbt (local)

### 1) Build + Test (`dbt deps` + `dbt build`)
Script: `scripts/dbt_local_build.sh`

**Usage**
```bash
./scripts/dbt_local_build.sh [project_dir] [target] [select]
```

**Examples**
```bash
# Build everything in the main dbt project using target=dev
./scripts/dbt_local_build.sh data_pipeline_snowflake dev

# Build a model and its children
./scripts/dbt_local_build.sh data_pipeline_snowflake dev stg_tpch_orders+

# Build within the dbt project embedded under Airflow DAGs
./scripts/dbt_local_build.sh dbt_dag/dags/data_pipeline_snowflake dev fct_orders+
```

**What it does**
- Validates the `project_dir` has a `dbt_project.yml`
- Picks a `profiles_dir` (prefers colocated `profiles.yml`, falls back to `data_pipeline_snowflake/`)
- Runs:
  - `dbt deps`
  - `dbt build` (optionally `--select <select>`)

**Snowflake auth overrides (optional)**
```bash
# Default profile uses SSO browser login
export DBT_SNOWFLAKE_AUTHENTICATOR=externalbrowser

# Fallback to username/password auth when SSO is unavailable
export DBT_SNOWFLAKE_AUTHENTICATOR=snowflake
export DBT_SNOWFLAKE_PASSWORD='<your_password>'

# Preferred for automation: key-pair auth (no browser flow)
export DBT_SNOWFLAKE_AUTHENTICATOR=snowflake
export DBT_SNOWFLAKE_PASSWORD=''
export DBT_SNOWFLAKE_PRIVATE_KEY_PATH='/absolute/path/to/rsa_key.p8'
export DBT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='<key_passphrase_if_any>'

# Optional connection overrides
export DBT_SNOWFLAKE_ACCOUNT='xjixzmx-sc04945'
export DBT_SNOWFLAKE_USER='SONICKUMAR'
export DBT_SNOWFLAKE_ROLE='dbt_role'
export DBT_SNOWFLAKE_WAREHOUSE='dbt_wh'
export DBT_SNOWFLAKE_DATABASE='DUCKCODE_TEST_DATA'
export DBT_SNOWFLAKE_SCHEMA='ANALYTICS'
```

### 2) Preview data (`dbt show`)
Script: `scripts/dbt_local_show.sh`

**Usage**
```bash
./scripts/dbt_local_show.sh [project_dir] [target] [selector] [limit]
```

**Examples**
```bash
./scripts/dbt_local_show.sh data_pipeline_snowflake dev fct_orders 10
./scripts/dbt_local_show.sh data_pipeline_snowflake dev stg_tpch_orders 5
```

## Airflow (Astro Runtime local dev)

These scripts run the Astro project under `dbt_dag/`.

### 3) Start Airflow locally
Script: `scripts/astro_local_start.sh`

**Usage**
```bash
./scripts/astro_local_start.sh
```

This runs:
- `cd dbt_dag && astro dev start`

Airflow UI defaults:
- http://localhost:8080
- username/password: `admin` / `admin`

### 4) Stop Airflow locally
Script: `scripts/astro_local_stop.sh`

**Usage**
```bash
./scripts/astro_local_stop.sh
```

This runs:
- `cd dbt_dag && astro dev stop`

## One-time setup (recommended)

Mark scripts executable:
```bash
chmod +x scripts/*.sh
