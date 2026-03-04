#!/usr/bin/env bash
set -euo pipefail

# Local dbt run helper for this repo.
#
# Supports both dbt project roots:
#   1) data_pipeline_snowflake/
#   2) dbt_dag/dags/data_pipeline_snowflake/   (copy embedded under Airflow DAGs)
#
# Usage:
#   ./scripts/dbt_local_build.sh [project_dir] [target] [select]
#
# Examples:
#   ./scripts/dbt_local_build.sh
#   ./scripts/dbt_local_build.sh data_pipeline_snowflake dev
#   ./scripts/dbt_local_build.sh data_pipeline_snowflake dev stg_tpch_orders+
#   ./scripts/dbt_local_build.sh dbt_dag/dags/data_pipeline_snowflake dev tag:nightly

PROJECT_DIR="${1:-data_pipeline_snowflake}"
TARGET="${2:-dev}"
SELECT="${3:-}"

if [[ ! -f "${PROJECT_DIR}/dbt_project.yml" ]]; then
  echo "ERROR: dbt_project.yml not found under PROJECT_DIR='${PROJECT_DIR}'" >&2
  exit 2
fi

# Prefer a profiles.yml colocated with the project. Fallback to the canonical one.
PROFILES_DIR="${PROJECT_DIR}"
if [[ ! -f "${PROFILES_DIR}/profiles.yml" ]]; then
  PROFILES_DIR="data_pipeline_snowflake"
fi

if [[ ! -f "${PROFILES_DIR}/profiles.yml" ]]; then
  echo "ERROR: profiles.yml not found. Looked in '${PROJECT_DIR}' and 'data_pipeline_snowflake'." >&2
  exit 2
fi

DBT_COMMON_ARGS=(--project-dir "${PROJECT_DIR}" --profiles-dir "${PROFILES_DIR}" --target "${TARGET}")

echo "== dbt context =="
echo "PROJECT_DIR  : ${PROJECT_DIR}"
echo "PROFILES_DIR : ${PROFILES_DIR}"
echo "TARGET       : ${TARGET}"
echo "SELECT       : ${SELECT:-<all>}"
echo

echo "== Running: dbt deps =="
dbt deps "${DBT_COMMON_ARGS[@]}"

if [[ -n "${SELECT}" ]]; then
  echo "== Running: dbt build --select ${SELECT} =="
  dbt build "${DBT_COMMON_ARGS[@]}" --select "${SELECT}"
else
  echo "== Running: dbt build (all models) =="
  dbt build "${DBT_COMMON_ARGS[@]}"
fi