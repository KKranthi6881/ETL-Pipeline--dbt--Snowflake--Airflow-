#!/usr/bin/env bash
set -euo pipefail

# Local dbt "show" helper for this repo.
#
# Usage:
#   ./scripts/dbt_local_show.sh [project_dir] [target] [selector] [limit]
#
# Examples:
#   ./scripts/dbt_local_show.sh data_pipeline_snowflake dev stg_tpch_orders 5
#   ./scripts/dbt_local_show.sh data_pipeline_snowflake dev fct_orders 10
#   ./scripts/dbt_local_show.sh dbt_dag/dags/data_pipeline_snowflake dev int_order_items_summary 20

PROJECT_DIR="${1:-data_pipeline_snowflake}"
TARGET="${2:-dev}"
SELECTOR="${3:-}"
LIMIT="${4:-10}"

if [[ -z "${SELECTOR}" ]]; then
  echo "ERROR: Missing selector (model name or selection string)." >&2
  echo "Example: ./scripts/dbt_local_show.sh data_pipeline_snowflake dev fct_orders 10" >&2
  exit 2
fi

if [[ ! -f "${PROJECT_DIR}/dbt_project.yml" ]]; then
  echo "ERROR: dbt_project.yml not found under PROJECT_DIR='${PROJECT_DIR}'" >&2
  exit 2
fi

PROFILES_DIR="${PROJECT_DIR}"
if [[ ! -f "${PROFILES_DIR}/profiles.yml" ]]; then
  PROFILES_DIR="data_pipeline_snowflake"
fi

if [[ ! -f "${PROFILES_DIR}/profiles.yml" ]]; then
  echo "ERROR: profiles.yml not found. Looked in '${PROJECT_DIR}' and 'data_pipeline_snowflake'." >&2
  exit 2
fi

DBT_COMMON_ARGS=(--project-dir "${PROJECT_DIR}" --profiles-dir "${PROFILES_DIR}" --target "${TARGET}")

echo "== dbt show context =="
echo "PROJECT_DIR  : ${PROJECT_DIR}"
echo "PROFILES_DIR : ${PROFILES_DIR}"
echo "TARGET       : ${TARGET}"
echo "SELECTOR     : ${SELECTOR}"
echo "LIMIT        : ${LIMIT}"
echo

dbt show "${DBT_COMMON_ARGS[@]}" --select "${SELECTOR}" --limit "${LIMIT}"