#!/usr/bin/env bash
set -euo pipefail

# Azure Blob Storage → Azure SQL Database ingestion helper (client-side).
#
# This script supports a simple, repeatable workflow:
#   Azure Blob (CSV) -> download to local temp file -> bulk load into Azure SQL Database via bcp
#
# Why client-side?
# - Azure SQL Database cannot directly read arbitrary blob paths from your laptop without
#   server-side external data source configuration. Client-side bcp is portable and explicit.
#
# Guardrails:
# - No secrets in code. Credentials are passed via environment variables or existing CLI auth.
# - The "load/ingest" commands mutate the database (INSERTs). Use intentionally.
# - Assumes the target table already exists and matches the file schema.
#
# Prereqs:
# - Azure CLI:   https://learn.microsoft.com/cli/azure/install-azure-cli
# - bcp/sqlcmd:  Microsoft ODBC + mssql-tools (bcp is required for bulk load)
#
# Usage:
#   ./scripts/azure_to_sql_ingest.sh <command> [args...]
#
# Commands:
#   download <blob_name> [local_file]
#       Downloads a blob to local_file (default: /tmp/<blob_basename>).
#
#   load <local_file>
#       Bulk loads local_file into Azure SQL Database table via bcp.
#
#   ingest <blob_name> [local_file]
#       Convenience: download then load.
#
#   query <sql>
#       Runs a read-only query via sqlcmd (useful for smoke checks like COUNT(*)).
#
# Examples:
#   # Download only
#   AZURE_STORAGE_ACCOUNT=myacct AZURE_STORAGE_CONTAINER=landing \\
#     ./scripts/azure_to_sql_ingest.sh download exports/customers.csv
#
#   # Ingest end-to-end (download + bulk load)
#   AZURE_STORAGE_ACCOUNT=myacct AZURE_STORAGE_CONTAINER=landing \\
#   SQL_SERVER_FQDN=myserver.database.windows.net SQL_DATABASE=mydb \\
#   SQL_USER=etl_user SQL_PASSWORD=... SQL_SCHEMA=dbo SQL_TABLE=stg_customers \\
#     ./scripts/azure_to_sql_ingest.sh ingest exports/customers.csv
#
#   # Post-load validation
#   SQL_SERVER_FQDN=myserver.database.windows.net SQL_DATABASE=mydb \\
#   SQL_USER=etl_user SQL_PASSWORD=... \\
#     ./scripts/azure_to_sql_ingest.sh query "SELECT COUNT(*) AS row_count FROM dbo.stg_customers;"
#
# Configuration via env vars:
#   Blob:
#     AZURE_STORAGE_ACCOUNT        (required) Storage account name
#     AZURE_STORAGE_CONTAINER      (required) Container name
#     AZURE_STORAGE_SAS_TOKEN      (optional) SAS token (no leading '?')
#     AZURE_STORAGE_AUTH_MODE      (default: login) Azure CLI auth-mode when SAS not used
#
#   SQL:
#     SQL_SERVER_FQDN              (required for load/query) e.g., myserver.database.windows.net
#     SQL_DATABASE                 (required for load/query) database name
#     SQL_USER                     (optional) SQL auth username (recommended for scripts)
#     SQL_PASSWORD                 (optional) SQL auth password
#
#     SQL_SCHEMA                   (default: dbo)
#     SQL_TABLE                    (required for load) table name only (no schema)
#
#   File/format:
#     CSV_FIELD_TERMINATOR         (default: ,)
#     CSV_ROW_TERMINATOR           (default: \\n) Note: bcp on mac typically handles \n
#     CSV_FIRST_ROW                (default: 2)  Use 1 if file has no header
#     BCP_EXTRA_ARGS               (optional) extra args appended to bcp
#
# Notes:
# - Ensure Azure SQL firewall allows your client IP, or use a private endpoint + VPN.
# - For large files, consider compressing at source and decompressing locally prior to load.
# - For robust ingestion (schema drift, transforms), prefer ADF/Synapse/Data Factory pipelines.

SCRIPT_NAME="$(basename "$0")"

AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-}"
AZURE_STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-}"
AZURE_STORAGE_SAS_TOKEN="${AZURE_STORAGE_SAS_TOKEN:-}"
AZURE_STORAGE_AUTH_MODE="${AZURE_STORAGE_AUTH_MODE:-login}"

SQL_SERVER_FQDN="${SQL_SERVER_FQDN:-}"
SQL_DATABASE="${SQL_DATABASE:-}"
SQL_USER="${SQL_USER:-}"
SQL_PASSWORD="${SQL_PASSWORD:-}"

SQL_SCHEMA="${SQL_SCHEMA:-dbo}"
SQL_TABLE="${SQL_TABLE:-}"

CSV_FIELD_TERMINATOR="${CSV_FIELD_TERMINATOR:-,}"
CSV_ROW_TERMINATOR="${CSV_ROW_TERMINATOR:-\\n}"
CSV_FIRST_ROW="${CSV_FIRST_ROW:-2}"
BCP_EXTRA_ARGS="${BCP_EXTRA_ARGS:-}"

usage() {
  cat >&2 <<EOF
${SCRIPT_NAME} - Azure Blob -> Azure SQL Database ingestion helper

Usage:
  ${SCRIPT_NAME} download <blob_name> [local_file]
  ${SCRIPT_NAME} load <local_file>
  ${SCRIPT_NAME} ingest <blob_name> [local_file]
  ${SCRIPT_NAME} query <sql>

Blob env:
  AZURE_STORAGE_ACCOUNT=...
  AZURE_STORAGE_CONTAINER=...
  AZURE_STORAGE_SAS_TOKEN=...         (optional; no leading '?')
  AZURE_STORAGE_AUTH_MODE=login       (default: login)

SQL env:
  SQL_SERVER_FQDN=myserver.database.windows.net
  SQL_DATABASE=mydb
  SQL_USER=etl_user
  SQL_PASSWORD=...

Load env:
  SQL_SCHEMA=dbo                      (default)
  SQL_TABLE=stg_customers             (required for load)
  CSV_FIELD_TERMINATOR=,              (default)
  CSV_ROW_TERMINATOR=\\n              (default)
  CSV_FIRST_ROW=2                     (default)
  BCP_EXTRA_ARGS="..."                (optional)

EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: missing required command: ${cmd}" >&2
    return 2
  fi
}

print_context_blob() {
  echo "== blob context =="
  echo "AZURE_STORAGE_ACCOUNT   : ${AZURE_STORAGE_ACCOUNT}"
  echo "AZURE_STORAGE_CONTAINER : ${AZURE_STORAGE_CONTAINER}"
  if [[ -n "${AZURE_STORAGE_SAS_TOKEN}" ]]; then
    echo "AZURE_STORAGE_SAS_TOKEN : <set>"
  else
    echo "AZURE_STORAGE_AUTH_MODE : ${AZURE_STORAGE_AUTH_MODE}"
  fi
  echo
}

print_context_sql() {
  echo "== sql context =="
  echo "SQL_SERVER_FQDN : ${SQL_SERVER_FQDN}"
  echo "SQL_DATABASE    : ${SQL_DATABASE}"
  echo "SQL_USER        : ${SQL_USER:-<unset>}"
  if [[ -n "${SQL_PASSWORD}" ]]; then
    echo "SQL_PASSWORD    : <set>"
  else
    echo "SQL_PASSWORD    : <unset>"
  fi
  echo "SQL_SCHEMA      : ${SQL_SCHEMA}"
  echo "SQL_TABLE       : ${SQL_TABLE:-<unset>}"
  echo
}

ensure_blob_context() {
  if [[ -z "${AZURE_STORAGE_ACCOUNT}" ]]; then
    echo "ERROR: AZURE_STORAGE_ACCOUNT is required." >&2
    exit 2
  fi
  if [[ -z "${AZURE_STORAGE_CONTAINER}" ]]; then
    echo "ERROR: AZURE_STORAGE_CONTAINER is required." >&2
    exit 2
  fi
}

ensure_sql_context() {
  if [[ -z "${SQL_SERVER_FQDN}" ]]; then
    echo "ERROR: SQL_SERVER_FQDN is required." >&2
    exit 2
  fi
  if [[ -z "${SQL_DATABASE}" ]]; then
    echo "ERROR: SQL_DATABASE is required." >&2
    exit 2
  fi
}

az_blob_download() {
  local blob_name="$1"
  local local_file="$2"

  require_cmd az
  ensure_blob_context

  print_context_blob

  echo "== downloading blob =="
  echo "blob : ${blob_name}"
  echo "file : ${local_file}"
  echo

  if [[ -n "${AZURE_STORAGE_SAS_TOKEN}" ]]; then
    az storage blob download \
      --account-name "${AZURE_STORAGE_ACCOUNT}" \
      --container-name "${AZURE_STORAGE_CONTAINER}" \
      --name "${blob_name}" \
      --file "${local_file}" \
      --sas-token "${AZURE_STORAGE_SAS_TOKEN}" \
      --only-show-errors
  else
    az storage blob download \
      --account-name "${AZURE_STORAGE_ACCOUNT}" \
      --container-name "${AZURE_STORAGE_CONTAINER}" \
      --name "${blob_name}" \
      --file "${local_file}" \
      --auth-mode "${AZURE_STORAGE_AUTH_MODE}" \
      --only-show-errors
  fi
}

bcp_load_csv() {
  local local_file="$1"

  require_cmd bcp
  ensure_sql_context

  if [[ -z "${SQL_TABLE}" ]]; then
    echo "ERROR: SQL_TABLE is required for 'load'." >&2
    exit 2
  fi
  if [[ ! -f "${local_file}" ]]; then
    echo "ERROR: local file not found: ${local_file}" >&2
    exit 2
  fi

  print_context_sql

  echo "== load settings =="
  echo "LOCAL_FILE           : ${local_file}"
  echo "CSV_FIRST_ROW        : ${CSV_FIRST_ROW}"
  echo "CSV_FIELD_TERMINATOR : ${CSV_FIELD_TERMINATOR}"
  echo "CSV_ROW_TERMINATOR   : ${CSV_ROW_TERMINATOR}"
  echo "BCP_EXTRA_ARGS       : ${BCP_EXTRA_ARGS:-<none>}"
  echo

  # bcp table name should be schema.table (database is provided via -d)
  local table_ref="${SQL_SCHEMA}.${SQL_TABLE}"

  # Connection args:
  # - Use SQL auth when SQL_USER/SQL_PASSWORD are provided.
  # - Otherwise, rely on integrated auth if configured (rare on macOS). We error for clarity.
  if [[ -z "${SQL_USER}" || -z "${SQL_PASSWORD}" ]]; then
    echo "ERROR: SQL_USER and SQL_PASSWORD are required for bcp in this script." >&2
    echo "Set env vars SQL_USER and SQL_PASSWORD." >&2
    exit 2
  fi

  echo "== running bcp (database mutation) =="
  # -c = character type
  # -t/-r = field/row terminators
  # -F = first row
  bcp "${table_ref}" in "${local_file}" \
    -S "${SQL_SERVER_FQDN}" \
    -d "${SQL_DATABASE}" \
    -U "${SQL_USER}" \
    -P "${SQL_PASSWORD}" \
    -c \
    -t"${CSV_FIELD_TERMINATOR}" \
    -r"${CSV_ROW_TERMINATOR}" \
    -F "${CSV_FIRST_ROW}" \
    ${BCP_EXTRA_ARGS}

  echo
  echo "== bcp completed =="
}

sqlcmd_query() {
  local query="$1"

  require_cmd sqlcmd
  ensure_sql_context

  if [[ -z "${SQL_USER}" || -z "${SQL_PASSWORD}" ]]; then
    echo "ERROR: SQL_USER and SQL_PASSWORD are required for sqlcmd in this script." >&2
    echo "Set env vars SQL_USER and SQL_PASSWORD." >&2
    exit 2
  fi

  print_context_sql

  echo "== running query (read-only intent) =="
  # -b = exit on error, -r1 = send errors to stderr, -h-1 = no headers, -W = trim spaces
  sqlcmd -S "${SQL_SERVER_FQDN}" -d "${SQL_DATABASE}" -U "${SQL_USER}" -P "${SQL_PASSWORD}" -b -r1 -h-1 -W -Q "${query}"
}

cmd_download() {
  local blob_name="$1"
  local local_file="${2:-}"

  if [[ -z "${local_file}" ]]; then
    local base
    base="$(basename "${blob_name}")"
    local_file="/tmp/${base}"
  fi

  az_blob_download "${blob_name}" "${local_file}"
  echo
  echo "Downloaded to: ${local_file}"
}

cmd_load() {
  local local_file="$1"
  bcp_load_csv "${local_file}"
}

cmd_ingest() {
  local blob_name="$1"
  local local_file="${2:-}"

  if [[ -z "${local_file}" ]]; then
    local base
    base="$(basename "${blob_name}")"
    local_file="/tmp/${base}"
  fi

  az_blob_download "${blob_name}" "${local_file}"
  echo
  bcp_load_csv "${local_file}"
}

cmd_query() {
  local query="$1"
  sqlcmd_query "${query}"
}

main() {
  local cmd="${1:-}"
  shift || true

  case "${cmd}" in
    download)
      if [[ $# -lt 1 || $# -gt 2 ]]; then
        usage
        exit 2
      fi
      cmd_download "$@"
      ;;
    load)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_load "$1"
      ;;
    ingest)
      if [[ $# -lt 1 || $# -gt 2 ]]; then
        usage
        exit 2
      fi
      cmd_ingest "$@"
      ;;
    query)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_query "$1"
      ;;
    -h|--help|help|"")
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown command: ${cmd}" >&2
      usage
      exit 2
      ;;
  esac
}

main "$@"