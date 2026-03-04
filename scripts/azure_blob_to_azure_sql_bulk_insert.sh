#!/usr/bin/env bash
set -euo pipefail

# Azure Blob Storage → Azure SQL Database ingestion helper (server-side BULK INSERT).
#
# This script supports a repeatable workflow:
#   Azure Blob (CSV) -> Azure SQL Database (BULK INSERT) using:
#     - CREATE DATABASE SCOPED CREDENTIAL (SAS token)
#     - CREATE EXTERNAL DATA SOURCE (Blob container URL)
#     - BULK INSERT into an existing target table
#
# Why server-side?
# - Avoids client-side bcp dependency for many cases.
# - Runs the actual load from within Azure SQL using an external data source.
#
# Guardrails:
# - No secrets in code. SAS token must be provided via environment variables.
# - setup/load/teardown mutate the database. Use intentionally.
# - Assumes the target table already exists and matches the CSV schema.
# - You must allow Azure SQL to reach the storage account (network rules/allowlist as required).
#
# Prereqs:
# - sqlcmd installed (Microsoft ODBC + mssql-tools).
#
# Usage:
#   ./scripts/azure_blob_to_azure_sql_bulk_insert.sh <command> [args...]
#
# Commands:
#   setup
#       Creates (or replaces) a DB scoped credential + external data source.
#
#   load <blob_path>
#       Runs BULK INSERT into target table from the provided blob path (relative to container).
#       Example blob_path: exports/customers.csv
#
#   ingest <blob_path>
#       Convenience: setup then load.
#
#   query <sql>
#       Runs a query via sqlcmd (use for verification like COUNT(*)).
#
#   teardown
#       Drops the external data source and credential created by this script.
#
# Examples:
#   # One-time setup + load
#   SQL_SERVER_FQDN=myserver.database.windows.net SQL_DATABASE=mydb \
#   SQL_USER=etl_user SQL_PASSWORD=... SQL_SCHEMA=dbo SQL_TABLE=stg_customers \
#   AZURE_BLOB_CONTAINER_URL="https://myacct.blob.core.windows.net/landing" \
#   AZURE_BLOB_SAS_TOKEN="sv=...&ss=...&srt=...&sp=rl&se=...&sig=..." \
#     ./scripts/azure_blob_to_azure_sql_bulk_insert.sh ingest exports/customers.csv
#
# Configuration via env vars:
#   SQL:
#     SQL_SERVER_FQDN              (required) e.g., myserver.database.windows.net
#     SQL_DATABASE                 (required)
#     SQL_USER                     (required) SQL auth username
#     SQL_PASSWORD                 (required) SQL auth password
#     SQL_SCHEMA                   (default: dbo)
#     SQL_TABLE                    (required for load) table name only (no schema)
#
#   Blob:
#     AZURE_BLOB_CONTAINER_URL     (required) e.g., https://<acct>.blob.core.windows.net/<container>
#     AZURE_BLOB_SAS_TOKEN         (required) SAS token (no leading '?')
#
#   Objects created in Azure SQL:
#     SQL_CREDENTIAL_NAME          (default: dc_blob_sas_cred)
#     SQL_EXTERNAL_DATA_SOURCE     (default: dc_blob_ext_ds)
#
#   CSV / BULK INSERT options:
#     CSV_FIRST_ROW                (default: 2)
#     CSV_FIELD_TERMINATOR         (default: ,)
#     CSV_ROW_TERMINATOR_HEX       (default: 0x0a)  # LF
#     CSV_CODEPAGE                 (default: 65001) # UTF-8
#     BULK_MAXERRORS               (default: 0)     # 0 means fail on first error (recommended)
#
#   sqlcmd options:
#     SQLCMD_EXTRA_ARGS            (optional) extra args appended to sqlcmd
#
# Notes:
# - Azure SQL Database BULK INSERT from blob requires an external data source + scoped credential.
# - Storage account firewall/private endpoints may block Azure SQL; ensure networking allows it.
# - SAS token should include at least read/list permissions for the container/path and be time-bounded.

SCRIPT_NAME="$(basename "$0")"

SQL_SERVER_FQDN="${SQL_SERVER_FQDN:-}"
SQL_DATABASE="${SQL_DATABASE:-}"
SQL_USER="${SQL_USER:-}"
SQL_PASSWORD="${SQL_PASSWORD:-}"

SQL_SCHEMA="${SQL_SCHEMA:-dbo}"
SQL_TABLE="${SQL_TABLE:-}"

AZURE_BLOB_CONTAINER_URL="${AZURE_BLOB_CONTAINER_URL:-}"
AZURE_BLOB_SAS_TOKEN="${AZURE_BLOB_SAS_TOKEN:-}"

SQL_CREDENTIAL_NAME="${SQL_CREDENTIAL_NAME:-dc_blob_sas_cred}"
SQL_EXTERNAL_DATA_SOURCE="${SQL_EXTERNAL_DATA_SOURCE:-dc_blob_ext_ds}"

CSV_FIRST_ROW="${CSV_FIRST_ROW:-2}"
CSV_FIELD_TERMINATOR="${CSV_FIELD_TERMINATOR:-,}"
CSV_ROW_TERMINATOR_HEX="${CSV_ROW_TERMINATOR_HEX:-0x0a}"
CSV_CODEPAGE="${CSV_CODEPAGE:-65001}"
BULK_MAXERRORS="${BULK_MAXERRORS:-0}"

SQLCMD_EXTRA_ARGS="${SQLCMD_EXTRA_ARGS:-}"

usage() {
  cat >&2 <<EOF
${SCRIPT_NAME} - Azure Blob -> Azure SQL Database (server-side BULK INSERT)

Usage:
  ${SCRIPT_NAME} setup
  ${SCRIPT_NAME} load <blob_path>
  ${SCRIPT_NAME} ingest <blob_path>
  ${SCRIPT_NAME} query <sql>
  ${SCRIPT_NAME} teardown

Required env:
  SQL_SERVER_FQDN=...
  SQL_DATABASE=...
  SQL_USER=...
  SQL_PASSWORD=...

  AZURE_BLOB_CONTAINER_URL=https://<acct>.blob.core.windows.net/<container>
  AZURE_BLOB_SAS_TOKEN=...   (no leading '?')

Load env:
  SQL_SCHEMA=dbo             (default)
  SQL_TABLE=...              (required)

Optional env:
  SQL_CREDENTIAL_NAME=dc_blob_sas_cred
  SQL_EXTERNAL_DATA_SOURCE=dc_blob_ext_ds

  CSV_FIRST_ROW=2
  CSV_FIELD_TERMINATOR=,
  CSV_ROW_TERMINATOR_HEX=0x0a
  CSV_CODEPAGE=65001
  BULK_MAXERRORS=0

  SQLCMD_EXTRA_ARGS="..."

EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: missing required command: ${cmd}" >&2
    return 2
  fi
}

print_context() {
  echo "== context =="
  echo "SQL_SERVER_FQDN          : ${SQL_SERVER_FQDN}"
  echo "SQL_DATABASE             : ${SQL_DATABASE}"
  echo "SQL_USER                 : ${SQL_USER:-<unset>}"
  if [[ -n "${SQL_PASSWORD}" ]]; then
    echo "SQL_PASSWORD             : <set>"
  else
    echo "SQL_PASSWORD             : <unset>"
  fi
  echo "SQL_SCHEMA               : ${SQL_SCHEMA}"
  echo "SQL_TABLE                : ${SQL_TABLE:-<unset>}"
  echo
  echo "AZURE_BLOB_CONTAINER_URL  : ${AZURE_BLOB_CONTAINER_URL}"
  if [[ -n "${AZURE_BLOB_SAS_TOKEN}" ]]; then
    echo "AZURE_BLOB_SAS_TOKEN      : <set>"
  else
    echo "AZURE_BLOB_SAS_TOKEN      : <unset>"
  fi
  echo
  echo "SQL_CREDENTIAL_NAME       : ${SQL_CREDENTIAL_NAME}"
  echo "SQL_EXTERNAL_DATA_SOURCE  : ${SQL_EXTERNAL_DATA_SOURCE}"
  echo
  echo "CSV_FIRST_ROW             : ${CSV_FIRST_ROW}"
  echo "CSV_FIELD_TERMINATOR      : ${CSV_FIELD_TERMINATOR}"
  echo "CSV_ROW_TERMINATOR_HEX    : ${CSV_ROW_TERMINATOR_HEX}"
  echo "CSV_CODEPAGE              : ${CSV_CODEPAGE}"
  echo "BULK_MAXERRORS            : ${BULK_MAXERRORS}"
  echo "SQLCMD_EXTRA_ARGS         : ${SQLCMD_EXTRA_ARGS:-<none>}"
  echo
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
  if [[ -z "${SQL_USER}" || -z "${SQL_PASSWORD}" ]]; then
    echo "ERROR: SQL_USER and SQL_PASSWORD are required for this script." >&2
    exit 2
  fi
}

ensure_blob_context() {
  if [[ -z "${AZURE_BLOB_CONTAINER_URL}" ]]; then
    echo "ERROR: AZURE_BLOB_CONTAINER_URL is required." >&2
    exit 2
  fi
  if [[ -z "${AZURE_BLOB_SAS_TOKEN}" ]]; then
    echo "ERROR: AZURE_BLOB_SAS_TOKEN is required (no leading '?')." >&2
    exit 2
  fi
}

sqlcmd_exec() {
  local query="$1"

  require_cmd sqlcmd
  ensure_sql_context

  # -b exit on error, -r1 errors to stderr, -W trim, -l login timeout seconds (keep default)
  sqlcmd \
    -S "${SQL_SERVER_FQDN}" \
    -d "${SQL_DATABASE}" \
    -U "${SQL_USER}" \
    -P "${SQL_PASSWORD}" \
    -b -r1 -W \
    ${SQLCMD_EXTRA_ARGS} \
    -Q "${query}"
}

escape_sql_string_literal() {
  # Escape a string for use inside single quotes in T-SQL (doubling single quotes).
  # Prints escaped value to stdout.
  local s="$1"
  printf "%s" "${s//\'/\'\'}"
}

cmd_setup() {
  ensure_blob_context
  ensure_sql_context
  print_context

  echo "== setup (database mutation) =="
  echo "Creating credential + external data source (drop if exists, then create)."
  echo

  local safe_url safe_sas
  safe_url="$(escape_sql_string_literal "${AZURE_BLOB_CONTAINER_URL}")"
  safe_sas="$(escape_sql_string_literal "${AZURE_BLOB_SAS_TOKEN}")"

  # Use IF EXISTS guards for idempotency.
  # NOTE: Names are user-controlled env vars; keep them simple (no brackets injection).
  sqlcmd_exec "
IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = '${SQL_EXTERNAL_DATA_SOURCE}')
BEGIN
  DROP EXTERNAL DATA SOURCE [${SQL_EXTERNAL_DATA_SOURCE}];
END;

IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = '${SQL_CREDENTIAL_NAME}')
BEGIN
  DROP DATABASE SCOPED CREDENTIAL [${SQL_CREDENTIAL_NAME}];
END;

CREATE DATABASE SCOPED CREDENTIAL [${SQL_CREDENTIAL_NAME}]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '${safe_sas}';

CREATE EXTERNAL DATA SOURCE [${SQL_EXTERNAL_DATA_SOURCE}]
WITH (
  TYPE = BLOB_STORAGE,
  LOCATION = '${safe_url}',
  CREDENTIAL = [${SQL_CREDENTIAL_NAME}]
);
"
  echo
  echo "== setup complete =="
}

cmd_load() {
  local blob_path="$1"

  ensure_blob_context
  ensure_sql_context

  if [[ -z "${SQL_TABLE}" ]]; then
    echo "ERROR: SQL_TABLE is required for 'load'." >&2
    exit 2
  fi

  # Basic validation for blob path (relative within container)
  if [[ "${blob_path}" == http*://* ]]; then
    echo "ERROR: Provide blob_path relative to the container (e.g., exports/file.csv), not a full URL." >&2
    exit 2
  fi

  print_context

  echo "== load (database mutation) =="
  echo "BULK INSERT will load into: ${SQL_SCHEMA}.${SQL_TABLE}"
  echo "From DATA_SOURCE='${SQL_EXTERNAL_DATA_SOURCE}', path='${blob_path}'"
  echo

  local safe_path
  safe_path="$(escape_sql_string_literal "${blob_path}")"

  # BULK INSERT options vary slightly by engine; keep to common options.
  # ROWTERMINATOR as hex for LF: 0x0a (works broadly for Unix newlines).
  # FIRSTROW=2 assumes header row.
  sqlcmd_exec "
BULK INSERT [${SQL_SCHEMA}].[${SQL_TABLE}]
FROM '${safe_path}'
WITH (
  DATA_SOURCE = '${SQL_EXTERNAL_DATA_SOURCE}',
  FORMAT = 'CSV',
  FIRSTROW = ${CSV_FIRST_ROW},
  FIELDTERMINATOR = '${CSV_FIELD_TERMINATOR}',
  ROWTERMINATOR = '${CSV_ROW_TERMINATOR_HEX}',
  CODEPAGE = '${CSV_CODEPAGE}',
  MAXERRORS = ${BULK_MAXERRORS}
);
"
  echo
  echo "== load complete =="
}

cmd_ingest() {
  local blob_path="$1"
  cmd_setup
  echo
  cmd_load "${blob_path}"
}

cmd_query() {
  local query="$1"
  ensure_sql_context
  print_context

  echo "== query =="
  # If you need headerless output, pass e.g.: SQLCMD_EXTRA_ARGS="-h -1"
  sqlcmd_exec "${query}"
}

cmd_teardown() {
  ensure_sql_context
  print_context

  echo "== teardown (database mutation / destructive) =="
  echo "Dropping external data source and scoped credential created by this script."
  echo

  sqlcmd_exec "
IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = '${SQL_EXTERNAL_DATA_SOURCE}')
BEGIN
  DROP EXTERNAL DATA SOURCE [${SQL_EXTERNAL_DATA_SOURCE}];
END;

IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = '${SQL_CREDENTIAL_NAME}')
BEGIN
  DROP DATABASE SCOPED CREDENTIAL [${SQL_CREDENTIAL_NAME}];
END;
"
  echo
  echo "== teardown complete =="
}

main() {
  local cmd="${1:-}"
  shift || true

  case "${cmd}" in
    setup)
      if [[ $# -ne 0 ]]; then
        usage
        exit 2
      fi
      cmd_setup
      ;;
    load)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_load "$1"
      ;;
    ingest)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_ingest "$1"
      ;;
    query)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_query "$1"
      ;;
    teardown)
      if [[ $# -ne 0 ]]; then
        usage
        exit 2
      fi
      cmd_teardown
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