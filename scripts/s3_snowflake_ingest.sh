#!/usr/bin/env bash
set -euo pipefail

# S3 → Snowflake ingestion helper for this repo (Snowpipe / COPY INTO).
#
# Designed to work with the Terraform stack under:
#   infra/platform/aws/s3-snowflake-copy
#
# Primary use-cases:
#   1) Smoke test: upload a file to S3 and monitor Snowpipe status until processed
#   2) Observe: check pipe status and target table counts
#   3) Backfill (optional): run an explicit COPY INTO from the external stage
#
# Guardrails:
# - No secrets are embedded in this script.
# - Snowflake access is done via your local SnowSQL (snowsql) or SnowCLI (snow) config/env.
# - The "backfill" subcommand executes COPY INTO (mutates warehouse); use intentionally.
#
# Usage:
#   ./scripts/s3_snowflake_ingest.sh <command> [args...]
#
# Commands:
#   smoke   <local_file>
#           Uploads a file to S3 and waits for Snowpipe to process it (polls pipe status).
#
#   status
#           Prints Snowpipe status JSON for the configured pipe.
#
#   count
#           Prints COUNT(*) for the configured target table.
#
#   backfill
#           Runs COPY INTO <target_table> FROM @<stage> (optional pattern + file_format clause).
#
# Examples:
#   # Smoke test upload + wait (uses terraform outputs if stack has been applied)
#   ./scripts/s3_snowflake_ingest.sh smoke ./data/sample.csv
#
#   # Just check status/count
#   ./scripts/s3_snowflake_ingest.sh status
#   ./scripts/s3_snowflake_ingest.sh count
#
#   # Backfill (explicit COPY INTO)
#   ./scripts/s3_snowflake_ingest.sh backfill
#
# Configuration precedence (highest → lowest):
#   1) Environment variables (see below)
#   2) Terraform outputs from STACK_DIR (if state exists)
#   3) Hardcoded defaults for object names (pipe/stage/integration)
#
# Environment variables (recommended for CI or non-Terraform usage):
#   STACK_DIR                    (default: infra/platform/aws/s3-snowflake-copy)
#   S3_BUCKET_NAME               (e.g., my-landing-bucket)
#   S3_KEY_PREFIX                (e.g., landing/  - optional; may be empty)
#
#   SNOWFLAKE_PIPE_FQN           (e.g., MYDB.MYSCHEMA.S3_INGEST_PIPE)
#   SNOWFLAKE_STAGE_FQN          (e.g., MYDB.MYSCHEMA.S3_INGEST_STAGE)
#   SNOWFLAKE_TARGET_TABLE       (e.g., MYDB.MYSCHEMA.MYTABLE)
#
#   COPY_FILE_FORMAT_CLAUSE      (default: TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1)
#   COPY_PATTERN_REGEX           (default: empty)
#
#   POLL_INTERVAL_SECONDS        (default: 10)
#   TIMEOUT_SECONDS              (default: 300)
#
# Snowflake CLI selection:
#   - If `snow` exists, uses: snow sql -q "<query>"
#   - Else if `snowsql` exists, uses: snowsql -q "<query>" -o friendly=false -o header=false
#
# Notes:
# - Snowpipe is event-driven; for smoke tests, upload a file that matches your pipe pattern/file format.
# - SYSTEM$PIPE_STATUS returns JSON; this script attempts to parse pendingFileCount when possible.

SCRIPT_NAME="$(basename "$0")"

STACK_DIR="${STACK_DIR:-infra/platform/aws/s3-snowflake-copy}"

S3_BUCKET_NAME="${S3_BUCKET_NAME:-}"
S3_KEY_PREFIX="${S3_KEY_PREFIX:-}"

SNOWFLAKE_PIPE_FQN="${SNOWFLAKE_PIPE_FQN:-}"
SNOWFLAKE_STAGE_FQN="${SNOWFLAKE_STAGE_FQN:-}"
SNOWFLAKE_TARGET_TABLE="${SNOWFLAKE_TARGET_TABLE:-}"

COPY_FILE_FORMAT_CLAUSE="${COPY_FILE_FORMAT_CLAUSE:-TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1}"
COPY_PATTERN_REGEX="${COPY_PATTERN_REGEX:-}"

POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-10}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"

usage() {
  cat >&2 <<EOF
${SCRIPT_NAME} - S3 → Snowflake ingestion helper (Snowpipe/COPY)

Usage:
  ${SCRIPT_NAME} smoke <local_file>
  ${SCRIPT_NAME} status
  ${SCRIPT_NAME} count
  ${SCRIPT_NAME} backfill

Optional env vars:
  STACK_DIR=infra/platform/aws/s3-snowflake-copy
  S3_BUCKET_NAME=...
  S3_KEY_PREFIX=...

  SNOWFLAKE_PIPE_FQN=...
  SNOWFLAKE_STAGE_FQN=...
  SNOWFLAKE_TARGET_TABLE=...

  COPY_FILE_FORMAT_CLAUSE="TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1"
  COPY_PATTERN_REGEX=".*[.]csv"

  POLL_INTERVAL_SECONDS=10
  TIMEOUT_SECONDS=300

EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: missing required command: ${cmd}" >&2
    return 2
  fi
}

tf_output_raw() {
  # Best-effort Terraform output fetch; prints empty string if unavailable.
  local name="$1"
  if command -v terraform >/dev/null 2>&1 && [[ -d "${STACK_DIR}" ]]; then
    terraform -chdir="${STACK_DIR}" output -raw "${name}" 2>/dev/null || true
  else
    true
  fi
}

resolve_defaults_from_terraform() {
  # Resolve values from terraform outputs if env vars weren't set.
  if [[ -z "${S3_BUCKET_NAME}" ]]; then
    S3_BUCKET_NAME="$(tf_output_raw s3_bucket_name)"
  fi
  if [[ -z "${SNOWFLAKE_STAGE_FQN}" ]]; then
    SNOWFLAKE_STAGE_FQN="$(tf_output_raw snowflake_stage_fqn)"
  fi
  if [[ -z "${SNOWFLAKE_PIPE_FQN}" ]]; then
    SNOWFLAKE_PIPE_FQN="$(tf_output_raw snowflake_pipe_fqn)"
  fi

  # Terraform stack does not output target_table (input var), so only env can set it.
}

sf_cli() {
  # Execute a Snowflake query via available CLI.
  # Output should be single-line-friendly for parsing where possible.
  local query="$1"

  if command -v snow >/dev/null 2>&1; then
    # SnowCLI prints headers by default; use --format json when possible? Keep simple text.
    snow sql -q "${query}"
    return $?
  fi

  if command -v snowsql >/dev/null 2>&1; then
    snowsql -q "${query}" -o friendly=false -o header=false -o timing=false
    return $?
  fi

  echo "ERROR: Need Snowflake CLI. Install/configure either:" >&2
  echo "  - snow (SnowCLI), or" >&2
  echo "  - snowsql (SnowSQL)" >&2
  return 2
}

pipe_status_json() {
  local pipe_fqn="$1"
  # SYSTEM$PIPE_STATUS expects a string. Use single quotes and escape any single quote chars.
  local safe_pipe="${pipe_fqn//\'/\'\'}"
  sf_cli "SELECT SYSTEM\$PIPE_STATUS('${safe_pipe}');"
}

parse_pending_file_count() {
  # Reads a JSON string on stdin; prints pendingFileCount or empty.
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import json, sys
s = sys.stdin.read().strip()
try:
  j = json.loads(s)
  v = j.get("pendingFileCount")
  if v is None:
    v = j.get("pendingFiles")  # fallback (older variants)
  if v is None:
    sys.exit(0)
  print(v)
except Exception:
  sys.exit(0)
PY
  else
    cat >/dev/null
  fi
}

ensure_required_context() {
  resolve_defaults_from_terraform

  if [[ -z "${SNOWFLAKE_PIPE_FQN}" ]]; then
    echo "ERROR: SNOWFLAKE_PIPE_FQN is not set and could not be resolved from Terraform outputs." >&2
    echo "Set env var SNOWFLAKE_PIPE_FQN or run after terraform apply in ${STACK_DIR}." >&2
    exit 2
  fi

  if [[ -z "${SNOWFLAKE_STAGE_FQN}" ]]; then
    echo "ERROR: SNOWFLAKE_STAGE_FQN is not set and could not be resolved from Terraform outputs." >&2
    echo "Set env var SNOWFLAKE_STAGE_FQN or run after terraform apply in ${STACK_DIR}." >&2
    exit 2
  fi

  if [[ -z "${S3_BUCKET_NAME}" ]]; then
    echo "ERROR: S3_BUCKET_NAME is not set and could not be resolved from Terraform outputs." >&2
    echo "Set env var S3_BUCKET_NAME or run after terraform apply in ${STACK_DIR}." >&2
    exit 2
  fi

  if [[ -z "${SNOWFLAKE_TARGET_TABLE}" ]]; then
    echo "ERROR: SNOWFLAKE_TARGET_TABLE is required for 'count' and 'backfill'." >&2
    echo "Set env var SNOWFLAKE_TARGET_TABLE='MYDB.MYSCHEMA.MYTABLE'." >&2
    # Don't exit here; smoke/status can still work without target table.
  fi
}

print_context() {
  echo "== context =="
  echo "STACK_DIR              : ${STACK_DIR}"
  echo "S3_BUCKET_NAME         : ${S3_BUCKET_NAME}"
  echo "S3_KEY_PREFIX          : ${S3_KEY_PREFIX:-<none>}"
  echo "SNOWFLAKE_STAGE_FQN    : ${SNOWFLAKE_STAGE_FQN}"
  echo "SNOWFLAKE_PIPE_FQN     : ${SNOWFLAKE_PIPE_FQN}"
  echo "SNOWFLAKE_TARGET_TABLE : ${SNOWFLAKE_TARGET_TABLE:-<unset>}"
  echo "COPY_FILE_FORMAT_CLAUSE: ${COPY_FILE_FORMAT_CLAUSE}"
  echo "COPY_PATTERN_REGEX     : ${COPY_PATTERN_REGEX:-<none>}"
  echo "POLL_INTERVAL_SECONDS  : ${POLL_INTERVAL_SECONDS}"
  echo "TIMEOUT_SECONDS        : ${TIMEOUT_SECONDS}"
  echo
}

cmd_smoke() {
  local local_file="$1"

  require_cmd aws

  if [[ ! -f "${local_file}" ]]; then
    echo "ERROR: local file not found: ${local_file}" >&2
    exit 2
  fi

  ensure_required_context
  print_context

  local base
  base="$(basename "${local_file}")"
  local ts
  ts="$(date +"%Y%m%d_%H%M%S")"

  local prefix="${S3_KEY_PREFIX}"
  # normalize prefix: allow empty, ensure ends with /
  if [[ -n "${prefix}" && "${prefix}" != */ ]]; then
    prefix="${prefix}/"
  fi

  local s3_key="${prefix}smoketest/${ts}_${base}"
  local s3_uri="s3://${S3_BUCKET_NAME}/${s3_key}"

  echo "== uploading to S3 =="
  echo "Local file: ${local_file}"
  echo "S3 URI    : ${s3_uri}"
  aws s3 cp "${local_file}" "${s3_uri}"
  echo

  echo "== checking Snowpipe status until pendingFileCount=0 (or timeout) =="
  local start_epoch
  start_epoch="$(date +%s)"

  while true; do
    local now_epoch elapsed status pending
    now_epoch="$(date +%s)"
    elapsed="$((now_epoch - start_epoch))"

    status="$(pipe_status_json "${SNOWFLAKE_PIPE_FQN}" | tr -d '\r' | tail -n 1)"
    pending="$(printf "%s" "${status}" | parse_pending_file_count || true)"

    echo "-- ${elapsed}s --"
    echo "${status}"
    if [[ -n "${pending}" ]]; then
      echo "pendingFileCount=${pending}"
      if [[ "${pending}" == "0" ]]; then
        echo "Snowpipe pendingFileCount=0."
        break
      fi
    else
      echo "pendingFileCount=<unparsed> (raw JSON printed above)"
    fi

    if (( elapsed >= TIMEOUT_SECONDS )); then
      echo "WARN: timed out after ${TIMEOUT_SECONDS}s waiting for Snowpipe to drain." >&2
      break
    fi

    sleep "${POLL_INTERVAL_SECONDS}"
  done
  echo

  if [[ -n "${SNOWFLAKE_TARGET_TABLE}" ]]; then
    echo "== target table count (informational) =="
    sf_cli "SELECT COUNT(*) AS row_count FROM ${SNOWFLAKE_TARGET_TABLE};"
    echo
  else
    echo "NOTE: SNOWFLAKE_TARGET_TABLE not set; skipping COUNT(*)." >&2
  fi
}

cmd_status() {
  ensure_required_context
  print_context
  echo "== SYSTEM\$PIPE_STATUS =="
  pipe_status_json "${SNOWFLAKE_PIPE_FQN}"
}

cmd_count() {
  ensure_required_context
  print_context

  if [[ -z "${SNOWFLAKE_TARGET_TABLE}" ]]; then
    echo "ERROR: SNOWFLAKE_TARGET_TABLE must be set for 'count'." >&2
    exit 2
  fi

  echo "== target table count =="
  sf_cli "SELECT COUNT(*) AS row_count FROM ${SNOWFLAKE_TARGET_TABLE};"
}

cmd_backfill() {
  ensure_required_context
  print_context

  if [[ -z "${SNOWFLAKE_TARGET_TABLE}" ]]; then
    echo "ERROR: SNOWFLAKE_TARGET_TABLE must be set for 'backfill'." >&2
    exit 2
  fi

  echo "== backfill: COPY INTO (warehouse mutation) =="
  echo "This will execute COPY INTO and may load data into the target table." >&2
  echo

  local from_stage="@${SNOWFLAKE_STAGE_FQN}"

  local pattern_clause=""
  if [[ -n "${COPY_PATTERN_REGEX}" ]]; then
    # Escape single quotes inside regex for SQL string
    local safe_pat="${COPY_PATTERN_REGEX//\'/\'\'}"
    pattern_clause="PATTERN='${safe_pat}'"
  fi

  # Note: FILE_FORMAT clause is injected directly (matches Terraform stack behavior).
  # COPY_FILE_FORMAT_CLAUSE should be like: TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1
  sf_cli "COPY INTO ${SNOWFLAKE_TARGET_TABLE} FROM ${from_stage} FILE_FORMAT=(${COPY_FILE_FORMAT_CLAUSE}) ${pattern_clause};"

  echo
  echo "== post-backfill count (informational) =="
  sf_cli "SELECT COUNT(*) AS row_count FROM ${SNOWFLAKE_TARGET_TABLE};"
}

main() {
  local cmd="${1:-}"
  shift || true

  case "${cmd}" in
    smoke)
      if [[ $# -ne 1 ]]; then
        usage
        exit 2
      fi
      cmd_smoke "$1"
      ;;
    status)
      if [[ $# -ne 0 ]]; then
        usage
        exit 2
      fi
      cmd_status
      ;;
    count)
      if [[ $# -ne 0 ]]; then
        usage
        exit 2
      fi
      cmd_count
      ;;
    backfill)
      if [[ $# -ne 0 ]]; then
        usage
        exit 2
      fi
      cmd_backfill
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