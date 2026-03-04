#!/usr/bin/env bash
set -euo pipefail

# Start Airflow locally using the Astronomer CLI (Astro Runtime).
#
# This repo includes an Astro project under: dbt_dag/
#
# Usage:
#   ./scripts/astro_local_start.sh
#
# Notes:
# - Requires `astro` CLI installed: https://docs.astronomer.io/astro/cli/install-cli
# - Airflow UI: http://localhost:8080 (default admin/admin)

ASTRO_DIR="dbt_dag"

if ! command -v astro >/dev/null 2>&1; then
  echo "ERROR: 'astro' CLI not found on PATH." >&2
  echo "Install: https://docs.astronomer.io/astro/cli/install-cli" >&2
  exit 2
fi

if [[ ! -d "${ASTRO_DIR}" ]]; then
  echo "ERROR: Astro project directory not found: ${ASTRO_DIR}" >&2
  exit 2
fi

echo "== Starting Astro Airflow locally =="
echo "Directory: ${ASTRO_DIR}"
echo

cd "${ASTRO_DIR}"
astro dev start