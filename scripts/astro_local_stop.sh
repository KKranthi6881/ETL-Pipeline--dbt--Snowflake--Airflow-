#!/usr/bin/env bash
set -euo pipefail

# Stop Airflow locally for the Astro project in this repo.
#
# Usage:
#   ./scripts/astro_local_stop.sh

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

echo "== Stopping Astro Airflow locally =="
echo "Directory: ${ASTRO_DIR}"
echo

cd "${ASTRO_DIR}"
astro dev stop