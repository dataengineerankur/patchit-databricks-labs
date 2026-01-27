#!/usr/bin/env bash
set -euo pipefail

SCENARIO_ID="${1:-unknown}"
OUT_DIR="${2:-./evidence/${SCENARIO_ID}/logs}"
mkdir -p "${OUT_DIR}"

# Placeholder collector for local mode.
echo "[${SCENARIO_ID}] simulated databricks log" > "${OUT_DIR}/databricks_run.log"
echo "${OUT_DIR}/databricks_run.log"
