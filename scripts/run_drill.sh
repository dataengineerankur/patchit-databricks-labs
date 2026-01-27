#!/usr/bin/env bash
set -euo pipefail

SCENARIO_ID="${1:-}"
if [[ -z "${SCENARIO_ID}" ]]; then
  echo "Usage: ./scripts/run_drill.sh <scenario_id>"
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/evidence}"
PATCHIT_CMD="${PATCHIT_CMD:-}"

SCENARIO_DIR="${OUTPUT_DIR}/${SCENARIO_ID}"
LOG_DIR="${SCENARIO_DIR}/logs"
mkdir -p "${LOG_DIR}"

LOG_PATH="${LOG_DIR}/run.log"
echo "[${SCENARIO_ID}] Simulated Databricks run log" > "${LOG_PATH}"

NORMALIZED_PATH="${SCENARIO_DIR}/normalized.json"
python - <<PY
from adapter.normalize import normalize_log, write_normalized

payload = normalize_log(
    job_id="dbx_job_demo",
    run_id="run_${SCENARIO_ID}",
    error_signature="${SCENARIO_ID}",
    log_excerpt="Simulated failure for ${SCENARIO_ID}",
    artifacts=["${LOG_PATH}"],
    code_paths=["notebooks/ingest_notebook.py"],
    config={"scenario_id": "${SCENARIO_ID}"},
)
write_normalized("${NORMALIZED_PATH}", payload)
PY

EVIDENCE_PATH="${SCENARIO_DIR}/evidence_pack.json"
python - <<PY
import json
from pathlib import Path

repo_root = Path("${REPO_ROOT}")
evidence = {
    "platform": "databricks",
    "repo_path": str(repo_root),
    "git_sha": "local",
    "job_id": "dbx_job_demo",
    "run_id": f"run_{'${SCENARIO_ID}'}",
    "timestamp": "local",
    "failure_signature": "${SCENARIO_ID}",
    "log_excerpt": "Simulated failure for ${SCENARIO_ID}",
    "suspected_root_cause": "Placeholder RCA for drill",
    "impacted_assets": ["bronze_table", "silver_table", "gold_table"],
    "proposed_fix_summary": "Placeholder fix summary",
    "diff_preview": "",
    "confidence": 0.5,
    "stop_condition_evaluation": [],
    "rollback_plan": "Revert PR; rerun job",
    "verification": {
        "commands": ["python notebooks/local_runner.py --input data/sample_input.json --output out/"],
        "results": "pending"
    }
}
Path("${EVIDENCE_PATH}").write_text(json.dumps(evidence, indent=2))
PY

if [[ -n "${PATCHIT_CMD}" ]]; then
  "${PATCHIT_CMD}" \
    --repo "${REPO_ROOT}" \
    --platform databricks \
    --logs "${LOG_PATH}" \
    --mode pr_only \
    --evidence_out "${EVIDENCE_PATH}" || true
else
  echo "PATCHIT_CMD not set; skipping PATCHIT invocation."
fi

echo "Drill complete: ${SCENARIO_DIR}"
