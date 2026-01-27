## Patchit Databricks Labs

Production-like Databricks Jobs pipeline with controlled failure drills for PATCHIT.

### Variables (set at runtime)

Use these placeholders in scripts/runbooks:
- `WORKSPACE_ROOT="<local folder where repos will live>"`
- `GITHUB_ORIGIN="<optional: remote github org/repo base>"`
- `PATCHIT_CMD="<command to invoke Patchit locally>"`
- `OUTPUT_DIR="<local folder for evidence packs and reports>"`
- `DATABRICKS_HOST="<workspace url>"`
- `DATABRICKS_TOKEN="<token>"`
- `DATABRICKS_WORKSPACE_ID="<optional>"`
- `DBX_CATALOG="<optional for UC>"`
- `DBX_SCHEMA="<optional>"`
- `DBX_CLUSTER_POLICY_ID="<optional>"`
- `DBX_REGION="<cloud region>"`

---

### Architecture

Notebook-based job:
1. Extracts records from Postgres (local docker or cloud if configured).
2. Writes Bronze Delta table.
3. Applies validation + quarantine, writes Silver.
4. Aggregates Gold.
5. Emits a small metrics file for verification.

Terraform deploys:
- notebook imports (ingest, quality, dedup)
- multiple jobs (ingest, quality, dedup)
- schedule + retry policy (optional)

---

### Setup

1) Create a local `.env` (do not commit) with your placeholders:

```bash
cat > .env <<'EOF'
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_WORKSPACE_ID=
DBX_CATALOG=
DBX_SCHEMA=
DBX_CLUSTER_POLICY_ID=
DBX_REGION=
PATCHIT_CMD=
OUTPUT_DIR=
EOF
```

2) Local-only validation (optional, cheap):

```bash
python notebooks/local_runner.py --input data/sample_input.json --output out/
```

---

### Terraform deploy (do NOT apply without credentials)

```bash
cd infra/terraform
terraform init
terraform plan \
  -var "databricks_host=${DATABRICKS_HOST}" \
  -var "databricks_token=${DATABRICKS_TOKEN}" \
  -var "dbx_region=${DBX_REGION}"
```

Apply (only after explicit confirmation + creds):

```bash
terraform apply \
  -var "databricks_host=${DATABRICKS_HOST}" \
  -var "databricks_token=${DATABRICKS_TOKEN}" \
  -var "dbx_region=${DBX_REGION}"
```

Destroy:

```bash
terraform destroy \
  -var "databricks_host=${DATABRICKS_HOST}" \
  -var "databricks_token=${DATABRICKS_TOKEN}" \
  -var "dbx_region=${DBX_REGION}"
```

---

### Failure drills

Drills are defined in `drills/drills.yaml`. Start with two (enabled) drills, then expand to all six.

Run a single drill:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_drill.sh DBX1_schema_drift
```

Run all enabled drills:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_all_drills.sh
```

Expected outputs per drill:
- Evidence pack JSON in `evidence/<scenario_id>/evidence_pack.json`
- Logs and artifacts under `evidence/<scenario_id>/logs/`
- PATCHIT report under `evidence/<scenario_id>/report.md`

---

### How PATCHIT is invoked

The runner calls:

```bash
${PATCHIT_CMD} \
  --repo "$(pwd)" \
  --platform databricks \
  --logs "$LOG_PATH" \
  --mode pr_only \
  --evidence_out "$EVIDENCE_PATH"
```

---

### Cost controls / cleanup

- Use smallest possible cluster in `infra/terraform/variables.tf`.
- Keep schedules disabled in drills.
- Always run `terraform destroy` after validation.

---

### How to onboard a real company later

- Replace notebook with a versioned Python wheel.
- Store secrets in the company secret manager (not in `.env`).
- Wire job failure events to PATCHIT via webhook → enrichment → PATCHIT ingest.
- Require PR-based change control only (no auto-apply).

---

### How to run and test (end-to-end)

1) Local smoke test:

```bash
python notebooks/local_runner.py --input data/sample_input.json --output out/
```

2) Run a drill (no cloud needed):

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_drill.sh DBX1_schema_drift
```

3) Review outputs:
- Evidence: `evidence/DBX1_schema_drift/evidence_pack.json`
- Logs: `evidence/DBX1_schema_drift/logs/`
- PATCHIT output: depends on `PATCHIT_CMD` (PR/diff/report)

4) Run all enabled drills:

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_all_drills.sh
```
