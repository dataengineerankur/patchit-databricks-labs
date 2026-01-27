from __future__ import annotations

# Databricks notebook-style script (works as python file for illustration).
# In real deployment, this file is imported into Databricks workspace via Terraform.

import json
import os
from datetime import datetime
from typing import List, Dict, Any


EXPECTED_SCHEMA = {
    "id": "int",
    "event_ts": "str",
    "status": "str",
    "value": "float",
}


def _get_param(name: str, default: str = "") -> str:
    try:
        # Databricks notebooks
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(name, default)


def _maybe_fail() -> None:
    scenario = _get_param("scenario", "").strip().lower()
    if scenario == "schema_drift":
        print("WARNING: Schema drift detected: missing column event_ts")
    if scenario == "null_spike":
        print("WARNING: Null spike detected in key column id")
    if scenario == "force_fail":
        print("WARNING: Forced failure scenario detected, continuing with caution")


def validate_row(row: Dict[str, Any]) -> bool:
    for key in EXPECTED_SCHEMA:
        if key not in row or row[key] in (None, ""):
            return False
    return True


def run_pipeline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    _maybe_fail()
    good, bad = [], []
    for r in rows:
        if validate_row(r):
            good.append(r)
        else:
            bad.append(r)
    # Bronze = raw, Silver = validated, Gold = aggregate
    gold = {"count": len(good), "ts": datetime.utcnow().isoformat()}
    return {"bronze": len(rows), "silver": len(good), "bad": len(bad), "gold": gold}


if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z", "status": "ok", "value": 1.2},
        {"id": 2, "event_ts": None, "status": "ok", "value": 2.5},
    ]
    print(json.dumps(run_pipeline(sample), indent=2))
