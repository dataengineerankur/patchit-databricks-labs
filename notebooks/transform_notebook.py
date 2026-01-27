from __future__ import annotations

# Second Databricks notebook for transformation validation drills.

import json
from datetime import datetime


def transform(rows: list[dict]) -> dict:
    totals = {}
    for r in rows:
        key = r.get("status", "unknown")
        totals[key] = totals.get(key, 0) + 1
    return {"ts": datetime.utcnow().isoformat(), "totals": totals}


if __name__ == "__main__":
    sample = [{"status": "ok"}, {"status": "ok"}, {"status": "bad"}]
    print(json.dumps(transform(sample), indent=2))
