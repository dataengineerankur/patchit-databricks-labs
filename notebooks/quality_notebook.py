from __future__ import annotations

# Quality check notebook stub (Databricks).

import os
from datetime import datetime
from typing import Dict, Any, List


def _get_param(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(name, default)


def _maybe_fail() -> None:
    scenario = _get_param("scenario", "").strip().lower()
    if scenario == "null_spike":
        raise ValueError("Null spike detected in key column id")
    if scenario == "force_fail":
        raise RuntimeError("Forced failure for drill validation")


def quality_checks(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    _maybe_fail()
    total = len(rows)
    null_keys = sum(1 for r in rows if r.get("id") is None)
    null_ratio = null_keys / total if total else 0
    return {
        "total_rows": total,
        "null_key_rows": null_keys,
        "null_ratio": round(null_ratio, 4),
        "ts": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z", "status": "ok"},
        {"id": None, "event_ts": "2025-01-01T01:00:00Z", "status": "ok"},
    ]
    print(quality_checks(sample))
