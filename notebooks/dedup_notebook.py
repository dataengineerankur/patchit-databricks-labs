from __future__ import annotations

# Dedup notebook stub (Databricks).

import os
from typing import Dict, Any, List


def _get_param(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(name, default)


def _maybe_fail() -> None:
    scenario = _get_param("scenario", "").strip().lower()
    if scenario == "duplicate_rerun":
        raise ValueError("Duplicate rerun detected; idempotency check failed")
    if scenario == "force_fail":
        raise RuntimeError("Forced failure for drill validation")


def deduplicate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    _maybe_fail()
    seen = set()
    output = []
    for r in rows:
        key = (r.get("id"), r.get("event_ts"))
        if key in seen:
            continue
        seen.add(key)
        output.append(r)
    return output


if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z"},
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z"},
    ]
    print(deduplicate(sample))
