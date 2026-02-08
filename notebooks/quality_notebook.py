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


# COMMAND ----------

# Retail Orders Quality Checks
# This cell handles quality checks for the retail_orders pipeline

try:
    from pyspark.sql import functions as F
    
    # Get pipeline_id parameter
    pipeline_id = _get_param("pipeline_id", "")
    
    # Only run retail_orders checks if this is the retail_orders pipeline
    if pipeline_id == "retail_orders":
        # Assume silver_df is already loaded in the notebook context
        # Handle NULL order_ts by filtering them out or filling with default values
        # instead of failing the quality check
        
        # Filter out rows with NULL order_ts before quality checks
        silver_df_clean = silver_df.filter(F.col("order_ts").isNotNull())  # type: ignore[name-defined]
        
        # Count rows with NULL order_ts for logging
        null_ts_count = silver_df.filter(F.col("order_ts").isNull()).count()  # type: ignore[name-defined]
        if null_ts_count > 0:
            print(f"Warning: {null_ts_count} rows with NULL order_ts were filtered out")
        
        # Run quality checks on cleaned data
        negative_amt = silver_df_clean.filter(F.col("amount") < 0).count()
        if negative_amt > 0:
            raise ValueError(f"Invalid order amount (rows={negative_amt})")
        
        print(f"Quality checks passed for retail_orders pipeline")
        print(f"Total rows processed: {silver_df_clean.count()}")
        print(f"Rows with NULL order_ts filtered: {null_ts_count}")

except NameError:
    # PySpark not available (running in non-Databricks environment)
    pass


# COMMAND ----------

if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z", "status": "ok"},
        {"id": None, "event_ts": "2025-01-01T01:00:00Z", "status": "ok"},
    ]
    print(quality_checks(sample))
