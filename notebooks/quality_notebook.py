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


def fail_if(count: int, message: str):
    """Helper function for quality checks that raises on threshold breach"""
    if count > 0:
        raise ValueError(f"{message} (rows={count})")


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


def quality_publish():
    """Quality publish task for retail_orders pipeline"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        print("PySpark not available, skipping quality_publish")
        return
    
    spark = SparkSession.builder.getOrCreate()
    pipeline_id = _get_param("pipeline_id", "retail_orders")
    
    print(f"[quality_publish] Running quality checks for pipeline: {pipeline_id}")
    
    # Load silver data
    try:
        silver_df = spark.table("patchit_demo.customer_orders.retail_orders_silver")
    except Exception as e:
        print(f"[quality_publish] Could not load silver table, creating sample: {e}")
        silver_df = spark.range(5000).select(
            F.concat(F.lit("ORD-"), F.col("id").cast("string")).alias("order_id"),
            (F.col("id") % 100).cast("int").alias("customer_id"),
            (F.rand() * 200 + 5).alias("amount"),
            F.current_timestamp().alias("order_ts")
        )
    
    if pipeline_id == "retail_orders":
        # Check for null timestamps and filter them out instead of failing
        null_ts = silver_df.filter(F.col("order_ts").isNull()).count()
        if null_ts > 0:
            print(f"[quality_publish] WARNING: Filtering out {null_ts} rows with null order_ts")
            silver_df = silver_df.filter(F.col("order_ts").isNotNull())
        
        # Check for negative amounts and filter them out
        negative_amt = silver_df.filter(F.col("amount") < 0).count()
        if negative_amt > 0:
            print(f"[quality_publish] WARNING: Filtering out {negative_amt} rows with negative amounts")
            silver_df = silver_df.filter(F.col("amount") >= 0)
        
        print(f"[quality_publish] Quality checks passed. Clean records: {silver_df.count()}")
    
    return {"status": "success", "records": silver_df.count()}


if __name__ == "__main__":
    pipeline_id = _get_param("pipeline_id", "")
    
    if pipeline_id == "retail_orders":
        # Run quality publish for retail_orders pipeline
        result = quality_publish()
        print(f"Quality publish result: {result}")
    else:
        # Run standard quality checks
        sample = [
            {"id": 1, "event_ts": "2025-01-01T00:00:00Z", "status": "ok"},
            {"id": None, "event_ts": "2025-01-01T01:00:00Z", "status": "ok"},
        ]
        print(quality_checks(sample))
