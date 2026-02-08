# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Compliance Audit Logs — ETL
# MAGIC
# MAGIC Reads system audit events, enriches with user metadata,
# MAGIC and writes to `compliance_audit_logs` for SOX/compliance reporting.
# MAGIC
# MAGIC **Owner:** Data Engineering — Compliance
# MAGIC **Schedule:** Every 3 hours
# MAGIC **SLA:** < 15 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, MapType
)
from datetime import datetime, timedelta
import json

# COMMAND ----------

try:
    lookback_hours = int(dbutils.widgets.get("lookback_hours"))  # type: ignore[name-defined]
except Exception:
    lookback_hours = 3

print(f"[audit_log_etl_job] lookback_hours={lookback_hours}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE AUDIT EVENTS ----------

audit_df = spark.range(200_000).select(
    F.concat(F.lit("evt_"), F.col("id").cast("string")).alias("event_id"),
    (F.col("id") % 5000).cast("int").alias("user_id"),
    F.when(F.col("id") % 6 == 0, "LOGIN")
     .when(F.col("id") % 6 == 1, "LOGOUT")
     .when(F.col("id") % 6 == 2, "QUERY")
     .when(F.col("id") % 6 == 3, "EXPORT")
     .when(F.col("id") % 6 == 4, "ADMIN_CHANGE")
     .otherwise("DATA_ACCESS").alias("action"),
    F.when(F.col("id") % 3 == 0, "PROD")
     .when(F.col("id") % 3 == 1, "STAGING")
     .otherwise("DEV").alias("environment"),
    (F.current_timestamp() - F.expr(f"INTERVAL {lookback_hours} HOURS") +
     (F.col("id") % (lookback_hours * 3600)).cast("int") * F.expr("INTERVAL 1 SECOND")
    ).alias("event_ts"),
    F.lit("system_v2").alias("source_system"),
)

# COMMAND ----------

# ---------- SIMULATE USER METADATA ----------

users_df = spark.range(5000).select(
    F.col("id").cast("int").alias("user_id"),
    F.concat(F.lit("user_"), F.col("id").cast("string"), F.lit("@corp.com")).alias("email"),
    F.when(F.col("id") % 10 == 0, "admin")
     .when(F.col("id") % 10 < 3, "analyst")
     .otherwise("viewer").alias("role"),
    F.when(F.col("id") % 4 == 0, "Finance")
     .when(F.col("id") % 4 == 1, "Engineering")
     .when(F.col("id") % 4 == 2, "Marketing")
     .otherwise("Operations").alias("department"),
)

# COMMAND ----------

# ---------- ENRICH ----------

enriched = (
    audit_df
    .join(users_df, on="user_id", how="left")
    .withColumn("is_sensitive",
        F.when(F.col("action").isin("ADMIN_CHANGE", "EXPORT", "DATA_ACCESS") &
               (F.col("environment") == "PROD"), True).otherwise(False))
    .withColumn("processing_ts", F.current_timestamp())
)

# COMMAND ----------

# ---------- COMPLIANCE FLAG ----------

# BUG: The `isin` call above needs each value as separate arg, but the & operator
# has wrong precedence — it does bitwise AND between the isin result and the string "PROD".
# The actual bug: `isin(...)` & `(col == "PROD")` — isin returns Column, & is bitwise,
# but the real issue is the expression evaluates but "is_sensitive" is always False
# because operator precedence makes it: isin("ADMIN_CHANGE", "EXPORT", "DATA_ACCESS") & ("PROD")
# Fix: wrap in parentheses or use separate when/conditions.

flagged = (
    enriched
    .withColumn("compliance_risk",
        F.when(
            (F.col("is_sensitive") == True) & (F.col("role") == "admin"),
            "HIGH"
        ).when(
            F.col("is_sensitive") == True,
            "MEDIUM"
        ).otherwise("LOW")
    )
)

# COMMAND ----------

# ---------- WRITE ----------

# BUG: MERGE INTO target table uses wrong database name
# "patchit_prod" doesn't exist; should be "patchit_demo"
flagged.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("patchit_prod.customer_payments.compliance_audit_logs")

print(f"[audit_log_etl_job] SUCCESS — wrote {flagged.count()} rows")
