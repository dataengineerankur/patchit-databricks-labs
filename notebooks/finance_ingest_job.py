# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Finance Transactions 2025 — Ingest & Validate
# MAGIC
# MAGIC Reads daily transaction feed (CSV from ADLS), validates schema,
# MAGIC applies business rules, and writes to `finance_transactions_2025` Delta table.
# MAGIC
# MAGIC **Owner:** Data Engineering — Finance
# MAGIC **Schedule:** Daily 03:00 UTC
# MAGIC **SLA:** < 30 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)
from datetime import datetime, timedelta

# COMMAND ----------

try:
    run_date = dbutils.widgets.get("run_date")  # type: ignore[name-defined]
except Exception:
    run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"[finance_ingest_job] run_date={run_date}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE FEED ----------

raw_df = spark.range(1_000_000).select(
    F.col("id").alias("txn_id"),
    F.concat(F.lit("ACC-"), (F.col("id") % 50000).cast("string")).alias("account_id"),
    F.when(F.col("id") % 5 == 0, "withdrawal")
     .when(F.col("id") % 5 == 1, "deposit")
     .when(F.col("id") % 5 == 2, "transfer")
     .when(F.col("id") % 5 == 3, "fee")
     .otherwise("interest").alias("txn_type"),
    # BUG: amount is STRING for ~2% of rows (simulates bad upstream feed)
    F.when(F.col("id") % 50 == 0, F.lit("N/A"))
     .otherwise((F.rand() * 10000 - 5000).cast("string")).alias("amount"),
    (F.current_timestamp() - F.expr("INTERVAL 1 DAY") + (F.col("id") % 86400).cast("int") * F.expr("INTERVAL 1 SECOND")).alias("txn_ts"),
    F.lit(run_date).alias("partition_date"),
)

print(f"[finance_ingest_job] raw rows = {raw_df.count()}")

# COMMAND ----------

# ---------- VALIDATE & CAST ----------

# BUG: cast will turn "N/A" strings into null, then the NOT NULL filter below
# incorrectly drops them silently. Correct fix: quarantine bad rows, don't drop.
validated_df = (
    raw_df
    .withColumn("amount_dbl", F.col("amount").cast(DoubleType()))
    .filter(F.col("amount_dbl").isNotNull())  # drops ~2% silently
    .drop("amount")
    .withColumnRenamed("amount_dbl", "amount")
)

# COMMAND ----------

# ---------- BUSINESS RULES ----------

enriched_df = (
    validated_df
    .withColumn("is_large_txn", F.when(F.abs(F.col("amount")) > 5000, True).otherwise(False))
    .withColumn("processing_ts", F.current_timestamp())
)

# COMMAND ----------

# ---------- WRITE ----------

# BUG: partitionBy uses "partition_dt" but column is "partition_date"
enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("partition_dt") \
    .option("overwriteSchema", "true") \
    .saveAsTable("patchit_demo.customer_payments.finance_transactions_2025")

print(f"[finance_ingest_job] SUCCESS — wrote {enriched_df.count()} rows")
