# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer 360 — Transactions Merge
# MAGIC
# MAGIC Merges daily transaction updates into the `customer_transactions` Delta table
# MAGIC using SCD Type 1 (overwrite on match). Handles late-arriving data.
# MAGIC
# MAGIC **Owner:** Data Engineering — Customer Analytics
# MAGIC **Schedule:** Every 10 days
# MAGIC **SLA:** < 60 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta
from delta.tables import DeltaTable

# COMMAND ----------

try:
    run_date = dbutils.widgets.get("run_date")  # type: ignore[name-defined]
except Exception:
    run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"[customer_txn_merge] run_date={run_date}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE INCOMING BATCH ----------

incoming_df = spark.range(300_000).select(
    F.concat(F.lit("TXN-"), F.col("id").cast("string")).alias("txn_id"),
    (F.col("id") % 80_000).cast("int").alias("customer_id"),
    F.when(F.col("id") % 3 == 0, "purchase")
     .when(F.col("id") % 3 == 1, "return")
     .otherwise("exchange").alias("txn_type"),
    (F.rand() * 500).alias("amount"),
    (F.current_timestamp() - F.expr("INTERVAL 1 DAY") +
     (F.col("id") % 86400).cast("int") * F.expr("INTERVAL 1 SECOND")).alias("txn_ts"),
    F.lit(run_date).alias("batch_date"),
)

# COMMAND ----------

# ---------- MERGE INTO TARGET ----------

target_table = "patchit_demo.customer_payments.customer_transactions"

# BUG: DeltaTable.forName requires the table to pre-exist.
# On first run the table doesn't exist yet, causing AnalysisException.
# Fix: check if table exists; if not, write directly; else merge.

target = DeltaTable.forName(spark, target_table)

(
    target.alias("tgt")
    .merge(
        incoming_df.alias("src"),
        "tgt.txn_id = src.txn_id"
    )
    .whenMatchedUpdate(set={
        "amount": "src.amount",
        "txn_ts": "src.txn_ts",
        "batch_date": "src.batch_date",
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

row_count = spark.table(target_table).count()
print(f"[customer_txn_merge] SUCCESS — target now has {row_count} rows")
