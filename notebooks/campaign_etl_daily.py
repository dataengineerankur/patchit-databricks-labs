# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Marketing Campaign Attribution — Daily ETL
# MAGIC
# MAGIC Ingests campaign impression/click events from landing zone,
# MAGIC joins with customer master and writes to `marketing_campaign_attribution` Delta table.
# MAGIC
# MAGIC **Owner:** Data Engineering — Marketing Analytics
# MAGIC **Schedule:** Daily 02:00 UTC
# MAGIC **SLA:** < 45 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from datetime import datetime, timedelta

# COMMAND ----------

# Widget / parameter support
try:
    run_date = dbutils.widgets.get("run_date")  # type: ignore[name-defined]
except Exception:
    run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"[campaign_etl_daily] run_date={run_date}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SCHEMAS ----------

impression_schema = StructType([
    StructField("impression_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("channel", StringType(), True),
    StructField("impression_ts", TimestampType(), False),
    StructField("cost_usd", DoubleType(), True),
])

click_schema = StructType([
    StructField("click_id", StringType(), False),
    StructField("impression_id", StringType(), False),
    StructField("click_ts", TimestampType(), False),
    StructField("converted", IntegerType(), True),
    StructField("revenue_usd", DoubleType(), True),
])

# COMMAND ----------

# ---------- INGEST ----------
# Simulate landing zone parquet reads

impressions_df = spark.range(500_000).select(
    F.concat(F.lit("imp_"), F.col("id").cast("string")).alias("impression_id"),
    F.concat(F.lit("camp_"), (F.col("id") % 50).cast("string")).alias("campaign_id"),
    (F.col("id") % 100_000).cast("int").alias("user_id"),
    F.when(F.col("id") % 4 == 0, "email")
     .when(F.col("id") % 4 == 1, "social")
     .when(F.col("id") % 4 == 2, "search")
     .otherwise("display").alias("channel"),
    (F.current_timestamp() - F.expr(f"INTERVAL {1} DAY") + (F.col("id") % 86400).cast("int") * F.expr("INTERVAL 1 SECOND")).alias("impression_ts"),
    (F.rand() * 5).alias("cost_usd"),
)

clicks_df = spark.range(80_000).select(
    F.concat(F.lit("clk_"), F.col("id").cast("string")).alias("click_id"),
    F.concat(F.lit("imp_"), (F.col("id") * 3).cast("string")).alias("impression_id"),
    (F.current_timestamp() - F.expr(f"INTERVAL {1} DAY") + (F.col("id") % 86400).cast("int") * F.expr("INTERVAL 1 SECOND")).alias("click_ts"),
    (F.col("id") % 2).cast("int").alias("converted"),
    F.when(F.col("id") % 2 == 0, F.rand() * 100).otherwise(F.lit(0.0)).alias("revenue_usd"),
)

# COMMAND ----------

# ---------- TRANSFORM ----------

# BUG: column name is "impression_ts" in impressions_df but we reference "event_ts" which does not exist
# This is a realistic schema-drift bug: upstream renamed the column
attributed = (
    impressions_df
    .join(clicks_df, on="impression_id", how="left")
    .withColumn("attributed_revenue",
                F.when(F.col("converted") == 1, F.col("revenue_usd")).otherwise(0.0))
    .withColumn("processing_ts", F.current_timestamp())
    .select(
        "campaign_id",
        "channel",
        "event_ts",         # <<< BUG: should be "impression_ts"
        "click_ts",
        "cost_usd",
        "attributed_revenue",
        "processing_ts",
    )
)

# COMMAND ----------

# ---------- WRITE ----------

attributed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("patchit_demo.customer_payments.marketing_campaign_attribution")

print(f"[campaign_etl_daily] SUCCESS — wrote {attributed.count()} rows")
