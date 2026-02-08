# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Info — Daily Refresh
# MAGIC
# MAGIC Refreshes store master data from HR + facilities feeds, computes
# MAGIC operational metrics, and writes to `store_info` Delta table.
# MAGIC
# MAGIC **Owner:** Data Engineering — Store Operations
# MAGIC **Schedule:** Every 3 days
# MAGIC **SLA:** < 10 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from datetime import datetime

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE STORE MASTER ----------

stores_df = spark.range(2000).select(
    F.col("id").cast("int").alias("store_id"),
    F.concat(F.lit("Store #"), F.col("id").cast("string")).alias("store_name"),
    F.when(F.col("id") % 5 == 0, "NY")
     .when(F.col("id") % 5 == 1, "CA")
     .when(F.col("id") % 5 == 2, "TX")
     .when(F.col("id") % 5 == 3, "FL")
     .otherwise("IL").alias("state"),
    F.when(F.col("id") % 4 == 0, "urban")
     .when(F.col("id") % 4 == 1, "suburban")
     .when(F.col("id") % 4 == 2, "rural")
     .otherwise("metro").alias("store_type"),
    (F.rand() * 50000 + 5000).alias("sqft"),
    (F.col("id") % 3 == 0).alias("has_pharmacy"),
    (F.col("id") % 5 != 0).alias("is_open"),
    F.current_timestamp().alias("last_updated_ts"),
)

# COMMAND ----------

# ---------- SIMULATE HR HEADCOUNT ----------

headcount_df = spark.range(2000).select(
    F.col("id").cast("int").alias("store_id"),
    (F.col("id") % 80 + 10).cast("int").alias("total_employees"),
    (F.col("id") % 20 + 2).cast("int").alias("managers"),
)

# COMMAND ----------

# ---------- JOIN & COMPUTE METRICS ----------

# BUG: join column mismatch — headcount uses "store_id" but we alias it wrong
merged = (
    stores_df.alias("s")
    .join(
        headcount_df.alias("h"),
        F.col("s.store_id") == F.col("h.store_id"),
        how="left"
    )
    .select(
        F.col("s.store_id"),
        "store_name",
        "state",
        "store_type",
        "sqft",
        "has_pharmacy",
        "is_open",
        "total_employees",
        "managers",
        "last_updated_ts",
    )
    .withColumn("revenue_per_sqft",
        F.round(F.rand() * 50 + 10, 2))
    .withColumn("employees_per_sqft",
        # BUG: division by zero when sqft is 0 (won't happen here but let's add a real bug)
        # Real BUG: We compute a column but reference the wrong name downstream
        F.round(F.col("total_employees") / F.col("sqft"), 6))
    .withColumn("processing_ts", F.current_timestamp())
)

# COMMAND ----------

# ---------- WRITE ----------

# BUG: schema "customer_orders" is expected but we use "customer_order" (typo)
merged.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("patchit_demo.customer_order.store_info")

print(f"[store_info_daily] SUCCESS — refreshed {merged.count()} stores")
