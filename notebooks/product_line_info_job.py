# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Line Info — Catalog Sync
# MAGIC
# MAGIC Reads product catalog from upstream API (simulated), normalizes hierarchy,
# MAGIC and upserts into `product_line_info` Delta table.
# MAGIC
# MAGIC **Owner:** Data Engineering — Product & Catalog
# MAGIC **Schedule:** Every 24 hours
# MAGIC **SLA:** < 20 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType, TimestampType
)
from datetime import datetime
import json

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE PRODUCT CATALOG API RESPONSE ----------

products_df = spark.range(50_000).select(
    F.concat(F.lit("SKU-"), F.col("id").cast("string")).alias("sku"),
    F.concat(F.lit("Product "), F.col("id").cast("string")).alias("product_name"),
    F.when(F.col("id") % 8 == 0, "Electronics")
     .when(F.col("id") % 8 == 1, "Grocery")
     .when(F.col("id") % 8 == 2, "Apparel")
     .when(F.col("id") % 8 == 3, "Home & Garden")
     .when(F.col("id") % 8 == 4, "Health")
     .when(F.col("id") % 8 == 5, "Automotive")
     .when(F.col("id") % 8 == 6, "Toys")
     .otherwise("Sports").alias("category"),
    F.concat(F.lit("BRAND-"), (F.col("id") % 200).cast("string")).alias("brand"),
    (F.rand() * 500 + 1).alias("msrp"),
    (F.rand() * 500 + 1).alias("cost"),
    F.when(F.col("id") % 20 == 0, "discontinued")
     .when(F.col("id") % 20 < 3, "clearance")
     .otherwise("active").alias("lifecycle_status"),
    F.current_timestamp().alias("last_synced_ts"),
)

# COMMAND ----------

# ---------- NORMALIZE HIERARCHY ----------

# BUG: We import a function from a non-existent internal library
# This simulates a missing dependency after a library refactor
from catalog_utils.hierarchy import build_product_hierarchy  # noqa: E402

hierarchy_df = build_product_hierarchy(products_df, level_col="category")

# COMMAND ----------

# ---------- ENRICH ----------

enriched = (
    hierarchy_df
    .withColumn("margin_pct",
        F.round((F.col("msrp") - F.col("cost")) / F.col("msrp") * 100, 2))
    .withColumn("margin_tier",
        F.when(F.col("margin_pct") > 50, "HIGH")
         .when(F.col("margin_pct") > 25, "MEDIUM")
         .otherwise("LOW"))
    .withColumn("processing_ts", F.current_timestamp())
)

# COMMAND ----------

# ---------- WRITE ----------

enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("patchit_demo.customer_payments.product_line_info")

print(f"[product_line_info] SUCCESS — synced {enriched.count()} products")
