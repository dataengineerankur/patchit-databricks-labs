# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Pipeline — Gold Aggregation
# MAGIC
# MAGIC Aggregates daily sales from bronze/silver into `sales_pipeline_gold` Delta table.
# MAGIC Computes KPIs: total_revenue, avg_order_value, conversion_rate per store per day.
# MAGIC
# MAGIC **Owner:** Data Engineering — Revenue Analytics
# MAGIC **Schedule:** Every 5 minutes (near real-time)
# MAGIC **SLA:** < 3 min

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

try:
    run_date = dbutils.widgets.get("run_date")  # type: ignore[name-defined]
except Exception:
    run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"[sales_pipeline_gold] run_date={run_date}")

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- SIMULATE SILVER ORDERS ----------

orders_df = spark.range(2_000_000).select(
    F.concat(F.lit("ORD-"), F.col("id").cast("string")).alias("order_id"),
    (F.col("id") % 500).cast("int").alias("store_id"),
    (F.col("id") % 200_000).cast("int").alias("customer_id"),
    F.when(F.col("id") % 4 == 0, "completed")
     .when(F.col("id") % 4 == 1, "completed")
     .when(F.col("id") % 4 == 2, "cancelled")
     .otherwise("pending").alias("status"),
    (F.rand() * 200 + 5).alias("order_total"),
    F.to_date(F.lit(run_date)).alias("order_date"),
    (F.current_timestamp() - (F.col("id") % 86400).cast("int") * F.expr("INTERVAL 1 SECOND")).alias("order_ts"),
)

# COMMAND ----------

# ---------- AGGREGATE KPIs ----------

completed = orders_df.filter(F.col("status") == "completed")
all_orders = orders_df

kpi_df = (
    completed.groupBy("store_id", "order_date")
    .agg(
        F.sum("order_total").alias("total_revenue"),
        F.avg("order_total").alias("avg_order_value"),
        F.count("order_id").alias("completed_orders"),
    )
    .join(
        all_orders.groupBy("store_id", "order_date")
        .agg(F.count("order_id").alias("total_orders")),
        on=["store_id", "order_date"],
        how="inner",
    )
    .withColumn("conversion_rate",
        F.round(F.col("completed_orders") / F.col("total_orders"), 4))
    .withColumn("processing_ts", F.current_timestamp())
)

# COMMAND ----------

# ---------- RANK & FILTER ----------

window = Window.partitionBy("order_date").orderBy(F.desc("total_revenue"))
ranked = kpi_df.withColumn("revenue_rank", F.row_number().over(window))

# COMMAND ----------

# ---------- WRITE ----------

# BUG: uses merge with wrong condition column "store_code" instead of "store_id"
# This causes AnalysisException because "store_code" doesn't exist
from delta.tables import DeltaTable

target_table = "patchit_demo.customer_orders.sales_pipeline_gold"

if spark.catalog.tableExists(target_table):
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("tgt")
        .merge(
            ranked.alias("src"),
            "tgt.store_code = src.store_code AND tgt.order_date = src.order_date"
            # BUG: should be "tgt.store_id = src.store_id AND tgt.order_date = src.order_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    ranked.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"[sales_pipeline_gold] SUCCESS — KPIs computed for {ranked.count()} store-day combos")
