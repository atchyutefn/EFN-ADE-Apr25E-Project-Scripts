# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Daily Sales Summary - Silver to Gold Layer
# MAGIC This notebook generates a gold-layer aggregate showing daily performance metrics for orders, revenue, and payments.
# MAGIC
# MAGIC 📊 Metrics Included:
# MAGIC TotalOrders
# MAGIC
# MAGIC UnitsSold
# MAGIC
# MAGIC GrossRevenue
# MAGIC
# MAGIC PaymentsCollected
# MAGIC
# MAGIC AvgOrderValue
# MAGIC
# MAGIC 🔗 Source Tables:
# MAGIC Orders, OrderItems, Payments
# MAGIC
# MAGIC 📁 Output:
# MAGIC Delta table at: dbfs:/FileStore/Gold/DailySalesSummary
# MAGIC
# MAGIC 💡 Use Cases:
# MAGIC Trend charts in Power BI
# MAGIC
# MAGIC Monitor business growth or seasonal drops
# MAGIC
# MAGIC Compare daily revenue vs. payments collected
# MAGIC
# MAGIC Identify best-performing days for campaigns
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# 1. Load Silver Tables
df_orders = spark.read.format("delta").load("dbfs:/FileStore/Silver/Orders").filter("IsCurrent = true")
df_orderitems = spark.read.format("delta").load("dbfs:/FileStore/Silver/OrderItems").filter("IsCurrent = true")
df_payments = spark.read.format("delta").load("dbfs:/FileStore/Silver/Payments")

# 2. Join Orders → OrderItems → Payments
df_daily = (
    df_orders.alias("o")
    .join(df_orderitems.alias("oi"), "OrderID", "left")
    .join(df_payments.alias("p"), "OrderID", "left")
)

# 3. Aggregate by OrderDate
df_daily_summary = (
    df_daily
    .withColumn("OrderDate", to_date("OrderDate"))
    .groupBy("OrderDate")
    .agg(
        countDistinct("OrderID").alias("TotalOrders"),
        sum("oi.Quantity").alias("UnitsSold"),
        sum("oi.TotalPrice").alias("GrossRevenue"),
        sum("p.PaymentAmount").alias("PaymentsCollected"),
        round(avg("oi.TotalPrice"), 2).alias("AvgOrderValue")
    )
    .fillna({
        "UnitsSold": 0,
        "GrossRevenue": 0.0,
        "PaymentsCollected": 0.0,
        "AvgOrderValue": 0.0
    })
    .withColumn("LoadDate", current_timestamp())
)



# 4. Write Gold Table
df_daily_summary.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Gold/DailySalesSummary")