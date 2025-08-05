# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Customer Order Summary - Silver to Gold Layer
# MAGIC This notebook builds a gold-layer aggregate that summarizes each customer's purchasing behavior for reporting and analysis.
# MAGIC
# MAGIC 📊 Metrics Included:
# MAGIC TotalOrders
# MAGIC
# MAGIC TotalSpent
# MAGIC
# MAGIC AvgOrderValue
# MAGIC
# MAGIC FirstOrderDate
# MAGIC
# MAGIC RecentOrderDate
# MAGIC
# MAGIC 🔗 Source Tables:
# MAGIC Customers, Orders, OrderItems, Payments
# MAGIC
# MAGIC 📁 Destination:
# MAGIC Delta table at: dbfs:/FileStore/Gold/CustomerOrderSummary
# MAGIC
# MAGIC 💡 Use Cases:
# MAGIC Power BI dashboards
# MAGIC
# MAGIC RFM segmentation
# MAGIC
# MAGIC Customer loyalty scoring
# MAGIC
# MAGIC Marketing insights

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# 1. Load required Silver tables
df_customers = spark.read.format("delta").load("dbfs:/FileStore/Silver/Customers")
df_orders = spark.read.format("delta").load("dbfs:/FileStore/Silver/Orders").filter("IsCurrent = true")
df_orderitems = spark.read.format("delta").load("dbfs:/FileStore/Silver/OrderItems").filter("IsCurrent = true")
df_payments = spark.read.format("delta").load("dbfs:/FileStore/Silver/Payments")

# 2. Join Orders → OrderItems → Payments
df_orders_joined = (
    df_orders.alias("o")
    .join(df_orderitems.alias("oi"), col("o.OrderID") == col("oi.OrderID"), "left")
    .join(df_payments.alias("p"), col("o.OrderID") == col("p.OrderID"), "left")
)

# 3. Aggregate at Customer level
df_customer_summary = (
    df_orders_joined
    .groupBy("o.CustomerID")
    .agg(
        countDistinct("o.OrderID").alias("TotalOrders"),
        sum("oi.TotalPrice").alias("TotalSpent"),
        round(avg("oi.TotalPrice"), 2).alias("AvgOrderValue"),
        min("o.OrderDate").alias("FirstOrderDate"),
        max("o.OrderDate").alias("RecentOrderDate")
    )
)

# 4. Join with Customer Details
df_customer_summary_final = (
    df_customers.select("CustomerID", "FirstName", "LastName", "Email")
    .join(df_customer_summary, "CustomerID", "left")
    .fillna({
        "TotalOrders": 0,
        "TotalSpent": 0.0,
        "AvgOrderValue": 0.0
    })
    .withColumn("LoadDate", current_timestamp())
)

# 5. Write Gold Table
df_customer_summary_final.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Gold/CustomerOrderSummary")

# COMMAND ----------

display(df_customer_summary_final)