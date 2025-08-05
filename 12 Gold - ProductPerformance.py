# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Product Performance - Silver to Gold Layer
# MAGIC This notebook creates the ProductPerformance gold table that helps track product popularity and profitability across the e-commerce platform.
# MAGIC
# MAGIC 📊 Metrics Included:
# MAGIC UnitsSold
# MAGIC
# MAGIC TotalRevenue
# MAGIC
# MAGIC AvgUnitPrice
# MAGIC
# MAGIC TotalReturns
# MAGIC
# MAGIC ReturnRate
# MAGIC
# MAGIC 🔗 Source Tables:
# MAGIC Products, OrderItems, Returns
# MAGIC
# MAGIC 📁 Destination:
# MAGIC Delta table at: dbfs:/FileStore/Gold/ProductPerformance
# MAGIC
# MAGIC 💡 Use Cases:
# MAGIC Power BI product dashboards
# MAGIC
# MAGIC Inventory & restocking planning
# MAGIC
# MAGIC Promotions targeting high-performing products
# MAGIC
# MAGIC Detecting low-performing or high-return items

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import DoubleType, IntegerType

# 1. Load Silver Layer Tables
df_products = spark.read.format("delta").load("dbfs:/FileStore/Silver/Products")
#df_orderitems = spark.read.format("delta").load("dbfs:/FileStore/Silver/OrderItems").filter("IsCurrent = true")
df_orderitems = (
    spark.read.format("delta").load("dbfs:/FileStore/Silver/OrderItems")
    .filter("IsCurrent = true")
    .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    .withColumn("TotalPrice", col("TotalPrice").cast(DoubleType()))
)
df_returns = spark.read.format("delta").load("dbfs:/FileStore/Silver/Returns").filter("IsCurrent = true")

# 2. Aggregate sales data per Product
df_sales = (
    df_orderitems
    .groupBy("ProductID")
    .agg(
        sum("Quantity").alias("UnitsSold"),
        sum("TotalPrice").alias("TotalRevenue"),
        round(avg(col("TotalPrice") / col("Quantity")), 2).alias("AvgUnitPrice")
    )
)

# 3. Aggregate returns per Product
df_product_returns = (
    df_returns
    .groupBy("ProductID")
    .agg(
        count("*").alias("TotalReturns")
    )
)

# 4. Join sales, returns, and product info
df_product_perf = (
    df_products
    .select("ProductID", "ProductName", "Category")
    .join(df_sales, "ProductID", "left")
    .join(df_product_returns, "ProductID", "left")
    .fillna({
        "UnitsSold": 0,
        "TotalRevenue": 0.0,
        "AvgUnitPrice": 0.0,
        "TotalReturns": 0
    })
    .withColumn("ReturnRate", round(col("TotalReturns") / when(col("UnitsSold") > 0, col("UnitsSold")).otherwise(1), 2))
    .withColumn("LoadDate", current_timestamp())
)

# 5. Write to Gold Path
df_product_perf.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Gold/ProductPerformance")