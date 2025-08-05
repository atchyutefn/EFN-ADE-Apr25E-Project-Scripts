# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Category-Wise Sales - Silver to Gold Layer
# MAGIC This notebook builds the CategoryWiseSales gold-layer aggregate to help understand which product categories drive the most revenue and sales.
# MAGIC
# MAGIC 📊 Metrics Included:
# MAGIC UnitsSold
# MAGIC
# MAGIC TotalRevenue
# MAGIC
# MAGIC AvgUnitPrice
# MAGIC
# MAGIC UniqueProductsSold
# MAGIC
# MAGIC 🔗 Source Tables:
# MAGIC Products, OrderItems
# MAGIC
# MAGIC 📁 Output:
# MAGIC Delta table at: dbfs:/FileStore/Gold/CategoryWiseSales
# MAGIC
# MAGIC 💡 Use Cases:
# MAGIC Power BI category filters and revenue heatmaps
# MAGIC
# MAGIC Merchandising decisions
# MAGIC
# MAGIC Promotional focus for high/low-performing categories
# MAGIC
# MAGIC Inventory planning by category
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# 1. Load Silver Tables
df_products = spark.read.format("delta").load("dbfs:/FileStore/Silver/Products")
df_orderitems = spark.read.format("delta").load("dbfs:/FileStore/Silver/OrderItems").filter("IsCurrent = true")

# 2. Join OrderItems with Products to get Category info
df_sales_with_category = (
    df_orderitems.alias("oi")
    .join(df_products.select("ProductID", "Category"), "ProductID", "left")
)

# 3. Cast TotalPrice to numeric type and Aggregate by Category
df_category_summary = (
    df_sales_with_category
    .withColumn("TotalPrice", col("TotalPrice").cast("double"))
    .groupBy("Category")
    .agg(
        sum("Quantity").alias("UnitsSold"),
        sum("TotalPrice").alias("TotalRevenue"),
        round(avg(col("TotalPrice") / col("Quantity")), 2).alias("AvgUnitPrice"),
        countDistinct("ProductID").alias("UniqueProductsSold")
    )
    .fillna({
        "UnitsSold": 0,
        "TotalRevenue": 0.0,
        "AvgUnitPrice": 0.0,
        "UniqueProductsSold": 0
    })
    .withColumn("LoadDate", current_timestamp())
)

# 4. Write Gold Table
df_category_summary.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Gold/CategoryWiseSales")