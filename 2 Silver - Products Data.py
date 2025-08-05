# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Notebook Summary: Products Table - Bronze to Silver Layer Transformation
# MAGIC This notebook is a part of the Azure Data Engineering pipeline that processes the Products table from the bronze layer (raw CSV files stored in Azure Blob Storage) to the silver layer (Delta Lake). The transformation focuses on maintaining clean, up-to-date product information using an SCD Type 1 approach.
# MAGIC
# MAGIC ## 🔁 Pipeline Flow (Bronze ➜ Silver)
# MAGIC - Read the efn_Products.csv file from a date-based partitioned path:
# MAGIC   /mnt/Bronze/yyyy/mm/dd/, where the date components are dynamically derived from the current system date.
# MAGIC
# MAGIC - Clean and transform the raw data:
# MAGIC   - Deduplicate records using ProductID
# MAGIC   - Cast Price to double for consistent numerical operations
# MAGIC   - Parse date fields like CreatedDate and ModifiedDate into timestamps
# MAGIC   - Handle null values (if needed) with appropriate defaults
# MAGIC
# MAGIC - Add metadata and audit information:
# MAGIC   - Append a LoadDate column for tracking ingestion time
# MAGIC
# MAGIC - Apply SCD Type 1 logic:
# MAGIC
# MAGIC Detect and update existing product records when any details (e.g., price, category) change
# MAGIC
# MAGIC Overwrite previous values without retaining history
# MAGIC
# MAGIC Merge transformed records into a Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/Products
# MAGIC
# MAGIC 🧹 Key Features
# MAGIC Ensures product data is deduplicated and properly typed
# MAGIC
# MAGIC Keeps only the latest state of each product using SCD Type 1
# MAGIC
# MAGIC Enables fast, reliable lookups for pricing, categories, and product attributes
# MAGIC
# MAGIC This notebook ensures the Products data is clean, consistent, and ready for real-time analytics and gold layer aggregation, forming the backbone for business-critical use cases like inventory planning, pricing dashboards, and product performance analysis.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# 1. Get Current Date
today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# COMMAND ----------

# 2. Define Paths
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Products.csv"
silver_path = "dbfs:/FileStore/Silver/Products"

# COMMAND ----------

# 3. Read CSV from Bronze (Today's File)
df_products_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_products_clean = (
    df_products_raw
    .dropDuplicates(["ProductID"])
    .withColumn("Price", col("Price").cast("double"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
)

# COMMAND ----------

# 5. Write Initial Table (run only once to initialize)
#df_products_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

# 6. SCD Type 1 Merge Logic (overwrite updates, no history)
delta_products = DeltaTable.forPath(spark, silver_path)

delta_products.alias("tgt") \
    .merge(
        df_products_clean.alias("src"),
        "tgt.ProductID = src.ProductID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()