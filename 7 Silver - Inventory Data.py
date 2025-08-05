# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Inventory Table - Bronze to Silver Layer Transformation
# MAGIC This notebook handles the transformation of the Inventory table from the bronze layer (CSV) to the silver layer (Delta Lake), with a focus on real-time stock monitoring and low-stock flagging.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today’s efn_Inventory.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and prepares data:
# MAGIC
# MAGIC Casts StockQuantity to int
# MAGIC
# MAGIC Fills missing stock values with 0
# MAGIC
# MAGIC Converts timestamps
# MAGIC
# MAGIC Removes duplicate ProductIDs
# MAGIC
# MAGIC Adds transformation:
# MAGIC
# MAGIC IsLowStock: True if stock < 10
# MAGIC
# MAGIC Applies SCD Type 1 logic to always reflect the latest stock levels
# MAGIC
# MAGIC Writes into Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/Inventory
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Tracks live inventory by product
# MAGIC
# MAGIC Identifies low-stock products for restocking
# MAGIC
# MAGIC Keeps most recent record only (no history)

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# 1. Get Current Date
today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# 2. Define Paths
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Inventory.csv"
silver_path = "dbfs:/FileStore/Silver/Inventory"

# 3. Read CSV from Bronze
df_inventory_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean, Transform
load_date = current_timestamp()

df_inventory_clean = (
    df_inventory_raw
    .dropDuplicates(["ProductID"])
    .fillna({"StockQuantity": 0})
    .withColumn("StockQuantity", col("StockQuantity").cast("int"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: Low stock flag (e.g., < 10 items)
    .withColumn("IsLowStock", when(col("StockQuantity") < 10, lit(True)).otherwise(lit(False)))

    .withColumn("LoadDate", load_date)
)

# 5. Write Initial Table (run once only)
#df_inventory_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge (SCD Type 1 logic)
delta_inventory = DeltaTable.forPath(spark,silver_path)

delta_inventory.alias("tgt").merge(
    df_inventory_clean.alias("src"),
    "tgt.ProductID = src.ProductID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()