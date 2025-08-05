# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: ShippingDetails Table - Bronze to Silver Layer Transformation
# MAGIC This notebook transforms the ShippingDetails table from the bronze (raw CSV) to the silver (Delta Lake) layer, focusing on data cleaning, transformation, and current state accuracy using SCD Type 1.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today's efn_ShippingDetails.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and prepares the data:
# MAGIC
# MAGIC Null handling for ShippingMethod and TrackingNumber
# MAGIC
# MAGIC Timestamp parsing
# MAGIC
# MAGIC Deduplication based on ShippingID
# MAGIC
# MAGIC Transformation:
# MAGIC
# MAGIC Adds a new field ShippingDelayDays to show days since shipment
# MAGIC
# MAGIC Applies SCD Type 1 logic to maintain the latest shipping info
# MAGIC
# MAGIC Merges into the Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/ShippingDetails
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Tracks only latest state of each shipment
# MAGIC
# MAGIC Enriches with ShippingDelayDays for delivery monitoring
# MAGIC
# MAGIC Enables fast querying and dashboard integration
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

# 2. Define Paths
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_ShippingDetails.csv"
silver_path = "dbfs:/FileStore/Silver/ShippingDetails"

# 3. Read CSV from Bronze
df_shipping_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean, Transform
load_date = current_timestamp()

df_shipping_clean = (
    df_shipping_raw
    .dropDuplicates(["ShippingID"])
    .fillna({
        "ShippingMethod": "Standard",
        "TrackingNumber": "NA"
    })
    .withColumn("ShipDate", to_timestamp("ShipDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: Days since shipped
    .withColumn("ShippingDelayDays", datediff(current_date(), to_date("ShipDate")))

    .withColumn("LoadDate", load_date)
)

# 5. Write Initial Table (only once)
#df_shipping_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge into Silver (SCD Type 1)
delta_shipping = DeltaTable.forPath(spark,silver_path)

delta_shipping.alias("tgt").merge(
    df_shipping_clean.alias("src"),
    "tgt.ShippingID = src.ShippingID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()