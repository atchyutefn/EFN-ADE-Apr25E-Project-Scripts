# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: OrderItems Table - Bronze to Silver Layer Transformation
# MAGIC This notebook transforms the OrderItems table from the raw bronze layer into a cleaned and enriched Delta table in the silver layer, applying SCD Type 2 logic to track quantity and price changes over time.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today's efn_OrderItems.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and parses:
# MAGIC
# MAGIC Fills missing Quantity and TotalPrice
# MAGIC
# MAGIC Calculates UnitPrice = TotalPrice / Quantity
# MAGIC
# MAGIC Deduplicates by OrderItemID
# MAGIC
# MAGIC Casts types and timestamps
# MAGIC
# MAGIC Adds audit and SCD columns
# MAGIC
# MAGIC Applies SCD Type 2:
# MAGIC
# MAGIC Expires old records on data change
# MAGIC
# MAGIC Inserts new versions with updated values
# MAGIC
# MAGIC Writes into Delta Lake silver path:
# MAGIC dbfs:/FileStore/Silver/OrderItems
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Tracks detailed item-level sales info
# MAGIC
# MAGIC Maintains price history for accurate financial tracking
# MAGIC
# MAGIC Feeds downstream aggregations like product or order summaries

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# 1. Get current date
today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# 2. Define paths
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Orderitems.csv"
silver_path = "dbfs:/FileStore/Silver/OrderItems"

# 3. Read bronze CSV
df_orderitems_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean, Transform, Add Audit Columns
load_date = current_timestamp()

df_orderitems_clean = (
    df_orderitems_raw
    .dropDuplicates(["OrderItemID"])
    .fillna({
        "Quantity": 1,
        "TotalPrice": 0.0
    })
    .withColumn("Quantity", col("Quantity").cast("int"))
    .withColumn("TotalPrice", col("TotalPrice").cast("double"))
    .withColumn("UnitPrice", when(col("Quantity") > 0, col("TotalPrice") / col("Quantity")).otherwise(0.0))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)

# 5. Write initial Delta table (run only once)
#df_orderitems_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge into Silver (SCD Type 2)
delta_orderitems = DeltaTable.forPath(spark, silver_path)

# Expire existing rows
delta_orderitems.alias("tgt") \
    .merge(
        df_orderitems_clean.alias("src"),
        """
        tgt.OrderItemID = src.OrderItemID AND tgt.IsCurrent = true AND (
            tgt.Quantity != src.Quantity OR
            tgt.TotalPrice != src.TotalPrice OR
            tgt.ProductID != src.ProductID OR
            tgt.OrderID != src.OrderID
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

# Insert new records
delta_orderitems.alias("tgt") \
    .merge(
        df_orderitems_clean.alias("src"),
        "tgt.OrderItemID = src.OrderItemID AND tgt.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()