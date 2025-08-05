# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Notebook Summary: Orders Table - Bronze to Silver Layer Transformation
# MAGIC This notebook is part of an end-to-end Azure Data Engineering pipeline, focused on processing the Orders table from the bronze layer (raw CSV files in blob storage) to the silver layer (Delta Lake) with proper transformations and data quality enforcement.
# MAGIC
# MAGIC ## 🔁 Pipeline Flow (Bronze ➜ Silver)
# MAGIC - **Read** the efn_Orders.csv file from blob storage path structured by current date (/mnt/Bronze/yyyy/mm/dd/).
# MAGIC - **Clean** and transform the data:
# MAGIC   - Parse timestamp fields (`OrderDate`, `CreatedDate`, `ModifiedDate`)
# MAGIC   - Handle nulls in critical columns like OrderStatus, PaymentMode, and ShippingCharge
# MAGIC   - Deduplicate records using OrderID
# MAGIC
# MAGIC - Enhance data with audit columns:
# MAGIC   - LoadDate, StartDate, EndDate, IsCurrent
# MAGIC
# MAGIC - Apply SCD Type 2 logic:
# MAGIC   - Track changes in order status, payment mode, or shipping charge
# MAGIC   - Retain full historical context for each order version
# MAGIC
# MAGIC - Merge the cleaned data into a Delta Lake table in the silver layer:
# MAGIC   - Path: dbfs:/FileStore/Silver/Orders
# MAGIC
# MAGIC - 🧹 Key Features
# MAGIC   - Ensures data integrity with null handling and type casting
# MAGIC   - Supports historical reporting via SCD Type 2
# MAGIC   - Enables scalable and efficient upserts using Delta Lake
# MAGIC
# MAGIC This notebook ensures that the Orders data is clean, accurate, and historically traceable, making it ready for downstream consumption in Power BI reports or aggregation in the gold layer.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

# 1. Get Current Date
today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# COMMAND ----------

# 2. Define Paths
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Orders.csv"
silver_path = "dbfs:/FileStore/Silver/Orders"

# COMMAND ----------

# 3. Read from Bronze Layer
df_orders_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_orders_clean = (
    df_orders_raw
    # Remove duplicates
    .dropDuplicates(["OrderID"])
    
    # Convert to correct types
    .withColumn("OrderDate", to_timestamp("OrderDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Handle nulls and invalid values
    .fillna({
        "OrderStatus": "Unknown"
    })

    # Add audit & SCD2 columns
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)

# COMMAND ----------

display(df_orders_clean)

# COMMAND ----------

# 5. Write Initial Table (Run only once to initialize)
#df_orders_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

# 6. SCD Type 2 Merge Logic
delta_orders = DeltaTable.forPath(spark, silver_path)
updates_df = df_orders_clean.alias("updates")

# A. Mark previous records as not current if data has changed
delta_orders.alias("existing") \
    .merge(
        updates_df,
        """
        existing.OrderID = updates.OrderID AND existing.IsCurrent = true AND (
            existing.OrderStatus != updates.OrderStatus
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

# B. Insert new version of changed or new records
delta_orders.alias("existing") \
    .merge(
        updates_df,
        "existing.OrderID = updates.OrderID AND existing.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()