# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Payments Table - Bronze to Silver Layer Transformation
# MAGIC This notebook is part of the Azure Data Engineering project, focusing on transforming the Payments table from the bronze layer (raw CSV from blob storage) to the silver layer (Delta Lake).
# MAGIC
# MAGIC 🔁 Pipeline Flow (Bronze ➜ Silver)
# MAGIC Read the efn_Payments.csv file from a date-partitioned blob path: /mnt/Bronze/yyyy/mm/dd/.
# MAGIC
# MAGIC Clean and transform the data:
# MAGIC
# MAGIC Deduplicate by PaymentID
# MAGIC
# MAGIC Cast PaymentAmount to double and parse timestamps
# MAGIC
# MAGIC Handle nulls:
# MAGIC
# MAGIC PaymentAmount → 0.0
# MAGIC
# MAGIC PaymentMode → "Unspecified"
# MAGIC
# MAGIC Enhance records with a LoadDate audit column.
# MAGIC
# MAGIC Apply SCD Type 1 logic:
# MAGIC
# MAGIC Update latest records without maintaining historical versions
# MAGIC
# MAGIC Merge data into Delta Lake at: dbfs:/FileStore/Silver/Payments
# MAGIC
# MAGIC 🧹 Key Features
# MAGIC Ensures payment data is accurate, typed, and deduplicated
# MAGIC
# MAGIC Null values are replaced with safe defaults
# MAGIC
# MAGIC Maintains only the current/latest state of each payment record
# MAGIC
# MAGIC This notebook ensures the Payments data is accurate, cleaned, and analytics-ready, powering dashboards for revenue, refund analysis, and customer payment patterns.

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
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Payments.csv"
silver_path = "dbfs:/FileStore/Silver/Payments"

# COMMAND ----------

# 3. Read from Bronze Layer
df_payments_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_payments_clean = (
    df_payments_raw
    .dropDuplicates(["PaymentID"])
    .withColumn("PaymentAmount", col("PaymentAmount").cast("double"))
    .withColumn("PaymentDate", to_timestamp("PaymentDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .fillna({
        "PaymentAmount": 0.0
    })
    .withColumn("LoadDate", load_date)
)


# COMMAND ----------


# 5. Write Initial Table (run once)
#df_payments_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------


# 6. Merge (SCD Type 1 - overwrite changes)
delta_payments = DeltaTable.forPath(spark, silver_path)

delta_payments.alias("tgt").merge(
    df_payments_clean.alias("src"),
    "tgt.PaymentID = src.PaymentID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()