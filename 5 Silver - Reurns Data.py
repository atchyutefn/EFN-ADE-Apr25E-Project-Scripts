# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Returns Table - Bronze to Silver Layer Transformation
# MAGIC This notebook transforms the Returns table from the bronze layer (CSV in blob storage) to the silver layer (Delta Lake) with SCD Type 2 logic and business-friendly enrichments.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today's efn_Returns.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and parses the data:
# MAGIC
# MAGIC Fills null ReturnReason with "Not Specified"
# MAGIC
# MAGIC Casts timestamps
# MAGIC
# MAGIC Removes duplicate ReturnIDs
# MAGIC
# MAGIC Adds derived column:
# MAGIC
# MAGIC ReturnAgeDays = Days since return occurred
# MAGIC
# MAGIC Applies SCD Type 2 logic to maintain history of return changes
# MAGIC
# MAGIC Writes/merges into the Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/Returns
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Tracks return reason and changes over time
# MAGIC
# MAGIC Adds actionable fields for business reporting (ReturnAgeDays)
# MAGIC
# MAGIC Fully SCD2-compliant for historical auditability

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
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Returns.csv"
silver_path = "dbfs:/FileStore/Silver/Returns"

# 3. Read CSV from Bronze
df_returns_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean, Transform, Add Derived Columns
load_date = current_timestamp()

df_returns_clean = (
    df_returns_raw
    .dropDuplicates(["ReturnID"])
    .fillna({
        "ReturnReason": "Not Specified",
    })
    .withColumn("ReturnDate", to_timestamp("ReturnDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Example Transformation: return age in days
    .withColumn("ReturnAgeDays", datediff(current_date(), to_date("ReturnDate")))

    # Add audit & SCD2 columns
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)

# 5. Write Initial Delta Table (run only once to initialize)
#df_returns_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge into Silver Table (SCD Type 2)
delta_returns = DeltaTable.forPath(spark, silver_path)

delta_returns.alias("existing") \
    .merge(
        df_returns_clean.alias("updates"),
        """
        existing.ReturnID = updates.ReturnID AND existing.IsCurrent = true AND (
            existing.ReturnReason != updates.ReturnReason OR
            existing.ProductID != updates.ProductID OR
            existing.OrderID != updates.OrderID
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

delta_returns.alias("existing") \
    .merge(
        df_returns_clean.alias("updates"),
        "existing.ReturnID = updates.ReturnID AND existing.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()