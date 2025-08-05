# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Promotions Table - Bronze to Silver Layer Transformation
# MAGIC This notebook transforms the Promotions table from the raw CSV format in the bronze layer to the Delta Lake silver layer, enriching and cleaning marketing campaign data.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today’s efn_Promotions.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and transforms:
# MAGIC
# MAGIC Fills missing PromotionName, Discount
# MAGIC
# MAGIC Casts Discount to double
# MAGIC
# MAGIC Converts timestamps
# MAGIC
# MAGIC Flags active promotions (IsActive)
# MAGIC
# MAGIC Applies SCD Type 1 logic to reflect only the latest campaign details
# MAGIC
# MAGIC Merges into silver table at:
# MAGIC dbfs:/FileStore/Silver/Promotions
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Supports promotion tracking and performance analysis
# MAGIC
# MAGIC Real-time flag for currently active campaigns
# MAGIC
# MAGIC Clean, deduplicated, up-to-date records for reporting
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
bronze_path = f"dbfs:/mnt/Bronze/{year}/07/29/febmar25m_Promotions.csv"
silver_path = "dbfs:/FileStore/Silver/Promotions"

# 3. Read from Bronze
df_promotions_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean & Transform
load_date = current_timestamp()

df_promotions_clean = (
    df_promotions_raw
    .dropDuplicates(["PromotionID"])
    .fillna({
        "PromotionName": "Unnamed Promo",
        "Discount": 0.0
    })
    .withColumn("Discount", col("Discount").cast("double"))
    .withColumn("StartDate", to_timestamp("StartDate"))
    .withColumn("EndDate", to_timestamp("EndDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: flag active promotions
    .withColumn("IsActive", when((current_date() >= to_date("StartDate")) & 
                                 (current_date() <= to_date("EndDate")), lit(True)).otherwise(lit(False)))

    .withColumn("LoadDate", load_date)
)

# 5. Write Initial Table (only once)
#df_promotions_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge to Silver (SCD Type 1)
delta_promotions = DeltaTable.forPath(spark,silver_path)

delta_promotions.alias("tgt").merge(
    df_promotions_clean.alias("src"),
    "tgt.PromotionID = src.PromotionID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()