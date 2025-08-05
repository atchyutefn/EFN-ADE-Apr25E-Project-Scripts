# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Reviews Table - Bronze to Silver Layer Transformation
# MAGIC This notebook processes the Reviews table from the bronze layer (raw CSV in blob storage) to the silver layer (Delta Lake), ensuring clean and enriched user feedback data.
# MAGIC
# MAGIC 🔁 Pipeline Flow:
# MAGIC Reads today’s efn_Reviews.csv from:
# MAGIC /mnt/Bronze/yyyy/mm/dd/
# MAGIC
# MAGIC Cleans and prepares the data:
# MAGIC
# MAGIC Handles nulls for Comment, Rating
# MAGIC
# MAGIC Casts Rating to integer
# MAGIC
# MAGIC Trims text in Comment
# MAGIC
# MAGIC Parses timestamp fields
# MAGIC
# MAGIC Adds transformation:
# MAGIC
# MAGIC Derives Sentiment based on Rating (Positive/Neutral/Negative)
# MAGIC
# MAGIC Uses SCD Type 1 logic to always reflect latest version of each review
# MAGIC
# MAGIC Merges into Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/Reviews
# MAGIC
# MAGIC 🧹 Key Features:
# MAGIC Helps downstream systems understand user satisfaction
# MAGIC
# MAGIC Enables Power BI filters by rating/sentiment
# MAGIC
# MAGIC Stores clean, enriched review content

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
bronze_path = f"dbfs:/mnt/Bronze/{year}/{month}/{day}/febmar25m_Reviews.csv"
silver_path = "dbfs:/FileStore/Silver/Reviews"

# 3. Read Bronze CSV
df_reviews_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# 4. Clean & Transform
load_date = current_timestamp()

df_reviews_clean = (
    df_reviews_raw
    .dropDuplicates(["ReviewID"])
    .fillna({
        "Comment": "No comment provided",
        "Rating": 0
    })
    .withColumn("Rating", col("Rating").cast("int"))
    .withColumn("Comment", trim(col("Comment")))
    .withColumn("Sentiment",
        when(col("Rating") >= 4, lit("Positive"))
       .when(col("Rating") == 3, lit("Neutral"))
       .otherwise(lit("Negative"))
    )
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
)

# 5. Write Initial Table (Run Once)
#df_reviews_clean.write.format("delta").mode("overwrite").save(silver_path)

# 6. Merge Using SCD Type 1
delta_reviews = DeltaTable.forPath(spark,silver_path)

delta_reviews.alias("tgt").merge(
    df_reviews_clean.alias("src"),
    "tgt.ReviewID = src.ReviewID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()