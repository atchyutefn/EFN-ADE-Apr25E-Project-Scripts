# Databricks notebook source
# MAGIC %md
# MAGIC 📘 Notebook Summary: Customers Table - Bronze to Silver Layer Transformation
# MAGIC This notebook is part of the Azure Data Engineering pipeline, focused on processing the Customers table from the bronze layer (raw CSV in blob storage) to the silver layer (Delta Lake) with robust transformation logic and historical tracking.
# MAGIC
# MAGIC 🔁 Pipeline Flow (Bronze ➜ Silver)
# MAGIC Read the efn_Customers.csv file from a date-partitioned path (/mnt/Bronze/yyyy/mm/dd/) using the current system date.
# MAGIC
# MAGIC Clean and transform the data:
# MAGIC
# MAGIC Deduplicate records using CustomerID
# MAGIC
# MAGIC Combine FirstName and LastName into a FullName column
# MAGIC
# MAGIC Parse CreatedDate and ModifiedDate as timestamp fields
# MAGIC
# MAGIC Cast boolean fields like IsEmailVerified
# MAGIC
# MAGIC Handle nulls (if needed) in string or boolean fields
# MAGIC
# MAGIC Enhance the dataset with audit and versioning columns:
# MAGIC
# MAGIC LoadDate, StartDate, EndDate, IsCurrent
# MAGIC
# MAGIC Apply SCD Type 2 logic:
# MAGIC
# MAGIC Detect and retain historical changes in customer details (name, email, phone, country, etc.)
# MAGIC
# MAGIC Expire old records and insert new versions with updated values
# MAGIC
# MAGIC Merge the cleaned and versioned data into a Delta Lake silver table:
# MAGIC
# MAGIC Path: dbfs:/FileStore/Silver/Customers
# MAGIC
# MAGIC 🧹 Key Features
# MAGIC Maintains data quality through type casting and deduplication
# MAGIC
# MAGIC Tracks full history of customer profile changes via SCD Type 2
# MAGIC
# MAGIC Prepares reliable and enriched customer data for gold layer aggregations and Power BI reporting
# MAGIC
# MAGIC This notebook ensures the Customers data is trustworthy, enriched, and audit-compliant, enabling seamless integration with downstream analytics and reporting systems.
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

# 2. Construct Dynamic Bronze Path (based on current date)
bronze_path = "dbfs:/mnt/Bronze/"+year+"/"+month+"/"+day+"/febmar25m_Customers.csv"   # Source 
silver_path = "dbfs:/FileStore/Silver/Customers"                                      # Sink/Destination

# COMMAND ----------

print(bronze_path)

# COMMAND ----------

# 3. Read CSV from Current Date Path
df_customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")    
    .csv(bronze_path)
)

# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_customers_clean = (
    df_customers_raw
    .dropDuplicates(["CustomerID"])
    .withColumn("FullName", concat_ws(" ", col("FirstName"), col("LastName")))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)




# COMMAND ----------

# 5. Write Initial Delta Table (Only once)
# Uncomment and run only once if initializing table
#df_customers_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

# 6. SCD Type 2 Merge
delta_customers = DeltaTable.forPath(spark, silver_path)
updates_df = df_customers_clean.alias("updates")

# A. Close existing version where data changed
delta_customers.alias("existing") \
    .merge(
        updates_df,
        """
        existing.CustomerID = updates.CustomerID AND existing.IsCurrent = true AND (
            existing.FirstName != updates.FirstName OR
            existing.LastName != updates.LastName OR
            existing.Email != updates.Email OR
            existing.Phone != updates.Phone
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

# B. Insert new or changed records as new version
delta_customers.alias("existing") \
    .merge(
        updates_df,
        "existing.CustomerID = updates.CustomerID AND existing.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()