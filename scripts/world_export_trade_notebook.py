# Databricks notebook source
# Read credentials from Databricks Secret Scope (never hardcode!)
tenant_id      = "d95edf1e-2c7d-42ee-8e84-163229f87133"
client_id      = "a5684ec6-081e-4fed-8f84-13ecbecc3081"
client_secret  = "kit8Q~A1bfdOnkQM38GCWxbPwkcDbBq5s4wDAbsC"
storage_account = "worldexportsdata"

# Configure Spark to access ADLS Gen2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Now read directly using the full path
RAW_CONTAINER = "raw"
CLEANED_CONTAINER = "cleaned"
PROCESSED_CONTAINER = "processed"
BASE_PATH = f"abfss://{CONTAINER}@{storage_account}.dfs.core.windows.net/"

# Example: list files
display(dbutils.fs.ls(BASE_PATH))

# COMMAND ----------

# Load raw CSV from ADLS:

raw_path = f"abfss://{RAW_CONTAINER}@{storage_account}.dfs.core.windows.net/world_exports_raw.csv"
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(raw_path)

display(df.head(10))

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Remove duplicates
df = df.dropDuplicates()

# 2. Fix quantity string errors
# We use regexp_replace to remove ' units' and cast to double
df = df.withColumn('quantity', 
                   F.regexp_replace(F.col('quantity').cast('string'), ' units', '')
                   .cast('double'))

# 3. Fix negative export values
df = df.withColumn('export_value_usd', F.abs(F.col('export_value_usd')))

# 4. Fill missing numeric values with median
# Spark doesn't have a built-in .median() in fillna, so we calculate it first
for col_name in ['export_value_usd', 'quantity', 'growth_rate_pct']:
    median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
    df = df.na.fill({col_name: median_val})

# 5. Fill missing categorical values
# Get the mode for product_category
mode_row = df.groupby('product_category').count().orderBy('count', ascending=False).first()
mode_val = mode_row['product_category'] if mode_row else 'Unknown'

df = df.na.fill({'country': 'Unknown', 'product_category': mode_val})

# 6. Standardize text
df = df.withColumn('country', F.initcap(F.trim(F.col('country'))))
df = df.withColumn('product_category', F.trim(F.col('product_category')))

# Output results
print(f"Cleaned records: {df.count()}")
# To check nulls in Spark, we aggregate the count of nulls per column
null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# COMMAND ----------

# Write the cleaned one

cleaned_path = f"abfss://{CLEANED_CONTAINER}@{storage_account}.dfs.core.windows.net/world_exports_cleaned.csv"

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(cleaned_path)

# COMMAND ----------

cleaned_path = f"abfss://{CLEANED_CONTAINER}@{storage_account}.dfs.core.windows.net/world_exports_cleaned.csv"
processed_path = f"abfss://{PROCESSED_CONTAINER}@{storage_account}.dfs.core.windows.net/world_exports_cleaned.csv"

cleaned_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(cleaned_path)


# Aggregate data for the Dashboard
processed_df = cleaned_df.groupBy("country", "year", "product_category") \
    .agg(
        F.sum("export_value_usd").alias("total_export_value"),
        F.avg("growth_rate_pct").alias("avg_growth_rate"),
        F.count("product_category").alias("transaction_count")
    )

# Save this to a 'processed' folder
processed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(processed_path)

# COMMAND ----------

processed_path = f"abfss://{PROCESSED_CONTAINER}@{storage_account}.dfs.core.windows.net/world_exports_cleaned.csv"
dashboard_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(processed_path)

dashboard_df.write \
    .mode("overwrite") \
    .saveAsTable("world_exports_dashboard")