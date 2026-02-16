# Databricks notebook source

# Read secrets securely from Databricks secret scope
client_id = dbutils.secrets.get(scope="retail-scope", key="databricks-client-id")
client_secret = dbutils.secrets.get(scope="retail-scope", key="databricks-client-secret")
tenant_id = dbutils.secrets.get(scope="retail-scope", key="databricks-tenant-id")

# (Optional validation – DO NOT print secrets in production)
assert client_id is not None
assert client_secret is not None
assert tenant_id is not None
spark.conf.set("fs.azure.account.auth.type.stonlineretaildatalake01.dfs.core.windows.net", "OAuth")
spark.conf.set(
    "fs.azure.account.oauth.provider.type.stonlineretaildatalake01.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.stonlineretaildatalake01.dfs.core.windows.net",
    client_id
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.stonlineretaildatalake01.dfs.core.windows.net",
    client_secret
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.stonlineretaildatalake01.dfs.core.windows.net",
    f"https://login.microsoftonline.com/c1ab7c82-6132-407d-9423-a8d3d9e36e57/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Config & Paths

# COMMAND ----------

storage_account = "stonlineretaildatalake01"
container = "datalake"

raw_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/sql/online_retail"
curated_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/curated"

fact_path = f"{curated_path}/sales/online_retail_sales"
dim_customer_path = f"{curated_path}/dimensions/customer"
dim_product_path = f"{curated_path}/dimensions/product"
dim_date_path = f"{curated_path}/dimensions/date"
log_path = f"{curated_path}/pipeline_logs"

watermark_path = f"{curated_path}/_control/incremental_watermark"

print("Raw Path:", raw_path)
print("Fact Path:", fact_path)
print("Customer Dim Path:", dim_customer_path)
print("Product Dim Path:", dim_product_path)
print("Date Dim Path:", dim_date_path)
print("Watermark Path:", watermark_path)


# COMMAND ----------

# MAGIC %md
# MAGIC Read Raw Data + Filter Incremental Files

# COMMAND ----------

# DBTITLE 1,Define run date
from datetime import datetime

# Define the run date for incremental processing
# Format: YYYY-MM-DD to match the file naming pattern
run_date = datetime.now().strftime("%Y-%m-%d")

print(f"Processing data for date: {run_date}")

# COMMAND ----------

from pyspark.sql.functions import input_file_name, regexp_extract

df_raw = spark.read.option("header", True).csv(f"{raw_path}/*.csv")

df_raw = df_raw.withColumn(
    "file_date",
    regexp_extract(input_file_name(), r"(\d{4}-\d{2}-\d{2})", 1)
)

df_raw_inc = df_raw.filter(df_raw.file_date == run_date)

print("Incremental record count:", df_raw_inc.count())
display(df_raw_inc.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC Clean, Parse, Deduplicate

# COMMAND ----------

# DBTITLE 1,Cell 8
from pyspark.sql.functions import col, to_timestamp

df_clean = (
    df_raw_inc
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
    .withColumn("Quantity", col("Quantity").cast("int"))
    .withColumn("UnitPrice", col("UnitPrice").cast("double"))
    .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
    .dropDuplicates(["InvoiceNo", "StockCode", "CustomerID"])
    .filter(col("InvoiceDate").isNotNull())
)

print("Clean record count:", df_clean.count())
display(df_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Merge into FACT Table (Delta Incremental Load)

# COMMAND ----------

from delta.tables import DeltaTable

delta_fact = DeltaTable.forPath(spark, fact_path)

(
    delta_fact.alias("t")
    .merge(
        df_clean.alias("s"),
        "t.InvoiceNo = s.InvoiceNo AND t.StockCode = s.StockCode"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("FACT table merge completed.")


# COMMAND ----------

# MAGIC %md
# MAGIC Prepare Customer Dimension Updates (SCD Type 2 Input)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

df_customer_updates = (
    df_clean
    .select("CustomerID", "Country")
    .dropDuplicates(["CustomerID"])
    .withColumn("start_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

display(df_customer_updates)


# COMMAND ----------

# MAGIC %md
# MAGIC Apply SCD Type 2 Logic to dim_customer

# COMMAND ----------

# DBTITLE 1,Cell 14
delta_dim = DeltaTable.forPath(spark, dim_customer_path)

(
    delta_dim.alias("t")
    .merge(
        df_customer_updates.alias("s"),
        "t.CustomerID = s.CustomerID"
    )
    .whenMatchedUpdate(
        set={
            "Country": "s.Country"
        }
    )
    .whenNotMatchedInsert(
        values={
            "CustomerID": "s.CustomerID",
            "Country": "s.Country"
        }
    )
    .execute()
)

print("Customer dimension merge completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Apply SCD Type 2 Logic to dim_product

# COMMAND ----------

# DBTITLE 1,Prepare product updates
df_products_updates = (
    df_clean
    .select("StockCode", "Description")
    .dropDuplicates(["StockCode"])
)

print(f"Product updates count: {df_products_updates.count()}")
display(df_products_updates.limit(10))

# COMMAND ----------

dim_product_delta = DeltaTable.forPath(spark, dim_product_path)

(
    dim_product_delta.alias("t")
    .merge(
        df_products_updates.alias("s"),
        "t.StockCode = s.StockCode"
    )
    .whenMatchedUpdate(set={
        "Description": "s.Description"
    })
    .whenNotMatchedInsert(values={
        "StockCode": "s.StockCode",
        "Description": "s.Description"
    })
    .execute()
)

print("dim_product merge completed successfully.")


# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline Logging

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

log_df = spark.createDataFrame(
    [(run_date, "nb_retail_incremental_load", "SUCCESS",)],
    ["run_date", "pipeline", "status"]
).withColumn("timestamp", current_timestamp())

log_df.write.format("delta").mode("append").save(log_path)

print("Pipeline log written.")


# COMMAND ----------

# MAGIC %md
# MAGIC Validation (Optional)

# COMMAND ----------

# DBTITLE 1,Cell 18
print("FACT count:")
spark.read.format("delta").load(fact_path).count()

print("DIM CUSTOMER (latest rows):")
display(
    spark.read.format("delta")
    .load(dim_customer_path)
    .orderBy("CustomerID")
    .limit(20)
)