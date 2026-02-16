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

from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, sum, col

spark = SparkSession.builder.getOrCreate()

# Read curated fact table
df_sales = spark.read.format("delta").load("abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/curated/sales/online_retail_sales")

# Read dimension tables
df_products = spark.read.format("delta").load("abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/curated/dimensions/product")
df_customers = spark.read.format("delta").load("abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/curated/dimensions/customer")

# COMMAND ----------

from pyspark.sql.functions import col

df_sales.filter(col("InvoiceDate").isNull()).display()


# COMMAND ----------

df_products.limit(5).display()
df_customers.limit(5).display()
df_sales.limit(100).display()

# COMMAND ----------

df_monthly_country = (
    df_sales
    .join(df_customers, "CustomerID")
    .groupBy(
        year("InvoiceDate").alias("Year"),
        month("InvoiceDate").alias("Month"),
        df_customers.Country
    )
    .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum("TotalAmount").alias("TotalSales")
    )
    .orderBy("Year", "Month", "Country")
)

df_monthly_country.display()


# COMMAND ----------

country_report_path = "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/presentation/reporting/monthly_sales_by_country"

df_monthly_country.write.format("delta").mode("overwrite").save(country_report_path)
#Visualization
df_monthly_country.groupBy("Country").agg(sum("TotalSales").alias("Sales")) \
    .orderBy(col("Sales").desc()).display()

# COMMAND ----------

df_sales.selectExpr("max(InvoiceDate) as last_loaded").display()
