# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType



# COMMAND ----------

# Raw data path (from nb_mount_adls)
raw_path = f"abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/raw/sql/online_retail"


# Bronze layer path
bronze_path = f"abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/curated/sales/online_retail"

# COMMAND ----------



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

retail_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])


# COMMAND ----------

# Check the path to confirm it exists
display(
    dbutils.fs.ls(
        "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/raw/sql/online_retail"
    )
)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df_clean = df_raw.withColumn(
    "InvoiceTimestamp",
    to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
)


# COMMAND ----------

df_raw = spark.read.csv(
    path=f"{raw_path}/*.csv",
    header=True,
    schema=retail_schema
)

df_raw.limit(5).display()
df_raw.select("InvoiceDate").show(5, truncate=False)
df_raw.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df_raw = df_raw.withColumn(
    "InvoiceTimestamp",
    to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
)


# COMMAND ----------

df_raw.select("InvoiceDate", "InvoiceTimestamp").show(10, truncate=False)


# COMMAND ----------

df_raw.selectExpr(
    "count(*) total",
    "sum(case when InvoiceTimestamp is null then 1 else 0 end) bad_rows"
).show()


# COMMAND ----------

df_raw.select('InvoiceDate').show(5, truncate=False)

# COMMAND ----------


#Data Cleaning
from pyspark.sql.functions import col

df_clean = (
    df_raw
    .filter(col("CustomerID").isNotNull())
    .filter(col("Quantity") > 0)
    .filter(col("InvoiceDate").isNotNull())
    .filter(col("UnitPrice") > 0)
)
# Add Business Column
from pyspark.sql.functions import expr

df_clean = df_clean.withColumn(
    "TotalAmount",
    expr("Quantity * UnitPrice")
)


# COMMAND ----------


#Create Dimension Tables
# Dim Customer
dim_customer = (
    df_clean
    .select("CustomerID", "Country")
    .dropDuplicates()
)
#DimProduct
dim_product = (
    df_clean
    .select("StockCode", "Description", "UnitPrice")
    .dropDuplicates()
)
#DimDate
from pyspark.sql.functions import to_date

dim_date = (
    df_clean
    .withColumn("InvoiceDate", to_date("InvoiceDate","M/d/yyyy H:mm"))
    .select("InvoiceDate")
    .dropDuplicates()
)



# COMMAND ----------

# DBTITLE 1,Cell 14
#Create Fact Tables

fact_sales = df_clean.select(
    "InvoiceNo",
    "CustomerID",
    "StockCode",
    col("InvoiceTimestamp").alias("InvoiceDate"),  # Use the timestamp column
    "Quantity",
    "UnitPrice",
    "TotalAmount"
)


# COMMAND ----------

fact_sales.printSchema()
fact_sales.select("InvoiceNo", "InvoiceDate", "CustomerID", "TotalAmount").show(5, truncate=False)

# COMMAND ----------

storage_account = "stonlineretaildatalake01"

# Fact table
fact_sales_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/curated/sales/online_retail_sales"

# Dimensions
dim_customer_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/curated/dimensions/customer"
dim_product_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/curated/dimensions/product"
dim_date_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/curated/dimensions/date"


# COMMAND ----------

# Assume fact_sales is created in Step 3.6
fact_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(fact_sales_path)

print("Fact table written to curated layer successfully.")


# COMMAND ----------

# Customer Dimension
dim_customer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_customer_path)

# Product Dimension
dim_product.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_product_path)

# Date Dimension
dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_date_path)

print("Dimension tables written to curated layer successfully.")


# COMMAND ----------

#Validation
print("Fact Table Row Count:", fact_sales.count())
print("Customer Dimension Row Count:", dim_customer.count())
print("Product Dimension Row Count:", dim_product.count())
print("Date Dimension Row Count:", dim_date.count())

# Preview some data
fact_sales.limit(5).display()
dim_customer.limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC Writing to Presentation layer 

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

df_products.limit(5).display()
df_customers.limit(5).display()
df_sales.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Business question: What is the total sales by product for each month?

# COMMAND ----------

# DBTITLE 1,Cell 23
from pyspark.sql.functions import month, year, sum, col

# Join using StockCode and use InvoiceDate directly (now timestamp)
df_sales_summary = (
    df_sales
    .join(df_products, df_sales.StockCode == df_products.StockCode, "left")
    .groupBy(
        year("InvoiceDate").alias("Year"),
        month("InvoiceDate").alias("Month"),
        df_products.Description.alias("ProductName")
    )
    .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum("TotalAmount").alias("TotalSales")
    )
)
df_sales_summary.limit(5).display()

# COMMAND ----------

presentation_path = "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/presentation/reporting/monthly_sales_by_product"

df_sales_summary.write.format("delta").mode("overwrite").save(presentation_path)
