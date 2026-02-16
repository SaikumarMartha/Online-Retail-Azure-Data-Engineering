# Databricks notebook source

# Read secrets securely from Databricks secret scope
client_id = dbutils.secrets.get(scope="retail-scope", key="databricks-client-id")
client_secret = dbutils.secrets.get(scope="retail-scope", key="databricks-client-secret")
tenant_id = dbutils.secrets.get(scope="retail-scope", key="databricks-tenant-id")

# (Optional validation – DO NOT print secrets in production)
assert client_id is not None
assert client_secret is not None
assert tenant_id is not None


# COMMAND ----------

storage_account_name = "stonlineretaildatalake01"

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set(
    "fs.azure.account.oauth.provider.type",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id",
    client_id
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret",
    client_secret
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint",
    f"https://login.microsoftonline.com/c1ab7c82-6132-407d-9423-a8d3d9e36e57/oauth2/token"
)


# COMMAND ----------

# Root container path
adls_root_path = "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/"

# List files/folders to validate access
dbutils.fs.ls(adls_root_path)


# COMMAND ----------

raw_retail_path = (
    "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/raw/sql/online_retail"
)

dbutils.fs.ls(raw_retail_path)


# COMMAND ----------

df_test = spark.read.option("header", True).csv(
    f"{raw_retail_path}/*.csv"
)

df_test.limit(5).display()


# COMMAND ----------

raw_path = "abfss://datalake@stonlineretaildatalake01.dfs.core.windows.net/raw/sql/online_retail"

files = dbutils.fs.ls(raw_path)

for f in files:
    if f.name.endswith(".csv") and not f.name.startswith("online_retail_"):
        new_name = f"{raw_path}/online_retail_2026-01-25.csv"
        dbutils.fs.mv(f.path, new_name)
        print(f"Renamed {f.name} -> online_retail_2026-01-25.csv")
