# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Schema
# MAGIC
# MAGIC Creates the demo schema under Unity Catalog and grants demo users access.

# COMMAND ----------

dbutils.widgets.text("catalog", "dante_azure_sea")
dbutils.widgets.text("schema", "qris_digital_banking_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
fqs = f"{catalog}.{schema}"

print(f"Setting up schema: {fqs}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqs} COMMENT 'Indonesian retail bank QRIS digital banking demo'")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

spark.sql(f"DESCRIBE SCHEMA EXTENDED {fqs}").show(truncate=False)
