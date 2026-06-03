# Databricks notebook source
# MAGIC %md
# MAGIC # QRIS Digital Banking — Lakeflow Declarative Pipeline
# MAGIC
# MAGIC Bronze → Silver → Gold transformations.
# MAGIC
# MAGIC Inputs come from `raw_*` tables produced by `01_generate_synthetic_data.py`.
# MAGIC Outputs are streaming tables + materialized views in the configured catalog/schema.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("bundle.catalog")
schema  = spark.conf.get("bundle.schema")
fqs     = f"{catalog}.{schema}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — raw landed copies (passthrough with ingest metadata)

# COMMAND ----------

@dlt.table(name="bronze_customers", comment="Raw customer master", table_properties={"quality": "bronze"})
def bronze_customers():
    return (spark.read.table(f"{fqs}.raw_customers")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_accounts", comment="Raw account balances", table_properties={"quality": "bronze"})
def bronze_accounts():
    return (spark.read.table(f"{fqs}.raw_accounts")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_merchants", comment="Raw QRIS merchants", table_properties={"quality": "bronze"})
def bronze_merchants():
    return (spark.read.table(f"{fqs}.raw_merchants")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_qris_transactions", comment="Raw QRIS payment events", table_properties={"quality": "bronze"})
def bronze_qris_transactions():
    return (spark.read.table(f"{fqs}.raw_qris_transactions")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_bifast_transfers", comment="Raw BI-Fast transfers", table_properties={"quality": "bronze"})
def bronze_bifast_transfers():
    return (spark.read.table(f"{fqs}.raw_bifast_transfers")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_mobile_sessions", comment="Raw mobile sessions", table_properties={"quality": "bronze"})
def bronze_mobile_sessions():
    return (spark.read.table(f"{fqs}.raw_mobile_sessions")
            .withColumn("_ingested_at", F.current_timestamp()))

@dlt.table(name="bronze_app_events", comment="Raw in-app events", table_properties={"quality": "bronze"})
def bronze_app_events():
    return (spark.read.table(f"{fqs}.raw_app_events")
            .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — cleansed, deduplicated, with data-quality expectations

# COMMAND ----------

@dlt.table(name="customers", comment="Cleansed customer dimension", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_customer_id",  "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_age",          "age BETWEEN 17 AND 100")
@dlt.expect_or_drop("valid_segment",      "segment IN ('Mass','Mass Affluent','Affluent','Priority','Private')")
def customers():
    return (
        dlt.read("bronze_customers")
           .select("customer_id","full_name","nik","phone","gender","city","province",
                   "segment","occupation","opening_date","date_of_birth","age",
                   "digital_active","kyc_status")
    )

@dlt.table(name="accounts", comment="Cleansed accounts", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_balance", "balance_idr >= 0")
@dlt.expect_or_drop("valid_type",    "account_type IN ('Savings','Giro','Deposito')")
def accounts():
    return dlt.read("bronze_accounts").select(
        "account_id","customer_id","account_type","balance_idr","opened_date","status"
    )

@dlt.table(name="merchants", comment="QRIS merchant dimension", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dlt.expect_or_drop("valid_tier",        "merchant_tier IN ('UMI','UKE','UMK','UBE')")
def merchants():
    return dlt.read("bronze_merchants").select(
        "merchant_id","merchant_name","mcc","mcc_description","merchant_tier",
        "city","province","onboarding_date","qris_static_or_dynamic"
    )

@dlt.table(name="qris_transactions",
           comment="QRIS payment fact (Indonesian retail QRIS, MPM + CPM)",
           table_properties={"quality": "silver"},
           partition_cols=["txn_date"])
@dlt.expect_or_drop("valid_amount", "amount_idr > 0")
@dlt.expect("status_known",         "status IN ('SUCCESS','FAILED','REVERSED')")
def qris_transactions():
    return (
        dlt.read("bronze_qris_transactions")
           .select("transaction_id","customer_id","merchant_id","mcc","merchant_tier",
                   "amount_idr","transaction_ts","txn_date","status","qris_type",
                   "issuer_bank","acquirer_bank")
    )

@dlt.table(name="bifast_transfers", comment="BI-Fast transfer fact",
           table_properties={"quality": "silver"}, partition_cols=["transfer_date"])
@dlt.expect_or_drop("valid_amount",  "amount_idr > 0")
@dlt.expect("status_known",          "status IN ('SUCCESS','FAILED')")
@dlt.expect("direction_known",       "direction IN ('IN','OUT')")
def bifast_transfers():
    return dlt.read("bronze_bifast_transfers").select(
        "transfer_id","customer_id","amount_idr","direction","counterparty_bank","status",
        "transfer_ts","transfer_date"
    )

@dlt.table(name="mobile_sessions", comment="Mobile-app session fact",
           table_properties={"quality": "silver"}, partition_cols=["session_date"])
@dlt.expect("nonzero_duration",  "duration_sec > 0")
def mobile_sessions():
    return dlt.read("bronze_mobile_sessions").select(
        "session_id","customer_id","login_ts","duration_sec","screens_visited",
        "device_os","app_version","session_date"
    )

@dlt.table(name="app_events", comment="In-app event fact",
           table_properties={"quality": "silver"}, partition_cols=["session_date"])
def app_events():
    return dlt.read("bronze_app_events").select(
        "event_id","customer_id","session_id","event_type","event_ts","session_date"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — aggregates that power the dashboard and Genie

# COMMAND ----------

@dlt.table(name="daily_customer_metrics",
           comment="Per-customer per-day KPIs (QRIS + BI-Fast + sessions)",
           table_properties={"quality": "gold"})
def daily_customer_metrics():
    qris = (
        dlt.read("qris_transactions")
           .filter(F.col("status") == "SUCCESS")
           .groupBy("customer_id", F.col("txn_date").alias("date"))
           .agg(
               F.count("*").alias("qris_txn_count"),
               F.sum("amount_idr").alias("qris_amount_idr"),
               F.countDistinct("merchant_id").alias("unique_merchants"),
               F.countDistinct("mcc").alias("unique_mcc"),
           )
    )
    bifast = (
        dlt.read("bifast_transfers")
           .filter(F.col("status") == "SUCCESS")
           .groupBy("customer_id", F.col("transfer_date").alias("date"))
           .agg(
               F.count("*").alias("bifast_txn_count"),
               F.sum(F.when(F.col("direction") == "OUT", F.col("amount_idr")).otherwise(F.lit(0))).alias("bifast_out_idr"),
               F.sum(F.when(F.col("direction") == "IN",  F.col("amount_idr")).otherwise(F.lit(0))).alias("bifast_in_idr"),
           )
    )
    sessions = (
        dlt.read("mobile_sessions")
           .groupBy("customer_id", F.col("session_date").alias("date"))
           .agg(
               F.count("*").alias("session_count"),
               F.sum("duration_sec").alias("total_duration_sec"),
               F.avg("screens_visited").alias("avg_screens_visited"),
           )
    )
    return (
        qris.join(bifast, ["customer_id","date"], "fullouter")
            .join(sessions, ["customer_id","date"], "fullouter")
            .fillna(0)
    )

@dlt.table(name="daily_merchant_metrics",
           comment="Per-merchant per-day KPIs",
           table_properties={"quality": "gold"})
def daily_merchant_metrics():
    return (
        dlt.read("qris_transactions")
           .filter(F.col("status") == "SUCCESS")
           .groupBy("merchant_id", F.col("txn_date").alias("date"))
           .agg(
               F.count("*").alias("qris_txn_count"),
               F.sum("amount_idr").alias("qris_amount_idr"),
               F.countDistinct("customer_id").alias("unique_customers"),
               F.avg("amount_idr").alias("avg_ticket_idr"),
           )
    )

@dlt.table(name="customer_overview",
           comment="Wide customer view joining profile + 30-day rolling KPIs",
           table_properties={"quality": "gold"})
def customer_overview():
    last30 = F.expr("date >= date_sub(current_date(), 30)")
    rolling = (
        dlt.read("daily_customer_metrics")
           .filter(last30)
           .groupBy("customer_id")
           .agg(
               F.sum("qris_txn_count").alias("qris_txn_30d"),
               F.sum("qris_amount_idr").alias("qris_amount_30d_idr"),
               F.sum("bifast_txn_count").alias("bifast_txn_30d"),
               F.sum("session_count").alias("session_count_30d"),
               F.countDistinct("date").alias("active_days_30d"),
           )
    )
    acct_summary = (
        dlt.read("accounts")
           .groupBy("customer_id")
           .agg(
               F.sum("balance_idr").alias("total_balance_idr"),
               F.sum(F.when(F.col("account_type") == "Savings",  F.col("balance_idr")).otherwise(0)).alias("savings_balance_idr"),
               F.sum(F.when(F.col("account_type") == "Giro",     F.col("balance_idr")).otherwise(0)).alias("giro_balance_idr"),
               F.sum(F.when(F.col("account_type") == "Deposito", F.col("balance_idr")).otherwise(0)).alias("deposito_balance_idr"),
               F.count("*").alias("num_accounts"),
           )
    )
    return (
        dlt.read("customers")
           .join(acct_summary, "customer_id", "left")
           .join(rolling, "customer_id", "left")
           .fillna(0)
    )
