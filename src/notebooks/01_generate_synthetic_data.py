# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Synthetic Indonesian Banking Data
# MAGIC
# MAGIC Produces raw (bronze-ready) tables for PT Bank Cendrawasih, a synthetic Indonesian retail bank.
# MAGIC
# MAGIC Tables produced (all in `<catalog>.<schema>.raw_*`):
# MAGIC - `raw_customers` — customer master with NIK, names, cities, segments
# MAGIC - `raw_accounts` — Savings / Giro / Deposito accounts with IDR balances
# MAGIC - `raw_merchants` — QRIS-registered merchants by MCC + tier (UMI/UKE/UMK/UBE)
# MAGIC - `raw_qris_transactions` — QRIS payment events (90 days history)
# MAGIC - `raw_bifast_transfers` — BI-Fast instant inter-bank transfers
# MAGIC - `raw_mobile_sessions` — mobile banking app sessions
# MAGIC - `raw_app_events` — in-app events (login, view, click, payment_init, payment_success, etc.)
# MAGIC
# MAGIC Volumes scale via widgets so the same notebook works for laptop-size and demo-size runs.

# COMMAND ----------

dbutils.widgets.text("catalog", "dante_azure_sea")
dbutils.widgets.text("schema", "qris_digital_banking_demo")
dbutils.widgets.text("rows_customers", "50000")
dbutils.widgets.text("rows_merchants", "5000")
dbutils.widgets.text("days_of_history", "90")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
fqs     = f"{catalog}.{schema}"
N_CUST  = int(dbutils.widgets.get("rows_customers"))
N_MERCH = int(dbutils.widgets.get("rows_merchants"))
DAYS    = int(dbutils.widgets.get("days_of_history"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Generating: {N_CUST:,} customers, {N_MERCH:,} merchants, {DAYS} days of history")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

SEED = 42
random.seed(SEED)

# Indonesia-flavored reference data
CITIES = [
    ("Jakarta",        "DKI Jakarta",         0.22),
    ("Surabaya",       "Jawa Timur",          0.10),
    ("Bandung",        "Jawa Barat",          0.08),
    ("Medan",          "Sumatera Utara",      0.06),
    ("Semarang",       "Jawa Tengah",         0.05),
    ("Makassar",       "Sulawesi Selatan",    0.05),
    ("Palembang",      "Sumatera Selatan",    0.04),
    ("Tangerang",      "Banten",              0.06),
    ("Depok",          "Jawa Barat",          0.05),
    ("Bekasi",         "Jawa Barat",          0.05),
    ("Denpasar",       "Bali",                0.04),
    ("Yogyakarta",     "DI Yogyakarta",       0.04),
    ("Balikpapan",     "Kalimantan Timur",    0.03),
    ("Pekanbaru",      "Riau",                0.03),
    ("Bogor",          "Jawa Barat",          0.04),
    ("Manado",         "Sulawesi Utara",      0.02),
    ("Banjarmasin",    "Kalimantan Selatan",  0.02),
    ("Pontianak",      "Kalimantan Barat",    0.02),
]
CITY_DF = spark.createDataFrame(CITIES, ["city", "province", "weight"])

FIRST_NAMES_M = ["Budi","Agus","Andi","Bambang","Dewa","Eko","Fajar","Gilang","Hadi","Iwan",
                 "Joko","Krisna","Lukman","Made","Nanda","Oki","Putra","Rendy","Sigit","Taufik",
                 "Umar","Vino","Wahyu","Yusuf","Zaki","Reza","Bayu","Galih","Harun","Indra"]
FIRST_NAMES_F = ["Siti","Ayu","Citra","Dewi","Eka","Fitri","Gita","Hana","Indah","Jihan",
                 "Kartika","Lestari","Maya","Nurul","Octa","Putri","Ratih","Sari","Tika","Umi",
                 "Vania","Wulan","Yanti","Zahra","Rini","Bunga","Cinta","Dini","Endah","Ika"]
SURNAMES = ["Wijaya","Susanto","Hartono","Pratama","Saputra","Setiawan","Nugroho","Wibowo","Hidayat","Kurniawan",
            "Halim","Tanudjaja","Gunawan","Salim","Iskandar","Mulyadi","Kusuma","Wahyudi","Sutanto","Cahyono",
            "Anwar","Sihotang","Nasution","Lubis","Simanjuntak","Tobing","Manurung","Sembiring","Sinaga","Hutapea"]

SEGMENTS  = [("Mass", 0.55), ("Mass Affluent", 0.25), ("Affluent", 0.12), ("Priority", 0.06), ("Private", 0.02)]
GENDERS   = [("M", 0.5), ("F", 0.5)]
OCCUPATIONS = [("Employee", 0.45), ("Self-Employed", 0.20), ("Civil Servant", 0.10),
               ("Student", 0.08), ("Entrepreneur", 0.07), ("Professional", 0.05),
               ("Retired", 0.03), ("Other", 0.02)]

MCC = [  # Indonesian QRIS-relevant Merchant Category Codes
    (5411, "Grocery Stores",         0.18),
    (5812, "Eating Places & Restaurants", 0.22),
    (5814, "Fast Food Restaurants",  0.14),
    (5541, "Service Stations",       0.06),
    (5912, "Drug Stores & Pharmacies", 0.05),
    (5311, "Department Stores",      0.04),
    (5651, "Clothing - Family",      0.03),
    (5994, "News Dealers & Newsstands", 0.02),
    (4111, "Transportation",         0.06),
    (5732, "Electronics Sales",      0.03),
    (5499, "Misc Food Stores",       0.05),
    (5942, "Book Stores",            0.02),
    (7230, "Beauty & Barber Shops",  0.03),
    (5814, "Fast Food",              0.04),
    (4900, "Utilities",              0.03),
]
MERCH_TIERS = [("UMI", 0.50), ("UKE", 0.30), ("UMK", 0.15), ("UBE", 0.05)]  # micro/small/medium/large

OTHER_BANKS = ["BCA","Mandiri","BNI","BRI","CIMB","Permata","Danamon","OCBC","BTPN","Jago",
               "Neo","Saqu","Allo","Seabank","Blu"]

def weighted_choice_udf(choices):
    items   = [c[0] for c in choices]
    weights = [c[-1] for c in choices]
    total = sum(weights)
    norm = [w/total for w in weights]
    def _f(seed):
        rng = random.Random(seed)
        r = rng.random()
        acc = 0.0
        for it, w in zip(items, norm):
            acc += w
            if r <= acc: return it
        return items[-1]
    return F.udf(_f, StringType())

choose_city       = weighted_choice_udf([(c, w) for c, _, w in CITIES])
choose_province   = F.udf(lambda city: next((p for c,p,_ in CITIES if c==city), "Lainnya"), StringType())
choose_segment    = weighted_choice_udf(SEGMENTS)
choose_gender     = weighted_choice_udf(GENDERS)
choose_occupation = weighted_choice_udf(OCCUPATIONS)
choose_mcc        = weighted_choice_udf([(c, w) for c, _, w in MCC])
choose_mcc_desc   = F.udf(lambda code: next((d for c,d,_ in MCC if c==code), "Other"), StringType())
choose_tier       = weighted_choice_udf(MERCH_TIERS)
choose_other_bank = F.udf(lambda seed: OTHER_BANKS[random.Random(seed).randint(0, len(OTHER_BANKS)-1)], StringType())

@F.udf(StringType())
def indonesian_name(seed):
    rng = random.Random(seed)
    gender = "M" if rng.random() < 0.5 else "F"
    first = rng.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
    if rng.random() < 0.4:
        middle = rng.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
        return f"{first} {middle} {rng.choice(SURNAMES)}"
    return f"{first} {rng.choice(SURNAMES)}"

@F.udf(StringType())
def gen_nik(seed):
    # 16-digit NIK (Indonesian national ID), structurally plausible: region(6) + DDMMYY(6) + serial(4)
    rng = random.Random(seed)
    region = f"{rng.randint(11, 94):02d}{rng.randint(1, 99):02d}{rng.randint(1, 99):02d}"
    dd = rng.randint(1, 28)
    mm = rng.randint(1, 12)
    yy = rng.randint(45, 5)  # 1945-2005 (some born in early 2000s)
    if yy < 10:
        yy_str = f"0{yy}"
    else:
        yy_str = f"{yy}"
    serial = f"{rng.randint(1, 9999):04d}"
    return f"{region}{dd:02d}{mm:02d}{yy_str}{serial}"

@F.udf(StringType())
def gen_phone(seed):
    rng = random.Random(seed)
    prefixes = ["0811","0812","0813","0821","0822","0851","0852","0853","0823","0855","0856","0857","0858","0859","0817"]
    return f"{rng.choice(prefixes)}{rng.randint(10000000, 99999999)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## customers

# COMMAND ----------

import datetime as dt

today = dt.date.today()

cust_df = (
    spark.range(1, N_CUST + 1)
        .withColumnRenamed("id", "customer_id_int")
        .withColumn("customer_id", F.concat(F.lit("C"), F.lpad(F.col("customer_id_int").cast("string"), 8, "0")))
        .withColumn("seed", F.col("customer_id_int") * F.lit(SEED))
        .withColumn("full_name", indonesian_name(F.col("seed")))
        .withColumn("nik", gen_nik(F.col("seed") + F.lit(1)))
        .withColumn("phone", gen_phone(F.col("seed") + F.lit(2)))
        .withColumn("gender", choose_gender(F.col("seed") + F.lit(3)))
        .withColumn("city", choose_city(F.col("seed") + F.lit(4)))
        .withColumn("province", choose_province(F.col("city")))
        .withColumn("segment", choose_segment(F.col("seed") + F.lit(5)))
        .withColumn("occupation", choose_occupation(F.col("seed") + F.lit(6)))
        # Customer was opened sometime in the last 8 years
        .withColumn("opening_date", F.expr(f"date_sub(current_date(), cast(rand({SEED}) * 365 * 8 as int))"))
        .withColumn("date_of_birth", F.expr(f"date_sub(current_date(), cast((20 + rand({SEED}+1) * 50) * 365 as int))"))
        .withColumn("age", (F.datediff(F.current_date(), F.col("date_of_birth")) / 365).cast("int"))
        .withColumn("digital_active",
                    F.when(F.col("segment").isin("Priority","Private","Affluent"), F.expr(f"rand({SEED}+7) < 0.95"))
                     .when(F.col("segment") == "Mass Affluent", F.expr(f"rand({SEED}+7) < 0.85"))
                     .otherwise(F.expr(f"rand({SEED}+7) < 0.60")))
        .withColumn("kyc_status", F.lit("VERIFIED"))
        .drop("seed", "customer_id_int")
)

cust_df.write.mode("overwrite").saveAsTable(f"{fqs}.raw_customers")
print(f"customers: {spark.table(f'{fqs}.raw_customers').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## accounts

# COMMAND ----------

# Each customer has 1-3 accounts. Distribution: ~all have Savings; ~25% have Giro; ~10% have Deposito.

acc_savings = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "opening_date")
        .withColumn("account_type", F.lit("Savings"))
        .withColumn("account_id", F.concat(F.lit("A-S-"), F.col("customer_id")))
        .withColumn("balance_idr",
            (F.when(F.col("segment") == "Mass", F.expr("rand() * 50000000"))      # < 50jt
              .when(F.col("segment") == "Mass Affluent", F.expr("rand() * 250000000 + 50000000"))
              .when(F.col("segment") == "Affluent", F.expr("rand() * 800000000 + 250000000"))
              .when(F.col("segment") == "Priority", F.expr("rand() * 3000000000 + 800000000"))
              .otherwise(F.expr("rand() * 20000000000 + 3000000000"))             # Private
            ).cast("decimal(18,2)"))
        .withColumn("opened_date", F.col("opening_date"))
        .withColumn("status", F.lit("ACTIVE"))
        .drop("segment", "opening_date")
)

acc_giro = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "opening_date")
        .filter(F.expr("rand(123) < 0.25"))
        .withColumn("account_type", F.lit("Giro"))
        .withColumn("account_id", F.concat(F.lit("A-G-"), F.col("customer_id")))
        .withColumn("balance_idr",
            (F.when(F.col("segment").isin("Priority","Private"), F.expr("rand() * 5000000000 + 500000000"))
              .when(F.col("segment") == "Affluent", F.expr("rand() * 1000000000 + 100000000"))
              .otherwise(F.expr("rand() * 200000000 + 10000000"))
            ).cast("decimal(18,2)"))
        .withColumn("opened_date", F.col("opening_date"))
        .withColumn("status", F.lit("ACTIVE"))
        .drop("segment", "opening_date")
)

acc_deposito = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "opening_date")
        .filter(F.expr("rand(456) < 0.10"))
        .withColumn("account_type", F.lit("Deposito"))
        .withColumn("account_id", F.concat(F.lit("A-D-"), F.col("customer_id")))
        .withColumn("balance_idr",
            (F.when(F.col("segment").isin("Priority","Private"), F.expr("rand() * 10000000000 + 1000000000"))
              .when(F.col("segment") == "Affluent", F.expr("rand() * 2000000000 + 200000000"))
              .otherwise(F.expr("rand() * 500000000 + 25000000"))
            ).cast("decimal(18,2)"))
        .withColumn("opened_date", F.col("opening_date"))
        .withColumn("status", F.lit("ACTIVE"))
        .drop("segment", "opening_date")
)

accounts_df = acc_savings.unionByName(acc_giro).unionByName(acc_deposito)
accounts_df.write.mode("overwrite").saveAsTable(f"{fqs}.raw_accounts")
print(f"accounts: {spark.table(f'{fqs}.raw_accounts').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## merchants

# COMMAND ----------

merch_df = (
    spark.range(1, N_MERCH + 1)
        .withColumnRenamed("id", "merchant_id_int")
        .withColumn("merchant_id", F.concat(F.lit("M"), F.lpad(F.col("merchant_id_int").cast("string"), 7, "0")))
        .withColumn("seed", F.col("merchant_id_int") * F.lit(SEED + 1))
        .withColumn("mcc", choose_mcc(F.col("seed")).cast(IntegerType()))
        .withColumn("mcc_description", choose_mcc_desc(F.col("mcc")))
        .withColumn("merchant_tier", choose_tier(F.col("seed") + F.lit(1)))
        .withColumn("city", choose_city(F.col("seed") + F.lit(2)))
        .withColumn("province", choose_province(F.col("city")))
        .withColumn("merchant_name",
                    F.concat(
                      F.when(F.col("mcc") == 5411, F.lit("Toko "))
                       .when(F.col("mcc") == 5812, F.lit("Resto "))
                       .when(F.col("mcc") == 5814, F.lit("Warung "))
                       .when(F.col("mcc") == 5541, F.lit("SPBU "))
                       .when(F.col("mcc") == 5912, F.lit("Apotek "))
                       .when(F.col("mcc") == 4111, F.lit("Transport "))
                       .when(F.col("mcc") == 7230, F.lit("Salon "))
                       .otherwise(F.lit("Merchant ")),
                      F.element_at(F.array([F.lit(n) for n in SURNAMES]), ((F.col("seed") % 30) + 1).cast(IntegerType()))
                    ))
        .withColumn("onboarding_date", F.expr(f"date_sub(current_date(), cast(rand({SEED}+9) * 365 * 4 as int))"))
        .withColumn("qris_static_or_dynamic",
                    F.when(F.col("merchant_tier").isin("UMI","UKE"), F.lit("static"))
                     .otherwise(F.expr(f"case when rand({SEED}+10) < 0.7 then 'dynamic' else 'static' end")))
        .drop("seed", "merchant_id_int")
)

merch_df.write.mode("overwrite").saveAsTable(f"{fqs}.raw_merchants")
print(f"merchants: {spark.table(f'{fqs}.raw_merchants').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## qris_transactions
# MAGIC
# MAGIC Volume scales with customer's segment + digital_active flag + recency.
# MAGIC Daily volume curve loosely follows a weekly pattern.

# COMMAND ----------

# Build a date dimension covering the last DAYS days
dates_df = (
    spark.range(0, DAYS)
        .withColumn("txn_date", F.expr(f"date_sub(current_date(), int(id))"))
        .drop("id")
)

# Customers expand into expected transaction counts by segment
cust_txn_propensity = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "digital_active", "city")
        .withColumn("avg_qris_per_day",
            F.when(~F.col("digital_active"), F.lit(0.0))
             .when(F.col("segment") == "Mass",          F.expr("0.3 + rand() * 0.6"))   # ~0.3-0.9 / day
             .when(F.col("segment") == "Mass Affluent", F.expr("0.7 + rand() * 1.2"))
             .when(F.col("segment") == "Affluent",      F.expr("1.0 + rand() * 1.8"))
             .when(F.col("segment") == "Priority",      F.expr("1.5 + rand() * 2.0"))
             .otherwise(                                F.expr("2.0 + rand() * 2.5")))   # Private
)

# Generate ~expected count of transactions per customer per day via Poisson approximation (cap)
# We can't easily use Poisson in pure SQL; cap with floor(propensity) plus a Bernoulli for the fractional part.
import math

# Expected total volume (rough): N_CUST * avg_propensity * DAYS
# For 50k customers, mean ~0.7 active → ~ 25k * 1.0 * 90 = 2.25M rows. OK.

cust_txns_per_day = (
    cust_txn_propensity.crossJoin(dates_df)
        .withColumn("rand_a", F.rand(SEED + 100))
        .withColumn("rand_b", F.rand(SEED + 101))
        # Day-of-week weight: weekend slightly busier
        .withColumn("dow", F.dayofweek("txn_date"))
        .withColumn("dow_mult", F.when(F.col("dow").isin(1, 7), 1.25).otherwise(1.0))
        .withColumn("expected", F.col("avg_qris_per_day") * F.col("dow_mult"))
        .withColumn("int_part",  F.floor("expected").cast("int"))
        .withColumn("frac_part", F.col("expected") - F.col("int_part"))
        .withColumn("count_today", F.col("int_part") + F.when(F.col("rand_a") < F.col("frac_part"), 1).otherwise(0))
        .filter(F.col("count_today") > 0)
        .select("customer_id", "txn_date", "count_today", "city")
)

# Explode each row by count_today
qris_rows = (
    cust_txns_per_day
        .withColumn("seq", F.expr("sequence(1, count_today)"))
        .withColumn("seq_idx", F.explode("seq"))
        .drop("seq", "count_today")
        .withColumn("txn_uuid", F.expr("uuid()"))
)

# Pick a random merchant — prefer merchant in same city ~70% of the time
merchants_idx = (
    spark.table(f"{fqs}.raw_merchants")
        .select("merchant_id", "city", "mcc", "merchant_tier")
        .withColumn("mid_rand_key", F.expr("rand(202)"))
)

# For simplicity use random merchant overall (city affinity adds skew but materially slows the join);
# city affinity will be approximated by re-weighting.
n_merchants = merchants_idx.count()

qris_with_merch = (
    qris_rows
        .withColumn("merch_idx", (F.expr(f"floor(rand({SEED}+200) * {n_merchants})") + F.lit(1)).cast("long"))
)

merchants_indexed = merchants_idx.withColumn("idx", F.row_number().over(
    __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy("merchant_id")
))

qris_joined = (
    qris_with_merch.alias("t")
        .join(merchants_indexed.alias("m"), F.col("t.merch_idx") == F.col("m.idx"), "left")
        .select(
            F.col("t.txn_uuid").alias("transaction_id"),
            F.col("t.customer_id"),
            F.col("m.merchant_id"),
            F.col("m.mcc"),
            F.col("m.merchant_tier"),
            F.col("t.txn_date"),
        )
)

# Amount distribution per MCC (rough Indonesia QRIS ticket sizes in IDR)
qris_final = (
    qris_joined
        .withColumn("amount_idr",
            (F.when(F.col("mcc") == 5411, F.expr("20000 + rand() * 250000"))
              .when(F.col("mcc") == 5812, F.expr("35000 + rand() * 250000"))
              .when(F.col("mcc") == 5814, F.expr("20000 + rand() * 100000"))
              .when(F.col("mcc") == 5541, F.expr("50000 + rand() * 500000"))
              .when(F.col("mcc") == 5912, F.expr("20000 + rand() * 200000"))
              .when(F.col("mcc") == 5311, F.expr("100000 + rand() * 2000000"))
              .when(F.col("mcc") == 5651, F.expr("150000 + rand() * 1500000"))
              .when(F.col("mcc") == 5994, F.expr("10000 + rand() * 50000"))
              .when(F.col("mcc") == 4111, F.expr("8000 + rand() * 80000"))
              .when(F.col("mcc") == 5732, F.expr("500000 + rand() * 8000000"))
              .when(F.col("mcc") == 5499, F.expr("15000 + rand() * 150000"))
              .when(F.col("mcc") == 5942, F.expr("30000 + rand() * 250000"))
              .when(F.col("mcc") == 7230, F.expr("30000 + rand() * 250000"))
              .when(F.col("mcc") == 4900, F.expr("50000 + rand() * 800000"))
              .otherwise(F.expr("20000 + rand() * 200000"))
            ).cast("decimal(18,2)"))
        .withColumn("hour_of_day",
            (F.expr("case when rand(7) < 0.7 then 8 + floor(rand(8) * 14) else floor(rand(9) * 24) end")).cast("int"))
        .withColumn("minute_of_hour", (F.expr("floor(rand(10) * 60)")).cast("int"))
        .withColumn("transaction_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("txn_date").cast("string"),
                                        F.concat(F.lpad(F.col("hour_of_day").cast("string"), 2, "0"),
                                                 F.lit(":"),
                                                 F.lpad(F.col("minute_of_hour").cast("string"), 2, "0"),
                                                 F.lit(":00")))))
        .withColumn("status",
            F.when(F.expr(f"rand({SEED}+50) < 0.985"), F.lit("SUCCESS"))
             .otherwise(F.when(F.expr(f"rand({SEED}+51) < 0.5"), F.lit("FAILED")).otherwise(F.lit("REVERSED"))))
        .withColumn("qris_type",
            F.when(F.col("merchant_tier").isin("UMI","UKE"), F.lit("MPM"))      # merchant-presented mode
             .otherwise(F.expr(f"case when rand({SEED}+52) < 0.65 then 'CPM' else 'MPM' end")))
        .withColumn("issuer_bank", F.lit("Bank Cendrawasih"))
        .withColumn("acquirer_bank",
            F.when(F.expr(f"rand({SEED}+53) < 0.65"), F.lit("Bank Cendrawasih"))
             .otherwise(choose_other_bank(F.col("transaction_id"))))
        .select("transaction_id","customer_id","merchant_id","mcc","merchant_tier",
                "amount_idr","transaction_ts","txn_date","status","qris_type",
                "issuer_bank","acquirer_bank")
)

qris_final.write.mode("overwrite").saveAsTable(f"{fqs}.raw_qris_transactions")
print(f"qris_transactions: {spark.table(f'{fqs}.raw_qris_transactions').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bifast_transfers

# COMMAND ----------

bifast_per_day = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "digital_active")
        .filter(F.col("digital_active"))
        .crossJoin(dates_df)
        .withColumn("rand_a", F.rand(SEED + 300))
        .withColumn("expected",
            F.when(F.col("segment") == "Mass",          F.lit(0.15))
             .when(F.col("segment") == "Mass Affluent", F.lit(0.30))
             .when(F.col("segment") == "Affluent",      F.lit(0.55))
             .when(F.col("segment") == "Priority",      F.lit(0.85))
             .otherwise(                                F.lit(1.10)))
        .filter(F.col("rand_a") < F.col("expected"))
        .withColumn("transfer_id", F.expr("uuid()"))
        .withColumn("amount_idr",
            F.when(F.col("segment").isin("Priority","Private"), F.expr("500000 + rand() * 95000000"))
             .when(F.col("segment") == "Affluent",              F.expr("100000 + rand() * 25000000"))
             .when(F.col("segment") == "Mass Affluent",         F.expr("50000 + rand() * 10000000"))
             .otherwise(                                        F.expr("10000 + rand() * 2500000"))
        )
        .withColumn("direction",
            F.expr("case when rand(99) < 0.55 then 'OUT' else 'IN' end"))
        .withColumn("counterparty_bank", choose_other_bank(F.col("transfer_id")))
        .withColumn("status",
            F.when(F.expr("rand(101) < 0.995"), F.lit("SUCCESS")).otherwise(F.lit("FAILED")))
        .withColumn("transfer_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("txn_date").cast("string"),
                                        F.concat(F.lpad(F.expr("cast(8 + floor(rand() * 14) as int)").cast("string"), 2, "0"),
                                                 F.lit(":"),
                                                 F.lpad(F.expr("cast(floor(rand() * 60) as int)").cast("string"), 2, "0"),
                                                 F.lit(":00")))))
        .select("transfer_id","customer_id","amount_idr","direction","counterparty_bank","status",
                "transfer_ts", F.col("txn_date").alias("transfer_date"))
        .withColumn("amount_idr", F.col("amount_idr").cast("decimal(18,2)"))
)

bifast_per_day.write.mode("overwrite").saveAsTable(f"{fqs}.raw_bifast_transfers")
print(f"bifast_transfers: {spark.table(f'{fqs}.raw_bifast_transfers').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mobile_sessions and app_events

# COMMAND ----------

sessions_per_day = (
    spark.table(f"{fqs}.raw_customers").select("customer_id", "segment", "digital_active")
        .filter(F.col("digital_active"))
        .crossJoin(dates_df)
        .withColumn("rand_a", F.rand(SEED + 400))
        .withColumn("p_login",
            F.when(F.col("segment") == "Mass",          F.lit(0.45))
             .when(F.col("segment") == "Mass Affluent", F.lit(0.65))
             .when(F.col("segment") == "Affluent",      F.lit(0.78))
             .when(F.col("segment") == "Priority",      F.lit(0.88))
             .otherwise(                                F.lit(0.92)))
        .filter(F.col("rand_a") < F.col("p_login"))
        .withColumn("session_id", F.expr("uuid()"))
        .withColumn("login_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("txn_date").cast("string"),
                                        F.concat(F.lpad(F.expr("cast(7 + floor(rand() * 15) as int)").cast("string"), 2, "0"),
                                                 F.lit(":"),
                                                 F.lpad(F.expr("cast(floor(rand() * 60) as int)").cast("string"), 2, "0"),
                                                 F.lit(":00")))))
        .withColumn("duration_sec", (F.expr("60 + rand() * 600")).cast("int"))
        .withColumn("screens_visited", (F.expr("1 + rand() * 12")).cast("int"))
        .withColumn("device_os",
            F.when(F.expr("rand(11) < 0.80"), F.lit("Android"))
             .otherwise(F.when(F.expr("rand(12) < 0.95"), F.lit("iOS")).otherwise(F.lit("Web"))))
        .withColumn("app_version", F.expr("concat('v', 4 + cast(rand(13)*3 as int), '.', cast(rand(14)*12 as int), '.', cast(rand(15)*9 as int))"))
        .select("session_id","customer_id","login_ts","duration_sec","screens_visited","device_os","app_version",
                F.col("txn_date").alias("session_date"))
)

sessions_per_day.write.mode("overwrite").saveAsTable(f"{fqs}.raw_mobile_sessions")
n_sessions = spark.table(f"{fqs}.raw_mobile_sessions").count()
print(f"mobile_sessions: {n_sessions:,}")

# COMMAND ----------

# In-app events — sample 1-5 events per session
events = (
    spark.table(f"{fqs}.raw_mobile_sessions")
        .withColumn("n_events", (F.expr("1 + rand() * 5")).cast("int"))
        .withColumn("seq", F.expr("sequence(1, n_events)"))
        .withColumn("seq_idx", F.explode("seq"))
        .drop("seq")
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("event_type",
            F.expr("element_at(array('login','view_balance','view_history','start_transfer','submit_transfer',"
                   "'qris_scan','qris_pay_success','qris_pay_fail','view_promotions','open_card_management',"
                   "'apply_loan','open_chat'), 1 + cast(rand() * 12 as int))"))
        .withColumn("event_ts",
            (F.col("login_ts").cast("long") + F.col("seq_idx") * 30 + (F.rand() * 60).cast("int")).cast("timestamp"))
        .select("event_id","customer_id","session_id","event_type","event_ts","session_date")
)
events.write.mode("overwrite").saveAsTable(f"{fqs}.raw_app_events")
print(f"app_events: {spark.table(f'{fqs}.raw_app_events').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("Tables written:")
for tbl in ["raw_customers","raw_accounts","raw_merchants","raw_qris_transactions",
            "raw_bifast_transfers","raw_mobile_sessions","raw_app_events"]:
    n = spark.table(f"{fqs}.{tbl}").count()
    print(f"  {tbl:30s} {n:>15,}")
