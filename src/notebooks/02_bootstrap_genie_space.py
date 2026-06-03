# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bootstrap Genie Space
# MAGIC
# MAGIC Creates (or updates) the QRIS Digital Banking Genie Space programmatically and loads:
# MAGIC - 10 UC table COMMENTs (so Genie auto-picks them up as table snippets)
# MAGIC - 1 TEXT_INSTRUCTION (general space-level instructions from `assets/genie/instructions.md`)
# MAGIC - 12 SQL_INSTRUCTION example queries (from `assets/genie/example_sql.sql`)
# MAGIC
# MAGIC Uses the `/api/2.0/data-rooms` endpoint, which is the production Genie Spaces REST API
# MAGIC on Azure Databricks.

# COMMAND ----------

dbutils.widgets.text("catalog", "dante_azure_sea")
dbutils.widgets.text("schema",  "qris_digital_banking_demo")
dbutils.widgets.text("warehouse_id", "6f64729f1845a7de")
dbutils.widgets.text("space_title", "QRIS Digital Banking (PT Bank Cendrawasih)")

CATALOG       = dbutils.widgets.get("catalog")
SCHEMA        = dbutils.widgets.get("schema")
WAREHOUSE_ID  = dbutils.widgets.get("warehouse_id")
SPACE_TITLE   = dbutils.widgets.get("space_title")
FQS = f"{CATALOG}.{SCHEMA}"

TABLES_FOR_GENIE = [
    "customers", "accounts", "merchants",
    "qris_transactions", "bifast_transfers",
    "mobile_sessions", "app_events",
    "daily_customer_metrics", "daily_merchant_metrics", "customer_overview",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set UC table COMMENTs (these become Genie's table snippets)

# COMMAND ----------

TABLE_COMMENTS = {
    "customers":              "ONE ROW PER CUSTOMER. PT Bank Cendrawasih retail customer master. Use for segment/city/age/occupation filters. Watch digital_active for any 'digital' question.",
    "accounts":               "ONE ROW PER (customer, account_type). Savings/Giro/Deposito balances in IDR. Sum balance_idr by customer_id for total relationship size.",
    "merchants":              "ONE ROW PER MERCHANT. QRIS-registered merchants. merchant_tier in (UMI, UKE, UMK, UBE) for micro/small/medium/large. mcc + mcc_description identify category.",
    "qris_transactions":      "ONE ROW PER QRIS PAYMENT (90 days of history). ALWAYS filter status='SUCCESS' for revenue questions. txn_date is the date column. acquirer_bank may be Bank Cendrawasih or another bank (leakage).",
    "bifast_transfers":       "ONE ROW PER BI-FAST TRANSFER. Inter-bank instant transfers via Bank Indonesia Fast Payment rail. direction is IN/OUT. counterparty_bank is the OTHER bank.",
    "mobile_sessions":        "ONE ROW PER MOBILE APP SESSION. session_date is the date. duration_sec and screens_visited indicate engagement intensity.",
    "app_events":             "ONE ROW PER IN-APP EVENT. event_type covers login, view_balance, qris_scan, qris_pay_success, qris_pay_fail, start_transfer, etc. Use for funnel analyses.",
    "daily_customer_metrics": "GOLD AGGREGATE: ONE ROW PER (customer, date). Pre-aggregated daily KPIs (qris counts/amount, bifast counts/amount, session counts). Use for time-series customer questions.",
    "daily_merchant_metrics": "GOLD AGGREGATE: ONE ROW PER (merchant, date). Pre-aggregated daily KPIs per merchant. Use for merchant trend/top-N questions.",
    "customer_overview":      "GOLD WIDE TABLE: ONE ROW PER CUSTOMER. Profile + total/savings/giro/deposito balances + 30-day rolling QRIS/BI-Fast/session KPIs + active_days_30d. Best for 'top N customers' or cross-sell questions.",
}

for tbl, comment in TABLE_COMMENTS.items():
    esc = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {FQS}.{tbl} IS '{esc}'")
    print(f"  COMMENT set on {tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Find or create the Genie space

# COMMAND ----------

import json, re, requests

ctx    = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WS_URL = ctx.apiUrl().get()
TOKEN  = ctx.apiToken().get()
HDRS   = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def get(p):   return requests.get(f"{WS_URL}{p}",  headers=HDRS).json()
def post(p,b):return requests.post(f"{WS_URL}{p}", headers=HDRS, data=json.dumps(b)).json()

# Look for existing space
listed = get("/api/2.0/data-rooms")
existing = None
for s in listed.get("data_rooms", []) or []:
    if s.get("display_name") == SPACE_TITLE:
        existing = s.get("space_id") or s.get("id")
        break

if existing:
    space_id = existing
    print(f"Found existing Genie space: {space_id}")
else:
    body = {
        "display_name": SPACE_TITLE,
        "description":  "Conversational Q&A for QRIS payments, BI-Fast, and digital engagement (Indonesian retail banking demo)",
        "warehouse_id": WAREHOUSE_ID,
        "table_identifiers": [f"{FQS}.{t}" for t in TABLES_FOR_GENIE],
        "run_as_type":  "VIEWER",
    }
    created = post("/api/2.0/data-rooms", body)
    space_id = created.get("space_id") or created.get("id")
    print(f"Created Genie space: {space_id}")
    if not space_id:
        print("Create failed:", json.dumps(created)[:500]); raise SystemExit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Wipe + reload TEXT_INSTRUCTION (general) and SQL_INSTRUCTIONs (examples)

# COMMAND ----------

# Delete existing TEXT and SQL instructions so reruns are idempotent.
existing_inst = get(f"/api/2.0/data-rooms/{space_id}/instructions").get("instructions", []) or []
delete_count = 0
for inst in existing_inst:
    if inst.get("instruction_type") in ("TEXT_INSTRUCTION", "SQL_INSTRUCTION"):
        iid = inst.get("instruction_id") or inst.get("id")
        r = requests.delete(f"{WS_URL}/api/2.0/data-rooms/{space_id}/instructions/{iid}", headers=HDRS)
        if r.status_code in (200, 204):
            delete_count += 1
print(f"Deleted {delete_count} prior instructions")

# COMMAND ----------

# Load instructions.md (general text) and post as TEXT_INSTRUCTION
import os
NB_PATH = ctx.notebookPath().get()
BUNDLE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(NB_PATH)))
INSTR_PATH = "/Workspace" + BUNDLE_ROOT + "/assets/genie/instructions.md"
SQL_PATH   = "/Workspace" + BUNDLE_ROOT + "/assets/genie/example_sql.sql"
print(f"Reading: {INSTR_PATH}")

with open(INSTR_PATH) as f:
    general = f.read()

res = post(f"/api/2.0/data-rooms/{space_id}/instructions", {
    "instruction_type": "TEXT_INSTRUCTION",
    "title": general.strip().split("\n", 1)[0][:200],
    "content": general,
    "use_as_tool": False,
})
print(f"  TEXT_INSTRUCTION -> {res.get('instruction_id') or res}")

# COMMAND ----------

# Parse example_sql.sql into 12 blocks, post each as SQL_INSTRUCTION
with open(SQL_PATH) as f:
    raw = f.read()

blocks = []
current = {"title": None, "sql_lines": []}
title_re = re.compile(r"^--\s+#\d+\s+(.+)$")
for line in raw.splitlines():
    m = title_re.match(line.strip())
    if m:
        if current["title"] and current["sql_lines"]:
            blocks.append({"title": current["title"], "sql": "\n".join(current["sql_lines"]).strip()})
        current = {"title": m.group(1).strip(), "sql_lines": []}
        continue
    if current["title"] is None or line.strip().startswith("--"):
        continue
    if not line.strip():
        if current["sql_lines"]:
            current["sql_lines"].append("")
        continue
    current["sql_lines"].append(line.rstrip())
if current["title"] and current["sql_lines"]:
    blocks.append({"title": current["title"], "sql": "\n".join(current["sql_lines"]).strip()})

TABLES_QUAL = list(TABLE_COMMENTS.keys())
def qualify(sql):
    for t in TABLES_QUAL:
        sql = re.sub(rf"(FROM|JOIN)\s+{t}\b",
                     lambda m: f"{m.group(1)} {FQS}.{t}", sql)
    return sql

print(f"Loading {len(blocks)} example SQL queries...")
for i, b in enumerate(blocks, 1):
    sql = qualify(b["sql"]).rstrip().rstrip(";") + ";"
    res = post(f"/api/2.0/data-rooms/{space_id}/instructions", {
        "instruction_type": "SQL_INSTRUCTION",
        "title": b["title"][:160],
        "content": sql,
        "use_as_tool": False,
    })
    print(f"  #{i:2d} {b['title'][:60]:60s} -> {res.get('instruction_id') or res}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Done

# COMMAND ----------

print(f"""
✓ Genie space ready:
    Space ID: {space_id}
    URL:      {WS_URL}/genie/rooms/{space_id}

Tables registered: {len(TABLES_FOR_GENIE)}
Table comments set: {len(TABLE_COMMENTS)}
Example SQL queries loaded: {len(blocks)}
General instructions: 1

Try benchmark question Q1: "What was our total QRIS volume in the last 30 days?"
""")
