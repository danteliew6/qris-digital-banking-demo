# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bootstrap Genie Space
# MAGIC
# MAGIC Creates (or updates) the QRIS Digital Banking Genie Space programmatically via the Databricks SDK,
# MAGIC then attaches the curated instructions and example SQL queries that ship with this bundle.
# MAGIC
# MAGIC The space is configured with:
# MAGIC - 8 tables: customers, accounts, merchants, qris_transactions, bifast_transfers,
# MAGIC   mobile_sessions, daily_customer_metrics, daily_merchant_metrics, customer_overview
# MAGIC - General instructions from `assets/genie/instructions.md`
# MAGIC - 12 example SQL queries from `assets/genie/example_sql.sql`
# MAGIC
# MAGIC The Genie Space *Create* API is still preview in some workspaces. If the SDK call returns
# MAGIC `RESOURCE_DOES_NOT_EXIST` or 404, the notebook falls back to printing detailed manual setup
# MAGIC instructions you can follow in the Workspace UI.

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
    "customers",
    "accounts",
    "merchants",
    "qris_transactions",
    "bifast_transfers",
    "mobile_sessions",
    "daily_customer_metrics",
    "daily_merchant_metrics",
    "customer_overview",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read curated instructions + example SQL from bundle assets

# COMMAND ----------

import os, json, requests

NOTEBOOK_DIR = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
BUNDLE_ROOT  = os.path.dirname(os.path.dirname(NOTEBOOK_DIR))
ASSETS_DIR   = "/Workspace" + BUNDLE_ROOT + "/assets/genie"

print(f"Assets dir: {ASSETS_DIR}")

def read_asset(name: str) -> str:
    path = f"{ASSETS_DIR}/{name}"
    try:
        with open(path, "r") as fh:
            return fh.read()
    except FileNotFoundError:
        # Fallback to dbfs-style path if bundle isn't on /Workspace
        alt = path.replace("/Workspace", "")
        with open(alt, "r") as fh:
            return fh.read()

instructions_md = read_asset("instructions.md")
example_sql     = read_asset("example_sql.sql")

print(f"instructions.md: {len(instructions_md):,} chars")
print(f"example_sql.sql: {len(example_sql):,} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try programmatic creation via Genie Spaces REST API

# COMMAND ----------

ctx     = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WS_URL  = ctx.apiUrl().get()
TOKEN   = ctx.apiToken().get()

def example_sql_blocks(sql_text: str):
    """Split the curated SQL file into one block per query, skipping the file-header comment."""
    blocks = []
    cur = []
    for line in sql_text.splitlines():
        if line.startswith("-- #") or line.startswith("-- ----"):
            if cur and any(l.strip() and not l.strip().startswith("--") for l in cur):
                blocks.append("\n".join(cur).strip())
                cur = []
        cur.append(line)
    if cur and any(l.strip() and not l.strip().startswith("--") for l in cur):
        blocks.append("\n".join(cur).strip())
    # Drop pure-comment heads
    blocks = [b for b in blocks if any(line.strip() and not line.strip().startswith("--") for line in b.splitlines())]
    return blocks

queries = example_sql_blocks(example_sql)
print(f"Parsed {len(queries)} example SQL queries from curated file.")

# COMMAND ----------

import json, requests

def api(method, path, body=None):
    r = requests.request(method, f"{WS_URL}{path}",
                         headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
                         data=json.dumps(body) if body else None)
    return r.status_code, (r.json() if r.text else {})

# Build space payload — schema may shift between Databricks releases; we use the most recent
# public preview shape and fall back gracefully if the workspace rejects it.
space_payload = {
    "title": SPACE_TITLE,
    "description": "Conversational Q&A for QRIS payments, BI-Fast, and digital engagement (Indonesian retail banking demo).",
    "warehouse_id": WAREHOUSE_ID,
    "tables": [{"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": t} for t in TABLES_FOR_GENIE],
    "general_instructions": instructions_md,
    "example_queries": [{"sql": q} for q in queries[:12]],
}

# Look for an existing space with our title first
status, listed = api("GET", "/api/2.0/data-rooms")  # legacy alias
if status == 404:
    status, listed = api("GET", "/api/2.0/genie/spaces")

existing_id = None
if status == 200:
    for sp in (listed.get("spaces") or listed.get("data_rooms") or []):
        if sp.get("title") == SPACE_TITLE or sp.get("display_name") == SPACE_TITLE:
            existing_id = sp.get("id") or sp.get("space_id")
            break

print(f"List endpoint status: {status}; existing_id: {existing_id}")

# COMMAND ----------

created_id = None
errors = []

if existing_id:
    # Try update
    for path in [f"/api/2.0/genie/spaces/{existing_id}", f"/api/2.0/data-rooms/{existing_id}"]:
        status, body = api("PATCH", path, space_payload)
        if status in (200, 204):
            created_id = existing_id
            print(f"Updated existing space {existing_id} at {path}")
            break
        errors.append((path, status, body))
else:
    for path in ["/api/2.0/genie/spaces", "/api/2.0/data-rooms"]:
        status, body = api("POST", path, space_payload)
        if status in (200, 201):
            created_id = body.get("id") or body.get("space_id")
            print(f"Created space {created_id} at {path}")
            break
        errors.append((path, status, body))

if created_id:
    print(f"\n✓ Genie Space ready: {created_id}")
    print(f"  Open it at: {WS_URL}/sql/genie/{created_id}  (or via Workspace UI → AI/BI → Genie)")
else:
    print("\n⚠ Programmatic Genie creation did not succeed on this workspace.")
    print("  This is expected on workspaces where the Genie REST API is still preview-gated.")
    print("  Errors tried:")
    for p, s, b in errors:
        print(f"    {p} → {s} — {json.dumps(b)[:200]}")
    print("\n→ Follow the manual setup steps printed below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual setup fallback (UI steps)

# COMMAND ----------

print("=" * 78)
print("MANUAL GENIE SPACE SETUP — follow these steps in the Workspace UI")
print("=" * 78)
print(f"""
1.  In the workspace, click 'AI / BI' → 'Genie' → 'New Genie space'.

2.  Title: {SPACE_TITLE}

3.  SQL Warehouse: select the warehouse with ID {WAREHOUSE_ID}
    (or any running serverless warehouse).

4.  Add these tables (catalog: {CATALOG}, schema: {SCHEMA}):
""")
for t in TABLES_FOR_GENIE:
    print(f"      - {CATALOG}.{SCHEMA}.{t}")
print(f"""
5.  Paste the contents of  {ASSETS_DIR}/instructions.md
    into the 'General instructions' field.

6.  Open  {ASSETS_DIR}/example_sql.sql  and add each of the 12 queries under
    'Example SQL queries' (one Example per block).

7.  Save the space. Test it with the 12 benchmark questions in
        {ASSETS_DIR}/benchmark_questions.md
""")
print("=" * 78)
