# QRIS Digital Banking Demo — Databricks Asset Bundle

End-to-end Indonesian retail-banking demo built around **QRIS adoption and digital engagement**, packaged as a Databricks Asset Bundle (DAB).

## What's in the demo

A synthetic Indonesian retail bank ("PT Bank Cendrawasih") wants to understand its QRIS payments business, BI-Fast transfer adoption, and digital-channel engagement. The demo walks through:

1. **Synthetic data generation** — Indonesian customers (NIK, names, cities), QRIS merchants by MCC, transactional events at IDR scale.
2. **Lakeflow Declarative Pipeline** — Bronze (raw ingest) → Silver (cleaned/validated) → Gold (daily metrics per customer + per merchant).
3. **AI/BI Dashboard** — Multi-page Lakeview dashboard: executive QRIS summary, customer insights, merchant analytics.
4. **Genie Space** — Conversational Q&A across 8 tables with curated instructions, ~12 example SQL queries, and a benchmark question set with golden answers.

## Data model

| Table | Grain | Role |
|---|---|---|
| `customers` | one row per customer | Profile, segment, city, KYC age, digital activation |
| `accounts` | one row per account | Savings / Giro / Deposito balances in IDR |
| `merchants` | one row per QRIS merchant | MCC, tier (UMI/UKE/UMK/UBE), onboarding date |
| `qris_transactions` | one row per QRIS payment | Customer → merchant, amount IDR, status |
| `bifast_transfers` | one row per BI-Fast transfer | Inter-bank instant transfers |
| `mobile_sessions` | one row per mobile session | Login, duration, screens visited |
| `app_events` | one row per in-app event | Event taxonomy (view, click, payment, etc.) |
| `daily_customer_metrics` | one row per customer per day | Aggregated KPIs (gold) |
| `daily_merchant_metrics` | one row per merchant per day | Aggregated KPIs (gold) |

## Deployment

### Prerequisites

- Databricks CLI v0.218+ authenticated to your workspace
- Unity Catalog catalog `dante_azure_sea` (or override with `--var catalog=<name>`)
- A serverless SQL warehouse (override with `--var warehouse_id=<id>`)

### Deploy

```bash
# from repo root
databricks bundle validate --profile dante-demo-env
databricks bundle deploy   --profile dante-demo-env

# kick off the data + pipeline run
databricks bundle run qris_demo_setup_job --profile dante-demo-env
```

The bundle creates:
- Schema `dante_azure_sea.qris_digital_banking_demo`
- A setup job (`qris_demo_setup_job`) that runs data gen + the pipeline
- A Lakeflow Declarative Pipeline (`qris_digital_banking_pipeline`)
- An AI/BI Dashboard (`QRIS Digital Banking Executive Dashboard`)
- A Genie Space asset (registered as a bundle resource — finalize Genie wiring via UI after first deploy)

### Override variables

```bash
databricks bundle deploy \
  --var rows_customers=200000 \
  --var rows_merchants=20000 \
  --var days_of_history=180
```

## File layout

```
qris-digital-banking-demo/
├── databricks.yml                        # Bundle root
├── resources/
│   ├── 01_setup_job.yml                  # Data-gen + pipeline trigger job
│   ├── 02_pipeline.yml                   # Lakeflow Declarative Pipeline
│   ├── 03_dashboard.yml                  # AI/BI Dashboard
│   └── 04_genie_space.yml                # Genie Space asset
├── src/
│   ├── notebooks/
│   │   ├── 00_setup_schema.py            # Create schema, grant permissions
│   │   └── 01_generate_synthetic_data.py # Indonesian banking data generator
│   └── pipeline/
│       └── qris_pipeline.py              # DLT bronze/silver/gold definitions
├── assets/
│   ├── dashboard/
│   │   └── qris_dashboard.lvdash.json    # Lakeview dashboard JSON
│   └── genie/
│       ├── instructions.md               # Genie instructions (general + per-table)
│       ├── example_sql.sql               # Curated example SQL
│       └── benchmark_questions.md        # 12 benchmark Qs + golden answers
└── README.md
```

## Genie Space configuration

The Genie space ships with three configuration layers (see `assets/genie/`):

1. **Instructions** — domain context (Indonesia, IDR currency, QRIS taxonomy, common joins)
2. **Example SQL queries** — 12 curated patterns covering all common analytical shapes
3. **Benchmark questions** — 12 questions with expected SQL and expected results for regression-testing the space

After bundle deploy, open the Genie space in the workspace UI, paste the contents of `instructions.md` into General instructions, and add each example SQL query under "Example SQL queries". The benchmark questions are the evaluation set.

## Use case narrative

PT Bank Cendrawasih is a mid-size Indonesian retail bank competing with BCA/Mandiri/Jago. They want answers like:

- Which cities are driving QRIS volume growth this month?
- What's the cross-sell potential for Deposito product among active QRIS users?
- Which merchant MCCs are seeing the highest customer churn?
- Are BI-Fast transfers cannibalizing or complementing QRIS volume?

The demo gives them a working dashboard for the recurring KPIs and a Genie space for the ad-hoc questions.
