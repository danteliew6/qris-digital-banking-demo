# Genie Space — Benchmark Questions

These 12 questions form the regression-test set for the QRIS Digital Banking Genie Space. Each one has an expected SQL and an expected answer shape so you can score Genie's responses for accuracy.

Run the benchmark by:
1. Opening the Genie Space in the workspace UI
2. Asking each question (copy/paste exactly)
3. Comparing Genie's SQL against the **Expected SQL** below
4. Confirming the result shape matches **Expected answer shape**

A pass = Genie generates SQL semantically equivalent to expected (same joins, same filters, same group-by, same ordering up to ties).

---

## Q1 — "What was our total QRIS volume in the last 30 days?"

**Expected SQL**
```sql
SELECT SUM(amount_idr) AS qris_volume_idr,
       COUNT(*)        AS qris_txn_count
FROM qris_transactions
WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 30);
```

**Expected answer shape**
- A single row with one or two numeric values.
- `qris_volume_idr` should be in the **hundreds of billions** of IDR for 50k customers × 90 days.

---

## Q2 — "Which 5 cities drive the most QRIS volume?"

**Expected SQL**
```sql
SELECT m.city, SUM(q.amount_idr) AS volume_idr
FROM qris_transactions q
JOIN merchants m USING (merchant_id)
WHERE q.status = 'SUCCESS' AND q.txn_date >= date_sub(current_date(), 30)
GROUP BY m.city
ORDER BY volume_idr DESC
LIMIT 5;
```

**Expected answer shape**
- 5 rows. **Jakarta should be #1** by a wide margin (≈22% city weight × volume).
- Other top cities: Surabaya, Bandung, Bekasi, Tangerang (approximately, depending on random seed).

---

## Q3 — "How did QRIS volume grow month over month?"

**Expected SQL**
```sql
WITH w AS (
  SELECT CASE WHEN txn_date >= date_sub(current_date(), 30) THEN 'last_30d'
              WHEN txn_date >= date_sub(current_date(), 60) THEN 'prior_30d' END AS w,
         amount_idr
  FROM qris_transactions
  WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 60)
)
SELECT w, SUM(amount_idr) AS volume_idr FROM w GROUP BY w;
```

**Expected answer shape**
- Two rows: `last_30d` and `prior_30d`.
- Volumes should be of **similar magnitude** (~±5%) because the data generator does not encode growth — flag this as honest signal, not a defect.

---

## Q4 — "Show me QRIS volume broken down by merchant tier and category."

**Expected SQL**
```sql
SELECT m.merchant_tier, m.mcc_description,
       SUM(q.amount_idr) AS volume_idr,
       COUNT(*)          AS txn_count
FROM qris_transactions q
JOIN merchants m USING (merchant_id)
WHERE q.status = 'SUCCESS' AND q.txn_date >= date_sub(current_date(), 30)
GROUP BY m.merchant_tier, m.mcc_description
ORDER BY volume_idr DESC;
```

**Expected answer shape**
- ~60 rows (4 tiers × 14 MCC descriptions).
- `UMI` should dominate row count; `UBE` should dominate per-row volume (department stores, electronics).

---

## Q5 — "Which Mass Affluent and Affluent digital customers do not have a Deposito account? Show me cross-sell candidates."

**Expected SQL**
```sql
SELECT c.customer_id, c.full_name, c.segment, c.city,
       co.total_balance_idr, co.qris_amount_30d_idr
FROM customer_overview co
JOIN customers c USING (customer_id)
WHERE c.segment IN ('Mass Affluent','Affluent')
  AND c.digital_active = true
  AND co.deposito_balance_idr = 0
ORDER BY co.total_balance_idr DESC
LIMIT 50;
```

**Expected answer shape**
- 50 rows ordered by balance descending.
- All rows have `deposito_balance_idr = 0` and segment in (Mass Affluent, Affluent).
- Should NOT include Mass / Priority / Private customers.

---

## Q6 — "Who are the top 10 QRIS merchants by volume in the last 30 days?"

**Expected SQL**
```sql
SELECT m.merchant_id, m.merchant_name, m.merchant_tier, m.mcc_description, m.city,
       SUM(dm.qris_amount_idr) AS volume_idr,
       SUM(dm.qris_txn_count)  AS txn_count
FROM daily_merchant_metrics dm
JOIN merchants m USING (merchant_id)
WHERE dm.date >= date_sub(current_date(), 30)
GROUP BY m.merchant_id, m.merchant_name, m.merchant_tier, m.mcc_description, m.city
ORDER BY volume_idr DESC
LIMIT 10;
```

**Expected answer shape**
- 10 rows.
- Top merchants tend to be `UBE` tier; MCC descriptions usually include "Department Stores", "Electronics Sales", "Service Stations".

---

## Q7 — "Which digital customers haven't made a QRIS payment in the last 14 days? Show me churn risks."

**Expected SQL**
```sql
WITH recent AS (
  SELECT customer_id, MAX(txn_date) AS last_txn_date
  FROM qris_transactions WHERE status = 'SUCCESS'
  GROUP BY customer_id
)
SELECT c.customer_id, c.full_name, c.segment, c.city, r.last_txn_date,
       datediff(current_date(), r.last_txn_date) AS days_since_last
FROM customers c
LEFT JOIN recent r USING (customer_id)
WHERE c.digital_active = true
  AND (r.last_txn_date IS NULL OR r.last_txn_date < date_sub(current_date(), 14))
ORDER BY days_since_last DESC NULLS LAST
LIMIT 100;
```

**Expected answer shape**
- 100 rows. Some have NULL `last_txn_date` (never transacted).
- Mix of segments.

---

## Q8 — "Show the daily QRIS volume trend for the last 30 days."

**Expected SQL**
```sql
SELECT txn_date, SUM(amount_idr) AS volume_idr, COUNT(*) AS txn_count
FROM qris_transactions
WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 30)
GROUP BY txn_date
ORDER BY txn_date;
```

**Expected answer shape**
- ~30 rows.
- Weekend (Sat/Sun) volume should be ~25% higher than weekday volume (encoded in the data generator's day-of-week multiplier).

---

## Q9 — "How does BI-Fast volume compare to QRIS volume last month?"

**Expected SQL**
```sql
SELECT 'QRIS' AS rail, SUM(amount_idr) AS volume_idr, COUNT(*) AS txn_count
FROM qris_transactions WHERE status='SUCCESS' AND txn_date >= date_sub(current_date(),30)
UNION ALL
SELECT 'BI-Fast', SUM(amount_idr), COUNT(*)
FROM bifast_transfers WHERE status='SUCCESS' AND transfer_date >= date_sub(current_date(),30);
```

**Expected answer shape**
- 2 rows.
- BI-Fast `volume_idr` is **higher** than QRIS volume (BI-Fast ticket sizes are much larger).
- QRIS `txn_count` is **higher** than BI-Fast count.

---

## Q10 — "What's the QRIS scan → payment success ratio?"

**Expected SQL**
```sql
WITH e AS (
  SELECT event_type, COUNT(*) AS n
  FROM app_events
  WHERE session_date >= date_sub(current_date(), 30)
    AND event_type IN ('qris_scan','qris_pay_success','qris_pay_fail')
  GROUP BY event_type
)
SELECT
  MAX(CASE WHEN event_type='qris_scan' THEN n END)             AS scans,
  MAX(CASE WHEN event_type='qris_pay_success' THEN n END)      AS payments,
  ROUND(MAX(CASE WHEN event_type='qris_pay_success' THEN n END) /
        MAX(CASE WHEN event_type='qris_scan' THEN n END) * 100, 2) AS success_pct
FROM e;
```

**Expected answer shape**
- A single row with `scans`, `payments`, and `success_pct`.
- Event volumes are roughly comparable since event_type is uniform-random over 12 types.

---

## Q11 — "Show me the top 25 digital customers by 30-day QRIS spend."

**Expected SQL**
```sql
SELECT customer_id, full_name, segment, city,
       qris_amount_30d_idr, qris_txn_30d,
       total_balance_idr, active_days_30d
FROM customer_overview
WHERE digital_active = true
ORDER BY qris_amount_30d_idr DESC NULLS LAST
LIMIT 25;
```

**Expected answer shape**
- 25 rows, all `digital_active = true`.
- Top spenders skew toward `Priority`/`Private`/`Affluent` segments.

---

## Q12 — "How much QRIS acquiring volume goes to other banks vs Bank Cendrawasih?"

**Expected SQL**
```sql
SELECT acquirer_bank,
       SUM(amount_idr) AS volume_idr,
       COUNT(*)        AS txn_count,
       ROUND(100.0 * SUM(amount_idr) / SUM(SUM(amount_idr)) OVER (), 2) AS pct_of_total
FROM qris_transactions
WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 30)
GROUP BY acquirer_bank
ORDER BY volume_idr DESC;
```

**Expected answer shape**
- ~16 rows (Bank Cendrawasih + 15 other banks).
- Bank Cendrawasih should be #1 with ≈65% of total (per data generator weighting).

---

## Scoring guide

For each question, score:
- **SQL correctness**: 0 (wrong) / 0.5 (partially correct) / 1 (matches expected up to alias/whitespace differences)
- **Result shape**: 0 (missing rows or extra rows) / 1 (matches expected shape)
- **Domain accuracy**: 0 (broke a domain rule) / 1 (respected IDR formatting, status filter, digital_active filter, etc.)

Total possible: 12 × 3 = **36 points**. Aim for ≥30 (83%) on first run.
