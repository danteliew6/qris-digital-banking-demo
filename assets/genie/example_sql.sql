-- ============================================================================
-- Genie Space — Example SQL Queries
-- ============================================================================
-- Paste each block individually into the Genie Space "Example SQL queries" UI
-- after deploying the bundle. Each block illustrates a common analytical
-- pattern Genie should learn for this domain.
--
-- All queries use the {{catalog}}.{{schema}} placeholders — replace at paste
-- time, or use the bundle-deployed schema (dante_azure_sea.qris_digital_banking_demo).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- #1  Total successful QRIS volume in the last 30 days
-- Question: "What's our QRIS volume this past month?"
-- ----------------------------------------------------------------------------
SELECT
  SUM(amount_idr) AS qris_volume_idr,
  COUNT(*)        AS qris_txn_count,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM qris_transactions
WHERE status = 'SUCCESS'
  AND txn_date >= date_sub(current_date(), 30);

-- ----------------------------------------------------------------------------
-- #2  QRIS volume by city — top 10
-- Question: "Which cities drive the most QRIS volume?"
-- ----------------------------------------------------------------------------
SELECT
  m.city,
  SUM(q.amount_idr) AS qris_volume_idr,
  COUNT(*)          AS txn_count,
  COUNT(DISTINCT q.customer_id) AS unique_customers
FROM qris_transactions q
JOIN merchants m USING (merchant_id)
WHERE q.status = 'SUCCESS'
  AND q.txn_date >= date_sub(current_date(), 30)
GROUP BY m.city
ORDER BY qris_volume_idr DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- #3  Month-over-month QRIS volume growth
-- Question: "How is QRIS growing month over month?"
-- ----------------------------------------------------------------------------
WITH windowed AS (
  SELECT
    CASE
      WHEN txn_date >= date_sub(current_date(), 30) THEN 'last_30d'
      WHEN txn_date >= date_sub(current_date(), 60) AND txn_date < date_sub(current_date(), 30) THEN 'prior_30d'
    END AS window_,
    amount_idr
  FROM qris_transactions
  WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 60)
)
SELECT
  window_,
  SUM(amount_idr) AS qris_volume_idr,
  COUNT(*)        AS txn_count
FROM windowed
GROUP BY window_
ORDER BY window_;

-- ----------------------------------------------------------------------------
-- #4  QRIS volume by merchant tier and MCC
-- Question: "Break down QRIS volume by merchant tier and category."
-- ----------------------------------------------------------------------------
SELECT
  m.merchant_tier,
  m.mcc_description,
  SUM(q.amount_idr)  AS volume_idr,
  COUNT(*)           AS txn_count,
  AVG(q.amount_idr)  AS avg_ticket_idr
FROM qris_transactions q
JOIN merchants m USING (merchant_id)
WHERE q.status = 'SUCCESS'
  AND q.txn_date >= date_sub(current_date(), 30)
GROUP BY m.merchant_tier, m.mcc_description
ORDER BY volume_idr DESC;

-- ----------------------------------------------------------------------------
-- #5  Customer segment QRIS adoption + cross-sell potential
-- Question: "What's QRIS adoption by segment and how many lack a Deposito?"
-- ----------------------------------------------------------------------------
WITH seg_qris AS (
  SELECT c.segment,
         COUNT(DISTINCT c.customer_id)                                              AS total_customers,
         COUNT(DISTINCT CASE WHEN c.digital_active THEN c.customer_id END)          AS digital_active,
         COUNT(DISTINCT q.customer_id)                                              AS qris_active_30d
  FROM customers c
  LEFT JOIN qris_transactions q
    ON q.customer_id = c.customer_id
   AND q.status = 'SUCCESS'
   AND q.txn_date >= date_sub(current_date(), 30)
  GROUP BY c.segment
),
seg_deposito AS (
  SELECT c.segment,
         COUNT(DISTINCT CASE WHEN a.account_type = 'Deposito' THEN c.customer_id END) AS has_deposito
  FROM customers c
  LEFT JOIN accounts a ON a.customer_id = c.customer_id
  GROUP BY c.segment
)
SELECT
  q.segment,
  q.total_customers,
  q.digital_active,
  q.qris_active_30d,
  d.has_deposito,
  q.qris_active_30d - d.has_deposito AS cross_sell_deposito_candidates
FROM seg_qris q
JOIN seg_deposito d USING (segment)
ORDER BY q.total_customers DESC;

-- ----------------------------------------------------------------------------
-- #6  Top 20 QRIS merchants by 30-day volume
-- Question: "Which are our top merchants?"
-- ----------------------------------------------------------------------------
SELECT
  m.merchant_id,
  m.merchant_name,
  m.merchant_tier,
  m.mcc_description,
  m.city,
  dm.volume_idr,
  dm.txn_count,
  dm.unique_customers
FROM (
  SELECT
    merchant_id,
    SUM(qris_amount_idr) AS volume_idr,
    SUM(qris_txn_count)  AS txn_count,
    SUM(unique_customers) AS unique_customers
  FROM daily_merchant_metrics
  WHERE date >= date_sub(current_date(), 30)
  GROUP BY merchant_id
) dm
JOIN merchants m USING (merchant_id)
ORDER BY volume_idr DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- #7  Likely-to-churn digital customers (no QRIS activity in last 14 days)
-- Question: "Which digital-active customers may be churning?"
-- ----------------------------------------------------------------------------
WITH recent AS (
  SELECT customer_id, MAX(txn_date) AS last_txn_date
  FROM qris_transactions
  WHERE status = 'SUCCESS'
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.full_name,
  c.segment,
  c.city,
  r.last_txn_date,
  datediff(current_date(), r.last_txn_date) AS days_since_last_qris
FROM customers c
LEFT JOIN recent r USING (customer_id)
WHERE c.digital_active = true
  AND (r.last_txn_date IS NULL OR r.last_txn_date < date_sub(current_date(), 14))
ORDER BY c.segment, c.customer_id
LIMIT 100;

-- ----------------------------------------------------------------------------
-- #8  Daily QRIS volume trend with day-of-week
-- Question: "Show the daily QRIS volume trend over the last 30 days."
-- ----------------------------------------------------------------------------
SELECT
  txn_date,
  dayofweek(txn_date) AS dow,
  date_format(txn_date, 'EEE') AS day_name,
  SUM(amount_idr) AS volume_idr,
  COUNT(*)        AS txn_count
FROM qris_transactions
WHERE status = 'SUCCESS'
  AND txn_date >= date_sub(current_date(), 30)
GROUP BY txn_date
ORDER BY txn_date;

-- ----------------------------------------------------------------------------
-- #9  BI-Fast vs QRIS volume comparison (last 30 days)
-- Question: "Are BI-Fast transfers larger than QRIS volume?"
-- ----------------------------------------------------------------------------
SELECT
  'QRIS' AS rail,
  SUM(amount_idr) AS volume_idr,
  COUNT(*)        AS txn_count,
  AVG(amount_idr) AS avg_ticket_idr
FROM qris_transactions
WHERE status = 'SUCCESS' AND txn_date >= date_sub(current_date(), 30)
UNION ALL
SELECT
  'BI-Fast' AS rail,
  SUM(amount_idr) AS volume_idr,
  COUNT(*)        AS txn_count,
  AVG(amount_idr) AS avg_ticket_idr
FROM bifast_transfers
WHERE status = 'SUCCESS' AND transfer_date >= date_sub(current_date(), 30);

-- ----------------------------------------------------------------------------
-- #10  Mobile-app engagement funnel
-- Question: "What's the QRIS payment-success funnel?"
-- ----------------------------------------------------------------------------
SELECT
  event_type,
  COUNT(*)                       AS event_count,
  COUNT(DISTINCT customer_id)    AS unique_customers
FROM app_events
WHERE session_date >= date_sub(current_date(), 30)
  AND event_type IN ('qris_scan', 'qris_pay_success', 'qris_pay_fail')
GROUP BY event_type
ORDER BY event_count DESC;

-- ----------------------------------------------------------------------------
-- #11  Top customers by combined wallet volume (QRIS + BI-Fast OUT)
-- Question: "Who are our biggest digital spenders?"
-- ----------------------------------------------------------------------------
SELECT
  customer_id,
  full_name,
  segment,
  city,
  qris_amount_30d_idr,
  bifast_txn_30d,
  total_balance_idr,
  active_days_30d
FROM customer_overview
WHERE digital_active = true
ORDER BY qris_amount_30d_idr DESC NULLS LAST
LIMIT 25;

-- ----------------------------------------------------------------------------
-- #12  QRIS leakage to other banks (acquirer ≠ Bank Cendrawasih)
-- Question: "How much QRIS volume is leaking to other acquiring banks?"
-- ----------------------------------------------------------------------------
SELECT
  acquirer_bank,
  SUM(amount_idr) AS volume_idr,
  COUNT(*)        AS txn_count,
  ROUND(100.0 * SUM(amount_idr) / SUM(SUM(amount_idr)) OVER (), 2) AS pct_of_total
FROM qris_transactions
WHERE status = 'SUCCESS'
  AND txn_date >= date_sub(current_date(), 30)
GROUP BY acquirer_bank
ORDER BY volume_idr DESC;
