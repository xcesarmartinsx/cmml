-- =============================================================================
-- Migration 003: Rebuild product_lifecycle with stricter thresholds
--
-- Changes:
--   - Minimum sample_size: 2 -> 5
--   - Minimum distinct_customers: 1 -> 3
--   - New columns: cv_days_between, reliability_tier
--
-- Idempotent: safe to run multiple times.
-- =============================================================================

-- Drop and recreate (materialized views don't support ALTER)
DROP MATERIALIZED VIEW IF EXISTS reco.product_lifecycle CASCADE;

CREATE MATERIALIZED VIEW reco.product_lifecycle AS
WITH purchase_intervals AS (
    SELECT
        oi.product_id,
        oi.customer_id,
        oi.sale_date,
        LAG(oi.sale_date) OVER (
            PARTITION BY oi.product_id, oi.customer_id
            ORDER BY oi.sale_date
        ) AS prev_date
    FROM cur.order_items oi
    JOIN cur.products p
      ON p.product_id = oi.product_id
     AND p.active = TRUE
),
intervals AS (
    SELECT
        product_id,
        customer_id,
        (sale_date - prev_date) AS days_between
    FROM purchase_intervals
    WHERE prev_date IS NOT NULL
      AND (sale_date - prev_date) > 0
),
product_stats AS (
    SELECT
        i.product_id,
        p.description                     AS product_name,
        p.category,
        ROUND(AVG(i.days_between), 1)     AS avg_days_between_purchases,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i.days_between)::numeric,
            1
        )                                  AS median_days_between_purchases,
        ROUND(
            STDDEV(i.days_between)::numeric, 1
        )                                  AS stddev_days_between,
        COUNT(*)                           AS sample_size,
        COUNT(DISTINCT i.customer_id)      AS distinct_customers
    FROM intervals i
    JOIN cur.products p ON p.product_id = i.product_id
    GROUP BY i.product_id, p.description, p.category
)
SELECT
    product_id,
    product_name,
    category,
    avg_days_between_purchases,
    median_days_between_purchases,
    CASE
        WHEN avg_days_between_purchases < 90  THEN 'short'
        WHEN avg_days_between_purchases < 365 THEN 'medium'
        ELSE 'long'
    END AS lifecycle_tier,
    sample_size,
    distinct_customers,
    ROUND(
        stddev_days_between / NULLIF(avg_days_between_purchases, 0), 3
    ) AS cv_days_between,
    CASE
        WHEN sample_size >= 20 AND distinct_customers >= 10 THEN 'high'
        WHEN sample_size >= 10 AND distinct_customers >= 5  THEN 'medium'
        ELSE 'low'
    END AS reliability_tier,
    NOW() AS updated_at
FROM product_stats
WHERE sample_size >= 5
  AND distinct_customers >= 3;

-- Recreate indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_product_lifecycle_product_id
    ON reco.product_lifecycle (product_id);

CREATE INDEX IF NOT EXISTS idx_product_lifecycle_tier
    ON reco.product_lifecycle (lifecycle_tier);

CREATE INDEX IF NOT EXISTS idx_product_lifecycle_reliability
    ON reco.product_lifecycle (reliability_tier);

-- Add score_raw column to reco.offers (stores pre-normalization probability)
ALTER TABLE reco.offers ADD COLUMN IF NOT EXISTS score_raw NUMERIC(6,4);
