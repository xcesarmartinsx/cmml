-- =============================================================================
-- sql/ddl/04_product_lifecycle.sql
-- Materialized view com metricas de ciclo de vida por produto.
--
-- Calcula o intervalo medio e mediano entre recompras do mesmo produto
-- pelo mesmo cliente. Usado pelo modelo ML para penalizar recomendacoes
-- de produtos de longa vida util quando o cliente comprou recentemente.
--
-- Execucao:
--   psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -f sql/ddl/04_product_lifecycle.sql
--
-- Refresh (apos novo ETL):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY reco.product_lifecycle;
-- =============================================================================

-- Materialized view para performance: a query com window functions e pesada.
-- CONCURRENTLY requer um unique index, adicionado abaixo.
CREATE MATERIALIZED VIEW IF NOT EXISTS reco.product_lifecycle AS
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
    NOW() AS updated_at
FROM product_stats
WHERE sample_size >= 3
  AND distinct_customers >= 2;

-- Unique index necessario para REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_product_lifecycle_product_id
    ON reco.product_lifecycle (product_id);

-- Indice para filtrar por tier
CREATE INDEX IF NOT EXISTS idx_product_lifecycle_tier
    ON reco.product_lifecycle (lifecycle_tier);
