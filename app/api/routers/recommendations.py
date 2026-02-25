"""
app/api/routers/recommendations.py
===================================
Router FastAPI para o dashboard de ofertas.

Endpoints:
  GET /api/recommendations/batches           — lista todos os batches com resumo
  GET /api/recommendations/offers            — ofertas do batch mais recente (ou específico)
  GET /api/recommendations/summary           — KPIs + funil de scores do batch atual
  GET /api/recommendations/product-lifecycle — Top 50 produtos com ciclo de vida e estatísticas
"""

import os
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Query

router = APIRouter()


def _get_db():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "reco"),
        user=os.getenv("PG_USER", "reco"),
        password=os.environ["PG_PASSWORD"],
    )


@router.get("/api/recommendations/batches")
def get_batches():
    """
    Lista todos os batches de recomendações em ordem cronológica reversa.

    Retorna por batch: offer_batch_id, generated_at, expires_at,
    n_offers (total de linhas), n_customers (clientes únicos), n_sent (WhatsApp).
    """
    conn = _get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    offer_batch_id::text,
                    MIN(generated_at)  AS generated_at,
                    MIN(expires_at)    AS expires_at,
                    COUNT(*)           AS n_offers,
                    COUNT(DISTINCT customer_id) AS n_customers,
                    COUNT(sent_via_whatsapp_at) AS n_sent
                FROM reco.offers
                GROUP BY offer_batch_id
                ORDER BY MIN(generated_at) DESC
            """)
            rows = cur.fetchall()
            result = []
            for r in rows:
                row = dict(r)
                if row.get("generated_at"):
                    row["generated_at"] = row["generated_at"].isoformat()
                if row.get("expires_at"):
                    row["expires_at"] = row["expires_at"].isoformat()
                row["n_offers"]    = int(row["n_offers"])
                row["n_customers"] = int(row["n_customers"])
                row["n_sent"]      = int(row["n_sent"])
                result.append(row)
            return result
    finally:
        conn.close()


@router.get("/api/recommendations/offers")
def get_offers(
    batch_id: Optional[str] = Query(None, description="UUID do batch; padrão: mais recente"),
    strategy: Optional[str] = Query(None, description="Filtrar por estratégia: 'modelo_a_ranker' ou 'modelo_b_colaborativo'"),
    limit:    int = Query(100, ge=1, le=5000),
    offset:   int = Query(0, ge=0),
):
    """
    Retorna as ofertas do batch especificado (ou do mais recente).

    Para cada oferta inclui: nome do cliente, telefone, produto, score,
    rank e rastreabilidade de envio WhatsApp.

    O telefone exibido é o celular (mobile) se disponível; caso contrário,
    o telefone fixo (phone). A preferência é sempre pelo número de celular.
    """
    conn = _get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Determina o batch_id alvo
            if batch_id is None:
                cur.execute("""
                    SELECT offer_batch_id::text
                    FROM reco.offers
                    ORDER BY generated_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row is None:
                    return []
                batch_id = row["offer_batch_id"]

            params = [batch_id]
            strategy_filter = ""
            if strategy:
                strategy_filter = "AND o.strategy = %s"
                params.append(strategy)

            query = f"""
                SELECT
                    o.offer_id,
                    o.offer_batch_id::text,
                    o.customer_id,
                    sc.name                                   AS customer_name,
                    -- Valida telefone: deve ter ao menos 6 dígitos numéricos para ser útil.
                    -- Filtra valores inválidos do ERP como '(  )    -', '0', '0000000000000'.
                    NULLIF(
                        CASE
                            WHEN LENGTH(REGEXP_REPLACE(COALESCE(sc.mobile, sc.phone), '[^0-9]', '', 'g')) >= 6
                            THEN COALESCE(sc.mobile, sc.phone)
                            ELSE NULL
                        END,
                    '') AS contact,
                    sc.mobile,
                    sc.phone,
                    o.product_id,
                    cp.description                            AS product_name,
                    o.strategy,
                    o.score,
                    ROUND((o.score * 100)::numeric, 0)::int  AS score_pct,
                    o.rank,
                    o.generated_at,
                    o.expires_at,
                    o.sent_via_whatsapp_at,
                    -- Última vez que este cliente comprou especificamente este produto.
                    -- NULL = nunca comprou (produto novo para o cliente).
                    lp.last_purchase_date
                FROM reco.offers o
                JOIN stg.customers sc
                    ON sc.customer_id_src = o.customer_id
                   AND sc.source_system   = 'sqlserver_gp'
                JOIN cur.products cp
                    ON cp.product_id    = o.product_id
                   AND cp.source_system = 'sqlserver_gp'
                LEFT JOIN (
                    SELECT customer_id, product_id, MAX(sale_date) AS last_purchase_date
                    FROM cur.order_items
                    GROUP BY customer_id, product_id
                ) lp ON lp.customer_id = o.customer_id
                     AND lp.product_id  = o.product_id
                WHERE o.offer_batch_id = %s::uuid
                  {strategy_filter}
                ORDER BY o.customer_id, o.rank
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            cur.execute(query, params)
            rows = cur.fetchall()

            result = []
            for r in rows:
                row = dict(r)
                row["offer_id"]   = int(row["offer_id"])
                row["customer_id"] = int(row["customer_id"])
                row["product_id"]  = int(row["product_id"])
                row["score"]       = float(row["score"])
                row["score_pct"]   = int(row["score_pct"]) if row["score_pct"] is not None else 0
                row["rank"]        = int(row["rank"])
                if row.get("generated_at"):
                    row["generated_at"] = row["generated_at"].isoformat()
                if row.get("expires_at"):
                    row["expires_at"] = row["expires_at"].isoformat()
                if row.get("sent_via_whatsapp_at"):
                    row["sent_via_whatsapp_at"] = row["sent_via_whatsapp_at"].isoformat()
                if row.get("last_purchase_date"):
                    row["last_purchase_date"] = row["last_purchase_date"].isoformat()
                result.append(row)
            return result
    finally:
        conn.close()


@router.get("/api/recommendations/summary")
def get_summary():
    """
    KPIs consolidados + funil de scores do batch mais recente.

    Retorna: total_offers, n_customers, avg_score_pct, pct_bought_before,
    offer_batch_id, generated_at, n_modelo_a, n_modelo_b e lista funnel[]
    com distribuição de ofertas por faixa de score separada por modelo.
    """
    conn = _get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH batch AS (
                    SELECT offer_batch_id
                    FROM   reco.offers
                    ORDER  BY generated_at DESC
                    LIMIT  1
                ),
                purchases AS (
                    SELECT DISTINCT customer_id, product_id
                    FROM   cur.order_items
                ),
                offers AS (
                    SELECT
                        o.*,
                        (p.customer_id IS NOT NULL) AS bought_before
                    FROM reco.offers o
                    LEFT JOIN purchases p
                           ON p.customer_id = o.customer_id
                          AND p.product_id  = o.product_id
                    WHERE o.offer_batch_id = (SELECT offer_batch_id FROM batch)
                )
                SELECT
                    offer_batch_id::text,
                    MIN(generated_at)                                                           AS generated_at,
                    COUNT(*)::int                                                               AS total_offers,
                    COUNT(DISTINCT customer_id)::int                                            AS n_customers,
                    ROUND(AVG(score) * 100)::int                                                AS avg_score_pct,
                    ROUND(AVG(CASE WHEN bought_before THEN 1.0 ELSE 0.0 END) * 100, 1)::float  AS pct_bought_before,
                    COUNT(*) FILTER (WHERE score >= 0.8)::int                                   AS score_80plus,
                    COUNT(*) FILTER (WHERE score >= 0.6 AND score < 0.8)::int                   AS score_60_79,
                    COUNT(*) FILTER (WHERE score >= 0.4 AND score < 0.6)::int                   AS score_40_59,
                    COUNT(*) FILTER (WHERE score  < 0.4)::int                                   AS score_below_40,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_a_ranker')::int                   AS n_modelo_a,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_b_colaborativo')::int             AS n_modelo_b,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_a_ranker' AND score >= 0.8)::int              AS a_80plus,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_a_ranker' AND score >= 0.6 AND score < 0.8)::int AS a_60_79,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_a_ranker' AND score >= 0.4 AND score < 0.6)::int AS a_40_59,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_a_ranker' AND score  < 0.4)::int              AS a_below_40,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_b_colaborativo' AND score >= 0.8)::int              AS b_80plus,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_b_colaborativo' AND score >= 0.6 AND score < 0.8)::int AS b_60_79,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_b_colaborativo' AND score >= 0.4 AND score < 0.6)::int AS b_40_59,
                    COUNT(*) FILTER (WHERE strategy = 'modelo_b_colaborativo' AND score  < 0.4)::int              AS b_below_40
                FROM offers
                GROUP BY offer_batch_id
            """)
            row = cur.fetchone()
            if row is None:
                return {}

            r = dict(row)
            if r.get("generated_at"):
                r["generated_at"] = r["generated_at"].isoformat()

            total = r["total_offers"] or 1
            r["funnel"] = [
                {
                    "label": "≥ 80%",  "color": "#16a34a",
                    "total": r.pop("score_80plus"),
                    "modelo_a": r.pop("a_80plus"),
                    "modelo_b": r.pop("b_80plus"),
                },
                {
                    "label": "60–79%", "color": "#2563eb",
                    "total": r.pop("score_60_79"),
                    "modelo_a": r.pop("a_60_79"),
                    "modelo_b": r.pop("b_60_79"),
                },
                {
                    "label": "40–59%", "color": "#d97706",
                    "total": r.pop("score_40_59"),
                    "modelo_a": r.pop("a_40_59"),
                    "modelo_b": r.pop("b_40_59"),
                },
                {
                    "label": "< 40%",  "color": "#dc2626",
                    "total": r.pop("score_below_40"),
                    "modelo_a": r.pop("a_below_40"),
                    "modelo_b": r.pop("b_below_40"),
                },
            ]
            for bracket in r["funnel"]:
                bracket["pct"] = round(bracket["total"] * 100 / total, 1)

            return r
    finally:
        conn.close()


@router.get("/api/recommendations/product-lifecycle")
def get_product_lifecycle():
    """
    Top 50 produtos presentes no batch atual, por número de ofertas geradas.

    Para cada produto: rank, product_name, lifecycle_type (Consumível/Sazonal/Durável),
    avg_repurchase_days, repeat_rate_pct, total_buyers, n_offers, avg_score_pct.
    """
    conn = _get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH latest_batch AS (
                    SELECT offer_batch_id
                    FROM   reco.offers
                    ORDER  BY generated_at DESC
                    LIMIT  1
                ),
                repurchase_intervals AS (
                    SELECT
                        product_id,
                        customer_id,
                        sale_date - LAG(sale_date) OVER (
                            PARTITION BY product_id, customer_id
                            ORDER BY sale_date
                        ) AS gap
                    FROM cur.order_items
                ),
                product_cycles AS (
                    SELECT
                        product_id,
                        ROUND(AVG(gap))::int AS avg_repurchase_days
                    FROM repurchase_intervals
                    WHERE gap IS NOT NULL
                    GROUP BY product_id
                ),
                product_buyers AS (
                    SELECT
                        product_id,
                        COUNT(DISTINCT customer_id)                                AS total_buyers,
                        COUNT(DISTINCT CASE WHEN cnt > 1 THEN customer_id END)     AS repeat_buyers
                    FROM (
                        SELECT product_id, customer_id, COUNT(*) AS cnt
                        FROM   cur.order_items
                        GROUP  BY product_id, customer_id
                    ) sub
                    GROUP BY product_id
                ),
                product_prices AS (
                    SELECT
                        product_id,
                        ROUND(AVG(total_value / NULLIF(quantity, 0))::numeric, 2) AS avg_unit_price
                    FROM cur.order_items
                    GROUP BY product_id
                ),
                offer_stats AS (
                    SELECT
                        product_id,
                        COUNT(*)::int                AS n_offers,
                        ROUND(AVG(score) * 100)::int AS avg_score_pct
                    FROM reco.offers
                    WHERE offer_batch_id = (SELECT offer_batch_id FROM latest_batch)
                    GROUP BY product_id
                )
                SELECT
                    ROW_NUMBER() OVER (ORDER BY os.n_offers DESC)::int  AS rank,
                    p.product_id::int,
                    p.description                                        AS product_name,
                    COALESCE(pc.avg_repurchase_days, 0)::int             AS avg_repurchase_days,
                    CASE
                        WHEN COALESCE(pc.avg_repurchase_days, 0) = 0 THEN 'Sem histórico'
                        WHEN pc.avg_repurchase_days < 90              THEN 'Consumível'
                        WHEN pc.avg_repurchase_days < 365             THEN 'Sazonal'
                        ELSE                                               'Durável'
                    END                                                  AS lifecycle_type,
                    ROUND(
                        COALESCE(pb.repeat_buyers, 0) * 100.0
                        / NULLIF(pb.total_buyers, 0), 1
                    )::float                                             AS repeat_rate_pct,
                    COALESCE(pb.total_buyers, 0)::int                    AS total_buyers,
                    COALESCE(pp.avg_unit_price, 0)::float                AS avg_unit_price,
                    os.n_offers,
                    os.avg_score_pct
                FROM cur.products p
                JOIN offer_stats os         ON os.product_id = p.product_id
                LEFT JOIN product_cycles pc ON pc.product_id = p.product_id
                LEFT JOIN product_buyers pb ON pb.product_id = p.product_id
                LEFT JOIN product_prices pp ON pp.product_id = p.product_id
                WHERE p.active = TRUE
                ORDER BY os.n_offers DESC
                LIMIT 50
            """)
            rows = cur.fetchall()
            result = []
            for r in rows:
                row = dict(r)
                row["repeat_rate_pct"] = float(row["repeat_rate_pct"] or 0)
                row["avg_unit_price"]  = float(row["avg_unit_price"]  or 0)
                result.append(row)
            return result
    finally:
        conn.close()
