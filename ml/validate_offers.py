"""
ml/validate_offers.py
=====================
Validador de qualidade das ofertas geradas.

Executa 8 checks de sanidade sobre um batch de ofertas em reco.offers
e produz um relatorio formatado no console + persiste resultados em
reco.offer_validation.

Checks implementados:
  1. lifecycle_coverage      - % ofertas com/sem lifecycle
  2. high_score_no_lifecycle - score >= 0.90 sem lifecycle (nao deveria existir)
  3. recent_purchase_high_score - score >= 0.80, mesmo produto nos ultimos 180d
  4. category_recent_purchase - score >= 0.70, mesma categoria nos ultimos 90d
  5. score_distribution      - estatisticas por lifecycle_tier
  6. category_concentration  - top categorias com score >= 0.90
  7. inactive_products       - ofertas para produtos inativos
  8. duplicate_category_per_customer - 3+ ofertas na mesma categoria por cliente

Uso:
    python ml/validate_offers.py                    # batch mais recente
    python ml/validate_offers.py --batch-id UUID    # batch especifico
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# -- Resolucao do path do projeto ------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging

LOG = setup_logging("ml.validate_offers")

# Status constants
PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
INFO = "INFO"


# ===========================================================================
# HELPERS
# ===========================================================================

def _ensure_validation_table(pg) -> None:
    """Cria reco.offer_validation se nao existir."""
    ddl = """
    CREATE TABLE IF NOT EXISTS reco.offer_validation (
        validation_id  SERIAL       PRIMARY KEY,
        offer_batch_id UUID         NOT NULL,
        validated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        check_name     TEXT         NOT NULL,
        status         TEXT         NOT NULL,
        summary        TEXT         NOT NULL,
        details        JSONB
    );
    """
    with pg.cursor() as cur:
        cur.execute(ddl)
    pg.commit()
    LOG.info("Table reco.offer_validation ensured.")


def _resolve_batch_id(pg, batch_id: Optional[str] = None) -> str:
    """Retorna o batch_id fornecido ou o mais recente em reco.offers."""
    if batch_id:
        return batch_id
    with pg.cursor() as cur:
        cur.execute("""
            SELECT offer_batch_id::text
            FROM reco.offers
            ORDER BY generated_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
    if not row:
        LOG.error("Nenhuma oferta encontrada em reco.offers.")
        sys.exit(1)
    resolved = row[0]
    LOG.info(f"Batch ID resolvido (mais recente): {resolved}")
    return resolved


def _make_serializable(obj: Any) -> Any:
    """Converte tipos numpy/pandas para tipos nativos Python (para JSON)."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(i) for i in obj]
    return obj


def _persist_result(pg, batch_id: str, result: Dict) -> None:
    """Insere um resultado de check em reco.offer_validation."""
    details = _make_serializable(result.get("details"))
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reco.offer_validation
                (offer_batch_id, validated_at, check_name, status, summary, details)
            VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                batch_id,
                datetime.now(tz=timezone.utc),
                result["check_name"],
                result["status"],
                result["summary"],
                json.dumps(details, ensure_ascii=False, default=str),
            ),
        )


# ===========================================================================
# CHECK 1 — LIFECYCLE COVERAGE
# ===========================================================================

def run_check_lifecycle_coverage(pg, batch_id: str) -> Dict:
    """
    Verifica a cobertura de lifecycle nas ofertas.

    Classifica cada oferta em:
    - com lifecycle individual (match direto em reco.product_lifecycle)
    - com lifecycle de categoria (fallback via primeira palavra do nome)
    - sem nenhum lifecycle

    Flags:
      WARN se > 5% sem lifecycle
      FAIL se > 15% sem lifecycle
    """
    check_name = "lifecycle_coverage"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.offer_id,
               o.product_id,
               pl.lifecycle_tier AS individual_tier,
               cat_pl.category_avg_lifecycle
        FROM reco.offers o
        LEFT JOIN reco.product_lifecycle pl
            ON pl.product_id = o.product_id
        LEFT JOIN LATERAL (
            SELECT AVG(pl2.avg_days_between_purchases) AS category_avg_lifecycle
            FROM reco.product_lifecycle pl2
            JOIN cur.products p2 ON p2.product_id = pl2.product_id
            JOIN cur.products p_offer ON p_offer.product_id = o.product_id
            WHERE SPLIT_PART(p2.description, ' ', 1) = SPLIT_PART(p_offer.description, ' ', 1)
        ) cat_pl ON TRUE
        WHERE o.offer_batch_id = %(batch_id)s::uuid
        """,
        pg,
        params={"batch_id": batch_id},
    )

    total = len(df)
    if total == 0:
        return {
            "check_name": check_name,
            "status": FAIL,
            "summary": "Nenhuma oferta encontrada para este batch.",
            "details": {"total": 0},
        }

    has_individual = df["individual_tier"].notna().sum()
    has_category_only = (
        df["individual_tier"].isna() & df["category_avg_lifecycle"].notna()
    ).sum()
    no_lifecycle = total - has_individual - has_category_only

    pct_individual = has_individual / total * 100
    pct_category = has_category_only / total * 100
    pct_none = no_lifecycle / total * 100

    if pct_none > 15:
        status = FAIL
    elif pct_none > 5:
        status = WARN
    else:
        status = PASS

    summary = (
        f"Total: {total:,} ofertas | "
        f"Individual: {has_individual:,} ({pct_individual:.1f}%) | "
        f"Categoria: {has_category_only:,} ({pct_category:.1f}%) | "
        f"Sem lifecycle: {no_lifecycle:,} ({pct_none:.1f}%)"
    )
    LOG.info(f"  [{status}] {summary}")

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": {
            "total": total,
            "has_individual": int(has_individual),
            "has_category_only": int(has_category_only),
            "no_lifecycle": int(no_lifecycle),
            "pct_individual": round(pct_individual, 2),
            "pct_category": round(pct_category, 2),
            "pct_none": round(pct_none, 2),
        },
    }


# ===========================================================================
# CHECK 2 — HIGH SCORE WITHOUT LIFECYCLE
# ===========================================================================

def run_check_high_score_no_lifecycle(pg, batch_id: str) -> Dict:
    """
    Ofertas com score >= 0.90 sem lifecycle individual NEM de categoria.

    Apos o uncertainty cap, essas ofertas nao deveriam existir.
    Flag: FAIL se existirem.
    """
    check_name = "high_score_no_lifecycle"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
               p.description AS product_name
        FROM reco.offers o
        JOIN cur.products p ON p.product_id = o.product_id
        LEFT JOIN reco.product_lifecycle pl ON pl.product_id = o.product_id
        WHERE o.offer_batch_id = %(batch_id)s::uuid
          AND o.score >= 0.90
          AND pl.product_id IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM reco.product_lifecycle pl2
              JOIN cur.products p2 ON p2.product_id = pl2.product_id
              WHERE SPLIT_PART(p2.description, ' ', 1) = SPLIT_PART(p.description, ' ', 1)
          )
        ORDER BY o.score DESC
        LIMIT 20
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    status = FAIL if count > 0 else PASS
    summary = (
        f"{count} ofertas com score >= 0.90 sem nenhum lifecycle (individual ou categoria)"
    )
    LOG.info(f"  [{status}] {summary}")

    details = {"count": count}
    if count > 0:
        details["top_20"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    offer_id={row['offer_id']} customer={row['customer_id']} "
                f"product={row['product_id']} score={row['score']:.4f} "
                f"strategy={row['strategy']}"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# CHECK 3 — RECENT PURCHASE, HIGH SCORE
# ===========================================================================

def run_check_recent_purchase_high_score(pg, batch_id: str) -> Dict:
    """
    Ofertas com score >= 0.80 onde o cliente comprou o MESMO produto
    nos ultimos 180 dias.

    Flag: WARN se existirem.
    """
    check_name = "recent_purchase_high_score"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
               p.description AS product_name,
               MAX(oi.sale_date) AS last_purchase_date,
               CURRENT_DATE - MAX(oi.sale_date) AS days_since_purchase
        FROM reco.offers o
        JOIN cur.products p ON p.product_id = o.product_id
        JOIN cur.order_items oi
            ON oi.customer_id = o.customer_id
           AND oi.product_id = o.product_id
           AND oi.sale_date >= CURRENT_DATE - INTERVAL '180 days'
        WHERE o.offer_batch_id = %(batch_id)s::uuid
          AND o.score >= 0.80
        GROUP BY o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
                 p.description
        ORDER BY o.score DESC
        LIMIT 20
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    status = WARN if count > 0 else PASS
    summary = (
        f"{count} ofertas com score >= 0.80 e compra do mesmo produto nos ultimos 180 dias"
    )
    LOG.info(f"  [{status}] {summary}")

    details = {"count": count}
    if count > 0:
        details["top_20"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    offer_id={row['offer_id']} customer={row['customer_id']} "
                f"product={row['product_id']} score={row['score']:.4f} "
                f"days_since={row['days_since_purchase']}"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# CHECK 4 — CATEGORY RECENT PURCHASE
# ===========================================================================

def run_check_category_recent_purchase(pg, batch_id: str) -> Dict:
    """
    Ofertas com score >= 0.70 onde o cliente comprou QUALQUER produto
    da mesma categoria (primeira palavra do nome) nos ultimos 90 dias.

    Flag: WARN se existirem.
    """
    check_name = "category_recent_purchase"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
               p_offer.description AS product_name,
               SPLIT_PART(p_offer.description, ' ', 1) AS category,
               MAX(oi.sale_date) AS last_category_purchase,
               CURRENT_DATE - MAX(oi.sale_date) AS days_since_category_purchase
        FROM reco.offers o
        JOIN cur.products p_offer ON p_offer.product_id = o.product_id
        JOIN cur.products p_cat
            ON SPLIT_PART(p_cat.description, ' ', 1) = SPLIT_PART(p_offer.description, ' ', 1)
        JOIN cur.order_items oi
            ON oi.customer_id = o.customer_id
           AND oi.product_id = p_cat.product_id
           AND oi.sale_date >= CURRENT_DATE - INTERVAL '90 days'
        WHERE o.offer_batch_id = %(batch_id)s::uuid
          AND o.score >= 0.70
        GROUP BY o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
                 p_offer.description
        ORDER BY o.score DESC
        LIMIT 20
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    status = WARN if count > 0 else PASS
    summary = (
        f"{count} ofertas com score >= 0.70 e compra na mesma categoria nos ultimos 90 dias"
    )
    LOG.info(f"  [{status}] {summary}")

    details = {"count": count}
    if count > 0:
        details["top_20"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    offer_id={row['offer_id']} customer={row['customer_id']} "
                f"product={row['product_id']} score={row['score']:.4f} "
                f"category={row['category']} days_since={row['days_since_category_purchase']}"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# CHECK 5 — SCORE DISTRIBUTION BY LIFECYCLE TIER
# ===========================================================================

def run_check_score_distribution(pg, batch_id: str) -> Dict:
    """
    Estatisticas de score por lifecycle_tier (short/medium/long/sem_dados).

    Para cada tier: count, mean, median, p90, max.
    Flag: WARN se mean de "sem_dados" > mean de "long".
    """
    check_name = "score_distribution"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.score,
               COALESCE(pl.lifecycle_tier, 'sem_dados') AS lifecycle_tier
        FROM reco.offers o
        LEFT JOIN reco.product_lifecycle pl ON pl.product_id = o.product_id
        WHERE o.offer_batch_id = %(batch_id)s::uuid
        """,
        pg,
        params={"batch_id": batch_id},
    )

    if df.empty:
        return {
            "check_name": check_name,
            "status": PASS,
            "summary": "Nenhuma oferta encontrada.",
            "details": {},
        }

    stats_list = []
    for tier, group in df.groupby("lifecycle_tier"):
        scores = group["score"]
        stats_list.append({
            "lifecycle_tier": tier,
            "count": int(len(scores)),
            "mean": round(float(scores.mean()), 4),
            "median": round(float(scores.median()), 4),
            "p90": round(float(scores.quantile(0.90)), 4),
            "max": round(float(scores.max()), 4),
        })

    stats_df = pd.DataFrame(stats_list)

    # Determine status
    mean_sem_dados = stats_df.loc[
        stats_df["lifecycle_tier"] == "sem_dados", "mean"
    ]
    mean_long = stats_df.loc[
        stats_df["lifecycle_tier"] == "long", "mean"
    ]

    status = PASS
    if not mean_sem_dados.empty and not mean_long.empty:
        if mean_sem_dados.iloc[0] > mean_long.iloc[0]:
            status = WARN

    summary_parts = []
    for _, row in stats_df.iterrows():
        summary_parts.append(
            f"{row['lifecycle_tier']}: n={row['count']:,} "
            f"mean={row['mean']:.4f} med={row['median']:.4f} "
            f"p90={row['p90']:.4f} max={row['max']:.4f}"
        )
    summary = " | ".join(summary_parts)
    LOG.info(f"  [{status}] Score distribution by lifecycle_tier:")
    for part in summary_parts:
        LOG.info(f"    {part}")

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": {"tiers": stats_list},
    }


# ===========================================================================
# CHECK 6 — CATEGORY CONCENTRATION
# ===========================================================================

def run_check_category_concentration(pg, batch_id: str) -> Dict:
    """
    Top 10 categorias por numero de ofertas com score >= 0.90.

    Flag: INFO (apenas informativo).
    """
    check_name = "category_concentration"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT SPLIT_PART(p.description, ' ', 1) AS category,
               COUNT(*) AS offer_count,
               ROUND(AVG(o.score)::numeric, 4) AS avg_score
        FROM reco.offers o
        JOIN cur.products p ON p.product_id = o.product_id
        WHERE o.offer_batch_id = %(batch_id)s::uuid
          AND o.score >= 0.90
        GROUP BY SPLIT_PART(p.description, ' ', 1)
        ORDER BY offer_count DESC
        LIMIT 10
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    total_high = int(df["offer_count"].sum()) if count > 0 else 0
    status = INFO
    summary = f"{total_high} ofertas com score >= 0.90 em {count} categorias (top 10)"
    LOG.info(f"  [{status}] {summary}")

    details = {"total_high_score_offers": total_high, "top_10": []}
    if count > 0:
        details["top_10"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    {row['category']}: {row['offer_count']} ofertas "
                f"(avg_score={row['avg_score']:.4f})"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# CHECK 7 — INACTIVE PRODUCTS
# ===========================================================================

def run_check_inactive_products(pg, batch_id: str) -> Dict:
    """
    Ofertas para produtos com active=false em cur.products.

    Flag: FAIL se existirem.
    """
    check_name = "inactive_products"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.offer_id, o.customer_id, o.product_id, o.score, o.strategy,
               p.description AS product_name
        FROM reco.offers o
        JOIN cur.products p
            ON p.product_id = o.product_id
           AND p.active = FALSE
        WHERE o.offer_batch_id = %(batch_id)s::uuid
        ORDER BY o.score DESC
        LIMIT 20
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    status = FAIL if count > 0 else PASS
    summary = f"{count} ofertas para produtos inativos"
    LOG.info(f"  [{status}] {summary}")

    details = {"count": count}
    if count > 0:
        details["top_20"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    offer_id={row['offer_id']} product={row['product_id']} "
                f"({row['product_name']}) score={row['score']:.4f}"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# CHECK 8 — DUPLICATE CATEGORY PER CUSTOMER
# ===========================================================================

def run_check_duplicate_category_per_customer(pg, batch_id: str) -> Dict:
    """
    Clientes com 3+ ofertas na mesma categoria (primeira palavra do nome).

    Indica falta de diversidade nas recomendacoes.
    Flag: WARN se existirem.
    """
    check_name = "duplicate_category_per_customer"
    LOG.info(f"--- Check: {check_name} ---")

    df = pd.read_sql(
        """
        SELECT o.customer_id,
               SPLIT_PART(p.description, ' ', 1) AS category,
               COUNT(*) AS offer_count,
               ROUND(AVG(o.score)::numeric, 4) AS avg_score
        FROM reco.offers o
        JOIN cur.products p ON p.product_id = o.product_id
        WHERE o.offer_batch_id = %(batch_id)s::uuid
        GROUP BY o.customer_id, SPLIT_PART(p.description, ' ', 1)
        HAVING COUNT(*) >= 3
        ORDER BY COUNT(*) DESC
        LIMIT 10
        """,
        pg,
        params={"batch_id": batch_id},
    )

    count = len(df)
    status = WARN if count > 0 else PASS
    summary = f"{count} pares (cliente, categoria) com 3+ ofertas duplicadas"
    LOG.info(f"  [{status}] {summary}")

    details = {"count": count}
    if count > 0:
        details["top_10"] = df.to_dict(orient="records")
        for _, row in df.iterrows():
            LOG.info(
                f"    customer={row['customer_id']} category={row['category']} "
                f"offers={row['offer_count']} avg_score={row['avg_score']:.4f}"
            )

    return {
        "check_name": check_name,
        "status": status,
        "summary": summary,
        "details": details,
    }


# ===========================================================================
# MAIN
# ===========================================================================

def main(batch_id: Optional[str] = None) -> bool:
    """
    Executa todos os checks de validacao para um batch de ofertas.

    Parametros
    ----------
    batch_id : UUID do batch a validar. Se None, usa o mais recente.

    Retorna
    -------
    bool : True se algum check resultou em FAIL, False caso contrario.
    """
    LOG.info("=" * 70)
    LOG.info("VALIDATE OFFERS — Validador de qualidade de ofertas")
    LOG.info("=" * 70)

    pg = get_pg_conn()

    # Garante que a tabela de resultados existe
    _ensure_validation_table(pg)

    # Resolve batch_id
    batch_id = _resolve_batch_id(pg, batch_id)
    LOG.info(f"Validating batch: {batch_id}")

    # Conta ofertas no batch para contexto
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM reco.offers WHERE offer_batch_id = %s::uuid",
            (batch_id,),
        )
        total_offers = cur.fetchone()[0]
    LOG.info(f"Total ofertas no batch: {total_offers:,}")

    if total_offers == 0:
        LOG.error(f"Batch {batch_id} nao contem ofertas. Abortando.")
        return True

    # Lista de checks a executar
    checks = [
        run_check_lifecycle_coverage,
        run_check_high_score_no_lifecycle,
        run_check_recent_purchase_high_score,
        run_check_category_recent_purchase,
        run_check_score_distribution,
        run_check_category_concentration,
        run_check_inactive_products,
        run_check_duplicate_category_per_customer,
    ]

    results: List[Dict] = []
    for check_fn in checks:
        try:
            result = check_fn(pg, batch_id)
            results.append(result)
        except Exception as exc:
            LOG.error(f"Check {check_fn.__name__} failed with error: {exc}")
            results.append({
                "check_name": check_fn.__name__.replace("run_check_", ""),
                "status": FAIL,
                "summary": f"Erro na execucao: {exc}",
                "details": {"error": str(exc)},
            })

    # Persistir resultados
    LOG.info("")
    LOG.info("Persisting validation results...")
    for result in results:
        _persist_result(pg, batch_id, result)
    pg.commit()
    LOG.info(f"Results saved to reco.offer_validation ({len(results)} checks).")

    # Sumario final
    LOG.info("")
    LOG.info("=" * 70)
    LOG.info("VALIDATION SUMMARY")
    LOG.info("=" * 70)
    LOG.info(f"Batch ID: {batch_id}")
    LOG.info(f"Total ofertas: {total_offers:,}")
    LOG.info(f"Checks executados: {len(results)}")
    LOG.info("-" * 70)

    has_failures = False
    for result in results:
        marker = result["status"]
        if marker == FAIL:
            has_failures = True
        LOG.info(f"  [{marker:4s}] {result['check_name']}: {result['summary']}")

    LOG.info("-" * 70)
    if has_failures:
        LOG.warning("RESULTADO: FAIL — Existem checks com falha.")
    else:
        LOG.info("RESULTADO: OK — Nenhum check com falha critica.")
    LOG.info("=" * 70)

    pg.close()
    return has_failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validador de qualidade das ofertas geradas (reco.offers).",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="UUID do batch a validar. Se omitido, usa o mais recente.",
    )
    args = parser.parse_args()
    has_failures = main(batch_id=args.batch_id)
    sys.exit(1 if has_failures else 0)
