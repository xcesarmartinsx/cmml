"""
ml/feedback_loop.py
===================
Motor de cross-reference automatico: cruza ofertas geradas com vendas reais
para medir a taxa de conversao e retroalimentar os modelos.

ESTRATEGIA
  1. Busca ofertas ativas (nao expiradas) sem outcome registrado
  2. JOIN com cur.order_items onde sale_date BETWEEN generated_at AND generated_at + window_days
  3. Match por (customer_id, product_id) -> insere converted=TRUE em offer_outcomes
  4. Ofertas cujo window expirou sem match -> insere converted=FALSE
  5. Registra execucao em reco.feedback_runs

USO
  python ml/feedback_loop.py                     # todos os batches ativos
  python ml/feedback_loop.py --batch-id <UUID>   # batch especifico
  python ml/feedback_loop.py --window-days 45    # janela customizada
  python ml/feedback_loop.py --dry-run           # preview sem gravar
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Resolucao do path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging

LOG = setup_logging("ml.feedback_loop")

DEFAULT_WINDOW_DAYS = 30


def run_cross_reference(
    pg,
    batch_id: Optional[str] = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    dry_run: bool = False,
    triggered_by: str = "cli",
) -> dict:
    """
    Executa o cross-reference entre ofertas e vendas reais.

    Retorna dict com estatisticas: offers_evaluated, offers_converted, conversion_rate.
    """
    started_at = datetime.now(tz=timezone.utc)
    run_id = None

    if not dry_run:
        with pg.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reco.feedback_runs
                    (offer_batch_id, started_at, status, conversion_window_days, triggered_by)
                VALUES (%s, %s, 'running', %s, %s)
                RETURNING run_id
                """,
                (batch_id, started_at, window_days, triggered_by),
            )
            run_id = cur.fetchone()[0]
        pg.commit()
        LOG.info(f"Feedback run #{run_id} started (window={window_days}d, batch={batch_id or 'all'})")

    try:
        # Step 1: Find converted offers (match with cur.order_items)
        batch_filter = ""
        params = [window_days]
        if batch_id:
            batch_filter = "AND o.offer_batch_id = %s::uuid"
            params.append(batch_id)

        query_converted = f"""
            SELECT
                o.offer_id,
                oi.order_id_src,
                oi.sale_date AS conversion_date,
                oi.quantity,
                oi.total_value
            FROM reco.offers o
            JOIN cur.order_items oi
                ON oi.customer_id = o.customer_id
               AND oi.product_id  = o.product_id
               AND oi.sale_date >= o.generated_at::date
               AND oi.sale_date <= (o.generated_at + make_interval(days => %s))::date
            WHERE NOT EXISTS (
                SELECT 1 FROM reco.offer_outcomes oo
                WHERE oo.offer_id = o.offer_id
            )
            {batch_filter}
        """
        with pg.cursor() as cur:
            cur.execute(query_converted, params)
            converted_rows = cur.fetchall()

        LOG.info(f"Converted matches found: {len(converted_rows)}")

        # Step 2: Find expired offers with no match (window passed, no conversion)
        query_expired = f"""
            SELECT o.offer_id
            FROM reco.offers o
            WHERE NOT EXISTS (
                SELECT 1 FROM reco.offer_outcomes oo
                WHERE oo.offer_id = o.offer_id
            )
            AND (o.generated_at + make_interval(days => %s))::date < CURRENT_DATE
            {batch_filter}
            AND NOT EXISTS (
                SELECT 1 FROM cur.order_items oi
                WHERE oi.customer_id = o.customer_id
                  AND oi.product_id  = o.product_id
                  AND oi.sale_date >= o.generated_at::date
                  AND oi.sale_date <= (o.generated_at + make_interval(days => %s))::date
            )
        """
        expired_params = [window_days]
        if batch_id:
            expired_params.append(batch_id)
        expired_params.append(window_days)

        with pg.cursor() as cur:
            cur.execute(query_expired, expired_params)
            expired_rows = cur.fetchall()

        LOG.info(f"Expired (no conversion) offers found: {len(expired_rows)}")

        offers_evaluated = len(converted_rows) + len(expired_rows)
        offers_converted = len(converted_rows)
        conversion_rate = (
            round(offers_converted * 100.0 / offers_evaluated, 2)
            if offers_evaluated > 0
            else 0.0
        )

        if dry_run:
            LOG.info("[DRY RUN] No changes written to database.")
            LOG.info(f"  Would mark {offers_converted} offers as converted")
            LOG.info(f"  Would mark {len(expired_rows)} offers as not converted")
            LOG.info(f"  Conversion rate: {conversion_rate}%")
            return {
                "offers_evaluated": offers_evaluated,
                "offers_converted": offers_converted,
                "conversion_rate": conversion_rate,
            }

        # Step 3: Insert converted outcomes
        if converted_rows:
            with pg.cursor() as cur:
                for row in converted_rows:
                    offer_id, order_id_src, conversion_date, quantity, total_value = row
                    cur.execute(
                        """
                        INSERT INTO reco.offer_outcomes
                            (offer_id, converted, conversion_date, conversion_source,
                             order_id_src, quantity, total_value, matched_by)
                        VALUES (%s, TRUE, %s, 'automatic', %s, %s, %s, 'cross_reference')
                        ON CONFLICT (offer_id, order_id_src) DO NOTHING
                        """,
                        (offer_id, conversion_date, order_id_src, quantity, total_value),
                    )
            pg.commit()

        # Step 4: Insert non-converted outcomes for expired offers
        if expired_rows:
            with pg.cursor() as cur:
                for (offer_id,) in expired_rows:
                    cur.execute(
                        """
                        INSERT INTO reco.offer_outcomes
                            (offer_id, converted, conversion_source, matched_by)
                        VALUES (%s, FALSE, 'automatic', 'cross_reference_expired')
                        ON CONFLICT (offer_id, order_id_src) DO NOTHING
                        """,
                        (offer_id,),
                    )
            pg.commit()

        # Step 5: Update feedback_run
        finished_at = datetime.now(tz=timezone.utc)
        with pg.cursor() as cur:
            cur.execute(
                """
                UPDATE reco.feedback_runs
                SET finished_at = %s,
                    status = 'completed',
                    offers_evaluated = %s,
                    offers_converted = %s,
                    conversion_rate = %s
                WHERE run_id = %s
                """,
                (finished_at, offers_evaluated, offers_converted, conversion_rate, run_id),
            )
        pg.commit()

        LOG.info(f"Feedback run #{run_id} completed: "
                 f"{offers_evaluated} evaluated, {offers_converted} converted "
                 f"({conversion_rate}%)")

        return {
            "run_id": run_id,
            "offers_evaluated": offers_evaluated,
            "offers_converted": offers_converted,
            "conversion_rate": conversion_rate,
        }

    except Exception as exc:
        LOG.error(f"Feedback run failed: {exc}", exc_info=True)
        if run_id and not dry_run:
            try:
                with pg.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE reco.feedback_runs
                        SET finished_at = now(), status = 'error', error_message = %s
                        WHERE run_id = %s
                        """,
                        (str(exc)[:500], run_id),
                    )
                pg.commit()
            except Exception:
                pass
        raise


def main(
    batch_id: Optional[str] = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    dry_run: bool = False,
) -> None:
    LOG.info("=" * 70)
    LOG.info(f"FEEDBACK LOOP | batch={batch_id or 'all'} | window={window_days}d | dry_run={dry_run}")
    LOG.info("=" * 70)

    pg = get_pg_conn()
    try:
        result = run_cross_reference(
            pg,
            batch_id=batch_id,
            window_days=window_days,
            dry_run=dry_run,
            triggered_by="cli",
        )
        LOG.info(f"Result: {result}")
    finally:
        pg.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cross-reference ofertas com vendas reais para medir conversao."
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="UUID do batch especifico (padrao: todos os batches ativos)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help=f"Janela de conversao em dias (padrao: {DEFAULT_WINDOW_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview sem gravar no banco",
    )
    args = parser.parse_args()
    main(batch_id=args.batch_id, window_days=args.window_days, dry_run=args.dry_run)
