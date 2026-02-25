"""
ml/generate_offers.py
=====================
Gera a lista de ofertas para toda a base de clientes ativos usando os
modelos A (LightGBM ranker) e B (SVD colaborativo) treinados, e persiste
os resultados em reco.offers com rastreabilidade completa.

Cada execução cria um novo offer_batch_id (UUID), permitindo comparar
batches ao longo do tempo e rastrear quais ofertas foram enviadas via
WhatsApp (coluna sent_via_whatsapp_at).

Uso:
    python ml/generate_offers.py                         # ambos os modelos, top-10
    python ml/generate_offers.py --strategy a            # apenas Modelo A
    python ml/generate_offers.py --strategy b            # apenas Modelo B
    python ml/generate_offers.py --top-n 5               # top-5 por cliente
    python ml/generate_offers.py --dry-run               # sem gravar no banco
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── Resolução do path ──────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging

# Importa funções do Modelo A
from ml.modelo_a_ranker import (
    HISTORY_WINDOW_DAYS as A_HISTORY_DAYS,
    load_model as load_model_a,
    load_order_history as load_history_a,
    generate_recommendations,
)

# Importa funções do Modelo B
from ml.modelo_b_colaborativo import (
    HISTORY_DAYS as B_HISTORY_DAYS,
    DEFAULT_K_NEIGHBORS,
    load_model_artifacts,
    load_order_history as load_history_b,
    compute_customer_similarity,
    generate_collaborative_recommendations,
)

LOG = setup_logging("ml.generate_offers")

# Validade padrão das ofertas geradas: 30 dias
OFFER_EXPIRY_DAYS = 30


# ===========================================================================
# GERAÇÃO — MODELO A
# ===========================================================================

def run_model_a(pg, top_n: int) -> pd.DataFrame:
    """Carrega histórico + modelo A e gera recomendações."""
    LOG.info("── Modelo A (LightGBM ranker) ──────────────────────────────────")
    df_history = load_history_a(pg, history_days=A_HISTORY_DAYS)
    model = load_model_a()
    reco_df = generate_recommendations(model, df_history, top_n=top_n)
    LOG.info(f"Modelo A: {len(reco_df):,} ofertas para {reco_df['customer_id'].nunique():,} clientes")
    return reco_df


# ===========================================================================
# GERAÇÃO — MODELO B
# ===========================================================================

def run_model_b(pg, top_n: int) -> pd.DataFrame:
    """Carrega artefatos do Modelo B + histórico e gera recomendações colaborativas."""
    LOG.info("── Modelo B (SVD colaborativo) ─────────────────────────────────")
    artifacts = load_model_artifacts()
    df_history = load_history_b(pg, history_days=B_HISTORY_DAYS)
    neighbors = compute_customer_similarity(
        artifacts["user_embeddings"],
        artifacts["customer_ids"],
        k_neighbors=DEFAULT_K_NEIGHBORS,
    )
    reco_df = generate_collaborative_recommendations(df_history, neighbors, top_n=top_n)
    LOG.info(f"Modelo B: {len(reco_df):,} ofertas para {reco_df['customer_id'].nunique():,} clientes")
    return reco_df


# ===========================================================================
# PERSISTÊNCIA
# ===========================================================================

def persist_offers(pg, df: pd.DataFrame, batch_id: str, generated_at: datetime, expires_at: datetime) -> int:
    """
    Insere as ofertas em reco.offers.

    Usa INSERT ON CONFLICT DO NOTHING para evitar duplicatas se o mesmo
    batch_id for inserido duas vezes (idempotente).

    Retorna o número de linhas efetivamente inseridas.
    """
    if df.empty:
        LOG.warning("DataFrame de ofertas vazio — nada a inserir.")
        return 0

    rows = [
        (
            batch_id,
            int(row["customer_id"]),
            int(row["product_id"]),
            str(row["strategy"]),
            float(row["score"]),
            int(row["rank"]),
            generated_at,
            expires_at,
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO reco.offers
            (offer_batch_id, customer_id, product_id, strategy, score, rank, generated_at, expires_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (offer_batch_id, customer_id, product_id) DO NOTHING
    """

    inserted = 0
    CHUNK = 1000
    with pg.cursor() as cur:
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i : i + CHUNK]
            cur.executemany(sql, chunk)
            inserted += cur.rowcount

    pg.commit()
    LOG.info(f"Inseridas {inserted:,} ofertas no banco (batch_id={batch_id})")
    return inserted


# ===========================================================================
# MAIN
# ===========================================================================

def main(strategy: str = "both", top_n: int = 10, dry_run: bool = False) -> None:
    LOG.info("=" * 70)
    LOG.info(f"GENERATE OFFERS | strategy={strategy} | top_n={top_n} | dry_run={dry_run}")
    LOG.info("=" * 70)

    pg = get_pg_conn()

    batch_id    = str(uuid.uuid4())
    generated_at = datetime.now(tz=timezone.utc)
    expires_at   = generated_at + timedelta(days=OFFER_EXPIRY_DAYS)

    LOG.info(f"Batch ID     : {batch_id}")
    LOG.info(f"Generated at : {generated_at.isoformat()}")
    LOG.info(f"Expires at   : {expires_at.isoformat()}")

    frames: List[pd.DataFrame] = []

    try:
        if strategy in ("a", "both"):
            frames.append(run_model_a(pg, top_n=top_n))

        if strategy in ("b", "both"):
            frames.append(run_model_b(pg, top_n=top_n))

        if not frames:
            LOG.error("Nenhuma estratégia selecionada.")
            sys.exit(1)

        df_all = pd.concat(frames, ignore_index=True)
        LOG.info(f"Total de ofertas geradas: {len(df_all):,} ({df_all['customer_id'].nunique():,} clientes únicos)")

        if dry_run:
            LOG.info("[DRY RUN] Sem gravação no banco. Amostra:")
            print(df_all.head(10).to_string(index=False))
            return

        inserted = persist_offers(pg, df_all, batch_id, generated_at, expires_at)
        LOG.info(f"Concluído. {inserted:,} ofertas salvas em reco.offers.")

    except Exception as exc:
        LOG.error(f"ERRO: {exc}", exc_info=True)
        try:
            pg.rollback()
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            pg.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera e persiste listas de ofertas para toda a base de clientes."
    )
    parser.add_argument(
        "--strategy",
        choices=["a", "b", "both"],
        default="both",
        help="Modelo(s) a usar: 'a' = LightGBM, 'b' = SVD colaborativo, 'both' = ambos (padrão)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Número de recomendações por cliente (padrão: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Gera as ofertas mas NÃO grava no banco — mostra amostra",
    )
    args = parser.parse_args()
    main(strategy=args.strategy, top_n=args.top_n, dry_run=args.dry_run)
