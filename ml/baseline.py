"""
ml/baseline.py
==============
Modelo 0 -- Baseline / Fallback (Top Vendidos por Loja).

ESTRATEGIA
  Recomenda os produtos mais populares (por compradores unicos) na loja
  principal do cliente nos ultimos 90 dias, excluindo produtos comprados
  recentemente pelo cliente (janela de recompra de 30 dias).

  Nao usa ML -- serve como:
    1. Fallback quando modelos A/B falham ou nao cobrem o cliente
    2. Baseline para comparacao justa de metricas (todo modelo ML deve superar)
    3. Cold start: clientes novos recebem o top global

CENARIOS
  | Cenario                          | Estrategia                                    |
  |----------------------------------|-----------------------------------------------|
  | Cliente com historico + loja     | Top vendidos na loja, excluindo ja comprados  |
  | Loja com < MIN_STORE_PURCHASES   | Fallback: top vendidos global                 |
  | Cliente novo (cold start)        | Top vendidos global                           |
  | Modelo A/B falhou                | Top vendidos global (nunca lista vazia)        |

PIPELINE
  1. Carrega historico de compras (mesmo filtro do Modelo A)
  2. Split temporal identico ao Modelo A (comparacao justa)
  3. Calcula top produtos por loja (compradores unicos, ultimos 90d do treino)
  4. Calcula top global (fallback)
  5. Para cada cliente: recomenda top da sua loja, excluindo compras recentes
  6. Avalia com as mesmas metricas: Precision@K, Recall@K, NDCG@K, MAP@K
  7. Salva metricas em reco.evaluation_runs

USO
  python ml/baseline.py                  # gera recomendacoes + avalia
  python ml/baseline.py --top-n 5        # top-5 por cliente
  python ml/baseline.py --dry-run        # mostra rankings sem avaliar
  python ml/baseline.py --history-days 730  # limita historico a 2 anos
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# Garante que a raiz do projeto esta no sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging
from ml.evaluate import (
    evaluate_ranking,
    print_ranking_report,
    save_metrics_to_db,
)

# Importa funcoes compartilhadas do Modelo A para manter consistencia
from ml.modelo_a_ranker import (
    HISTORY_WINDOW_DAYS,
    TEST_WINDOW_DAYS,
    VAL_WINDOW_DAYS,
    load_order_history,
    temporal_split,
)

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

LOG = setup_logging("ml.baseline")

# Numero minimo de compras na loja para usar o ranking local.
# Lojas abaixo desse limiar usam o ranking global (dados insuficientes).
MIN_STORE_PURCHASES = 10

# Top-N recomendacoes por cliente (padrao)
DEFAULT_TOP_N = 10

# Janela para calcular popularidade (dias antes da data de referencia)
POPULARITY_WINDOW_DAYS = 90

# Janela de recompra: exclui produtos comprados nos ultimos N dias
RECOMPRA_WINDOW_DAYS = 30


# ===========================================================================
# SECAO 1 -- RANKINGS DE POPULARIDADE
# ===========================================================================

def compute_store_rankings(
    df_train: pd.DataFrame,
    reference_date: pd.Timestamp,
    popularity_days: int = POPULARITY_WINDOW_DAYS,
) -> pd.DataFrame:
    """
    Calcula o ranking de produtos por loja baseado em compradores unicos.

    Usa apenas dados dentro da janela de popularidade (ultimos N dias do
    periodo de treino) para refletir tendencias recentes.

    Parametros
    ----------
    df_train       : historico de compras do periodo de treino
    reference_date : ultimo dia do periodo de treino
    popularity_days: janela de popularidade em dias

    Retorna
    -------
    pd.DataFrame com colunas: [store_id, product_id, n_buyers, store_rank]
    """
    window_start = reference_date - pd.Timedelta(days=popularity_days)
    df_window = df_train[df_train["sale_date"] >= window_start]

    LOG.info(
        f"Calculando rankings por loja "
        f"(janela: {window_start.date()} a {reference_date.date()}, "
        f"{len(df_window):,} transacoes)"
    )

    # Compradores unicos por (loja, produto)
    store_product_buyers = (
        df_window.groupby(["store_id", "product_id"])["customer_id"]
        .nunique()
        .reset_index(name="n_buyers")
    )

    # Rank dentro de cada loja (1 = mais popular)
    store_product_buyers["store_rank"] = (
        store_product_buyers.groupby("store_id")["n_buyers"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    store_product_buyers = store_product_buyers.sort_values(
        ["store_id", "store_rank"]
    )

    n_stores = store_product_buyers["store_id"].nunique()
    LOG.info(f"Rankings calculados para {n_stores} lojas")

    return store_product_buyers


def compute_global_ranking(
    df_train: pd.DataFrame,
    reference_date: pd.Timestamp,
    popularity_days: int = POPULARITY_WINDOW_DAYS,
) -> pd.DataFrame:
    """
    Calcula o ranking global de produtos (todas as lojas) por compradores unicos.

    Usado como fallback quando:
    - A loja do cliente tem poucos dados (< MIN_STORE_PURCHASES)
    - O cliente nao tem loja principal identificada (cold start)

    Retorna
    -------
    pd.DataFrame com colunas: [product_id, n_buyers, global_rank]
    """
    window_start = reference_date - pd.Timedelta(days=popularity_days)
    df_window = df_train[df_train["sale_date"] >= window_start]

    global_buyers = (
        df_window.groupby("product_id")["customer_id"]
        .nunique()
        .reset_index(name="n_buyers")
    )

    global_buyers["global_rank"] = (
        global_buyers["n_buyers"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    global_buyers = global_buyers.sort_values("global_rank")

    LOG.info(f"Ranking global: {len(global_buyers)} produtos")

    return global_buyers


# ===========================================================================
# SECAO 2 -- IDENTIFICACAO DA LOJA PRINCIPAL DO CLIENTE
# ===========================================================================

def identify_customer_stores(df_train: pd.DataFrame) -> Dict[int, int]:
    """
    Identifica a loja principal de cada cliente (onde mais comprou).

    Criterio: loja com maior numero de transacoes no historico de treino.
    Em caso de empate, usa a loja com a compra mais recente.

    Retorna
    -------
    dict {customer_id: store_id}
    """
    # Conta transacoes por (cliente, loja)
    customer_store_counts = (
        df_train.groupby(["customer_id", "store_id"])
        .agg(
            n_transactions=("sale_date", "count"),
            last_purchase=("sale_date", "max"),
        )
        .reset_index()
    )

    # Ordena por contagem (desc) e data mais recente (desc) para desempate
    customer_store_counts = customer_store_counts.sort_values(
        ["customer_id", "n_transactions", "last_purchase"],
        ascending=[True, False, False],
    )

    # Pega a primeira loja de cada cliente (a principal)
    main_stores = (
        customer_store_counts.groupby("customer_id")
        .first()
        .reset_index()
    )

    store_map = dict(zip(main_stores["customer_id"], main_stores["store_id"]))

    LOG.info(f"Lojas principais identificadas para {len(store_map):,} clientes")

    return store_map


# ===========================================================================
# SECAO 3 -- PRODUTOS COMPRADOS RECENTEMENTE (JANELA DE RECOMPRA)
# ===========================================================================

def get_recently_bought(
    df_train: pd.DataFrame,
    reference_date: pd.Timestamp,
    recompra_days: int = RECOMPRA_WINDOW_DAYS,
) -> Dict[int, Set[int]]:
    """
    Identifica produtos comprados recentemente por cada cliente.

    Estes produtos serao excluidos das recomendacoes (janela de recompra).

    Retorna
    -------
    dict {customer_id: set(product_ids)}
    """
    cutoff = reference_date - pd.Timedelta(days=recompra_days)
    recent = df_train[df_train["sale_date"] >= cutoff]

    recently_bought = (
        recent.groupby("customer_id")["product_id"]
        .apply(set)
        .to_dict()
    )

    return recently_bought


# ===========================================================================
# SECAO 4 -- GERACAO DE RECOMENDACOES BASELINE
# ===========================================================================

def generate_baseline_recommendations(
    df_train: pd.DataFrame,
    top_n: int = DEFAULT_TOP_N,
    min_store_purchases: int = MIN_STORE_PURCHASES,
    popularity_days: int = POPULARITY_WINDOW_DAYS,
    recompra_days: int = RECOMPRA_WINDOW_DAYS,
) -> pd.DataFrame:
    """
    Gera recomendacoes baseline para todos os clientes do historico de treino.

    Algoritmo por cliente:
    1. Identifica a loja principal do cliente
    2. Se a loja tem >= min_store_purchases compras na janela:
       -> Usa ranking da loja
    3. Senao:
       -> Usa ranking global (fallback)
    4. Exclui produtos que o cliente comprou nos ultimos recompra_days
    5. Retorna top_n produtos

    Parametros
    ----------
    df_train              : historico de compras (periodo de treino)
    top_n                 : numero de recomendacoes por cliente
    min_store_purchases   : limiar minimo de compras por loja
    popularity_days       : janela de popularidade
    recompra_days         : janela de recompra (exclusao)

    Retorna
    -------
    pd.DataFrame com colunas: [customer_id, product_id, score, rank, strategy]
    """
    reference_date = df_train["sale_date"].max()

    LOG.info(f"Gerando recomendacoes baseline (top-{top_n}) | ref={reference_date.date()}")

    # Pre-computa os rankings e dados auxiliares
    store_rankings = compute_store_rankings(df_train, reference_date, popularity_days)
    global_ranking = compute_global_ranking(df_train, reference_date, popularity_days)
    customer_stores = identify_customer_stores(df_train)
    recently_bought = get_recently_bought(df_train, reference_date, recompra_days)

    # Conta total de compras por loja na janela de popularidade para decidir fallback
    window_start = reference_date - pd.Timedelta(days=popularity_days)
    df_window = df_train[df_train["sale_date"] >= window_start]
    store_purchase_counts = df_window.groupby("store_id").size().to_dict()

    # Converte rankings para dicts para lookup rapido
    # {store_id: [(product_id, n_buyers), ...]} ordenados por rank
    store_ranking_dict: Dict[int, List[Tuple[int, int]]] = {}
    for store_id, group in store_rankings.groupby("store_id"):
        store_ranking_dict[store_id] = list(
            zip(group["product_id"], group["n_buyers"])
        )

    global_ranking_list = list(
        zip(global_ranking["product_id"], global_ranking["n_buyers"])
    )

    # Normaliza scores: n_buyers / max_buyers para ter score em [0, 1]
    max_global_buyers = global_ranking["n_buyers"].max() if len(global_ranking) > 0 else 1

    all_customers = df_train["customer_id"].unique()
    results = []
    n_store_reco = 0
    n_global_reco = 0

    for customer_id in all_customers:
        excluded = recently_bought.get(customer_id, set())
        store_id = customer_stores.get(customer_id)

        # Decide: ranking da loja ou global
        use_store = (
            store_id is not None
            and store_purchase_counts.get(store_id, 0) >= min_store_purchases
            and store_id in store_ranking_dict
        )

        if use_store:
            ranking = store_ranking_dict[store_id]
            source = "baseline_store"
        else:
            ranking = global_ranking_list
            source = "baseline_global"

        # Filtra produtos ja comprados e pega top_n
        rank_pos = 0
        for product_id, n_buyers in ranking:
            if product_id in excluded:
                continue
            rank_pos += 1
            score = n_buyers / max_global_buyers  # score normalizado
            results.append({
                "customer_id": customer_id,
                "product_id": product_id,
                "score": float(score),
                "rank": rank_pos,
                "strategy": "baseline",
            })
            if rank_pos >= top_n:
                break

        if use_store:
            n_store_reco += 1
        else:
            n_global_reco += 1

    reco_df = pd.DataFrame(results)

    LOG.info(
        f"Recomendacoes baseline geradas: {len(reco_df):,} sugestoes | "
        f"{reco_df['customer_id'].nunique():,} clientes"
    )
    LOG.info(f"  Ranking por loja: {n_store_reco:,} clientes")
    LOG.info(f"  Ranking global (fallback): {n_global_reco:,} clientes")

    return reco_df


# ===========================================================================
# SECAO 5 -- PIPELINE PRINCIPAL
# ===========================================================================

def run_pipeline(
    top_n: int = DEFAULT_TOP_N,
    dry_run: bool = False,
    history_days: int = HISTORY_WINDOW_DAYS,
) -> None:
    """
    Orquestra o pipeline completo do Modelo 0 (Baseline).

    Fluxo:
    1. Carrega historico de compras (mesmo filtro do Modelo A)
    2. Split temporal identico ao Modelo A
    3. Gera recomendacoes baseline no conjunto de treino
    4. Avalia contra o conjunto de teste
    5. Salva metricas em reco.evaluation_runs
    """
    LOG.info("=" * 70)
    LOG.info("MODELO 0 -- BASELINE (Top Vendidos por Loja)")
    LOG.info(f"  top_n={top_n} | dry_run={dry_run} | history_days={history_days}")
    LOG.info("=" * 70)

    pg = get_pg_conn()

    try:
        # -- 1. Carrega dados (mesmo filtro do Modelo A) ----------------------
        df = load_order_history(pg, history_days=history_days)

        if len(df) == 0:
            LOG.error("Nenhum dado encontrado. Verifique se o ETL foi executado.")
            sys.exit(1)

        # -- 2. Split temporal (identico ao Modelo A) -------------------------
        df_train, df_val, df_test = temporal_split(df)

        # Usa treino + validacao como historico para gerar recomendacoes
        # (mesmo que Modelo A usa treino+val para avaliacao final)
        df_history = pd.concat([df_train, df_val])

        if dry_run:
            LOG.info("[DRY RUN] Gerando rankings sem avaliar:")
            reference_date = df_history["sale_date"].max()
            store_rankings = compute_store_rankings(df_history, reference_date)
            global_ranking = compute_global_ranking(df_history, reference_date)
            LOG.info("\nTop 20 produtos GLOBAL:")
            LOG.info(global_ranking.head(20).to_string(index=False))
            LOG.info("\nTop 10 por loja (primeiras 3 lojas):")
            for store_id in store_rankings["store_id"].unique()[:3]:
                store_data = store_rankings[store_rankings["store_id"] == store_id]
                LOG.info(f"\n  Loja {store_id}:")
                LOG.info(store_data.head(10).to_string(index=False))
            return

        # -- 3. Gera recomendacoes baseline -----------------------------------
        reco_df = generate_baseline_recommendations(df_history, top_n=top_n)

        # -- 4. Avaliacao contra o conjunto de teste --------------------------
        LOG.info("\nAVALIACAO NO CONJUNTO DE TESTE:")

        # Agrupa recomendacoes por cliente
        reco_per_customer = (
            reco_df.sort_values(["customer_id", "rank"])
            .groupby("customer_id")["product_id"]
            .apply(list)
            .to_dict()
        )

        # Ground truth: produtos que cada cliente comprou no teste
        relevant_per_customer = (
            df_test.groupby("customer_id")["product_id"]
            .apply(list)
            .to_dict()
        )

        # Filtra apenas clientes presentes nos dois conjuntos
        common_customers = [
            c for c in reco_per_customer if c in relevant_per_customer
        ]
        LOG.info(f"Clientes com recomendacoes E compras no teste: {len(common_customers):,}")

        if len(common_customers) == 0:
            LOG.warning(
                "Nenhum cliente em comum entre recomendacoes e periodo de teste. "
                "Verifique se o split temporal esta correto."
            )
            return

        df_metrics = evaluate_ranking(
            reco_per_customer,
            relevant_per_customer,
            k_values=[5, 10, 20],
        )

        print_ranking_report(df_metrics, strategy_name="baseline")

        # -- 5. Salva metricas no banco ---------------------------------------
        notes = (
            f"history_days={history_days} | top_n={top_n} | "
            f"popularity_window={POPULARITY_WINDOW_DAYS}d | "
            f"recompra_window={RECOMPRA_WINDOW_DAYS}d | "
            f"min_store_purchases={MIN_STORE_PURCHASES} | "
            f"n_customers_evaluated={len(common_customers)}"
        )
        save_metrics_to_db(pg, df_metrics, strategy="baseline", notes=notes)

        LOG.info("Pipeline do Modelo 0 (Baseline) concluido com sucesso.")

    except Exception as exc:
        LOG.error(f"ERRO FATAL no pipeline do Baseline: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        pg.close()


# ===========================================================================
# ENTRYPOINT CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Modelo 0 -- Baseline (Top Vendidos por Loja)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python ml/baseline.py                    # gera recomendacoes + avalia
  python ml/baseline.py --top-n 5          # top-5 por cliente
  python ml/baseline.py --dry-run          # mostra rankings sem avaliar
  python ml/baseline.py --history-days 730 # limita historico a 2 anos
        """,
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Numero de recomendacoes por cliente (padrao: {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra rankings de popularidade sem avaliar no conjunto de teste",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=HISTORY_WINDOW_DAYS,
        help=f"Janela de historico em dias (padrao: {HISTORY_WINDOW_DAYS})",
    )
    args = parser.parse_args()

    run_pipeline(
        top_n=args.top_n,
        dry_run=args.dry_run,
        history_days=args.history_days,
    )
