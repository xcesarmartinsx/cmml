"""
ml/modelo_b_colaborativo.py
============================
Modelo B — Filtragem Colaborativa (Clientes Similares Compraram).

ESTRATÉGIA
  "Clientes que compraram os mesmos produtos que você também compraram X."

  Usa Decomposição em Valores Singulares (SVD) para aprender representações
  vetoriais (embeddings) de clientes e produtos. Clientes próximos no espaço
  vetorial têm padrões de compra similares.

PIPELINE COMPLETO
  1. Carrega histórico de compras de cur.order_items
  2. Constrói a matriz usuário × produto (matrix de compras)
  3. Split temporal
  4. Aplica SVD truncado para obter embeddings de clientes e produtos
  5. Para cada cliente, encontra os K vizinhos mais similares
  6. Recomenda produtos comprados pelos vizinhos (que o cliente ainda não tem)
  7. Avalia: Precision@K, Recall@K, NDCG@K, MAP@K, HitRate@K
  8. Salva embeddings e métricas

POR QUE SVD?
  A matriz usuário × produto é extremamente esparsa (cada cliente compra
  uma fração minúscula do catálogo). SVD encontra fatores latentes que
  explicam os padrões de compra, mesmo com dados esparsos.

  Alternativas consideradas:
  - ALS (Alternating Least Squares): melhor para dados implícitos (clicar,
    visualizar), mas mais pesado computacionalmente. Indicado quando a base
    crescer muito.
  - Word2Vec de produtos: muito bom para capturar sequência temporal de
    compras (produto A costuma ser comprado antes do produto B).
    Implementar em versão futura.
  - Redes neurais (Neural CF, BERT4Rec): melhor resultado, mais difícil
    de manter e re-treinar. Reservar para Fase 2.

COLD START
  Clientes novos (sem histórico) não podem ser representados por SVD.
  Estratégia: fallback para Modelo 0 (top vendidos por loja/categoria).
  O campo `strategy` na tabela reco.sugestoes identifica qual modelo foi usado.

USO
  # Treinar e salvar modelo:
  python ml/modelo_b_colaborativo.py

  # Apenas avaliar:
  python ml/modelo_b_colaborativo.py --eval-only

  # Controlar dimensionalidade do embedding:
  python ml/modelo_b_colaborativo.py --n-factors 100

  # Dry-run (exibe matriz esparsa sem treinar):
  python ml/modelo_b_colaborativo.py --dry-run
"""

import argparse
import logging
import pickle
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.linalg import svds
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize

# Garante que a raiz do projeto está no sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging
from ml.evaluate import (
    evaluate_ranking,
    print_ranking_report,
    save_metrics_to_db,
)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

LOG = setup_logging("ml.modelo_b")

# Diretório de modelos salvos
MODEL_DIR  = _PROJECT_ROOT / "app" / "ml" / "models" / "classification"
MODEL_PATH = MODEL_DIR / "modelo_b_colaborativo_v1.pkl"

# Produtos a excluir — não são produtos reais de venda (serviços/taxas).
PRODUCT_BLACKLIST = [
    'BORDADO',
    'TAXA DE ENTREGA',
    'TAXA ENTREGA',
]

# Parâmetros do SVD
DEFAULT_N_FACTORS = 64     # dimensões do espaço latente
                            # ↑ mais fatores = mais expressivo, mais lento
                            # 32-128 é bom ponto de partida

# Vizinhos para collaborative filtering
DEFAULT_K_NEIGHBORS = 50   # clientes similares usados para gerar candidatos

# Janela de split temporal
TEST_WINDOW_DAYS = 30
VAL_WINDOW_DAYS  = 60
HISTORY_DAYS     = 1825  # ~5 anos; ajustável via --history-days na CLI

# Recomendações por cliente
DEFAULT_TOP_N = 10


# ===========================================================================
# SEÇÃO 1 — EXTRAÇÃO DE DADOS
# ===========================================================================

def load_order_history(pg, history_days: int = HISTORY_DAYS) -> pd.DataFrame:
    """
    Carrega histórico de compras para construção da matriz usuário × produto.

    Parâmetros
    ----------
    pg           : conexão psycopg2
    history_days : janela de histórico em dias

    Retorna
    -------
    pd.DataFrame com colunas: [customer_id, product_id, sale_date, quantity, total_value]
    """
    cutoff = (date.today() - timedelta(days=history_days)).isoformat()

    query = f"""
        SELECT
            oi.customer_id,
            oi.product_id,
            oi.sale_date,
            oi.quantity,
            oi.total_value
        FROM cur.order_items oi
        JOIN cur.products p ON p.product_id = oi.product_id
        JOIN stg.customers sc
            ON sc.customer_id_src = oi.customer_id
           AND sc.source_system   = 'sqlserver_gp'
        WHERE oi.sale_date >= '{cutoff}'
          AND p.active = TRUE
          AND p.description NOT IN ({','.join(f"'{p}'" for p in PRODUCT_BLACKLIST)})
          AND sc.name NOT ILIKE '%BALCAO%'     -- exclui CLIENTE BALCAO (walk-in sem cadastro)
          AND sc.name NOT ILIKE '%CONSUMIDOR%' -- exclui CONSUMIDOR FINAL
          AND sc.name NOT ILIKE '%GENERICO%'   -- exclui clientes genéricos
          AND sc.name NOT ILIKE '%AVULSO%'     -- exclui vendas avulsas
        ORDER BY oi.sale_date ASC
    """

    LOG.info(f"Carregando histórico de compras (últimos {history_days} dias)...")
    df = pd.read_sql(query, pg)
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    LOG.info(
        f"Histórico: {len(df):,} linhas | "
        f"{df['customer_id'].nunique():,} clientes | "
        f"{df['product_id'].nunique():,} produtos"
    )
    return df


# ===========================================================================
# SEÇÃO 2 — SPLIT TEMPORAL
# ===========================================================================

def temporal_split(
    df: pd.DataFrame,
    test_days: int = TEST_WINDOW_DAYS,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Divide o dataset em treino (passado) e teste (futuro).

    Para o modelo colaborativo, usamos split simples (treino/teste) porque
    não precisamos de validação para tuning de hiperparâmetros.
    O número de fatores (n_factors) pode ser ajustado por cross-validation
    leave-one-out em versão futura.

    Retorna
    -------
    (df_train, df_test)
    """
    max_date   = df["sale_date"].max()
    test_start = max_date - pd.Timedelta(days=test_days)

    df_train = df[df["sale_date"] <= test_start]
    df_test  = df[df["sale_date"] >  test_start]

    LOG.info("SPLIT TEMPORAL (Modelo B):")
    LOG.info(f"  Treino:  até {test_start.date()}  → {len(df_train):,} linhas")
    LOG.info(f"  Teste:   {test_start.date()} → {max_date.date()} → {len(df_test):,} linhas")

    return df_train, df_test


# ===========================================================================
# SEÇÃO 3 — CONSTRUÇÃO DA MATRIZ ESPARSA
# ===========================================================================

def build_interaction_matrix(
    df: pd.DataFrame,
    value_col: str = "purchase_count",
) -> Tuple[sparse.csr_matrix, np.ndarray, np.ndarray]:
    """
    Constrói a matriz esparsa de interações usuário × produto.

    Cada célula representa o "interesse" do cliente no produto.
    Usamos o número de vezes que comprou (contagem implícita) em vez de
    valores monetários para evitar que clientes com alto ticket distorçam
    a similaridade.

    Estratégia de pontuação:
      - Compra = 1+ (dado implícito positivo)
      - Sem compra = 0 (não sabemos se não gosta ou não conhece)
      - Compras repetidas aumentam o score (reforço)

    Parâmetros
    ----------
    df        : DataFrame com colunas [customer_id, product_id, ...]
    value_col : coluna com o valor da interação

    Retorna
    -------
    (matriz esparsa CSR, array de customer_ids, array de product_ids)
    """
    LOG.info("Construindo matriz de interações usuário × produto...")

    # Conta o número de transações por par (cliente, produto)
    interactions = (
        df.groupby(["customer_id", "product_id"])
        .size()
        .reset_index(name="purchase_count")
    )

    # Índices para criação da matriz esparsa
    customer_ids = interactions["customer_id"].unique()
    product_ids  = interactions["product_id"].unique()

    customer_index = {cid: i for i, cid in enumerate(customer_ids)}
    product_index  = {pid: i for i, pid in enumerate(product_ids)}

    # Mapeia IDs para índices inteiros da matriz
    row_idx = interactions["customer_id"].map(customer_index).values
    col_idx = interactions["product_id"].map(product_index).values
    data    = interactions["purchase_count"].values.astype(np.float32)

    # Normalização logarítmica: log(1 + count) suaviza o efeito de clientes
    # que compraram muito o mesmo produto (compradores "fanáticos" não devem
    # dominar completamente a similaridade)
    data = np.log1p(data)

    # Boost for offer conversions: pairs that converted from offers get extra weight
    try:
        import psycopg2 as _pg2
        feedback_pg = get_pg_conn()
        conversion_query = """
            SELECT o.customer_id, o.product_id, COUNT(*) AS conversion_count
            FROM reco.offers o
            JOIN reco.offer_outcomes oo ON oo.offer_id = o.offer_id AND oo.converted = TRUE
            GROUP BY o.customer_id, o.product_id
        """
        conv_df = pd.read_sql(conversion_query, feedback_pg)
        feedback_pg.close()

        if not conv_df.empty:
            conv_merged = interactions.merge(conv_df, on=["customer_id", "product_id"], how="left")
            conversion_counts = conv_merged["conversion_count"].fillna(0).values.astype(np.float32)
            # Boost: weight = log(1 + purchase_count + 0.5 * offer_conversion_count)
            raw_counts = interactions["purchase_count"].values.astype(np.float32)
            data = np.log1p(raw_counts + 0.5 * conversion_counts)
            LOG.info(f"Offer conversion boost applied to {(conversion_counts > 0).sum():,} pairs")
    except Exception as exc:
        LOG.warning(f"Could not apply offer conversion boost (table may not exist yet): {exc}")

    # Cria matriz esparsa CSR (Compressed Sparse Row) — eficiente para SVD
    matrix = sparse.csr_matrix(
        (data, (row_idx, col_idx)),
        shape=(len(customer_ids), len(product_ids)),
    )

    # Estatísticas da esparsidade
    total_cells   = matrix.shape[0] * matrix.shape[1]
    filled_cells  = matrix.nnz
    sparsity      = 1 - (filled_cells / total_cells)

    LOG.info(f"Matriz: {matrix.shape[0]:,} clientes × {matrix.shape[1]:,} produtos")
    LOG.info(f"Células preenchidas: {filled_cells:,} ({100*(1-sparsity):.4f}% denso)")
    LOG.info(f"Esparsidade: {sparsity*100:.2f}%")

    return matrix, customer_ids, product_ids


# ===========================================================================
# SEÇÃO 4 — SVD TRUNCADO (Fatoração de Matriz)
# ===========================================================================

def fit_svd(
    matrix: sparse.csr_matrix,
    n_factors: int = DEFAULT_N_FACTORS,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Aplica SVD truncado para obter embeddings de clientes e produtos.

    SVD decompõe a matriz R (usuários × produtos) em:
      R ≈ U × Σ × Vt

    Onde:
      U  (n_users × n_factors)   : embeddings de clientes
      Σ  (n_factors,)            : valores singulares (importância de cada fator)
      Vt (n_factors × n_products): embeddings de produtos

    Após o SVD, cliente_embedding = U[i] × sqrt(Σ)
    Isso normaliza a contribuição de cada fator.

    Por que SVD truncado?
      O SVD completo seria inviável para matrizes grandes. O truncado
      encontra os `n_factors` maiores valores singulares, capturando os
      padrões mais importantes sem guardar todos os vetores.

    Parâmetros
    ----------
    matrix    : matriz esparsa usuário × produto
    n_factors : dimensões do espaço latente

    Retorna
    -------
    (U_scaled, Sigma, Vt_scaled) — embeddings prontos para similaridade
    """
    LOG.info(f"Aplicando SVD truncado com {n_factors} fatores...")

    # Limita n_factors ao máximo possível dado o tamanho da matriz
    max_factors = min(matrix.shape) - 1
    n_factors   = min(n_factors, max_factors)
    LOG.info(f"  n_factors ajustado: {n_factors} (max disponível: {max_factors})")

    # svds retorna os MENORES valores singulares por padrão — invertemos
    U, sigma, Vt = svds(matrix, k=n_factors)

    # Ordena por valor singular decrescente (mais importantes primeiro)
    order = np.argsort(sigma)[::-1]
    U     = U[:, order]
    sigma = sigma[order]
    Vt    = Vt[order, :]

    # Escala os embeddings pelo raiz dos valores singulares.
    # Isso garante que clientes com padrões mais fortes tenham vetores maiores.
    sqrt_sigma  = np.sqrt(sigma)
    U_scaled    = U  * sqrt_sigma[np.newaxis, :]   # (n_users × n_factors)
    Vt_scaled   = Vt * sqrt_sigma[:, np.newaxis]   # (n_factors × n_products)

    LOG.info(f"SVD concluído:")
    LOG.info(f"  Embeddings de clientes: {U_scaled.shape}")
    LOG.info(f"  Embeddings de produtos: {Vt_scaled.T.shape}")
    LOG.info(f"  Variância explicada pelos top-10 fatores: "
             f"{(sigma[:10]**2).sum() / (sigma**2).sum()*100:.1f}%")

    return U_scaled, sigma, Vt_scaled.T  # Vt.T para ter (n_products × n_factors)


# ===========================================================================
# SEÇÃO 5 — GERAÇÃO DE RECOMENDAÇÕES
# ===========================================================================

def compute_customer_similarity(
    user_embeddings: np.ndarray,
    customer_ids: np.ndarray,
    k_neighbors: int = DEFAULT_K_NEIGHBORS,
) -> Dict[int, List[int]]:
    """
    Calcula os K clientes mais similares para cada cliente.

    Usa similaridade de cosseno entre os embeddings (vetores SVD).
    Cosseno é preferível à distância euclidiana para embeddings porque
    clientes que compram os mesmos produtos na mesma proporção são similares,
    independente do volume total de compras.

    Parâmetros
    ----------
    user_embeddings : array (n_users × n_factors) do SVD
    customer_ids    : array de IDs originais dos clientes
    k_neighbors     : número de vizinhos mais similares

    Retorna
    -------
    dict {customer_id: [neighbor_id_1, ..., neighbor_id_K]}
    """
    LOG.info(f"Calculando similaridade entre {len(customer_ids):,} clientes (k={k_neighbors})...")

    # Normaliza para obter vetores unitários (necessário para cosseno)
    embeddings_norm = normalize(user_embeddings, norm="l2")

    # Calcula a matriz de similaridade de cosseno completa
    # ATENÇÃO: se n_users for muito grande (>100k), usar indexação aproximada
    # (Faiss, Annoy ou pgvector) ao invés desta abordagem densa.
    sim_matrix = cosine_similarity(embeddings_norm)

    # Para cada cliente, encontra os K mais similares (excluindo ele mesmo)
    neighbors = {}
    n = len(customer_ids)

    for i in range(n):
        # Ordena por similaridade decrescente, exclui o próprio cliente (diagonal)
        sim_row = sim_matrix[i].copy()
        sim_row[i] = -1  # exclui self-similarity

        top_k_indices = np.argsort(sim_row)[::-1][:k_neighbors]
        neighbors[customer_ids[i]] = [customer_ids[j] for j in top_k_indices]

    LOG.info("Similaridade calculada.")
    return neighbors


def generate_collaborative_recommendations(
    df_train: pd.DataFrame,
    neighbors: Dict[int, List[int]],
    top_n: int = DEFAULT_TOP_N,
    exclude_recently_bought_days: int = 30,
    include_ever_bought: bool = True,
) -> pd.DataFrame:
    """
    Gera recomendações colaborativas usando os vizinhos mais similares.

    Algoritmo:
    1. Para o cliente C, identifica seus K vizinhos mais similares
    2. Coleta todos os produtos comprados pelos vizinhos
    3. Exclui produtos que C comprou recentemente (janela de recompra)
    4. Ordena por popularidade entre os vizinhos (quantos vizinhos compraram)
    5. Retorna top_n

    Score = numero de vizinhos que compraram o produto
    (normalizado pelo total de vizinhos)

    Parametros
    ----------
    df_train                    : historico de treino (para saber o que cada um comprou)
    neighbors                   : {customer_id: [neighbor_ids]}
    top_n                       : numero de recomendacoes por cliente
    exclude_recently_bought_days: janela de exclusao (produtos comprados recentemente)
    include_ever_bought         : se True, permite recomendar produtos que o cliente ja
                                  comprou (foco em recompra); filtra apenas compras recentes.
                                  Se False, comportamento original (exclui tudo que ja comprou).

    Retorna
    -------
    pd.DataFrame com colunas: [customer_id, product_id, score, rank, strategy]
    """
    LOG.info(f"Gerando recomendações colaborativas (top-{top_n} por cliente)...")

    # Pré-computa: produtos comprados por cada cliente (conjunto para lookup rápido)
    customer_products = (
        df_train.groupby("customer_id")["product_id"]
        .apply(set)
        .to_dict()
    )

    # Produtos comprados recentemente (excluídos das recomendações)
    max_date = df_train["sale_date"].max()
    recent_cutoff = max_date - pd.Timedelta(days=exclude_recently_bought_days)
    recently_bought = (
        df_train[df_train["sale_date"] >= recent_cutoff]
        .groupby("customer_id")["product_id"]
        .apply(set)
        .to_dict()
    )

    results = []
    processed = 0

    for customer_id, neighbor_ids in neighbors.items():
        # Produtos que este cliente já comprou (histórico completo)
        own_products  = customer_products.get(customer_id, set())
        recent_bought = recently_bought.get(customer_id, set())

        # Conta em quantos vizinhos cada produto aparece
        product_votes: Dict[int, int] = {}

        for neighbor_id in neighbor_ids:
            for pid in customer_products.get(neighbor_id, set()):
                if include_ever_bought:
                    # Modo recompra: exclui apenas compras recentes, permite
                    # recomendar produtos que o cliente ja comprou no passado
                    if pid not in recent_bought:
                        product_votes[pid] = product_votes.get(pid, 0) + 1
                else:
                    # Modo original: exclui tudo que o cliente ja comprou
                    if pid not in own_products and pid not in recent_bought:
                        product_votes[pid] = product_votes.get(pid, 0) + 1

        if not product_votes:
            # Sem candidatos — o cliente e seus vizinhos compraram tudo
            # (caso raro, mas tratado para robustez)
            LOG.debug(f"Cliente {customer_id}: sem candidatos colaborativos")
            processed += 1
            continue

        # Ordena por votos (popularidade entre vizinhos) e pega top_n
        n_neighbors = len(neighbor_ids)
        sorted_products = sorted(product_votes.items(), key=lambda x: x[1], reverse=True)

        for rank, (pid, votes) in enumerate(sorted_products[:top_n], start=1):
            results.append({
                "customer_id": customer_id,
                "product_id":  pid,
                "score":       votes / n_neighbors,  # proporção de vizinhos que compraram
                "rank":        rank,
                "strategy":    "modelo_b_colaborativo",
            })

        processed += 1
        if processed % 1000 == 0:
            LOG.info(f"  Processados: {processed:,}/{len(neighbors):,} clientes")

    reco_df = pd.DataFrame(results)
    LOG.info(
        f"Recomendações colaborativas: {len(reco_df):,} sugestões para "
        f"{reco_df['customer_id'].nunique():,} clientes"
    )
    return reco_df


# ===========================================================================
# SEÇÃO 6 — ANÁLISE DE QUALIDADE DOS EMBEDDINGS
# ===========================================================================

def analyze_embeddings(
    user_embeddings: np.ndarray,
    product_embeddings: np.ndarray,
    customer_ids: np.ndarray,
    product_ids: np.ndarray,
    sigma: np.ndarray,
) -> None:
    """
    Diagnóstico dos embeddings SVD para verificar qualidade.

    Verifica:
    - Distribuição dos valores singulares (queda brusca = boa separação)
    - Variância explicada acumulada
    - Norma média dos vetores (embeddings muito pequenos = produto raro)
    """
    LOG.info("\nDIAGNÓSTICO DOS EMBEDDINGS SVD:")
    LOG.info("-" * 50)

    # Variância explicada por fator
    total_var   = (sigma ** 2).sum()
    cum_var     = np.cumsum(sigma ** 2) / total_var * 100
    LOG.info(f"Variância explicada (cumulativa):")
    for i, pct in zip([1, 5, 10, 20, 50], [cum_var[min(i-1, len(cum_var)-1)] for i in [1, 5, 10, 20, 50]]):
        LOG.info(f"  Top-{i:>2} fatores: {pct:.1f}%")

    # Norma dos embeddings de clientes
    user_norms    = np.linalg.norm(user_embeddings, axis=1)
    product_norms = np.linalg.norm(product_embeddings, axis=1)

    LOG.info(f"\nNorma dos embeddings de clientes:")
    LOG.info(f"  Média: {user_norms.mean():.4f} | Std: {user_norms.std():.4f}")
    LOG.info(f"  Min: {user_norms.min():.4f} | Max: {user_norms.max():.4f}")

    LOG.info(f"Norma dos embeddings de produtos:")
    LOG.info(f"  Média: {product_norms.mean():.4f} | Std: {product_norms.std():.4f}")

    # Alerta: clientes com norma muito baixa têm pouquíssimas compras
    # (embeddings fracos = recomendações menos confiáveis)
    weak_users = (user_norms < np.percentile(user_norms, 10)).sum()
    LOG.info(f"\nClientes com embedding fraco (norma < p10): {weak_users:,} "
             f"({weak_users/len(user_norms)*100:.1f}%) — cold start candidatos")


# ===========================================================================
# SEÇÃO 7 — AVALIAÇÃO DO MODELO
# ===========================================================================

def evaluate_model(
    reco_per_customer: Dict[int, List[int]],
    df_test: pd.DataFrame,
) -> pd.DataFrame:
    """
    Avalia as recomendações colaborativas contra o conjunto de teste.

    Parâmetros
    ----------
    reco_per_customer : {customer_id: [product_ids recomendados, ordenados]}
    df_test           : compras do período de teste (ground truth)

    Retorna
    -------
    DataFrame de métricas por K
    """
    # Ground truth: produtos que cada cliente realmente comprou no teste
    relevant_per_customer = (
        df_test.groupby("customer_id")["product_id"]
        .apply(list)
        .to_dict()
    )

    # Apenas avalia clientes que aparecem nos dois conjuntos
    common_customers = [
        c for c in reco_per_customer if c in relevant_per_customer
    ]
    LOG.info(f"Clientes com recomendações E compras no teste: {len(common_customers):,}")

    if len(common_customers) == 0:
        LOG.warning(
            "Nenhum cliente em comum entre recomendações e período de teste. "
            "Verifique se o split temporal está correto."
        )
        return pd.DataFrame()

    df_metrics = evaluate_ranking(
        reco_per_customer,
        relevant_per_customer,
        k_values=[5, 10, 20],
    )
    return df_metrics


# ===========================================================================
# SEÇÃO 8 — PERSISTÊNCIA
# ===========================================================================

def save_model_artifacts(
    user_embeddings:    np.ndarray,
    product_embeddings: np.ndarray,
    customer_ids:       np.ndarray,
    product_ids:        np.ndarray,
    sigma:              np.ndarray,
    n_factors:          int,
    path:               Path = MODEL_PATH,
) -> None:
    """
    Salva todos os artefatos do modelo em disco.

    Artefatos salvos:
    - Embeddings de clientes (U × sqrt(Σ))
    - Embeddings de produtos (Vt.T × sqrt(Σ))
    - Mapeamento de IDs para índices da matriz
    - Valores singulares (Σ)
    - Metadados (n_factors, data de treino)
    """
    import datetime

    path.parent.mkdir(parents=True, exist_ok=True)

    artifacts = {
        "user_embeddings":    user_embeddings,
        "product_embeddings": product_embeddings,
        "customer_ids":       customer_ids,
        "product_ids":        product_ids,
        "sigma":              sigma,
        "n_factors":          n_factors,
        "trained_at":         datetime.datetime.now().isoformat(),
        # Índices para lookup rápido (ID → posição na matriz)
        "customer_index":     {cid: i for i, cid in enumerate(customer_ids)},
        "product_index":      {pid: i for i, pid in enumerate(product_ids)},
    }

    with open(path, "wb") as f:
        pickle.dump(artifacts, f, protocol=pickle.HIGHEST_PROTOCOL)

    LOG.info(f"Artefatos do Modelo B salvos em: {path}")
    LOG.info(f"  Clientes: {len(customer_ids):,} | Produtos: {len(product_ids):,} | Fatores: {n_factors}")


def load_model_artifacts(path: Path = MODEL_PATH) -> dict:
    """
    Carrega artefatos do Modelo B previamente treinado.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Modelo não encontrado em '{path}'. "
            "Execute sem --eval-only para treinar primeiro."
        )

    with open(path, "rb") as f:
        artifacts = pickle.load(f)

    LOG.info(
        f"Artefatos carregados: "
        f"{len(artifacts['customer_ids']):,} clientes | "
        f"{len(artifacts['product_ids']):,} produtos | "
        f"n_factors={artifacts['n_factors']}"
    )
    return artifacts


# ===========================================================================
# SEÇÃO 9 — MAIN PIPELINE
# ===========================================================================

def run_pipeline(
    n_factors:     int  = DEFAULT_N_FACTORS,
    k_neighbors:   int  = DEFAULT_K_NEIGHBORS,
    top_n:         int  = DEFAULT_TOP_N,
    eval_only:     bool = False,
    force_retrain: bool = False,
    dry_run:       bool = False,
    history_days:  int  = HISTORY_DAYS,
) -> None:
    """
    Orquestra o pipeline completo do Modelo B.

    Fluxo:
    ┌──────────────────────────────────────────────────────────────────┐
    │  1. Carrega histórico de compras                                  │
    │  2. Split temporal (treino / teste)                               │
    │  3. Constrói matriz esparsa usuário × produto                     │
    │  4. [Se não eval_only] Aplica SVD truncado                        │
    │  5. Analisa qualidade dos embeddings                              │
    │  6. Calcula similaridade entre clientes                           │
    │  7. Gera recomendações colaborativas                              │
    │  8. Avalia: Precision@K, Recall@K, NDCG@K, MAP@K, HitRate@K     │
    │  9. Salva artefatos e métricas                                    │
    └──────────────────────────────────────────────────────────────────┘
    """
    LOG.info("=" * 70)
    LOG.info("MODELO B — FILTRAGEM COLABORATIVA (Clientes Similares)")
    LOG.info(f"  n_factors={n_factors} | k_neighbors={k_neighbors} | top_n={top_n}")
    LOG.info(f"  eval_only={eval_only} | force_retrain={force_retrain} | dry_run={dry_run}")
    LOG.info("=" * 70)

    pg = get_pg_conn()

    try:
        # ── 1. Carrega dados ────────────────────────────────────────────────
        df = load_order_history(pg, history_days=history_days)

        if len(df) == 0:
            LOG.error("Nenhum dado. Verifique se o ETL foi executado.")
            sys.exit(1)

        # ── 2. Split temporal ───────────────────────────────────────────────
        df_train, df_test = temporal_split(df)

        # ── 3. Matriz esparsa ────────────────────────────────────────────────
        matrix, customer_ids, product_ids = build_interaction_matrix(df_train)

        if dry_run:
            LOG.info("[DRY RUN] Matriz construída. Sem treinamento.")
            LOG.info(f"  Shape: {matrix.shape}")
            LOG.info(f"  NNZ (não-zeros): {matrix.nnz:,}")
            return

        # ── 4. SVD (treino ou carregamento) ─────────────────────────────────
        if eval_only and MODEL_PATH.exists() and not force_retrain:
            artifacts = load_model_artifacts()
            user_embeddings    = artifacts["user_embeddings"]
            product_embeddings = artifacts["product_embeddings"]
            customer_ids       = artifacts["customer_ids"]
            product_ids        = artifacts["product_ids"]
            sigma              = artifacts["sigma"]
        else:
            if eval_only and not MODEL_PATH.exists():
                LOG.warning("--eval-only solicitado mas modelo não existe. Treinando.")

            user_embeddings, sigma, product_embeddings = fit_svd(matrix, n_factors)
            analyze_embeddings(user_embeddings, product_embeddings, customer_ids, product_ids, sigma)
            save_model_artifacts(
                user_embeddings, product_embeddings,
                customer_ids, product_ids, sigma, n_factors,
            )

        # ── 5. Similaridade entre clientes ──────────────────────────────────
        neighbors = compute_customer_similarity(user_embeddings, customer_ids, k_neighbors)

        # ── 6. Gerar recomendações ──────────────────────────────────────────
        reco_df = generate_collaborative_recommendations(df_train, neighbors, top_n)

        # ── 7. Avaliação ────────────────────────────────────────────────────
        LOG.info("\nAVALIAÇÃO NO CONJUNTO DE TESTE:")

        reco_per_customer = (
            reco_df.sort_values(["customer_id", "score"], ascending=[True, False])
            .groupby("customer_id")["product_id"]
            .apply(list)
            .to_dict()
        )

        df_metrics = evaluate_model(reco_per_customer, df_test)

        if not df_metrics.empty:
            print_ranking_report(df_metrics, strategy_name="modelo_b_colaborativo")

            # ── 8. Salva métricas no banco ──────────────────────────────────
            notes = (
                f"history_days={history_days} | "
                f"n_factors={n_factors} | k_neighbors={k_neighbors} | "
                f"n_users={len(customer_ids):,} | n_items={len(product_ids):,}"
            )
            save_metrics_to_db(pg, df_metrics, strategy="modelo_b_colaborativo", notes=notes)

        LOG.info("Pipeline do Modelo B concluído com sucesso.")

    except Exception as exc:
        LOG.error(f"ERRO FATAL no pipeline do Modelo B: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        pg.close()


# ===========================================================================
# ENTRYPOINT CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Modelo B — Filtragem Colaborativa por SVD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python ml/modelo_b_colaborativo.py                     # treino + avaliação
  python ml/modelo_b_colaborativo.py --eval-only         # só avalia (modelo salvo)
  python ml/modelo_b_colaborativo.py --n-factors 128     # mais fatores latentes
  python ml/modelo_b_colaborativo.py --k-neighbors 100   # mais vizinhos
  python ml/modelo_b_colaborativo.py --dry-run           # exibe matriz sem treinar
        """,
    )
    parser.add_argument(
        "--n-factors",
        type=int,
        default=DEFAULT_N_FACTORS,
        help=f"Dimensões do espaço latente SVD (padrão: {DEFAULT_N_FACTORS})",
    )
    parser.add_argument(
        "--k-neighbors",
        type=int,
        default=DEFAULT_K_NEIGHBORS,
        help=f"Vizinhos similares por cliente (padrão: {DEFAULT_K_NEIGHBORS})",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Recomendações por cliente (padrão: {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--eval-only",
        action="store_true",
        help="Carrega modelo salvo e apenas avalia (sem re-treinar)",
    )
    parser.add_argument(
        "--force-retrain",
        action="store_true",
        help="Força re-treino mesmo se modelo salvo existir",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Constrói a matriz esparsa e exibe estatísticas sem treinar",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=HISTORY_DAYS,
        help=f"Janela de histórico em dias (padrão: {HISTORY_DAYS})",
    )
    args = parser.parse_args()

    run_pipeline(
        n_factors=args.n_factors,
        k_neighbors=args.k_neighbors,
        top_n=args.top_n,
        eval_only=args.eval_only,
        force_retrain=args.force_retrain,
        dry_run=args.dry_run,
        history_days=args.history_days,
    )
