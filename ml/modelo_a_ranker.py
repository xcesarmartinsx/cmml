"""
ml/modelo_a_ranker.py
=====================
Modelo A — Ranker Supervisionado (Next Best Product).

ESTRATÉGIA
  Dado um par (cliente, produto_candidato), prever se o cliente comprará
  esse produto nos próximos 30 dias. Score de probabilidade = ranker.

  Label binário:
    1 → o cliente comprou o produto no período de teste (futuro)
    0 → não comprou

  Algoritmo:
    LightGBM (gradient boosting) — estado da arte para tabular data.
    Rápido, lida bem com dados esparsos e desbalanceados.

PIPELINE COMPLETO
  1. Carrega histórico de compras de cur.order_items
  2. Constrói features para pares (cliente × produto candidato)
  3. Split temporal (NUNCA aleatório — evita data leakage)
  4. Treina LightGBM com early stopping
  5. Avalia no conjunto de teste: AUC-ROC + Matriz de Confusão
  6. Avalia métricas de ranking: Precision@K, Recall@K, NDCG@K
  7. Salva o modelo treinado e métricas no banco

POR QUE SPLIT TEMPORAL (NÃO ALEATÓRIO)?
  Se usarmos split aleatório, compras futuras acabam no treino — o modelo
  "vê o futuro" durante o treinamento e obtém métricas artificialmente boas.
  Isso é chamado de data leakage temporal. Em produção, o modelo não teria
  acesso a essas informações e a performance seria muito pior.

  Solução: treino = passado, teste = futuro (como seria em produção).

FEATURES USADAS
  Por Cliente:
    - total de compras (últimos 90 dias)
    - ticket médio
    - número de categorias distintas
    - dias desde a última compra

  Por Produto:
    - popularidade (compradores únicos nos últimos 30 dias)
    - rank na loja do cliente

  Por Par (Cliente × Produto):
    - o cliente já comprou esse produto antes? (binário)
    - dias desde a última compra desse produto pelo cliente
    - afinidade por categoria (% de compras nessa categoria)
    - frequência de compra do produto pelo cliente

  Ciclo de Vida (Par × Produto):
    - lifecycle_ratio: days_since_last_purchase / avg_repurchase_days
    - lifecycle_too_soon: flag binária se lifecycle_ratio < 0.5 (compra prematura)

USO
  # Treinar e salvar modelo:
  python ml/modelo_a_ranker.py

  # Apenas avaliar (sem re-treinar — carrega modelo salvo):
  python ml/modelo_a_ranker.py --eval-only

  # Forçar re-treino ignorando modelo salvo:
  python ml/modelo_a_ranker.py --force-retrain

  # Dry-run: mostra features sem treinar:
  python ml/modelo_a_ranker.py --dry-run
"""

import argparse
import logging
import os
import pickle
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Garante que a raiz do projeto está no sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from etl.common import get_pg_conn, setup_logging
from ml.evaluate import (
    evaluate_binary_classifier,
    evaluate_ranking,
    find_optimal_threshold,
    print_ranking_report,
    save_metrics_to_db,
)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

LOG = setup_logging("ml.modelo_a")

# Diretório onde o modelo treinado será salvo
MODEL_DIR  = _PROJECT_ROOT / "app" / "ml" / "models" / "classification"
MODEL_PATH = MODEL_DIR / "modelo_a_ranker_v1.lgb"

# Janela de teste: últimos N dias → usados como "futuro" para avaliação
TEST_WINDOW_DAYS  = 30
# Janela de validação (tuning de hiperparâmetros): os N dias antes do teste
VAL_WINDOW_DAYS   = 60
# Janela máxima de histórico usado para features (evita dados muito antigos)
HISTORY_WINDOW_DAYS = 1095  # ~3 anos; ajustável via --history-days na CLI

# Número de candidatos por cliente (top produtos mais populares ainda não comprados)
N_CANDIDATES = 500

# Hiperparâmetros do LightGBM — ponto de partida conservador
# Para otimização, use Optuna (ver seção de tuning abaixo)
LGBM_PARAMS = {
    "objective":        "binary",        # problema de classificação binária
    "metric":           "auc",           # métrica de early stopping
    "verbosity":        -1,              # silencioso (logs pelo nosso logger)
    "learning_rate":    0.05,            # baixo → mais rounds, mais estável
    "num_leaves":       63,              # controla capacidade do modelo
    "min_child_samples": 50,             # regularização → evita overfitting em grupos pequenos
    "feature_fraction": 0.8,            # bagging de features (regularização)
    "bagging_fraction": 0.8,            # bagging de amostras
    "bagging_freq":     5,              # frequência do bagging
    "lambda_l1":        0.1,            # regularização L1
    "lambda_l2":        0.1,            # regularização L2
    "scale_pos_weight": None,           # será calculado dinamicamente (desbalanceamento)
    "seed":             42,
}

LGBM_NUM_ROUNDS   = 1000   # máximo de árvores (early stopping vai cortar)
LGBM_EARLY_STOP   = 50    # para se AUC não melhorar em 50 rounds


# ===========================================================================
# SEÇÃO 1 — EXTRAÇÃO DE DADOS
# ===========================================================================

def load_order_history(pg, history_days: int = HISTORY_WINDOW_DAYS) -> pd.DataFrame:
    """
    Carrega o histórico de compras de cur.order_items.

    Retorna apenas os campos necessários para feature engineering.
    Limita ao período de `history_days` para evitar dados muito antigos
    que não refletem o comportamento atual do cliente.

    Parâmetros
    ----------
    pg           : conexão psycopg2 com o PostgreSQL
    history_days : janela de histórico em dias (padrão: 365)

    Retorna
    -------
    pd.DataFrame com colunas:
        customer_id, product_id, store_id, sale_date, quantity, total_value
    """
    cutoff_date = (date.today() - timedelta(days=history_days)).isoformat()

    query = f"""
        SELECT
            oi.customer_id,
            oi.product_id,
            oi.store_id,
            oi.sale_date,
            oi.quantity,
            oi.total_value,
            p.category,
            p.active AS product_active
        FROM cur.order_items oi
        JOIN cur.products p ON p.product_id = oi.product_id
        JOIN stg.customers sc
            ON sc.customer_id_src = oi.customer_id
           AND sc.source_system   = 'sqlserver_gp'
        WHERE oi.sale_date >= '{cutoff_date}'
          AND p.active = TRUE          -- exclui produtos descontinuados
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
        f"Histórico carregado: {len(df):,} linhas | "
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
    val_days:  int = VAL_WINDOW_DAYS,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Divide o dataset em treino / validação / teste usando corte temporal.

    ──────────────────────────────────────────────────────────────
    NUNCA use split aleatório em séries temporais — isso cria
    data leakage (o modelo "vê o futuro" no treino).
    ──────────────────────────────────────────────────────────────

    Exemplo (hoje = 2025-12-16):
      Treino:     até 2025-09-16 (mais de 90 dias atrás)
      Validação:  2025-09-17 → 2025-11-16 (60 dias)
      Teste:      2025-11-17 → 2025-12-16 (30 dias)

    Parâmetros
    ----------
    df        : DataFrame com coluna 'sale_date' (datetime)
    test_days : dias para o conjunto de teste (o período mais recente)
    val_days  : dias para validação (entre treino e teste)

    Retorna
    -------
    (df_train, df_val, df_test)
    """
    max_date = df["sale_date"].max()
    test_start = max_date - pd.Timedelta(days=test_days)
    val_start  = test_start - pd.Timedelta(days=val_days)

    df_test  = df[df["sale_date"] >  test_start]
    df_val   = df[(df["sale_date"] > val_start) & (df["sale_date"] <= test_start)]
    df_train = df[df["sale_date"] <= val_start]

    LOG.info("SPLIT TEMPORAL:")
    LOG.info(f"  Treino:    até {val_start.date()}   → {len(df_train):,} linhas")
    LOG.info(f"  Validação: {val_start.date()} → {test_start.date()} → {len(df_val):,} linhas")
    LOG.info(f"  Teste:     {test_start.date()} → {max_date.date()}   → {len(df_test):,} linhas")

    return df_train, df_val, df_test


# ===========================================================================
# SEÇÃO 3 — FEATURE ENGINEERING
# ===========================================================================

def build_customer_features(df_train: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Constrói features por cliente baseadas no histórico de treino.

    Todas as features são calculadas APENAS com dados de df_train (passado).
    Nunca usar dados do futuro aqui.

    Parâmetros
    ----------
    df_train       : histórico de compras do período de treino
    reference_date : data de referência (último dia do treino)

    Retorna
    -------
    pd.DataFrame indexado por customer_id
    """
    LOG.info("Construindo features por cliente...")

    # Janela de 90 dias antes da data de referência
    window_90d = df_train[df_train["sale_date"] >= (reference_date - pd.Timedelta(days=90))]

    # Contagem de transações nos últimos 90 dias
    purchase_count = (
        window_90d.groupby("customer_id")["sale_date"]
        .count()
        .rename("purchase_count_90d")
    )

    # Ticket médio nos últimos 90 dias
    avg_ticket = (
        window_90d.groupby("customer_id")["total_value"]
        .mean()
        .rename("avg_ticket_90d")
    )

    # Dias desde a última compra (recência)
    last_purchase = (
        df_train.groupby("customer_id")["sale_date"]
        .max()
    )
    days_since_last = (reference_date - last_purchase).dt.days.rename("days_since_last_purchase")

    # Taxa de recompra do cliente: fração dos produtos únicos comprados mais de uma vez.
    # Clientes com alta taxa tendem a reter produtos no seu "mix de compras".
    product_buy_counts = (
        df_train.groupby(["customer_id", "product_id"])["sale_date"]
        .count()
        .reset_index(name="times_bought")
    )
    repeat_counts = (
        product_buy_counts[product_buy_counts["times_bought"] > 1]
        .groupby("customer_id")["product_id"]
        .count()
        .rename("repeat_product_count")
    )
    unique_products = (
        df_train.groupby("customer_id")["product_id"]
        .nunique()
        .rename("unique_products_total")
    )
    repeat_base = pd.concat([repeat_counts, unique_products], axis=1).fillna(0)
    customer_repeat_rate = (
        repeat_base["repeat_product_count"] / repeat_base["unique_products_total"].replace(0, 1)
    ).rename("customer_repeat_rate")

    # Combina todas as features do cliente em um único DataFrame
    # Removidos: category_diversity (category=NULL no ERP), main_store_id (store_id constante=0)
    customer_features = pd.concat(
        [purchase_count, avg_ticket, days_since_last, customer_repeat_rate],
        axis=1,
    ).fillna(0)

    LOG.info(f"Features de clientes: {len(customer_features):,} clientes, {len(customer_features.columns)} features")
    return customer_features


def build_product_features(df_train: pd.DataFrame, reference_date: pd.Timestamp) -> pd.DataFrame:
    """
    Constrói features por produto baseadas na popularidade histórica.

    Parâmetros
    ----------
    df_train       : histórico de compras
    reference_date : data de referência

    Retorna
    -------
    pd.DataFrame indexado por product_id
    """
    LOG.info("Construindo features por produto...")

    # Popularidade: compradores únicos nos últimos 30 dias
    window_30d = df_train[df_train["sale_date"] >= (reference_date - pd.Timedelta(days=30))]
    popularity_30d = (
        window_30d.groupby("product_id")["customer_id"]
        .nunique()
        .rename("product_popularity_30d")
    )

    # Volume total nos últimos 90 dias
    window_90d = df_train[df_train["sale_date"] >= (reference_date - pd.Timedelta(days=90))]
    volume_90d = (
        window_90d.groupby("product_id")["quantity"]
        .sum()
        .rename("product_volume_90d")
    )

    # Receita total nos últimos 90 dias
    revenue_90d = (
        window_90d.groupby("product_id")["total_value"]
        .sum()
        .rename("product_revenue_90d")
    )

    # Número de clientes distintos que compraram o produto (histórico completo)
    total_buyers = (
        df_train.groupby("product_id")["customer_id"]
        .nunique()
        .rename("product_total_buyers")
    )

    # Taxa de recompra do produto: fração de compradores que comprou o produto ≥2 vezes.
    # Produtos com alta taxa são "consumíveis" ou têm alta fidelização.
    buyer_freq = (
        df_train.groupby(["product_id", "customer_id"])["sale_date"]
        .count()
        .reset_index(name="times_bought")
    )
    repeat_buyers = (
        buyer_freq[buyer_freq["times_bought"] > 1]
        .groupby("product_id")["customer_id"]
        .count()
        .rename("repeat_buyers_count")
    )
    repeat_base_prod = pd.concat([repeat_buyers, total_buyers], axis=1).fillna(0)
    product_repeat_rate = (
        repeat_base_prod["repeat_buyers_count"] / repeat_base_prod["product_total_buyers"].replace(0, 1)
    ).rename("product_repeat_rate")

    # Intervalo médio em dias entre recompras consecutivas do produto.
    # Captura o ciclo de vida natural: consumíveis ~30d, duráveis (toalha, rede) ~365-1095d.
    # Calculado por par (cliente, produto): diferença entre datas consecutivas de compra.
    # Produtos sem recompras (comprados uma única vez) → 0 (usado como fallback no lifecycle_ratio).
    intervals = (
        df_train.sort_values("sale_date")
        .groupby(["product_id", "customer_id"])["sale_date"]
        .apply(lambda s: s.diff().dt.days.dropna())
    )
    if not intervals.empty:
        # Reindex para DataFrame plano: product_id como coluna após reset
        intervals_df = intervals.reset_index(level=[0, 1], drop=False)
        intervals_df.columns = ["product_id", "customer_id", "days_interval"]
        product_avg_repurchase_days = (
            intervals_df.groupby("product_id")["days_interval"]
            .mean()
            .rename("product_avg_repurchase_days")
        )
    else:
        product_avg_repurchase_days = pd.Series(dtype=float, name="product_avg_repurchase_days")

    product_features = pd.concat(
        [popularity_30d, volume_90d, revenue_90d, total_buyers, product_repeat_rate,
         product_avg_repurchase_days],
        axis=1,
    ).fillna(0)

    LOG.info(f"Features de produtos: {len(product_features):,} produtos, {len(product_features.columns)} features")
    return product_features


def build_interaction_features(
    df_train: pd.DataFrame,
    customer_ids: np.ndarray,
    product_ids: np.ndarray,
    reference_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Constrói features de interação para pares (cliente × produto candidato).

    Estas são as features mais preditivas: capturam o comportamento
    específico de ESTE cliente com ESTE produto.

    Parâmetros
    ----------
    df_train       : histórico de treino
    customer_ids   : array de customer_id para os candidatos
    product_ids    : array de product_id para os candidatos
    reference_date : data de referência

    Retorna
    -------
    pd.DataFrame com uma linha por par (customer_id, product_id)
    """
    LOG.info("Construindo features de interação (cliente × produto)...")

    # Cria DataFrame de pares candidatos
    pairs = pd.DataFrame({
        "customer_id": customer_ids,
        "product_id":  product_ids,
    })

    # ---- Feature: O cliente já comprou esse produto antes? ----
    # Histórico completo de compras por cliente
    customer_product_history = (
        df_train.groupby(["customer_id", "product_id"])
        .agg(
            bought_before      = ("sale_date", "count"),         # vezes que comprou
            last_bought_date   = ("sale_date", "max"),           # última compra
            total_qty_bought   = ("quantity",  "sum"),           # total de unidades
            total_spent        = ("total_value", "sum"),         # total gasto
        )
        .reset_index()
    )

    # Merge com os pares candidatos
    pairs = pairs.merge(customer_product_history, on=["customer_id", "product_id"], how="left")

    # Preenche 0 para pares sem histórico (produto novo para o cliente)
    pairs["bought_before"] = pairs["bought_before"].fillna(0).astype(int)

    # Quantidade e valor médios por compra (evita colinearidade com bought_before).
    # Quando bought_before=0: avg = 0. Quando > 0: captura intensidade de uso.
    pairs["total_qty_bought"] = pairs["total_qty_bought"].fillna(0)
    pairs["total_spent"]      = pairs["total_spent"].fillna(0)
    pairs["avg_qty_per_purchase"]   = np.where(
        pairs["bought_before"] > 0,
        pairs["total_qty_bought"] / pairs["bought_before"],
        0.0,
    )
    pairs["avg_value_per_purchase"] = np.where(
        pairs["bought_before"] > 0,
        pairs["total_spent"] / pairs["bought_before"],
        0.0,
    )
    pairs = pairs.drop(columns=["total_qty_bought", "total_spent"])

    # Dias desde a última compra desse produto pelo cliente.
    # Codificação: se nunca comprou → max_days + 1 (sinal contínuo "mais antigo que qualquer treino").
    # Isso evita a colinearidade binária com bought_before que ocorria com -1.
    max_days = int((reference_date - df_train["sale_date"].min()).days) + 1
    pairs["days_since_last_product_purchase"] = np.where(
        pairs["last_bought_date"].notna(),
        (reference_date - pairs["last_bought_date"]).dt.days,
        max_days,
    )
    pairs = pairs.drop(columns=["last_bought_date"])

    # Removido: category_affinity — category (GRUPOID) é NULL para todos os produtos
    # neste cliente de ERP. A feature ficava sempre 0 e adicionava apenas ruído.

    LOG.info(f"Features de interação: {len(pairs):,} pares candidatos")
    return pairs


def build_training_dataset(
    df_train: pd.DataFrame,
    df_label: pd.DataFrame,
    n_candidates: int = N_CANDIDATES,
) -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Monta o dataset de treino completo com features e labels.

    Estratégia de amostragem negativa:
    Para cada compra REAL no df_label (positivos = label 1), geramos
    `n_neg_ratio` exemplos negativos aleatórios de produtos que o cliente
    NÃO comprou. Isso é necessário porque o dataset real é muito desbalanceado
    (clientes compram uma fração mínima do catálogo).

    Parâmetros
    ----------
    df_train    : compras do período de treino (features)
    df_label    : compras do período de validação/teste (labels)
    n_candidates: candidatos negativos por cliente

    Retorna
    -------
    (X, y) onde X é o DataFrame de features e y é o array de labels
    """
    reference_date = df_train["sale_date"].max()
    all_products   = df_train["product_id"].unique()

    LOG.info("Montando dataset de treino...")
    LOG.info(f"  Período de referência (features): até {reference_date.date()}")
    LOG.info(f"  Candidatos negativos por cliente: {n_candidates}")

    # --- Labels positivos (reais) ---
    positives = (
        df_label[["customer_id", "product_id"]]
        .drop_duplicates()
        .copy()
    )
    positives["label"] = 1

    # --- Labels negativos (amostrados) ---
    # Para cada cliente, amostrar produtos que ele NÃO comprou no período de label
    customers = positives["customer_id"].unique()
    rng = np.random.default_rng(seed=42)
    negatives_list = []

    for cid in customers:
        # Produtos que esse cliente comprou (qualquer período) — excluir dos negativos
        bought = set(df_train[df_train["customer_id"] == cid]["product_id"].tolist())
        bought |= set(positives[positives["customer_id"] == cid]["product_id"].tolist())

        # Candidatos negativos: produtos populares que o cliente não comprou
        candidates = [p for p in all_products if p not in bought]

        if not candidates:
            continue

        n_sample = min(n_candidates, len(candidates))
        sampled = rng.choice(candidates, size=n_sample, replace=False)
        negatives_list.append(
            pd.DataFrame({"customer_id": cid, "product_id": sampled, "label": 0})
        )

    negatives = pd.concat(negatives_list, ignore_index=True) if negatives_list else pd.DataFrame()

    LOG.info(f"  Positivos: {len(positives):,} | Negativos: {len(negatives):,}")
    LOG.info(f"  Ratio desbalanceamento: 1:{len(negatives)//max(len(positives),1)}")

    # Combina positivos e negativos
    all_samples = pd.concat([positives, negatives], ignore_index=True)

    # --- Features ---
    customer_feats = build_customer_features(df_train, reference_date)
    product_feats  = build_product_features(df_train, reference_date)
    interaction_feats = build_interaction_features(
        df_train,
        all_samples["customer_id"].values,
        all_samples["product_id"].values,
        reference_date,
    )

    # Merge de todas as features
    dataset = all_samples.merge(
        customer_feats.reset_index(), on="customer_id", how="left"
    )
    dataset = dataset.merge(
        product_feats.reset_index(), on="product_id", how="left"
    )
    dataset = dataset.merge(
        interaction_feats, on=["customer_id", "product_id"], how="left"
    )

    # lifecycle_ratio: fração do ciclo médio de recompra já decorrida para este par.
    # Responde: "está na hora de re-comprar?"
    #   0.0  → produto nunca comprado pelo cliente ou sem dado de ciclo
    #   ~1.0 → cliente está no momento ideal de recompra
    #   >1.0 → cliente está atrasado — maior oportunidade de venda
    dataset["lifecycle_ratio"] = np.where(
        (dataset["bought_before"] > 0) & (dataset["product_avg_repurchase_days"] > 0),
        dataset["days_since_last_product_purchase"] / dataset["product_avg_repurchase_days"],
        0.0,
    )

    # lifecycle_too_soon: flag binária indicando que o cliente comprou recentemente
    # relativo ao ciclo médio do produto (ratio < 0.5 = menos da metade do ciclo).
    # Ajuda o modelo a aprender a penalizar recomendações prematuras de forma explícita.
    dataset["lifecycle_too_soon"] = np.where(
        (dataset["bought_before"] > 0) & (dataset["lifecycle_ratio"] > 0),
        (dataset["lifecycle_ratio"] < 0.5).astype(float),
        0.0,
    )

    # Separa features (X) e labels (y)
    y = dataset["label"].values.astype(int)
    X = dataset.drop(columns=["label", "customer_id", "product_id"])

    # Preenche NaN remanescentes com 0 (produtos/clientes sem histórico)
    X = X.fillna(0)

    LOG.info(f"Dataset de treino: {len(X):,} amostras × {len(X.columns)} features")
    LOG.info(f"  Balanceamento: {y.mean()*100:.2f}% positivos")

    return X, y, dataset[["customer_id", "product_id"]]


# ===========================================================================
# SEÇÃO 4 — TREINAMENTO
# ===========================================================================

def train_lightgbm(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_val: pd.DataFrame,
    y_val: np.ndarray,
) -> "lgb.Booster":
    """
    Treina o modelo LightGBM com early stopping.

    Por que LightGBM?
    - Melhor performance em dados tabulares vs redes neurais
    - Treino rápido: pode re-treinar diariamente
    - Interpretável via feature importance
    - Lida nativamente com dados esparsos e valores faltantes

    Parâmetros
    ----------
    X_train, y_train : dados de treino
    X_val,   y_val   : dados de validação (para early stopping)

    Retorna
    -------
    lgb.Booster treinado
    """
    try:
        import lightgbm as lgb
    except ImportError:
        raise ImportError(
            "LightGBM não instalado. Execute: pip install lightgbm"
        )

    # Ajusta o peso da classe positiva proporcionalmente ao desbalanceamento.
    # Usa raiz quadrada do ratio (sqrt strategy) para otimização de AUC:
    # o ratio bruto (~175) causa early stopping no round 1 porque o primeiro tree
    # captura todo o sinal de popularidade e rounds subsequentes só pioram.
    # sqrt(175) ≈ 13 distribui o aprendizado por muitos rounds preservando recall.
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    scale_pos = (neg_count / max(pos_count, 1)) ** 0.5
    LOG.info(f"scale_pos_weight automático: {scale_pos:.1f} (neg={neg_count:,}, pos={pos_count:,})")

    params = {**LGBM_PARAMS, "scale_pos_weight": scale_pos}

    train_data = lgb.Dataset(X_train, label=y_train, feature_name=list(X_train.columns))
    val_data   = lgb.Dataset(X_val,   label=y_val,   reference=train_data)

    LOG.info("Iniciando treinamento do LightGBM...")
    LOG.info(f"  Rounds máximos: {LGBM_NUM_ROUNDS} | Early stopping: {LGBM_EARLY_STOP}")

    callbacks = [
        lgb.early_stopping(stopping_rounds=LGBM_EARLY_STOP, verbose=False),
        lgb.log_evaluation(period=50),  # loga AUC a cada 50 rounds
    ]

    model = lgb.train(
        params,
        train_data,
        num_boost_round=LGBM_NUM_ROUNDS,
        valid_sets=[val_data],
        valid_names=["val"],
        callbacks=callbacks,
    )

    LOG.info(f"Treinamento concluído: melhor round = {model.best_iteration}")
    return model


def log_feature_importance(model: "lgb.Booster", top_n: int = 20) -> None:
    """
    Exibe as features mais importantes do modelo.

    Feature importance por 'gain' mede a contribuição de cada feature
    para a redução da perda durante o treinamento.
    É mais informativa que 'split' (contagem de usos).
    """
    try:
        import lightgbm as lgb
    except ImportError:
        return

    importance = pd.DataFrame({
        "feature":    model.feature_name(),
        "importance": model.feature_importance(importance_type="gain"),
    }).sort_values("importance", ascending=False)

    LOG.info(f"\nTOP {top_n} FEATURES MAIS IMPORTANTES (gain):")
    LOG.info("-" * 50)
    for _, row in importance.head(top_n).iterrows():
        bar = "█" * int(row["importance"] / importance["importance"].max() * 30)
        LOG.info(f"  {row['feature']:<40} {bar} {row['importance']:.1f}")


# ===========================================================================
# SEÇÃO 5 — GERAÇÃO DE RECOMENDAÇÕES (INFERÊNCIA)
# ===========================================================================

def generate_recommendations(
    model: "lgb.Booster",
    df_history: pd.DataFrame,
    customer_ids: Optional[List[int]] = None,
    top_n: int = 10,
    n_candidates: int = N_CANDIDATES,
) -> pd.DataFrame:
    """
    Gera recomendações para os clientes usando o modelo treinado.

    Para cada cliente:
    1. Identifica produtos candidatos (populares + não comprados recentemente)
    2. Constrói features para cada par (cliente × produto_candidato)
    3. Prediz a probabilidade de compra
    4. Retorna os top_n com maior probabilidade

    Parâmetros
    ----------
    model        : modelo LightGBM treinado
    df_history   : histórico de compras (para features e exclusão de comprados)
    customer_ids : lista de clientes (None = todos)
    top_n        : número de recomendações por cliente
    n_candidates : candidatos a avaliar por cliente

    Retorna
    -------
    pd.DataFrame com colunas: [customer_id, product_id, score, rank, strategy]
    """
    if customer_ids is None:
        customer_ids = df_history["customer_id"].unique().tolist()

    reference_date = df_history["sale_date"].max()
    all_products   = df_history["product_id"].unique()

    LOG.info(f"Gerando recomendações para {len(customer_ids):,} clientes (top-{top_n})...")

    # Features de base (calculadas uma vez para todos)
    customer_feats = build_customer_features(df_history, reference_date)
    product_feats  = build_product_features(df_history, reference_date)

    results = []
    rng = np.random.default_rng(seed=42)

    for cid in customer_ids:
        # Produtos que o cliente comprou nos últimos 30 dias (janela de recompra)
        recent_window = reference_date - pd.Timedelta(days=30)
        recently_bought = set(
            df_history[
                (df_history["customer_id"] == cid) &
                (df_history["sale_date"] >= recent_window)
            ]["product_id"].tolist()
        )

        # Candidatos: produtos populares que o cliente não comprou recentemente
        candidates = [p for p in all_products if p not in recently_bought]
        if not candidates:
            LOG.debug(f"Cliente {cid}: sem candidatos disponíveis")
            continue

        n_sample = min(n_candidates, len(candidates))
        sampled_candidates = rng.choice(candidates, size=n_sample, replace=False)

        # Monta features para os candidatos
        candidate_pairs = pd.DataFrame({
            "customer_id": cid,
            "product_id":  sampled_candidates,
        })

        interaction = build_interaction_features(
            df_history,
            candidate_pairs["customer_id"].values,
            candidate_pairs["product_id"].values,
            reference_date,
        )

        X_inf = candidate_pairs.merge(
            customer_feats.reset_index(), on="customer_id", how="left"
        ).merge(
            product_feats.reset_index(), on="product_id", how="left"
        ).merge(
            interaction, on=["customer_id", "product_id"], how="left"
        )

        # lifecycle_ratio — mesma fórmula usada no treino (build_training_dataset)
        X_inf["lifecycle_ratio"] = np.where(
            (X_inf["bought_before"] > 0) & (X_inf["product_avg_repurchase_days"] > 0),
            X_inf["days_since_last_product_purchase"] / X_inf["product_avg_repurchase_days"],
            0.0,
        )

        # lifecycle_too_soon — mesma fórmula usada no treino
        X_inf["lifecycle_too_soon"] = np.where(
            (X_inf["bought_before"] > 0) & (X_inf["lifecycle_ratio"] > 0),
            (X_inf["lifecycle_ratio"] < 0.5).astype(float),
            0.0,
        )

        # Garante que as colunas estão na mesma ordem do treino
        feature_cols = model.feature_name()
        X_pred = X_inf[feature_cols].fillna(0)

        # Predição de probabilidade
        scores = model.predict(X_pred)

        # Ordena por score e pega os top_n
        top_idx = np.argsort(scores)[::-1][:top_n]

        for rank, idx in enumerate(top_idx, start=1):
            results.append({
                "customer_id": cid,
                "product_id":  sampled_candidates[idx],
                "score":       float(scores[idx]),
                "rank":        rank,
                "strategy":    "modelo_a_ranker",
            })

    reco_df = pd.DataFrame(results)
    LOG.info(f"Recomendações geradas: {len(reco_df):,} sugestões para {reco_df['customer_id'].nunique():,} clientes")
    return reco_df


# ===========================================================================
# SEÇÃO 6 — PERSISTÊNCIA DO MODELO
# ===========================================================================

def save_model(model: "lgb.Booster", path: Path = MODEL_PATH) -> None:
    """
    Salva o modelo LightGBM em disco.

    O modelo é salvo no formato nativo do LightGBM (.lgb), que é mais eficiente
    que pickle para modelos de gradient boosting.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(path))
    LOG.info(f"Modelo salvo em: {path}")


def load_model(path: Path = MODEL_PATH) -> "lgb.Booster":
    """
    Carrega modelo LightGBM previamente treinado.
    """
    try:
        import lightgbm as lgb
    except ImportError:
        raise ImportError("LightGBM não instalado. Execute: pip install lightgbm")

    if not path.exists():
        raise FileNotFoundError(
            f"Modelo não encontrado em '{path}'. "
            "Execute sem --eval-only para treinar primeiro."
        )
    model = lgb.Booster(model_file=str(path))
    LOG.info(f"Modelo carregado de: {path}")
    return model


# ===========================================================================
# SEÇÃO 7 — MAIN PIPELINE
# ===========================================================================

def run_pipeline(
    eval_only:     bool = False,
    force_retrain: bool = False,
    dry_run:       bool = False,
    history_days:  int  = HISTORY_WINDOW_DAYS,
) -> None:
    """
    Orquestra o pipeline completo do Modelo A.

    Fluxo:
    ┌─────────────────────────────────────────────────────────────┐
    │  1. Carrega histórico                                        │
    │  2. Split temporal (treino / val / teste)                    │
    │  3. Monta dataset com features e labels                      │
    │  4. [Se não eval_only] Treina LightGBM                       │
    │  5. Avalia no conjunto de teste                              │
    │     ├── Matriz de confusão + AUC-ROC                         │
    │     └── Precision@K, Recall@K, NDCG@K, MAP@K               │
    │  6. Salva modelo e métricas                                  │
    └─────────────────────────────────────────────────────────────┘
    """
    LOG.info("=" * 70)
    LOG.info("MODELO A — RANKER SUPERVISIONADO (Next Best Product)")
    LOG.info(f"  eval_only={eval_only} | force_retrain={force_retrain} | dry_run={dry_run}")
    LOG.info("=" * 70)

    pg = get_pg_conn()

    try:
        # ── 1. Carrega dados ────────────────────────────────────────────────
        df = load_order_history(pg, history_days=history_days)

        if len(df) == 0:
            LOG.error("Nenhum dado encontrado. Verifique se o ETL foi executado.")
            sys.exit(1)

        if dry_run:
            LOG.info("[DRY RUN] Dados carregados. Exibindo amostra de features:")
            ref = df["sale_date"].max()
            cust_feats = build_customer_features(df, ref)
            LOG.info(cust_feats.head().to_string())
            return

        # ── 2. Split temporal ───────────────────────────────────────────────
        df_train, df_val, df_test = temporal_split(df)

        # ── 3. Dataset de treino + early stopping + avaliação ─────────────
        LOG.info("Montando dataset de treino...")
        X_train, y_train, pairs_train = build_training_dataset(df_train, df_val)

        # Early stopping: usa os últimos 30 dias do período de treino como labels.
        # Garante mesma distribuição de clientes/produtos do treino.
        # Antes era df_test (distribuição diferente → early stop no round 1 = 1 árvore).
        es_cutoff = df_train["sale_date"].max() - pd.Timedelta(days=30)
        df_es_hist   = df_train[df_train["sale_date"] <= es_cutoff]
        df_es_labels = df_train[df_train["sale_date"] >  es_cutoff]
        LOG.info(f"Montando dataset de early stopping (últimos 30d do treino: {df_es_labels['sale_date'].min().date()} → {df_es_labels['sale_date'].max().date()})...")
        X_es, y_es, _ = build_training_dataset(df_es_hist, df_es_labels)

        # Dataset de avaliação final: features = treino + val, labels = teste.
        # Usado APENAS para métricas finais, nunca para early stopping.
        LOG.info("Montando dataset de avaliação (teste)...")
        X_eval, y_eval, pairs_eval = build_training_dataset(
            pd.concat([df_train, df_val]),
            df_test,
        )

        # ── 4. Treino ────────────────────────────────────────────────────────
        if eval_only and MODEL_PATH.exists() and not force_retrain:
            model = load_model()
        else:
            if eval_only and not MODEL_PATH.exists():
                LOG.warning("--eval-only solicitado mas modelo não existe. Treinando do zero.")
            model = train_lightgbm(X_train, y_train, X_es, y_es)
            log_feature_importance(model)
            save_model(model)

        # ── 5. Avaliação binária (AUC + Matriz de Confusão) ─────────────────
        LOG.info("\nAVALIAÇÃO NO CONJUNTO DE TESTE:")

        feature_cols = model.feature_name()
        y_proba = model.predict(X_eval[feature_cols].fillna(0))

        # Matriz de confusão + AUC-ROC
        eval_results = evaluate_binary_classifier(y_eval, y_proba)

        # Threshold ótimo para maximizar F1 (balanceamento precision/recall)
        best_threshold, best_f1 = find_optimal_threshold(y_eval, y_proba, metric="f1")

        # ── 6. Avaliação de ranking (Precision@K, Recall@K, etc.) ───────────
        # Agrupa previsões por cliente para calcular métricas de ranking
        pairs_eval["score"] = y_proba

        # Produtos que cada cliente realmente comprou no período de teste
        relevant_per_customer = (
            df_test.groupby("customer_id")["product_id"]
            .apply(list)
            .to_dict()
        )

        # Top-N recomendados por cliente (ordenados por score decrescente)
        reco_per_customer = (
            pairs_eval.sort_values(["customer_id", "score"], ascending=[True, False])
            .groupby("customer_id")["product_id"]
            .apply(list)
            .to_dict()
        )

        df_metrics = evaluate_ranking(reco_per_customer, relevant_per_customer, k_values=[5, 10, 20])
        print_ranking_report(df_metrics, strategy_name="modelo_a_ranker")

        # ── 7. Salva métricas no banco ──────────────────────────────────────
        notes = (
            f"history_days={history_days} | "
            f"best_threshold={best_threshold:.2f} | best_f1={best_f1:.4f} | "
            f"auc_roc={eval_results.get('auc_roc', 'N/A'):.4f} | "
            f"n_rounds={getattr(model, 'best_iteration', 'N/A')}"
        )
        save_metrics_to_db(pg, df_metrics, strategy="modelo_a_ranker", notes=notes)

        LOG.info("Pipeline do Modelo A concluído com sucesso.")

    except Exception as exc:
        LOG.error(f"ERRO FATAL no pipeline do Modelo A: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        pg.close()


# ===========================================================================
# ENTRYPOINT CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Modelo A — Ranker Supervisionado (Next Best Product)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python ml/modelo_a_ranker.py                  # treino + avaliação completa
  python ml/modelo_a_ranker.py --eval-only      # só avalia (carrega modelo salvo)
  python ml/modelo_a_ranker.py --force-retrain  # força re-treino mesmo com modelo salvo
  python ml/modelo_a_ranker.py --dry-run        # mostra features sem treinar
        """,
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
        help="Carrega dados e exibe features sem treinar o modelo",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=HISTORY_WINDOW_DAYS,
        help=f"Janela de histórico em dias (padrão: {HISTORY_WINDOW_DAYS})",
    )
    args = parser.parse_args()
    run_pipeline(
        eval_only=args.eval_only,
        force_retrain=args.force_retrain,
        dry_run=args.dry_run,
        history_days=args.history_days,
    )
