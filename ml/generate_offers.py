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

import numpy as np
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
# LIFECYCLE-AWARE SCORE ADJUSTMENT
# ===========================================================================

def load_lifecycle_data(pg) -> pd.DataFrame:
    """
    Carrega dados de ciclo de vida de reco.product_lifecycle (materialized view).

    Retorna DataFrame com product_id, avg_days_between_purchases, lifecycle_tier, sample_size.
    """
    query = """
        SELECT product_id, avg_days_between_purchases, median_days_between_purchases,
               lifecycle_tier, sample_size, distinct_customers
        FROM reco.product_lifecycle
    """
    try:
        df = pd.read_sql(query, pg)
        LOG.info(f"Lifecycle data loaded: {len(df):,} products")
        return df
    except Exception as exc:
        LOG.warning(f"Could not load reco.product_lifecycle: {exc}. Skipping lifecycle adjustment.")
        return pd.DataFrame()


def load_last_purchase_per_customer_product(pg) -> pd.DataFrame:
    """
    Carrega a data da ultima compra de cada par (cliente, produto).
    Usado para calcular dias desde a ultima compra e aplicar desconto de ciclo de vida.
    """
    query = """
        SELECT customer_id, product_id, MAX(sale_date) AS last_purchase_date
        FROM cur.order_items
        GROUP BY customer_id, product_id
    """
    df = pd.read_sql(query, pg)
    df["last_purchase_date"] = pd.to_datetime(df["last_purchase_date"])
    return df


def filter_unreliable_lifecycle(
    lifecycle_df: pd.DataFrame,
    min_sample_size: int = 5,
    min_distinct_customers: int = 3,
) -> pd.DataFrame:
    """
    Remove lifecycle entries com evidencia estatistica insuficiente.

    Produtos abaixo dos thresholds serao tratados como 'sem dados de lifecycle',
    ou seja, nao recebem filtro nem desconto de ciclo de vida.
    """
    if lifecycle_df.empty:
        return lifecycle_df

    before = len(lifecycle_df)
    mask = (
        (lifecycle_df["sample_size"] >= min_sample_size) &
        (lifecycle_df["distinct_customers"] >= min_distinct_customers)
    )
    filtered = lifecycle_df[mask].copy()
    removed = before - len(filtered)
    LOG.info(
        f"Lifecycle reliability filter: kept {len(filtered):,} of {before:,} products "
        f"(removed {removed:,} with sample<{min_sample_size} or customers<{min_distinct_customers})"
    )
    return filtered


def extract_product_category(description: str) -> str:
    """Extrai categoria do produto a partir da primeira palavra do nome."""
    if not description or not description.strip():
        return ""
    return description.strip().split()[0].upper()


def load_category_lifecycle(pg) -> pd.DataFrame:
    """
    Calcula lifecycle médio por categoria de produto (primeira palavra do nome).
    Usado como fallback para produtos sem lifecycle individual.
    """
    query = """
        SELECT SPLIT_PART(product_name, ' ', 1) AS product_category,
               AVG(avg_days_between_purchases) AS category_avg_lifecycle,
               COUNT(*) AS category_n_products
        FROM reco.product_lifecycle
        GROUP BY SPLIT_PART(product_name, ' ', 1)
        HAVING COUNT(*) >= 3
    """
    try:
        df = pd.read_sql(query, pg)
        LOG.info(f"Category lifecycle loaded: {len(df):,} categories")
        return df
    except Exception as exc:
        LOG.warning(f"Could not load category lifecycle: {exc}")
        return pd.DataFrame()


def load_last_purchase_per_customer_category(pg) -> pd.DataFrame:
    """
    Carrega a data da última compra por (cliente, categoria).
    Categoria = primeira palavra do nome do produto.
    Usado para filtrar ofertas quando o cliente comprou outro produto
    da mesma categoria recentemente (cross-product substitution).
    """
    query = """
        SELECT oi.customer_id,
               SPLIT_PART(p.description, ' ', 1) AS product_category,
               MAX(oi.sale_date) AS last_category_purchase_date
        FROM cur.order_items oi
        JOIN cur.products p ON p.product_id = oi.product_id
        GROUP BY oi.customer_id, SPLIT_PART(p.description, ' ', 1)
    """
    try:
        df = pd.read_sql(query, pg)
        df["last_category_purchase_date"] = pd.to_datetime(df["last_category_purchase_date"])
        LOG.info(f"Category last-purchase pairs loaded: {len(df):,}")
        return df
    except Exception as exc:
        LOG.warning(f"Could not load category last-purchase: {exc}")
        return pd.DataFrame()


def load_product_descriptions(pg) -> pd.DataFrame:
    """Carrega product_id e description de cur.products para derivar categorias."""
    query = "SELECT product_id, description FROM cur.products"
    return pd.read_sql(query, pg)


def apply_lifecycle_discount(
    reco_df: pd.DataFrame,
    lifecycle_df: pd.DataFrame,
    last_purchase_df: pd.DataFrame,
    category_lifecycle_df: pd.DataFrame = None,
    category_last_purchase_df: pd.DataFrame = None,
    product_desc_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Aplica desconto no score baseado no ciclo de vida do produto.

    Duas camadas:
    1. Produto individual: usa lifecycle_ratio do produto.
    2. Categoria (fallback): para produtos sem lifecycle individual,
       usa lifecycle médio da categoria e data da última compra na categoria.

    Parametros
    ----------
    reco_df                  : DataFrame com colunas [customer_id, product_id, score, ...]
    lifecycle_df             : DataFrame com colunas [product_id, avg_days_between_purchases, sample_size, ...]
    last_purchase_df         : DataFrame com colunas [customer_id, product_id, last_purchase_date]
    category_lifecycle_df    : DataFrame com colunas [product_category, category_avg_lifecycle] (opcional)
    category_last_purchase_df: DataFrame com colunas [customer_id, product_category, last_category_purchase_date] (opcional)
    product_desc_df          : DataFrame com colunas [product_id, description] (opcional)
    """
    if lifecycle_df.empty:
        LOG.info("No lifecycle data available. Skipping discount.")
        return reco_df

    today = pd.Timestamp.now()

    # Merge lifecycle data (include sample_size for reliability weighting)
    lc_cols = ["product_id", "avg_days_between_purchases", "sample_size"]
    if "distinct_customers" in lifecycle_df.columns:
        lc_cols.append("distinct_customers")
    df = reco_df.merge(
        lifecycle_df[lc_cols],
        on="product_id",
        how="left",
    )

    # Merge last purchase date per (customer, product) pair
    df = df.merge(
        last_purchase_df,
        on=["customer_id", "product_id"],
        how="left",
    )

    # Calculate days since last purchase
    df["days_since_last"] = (today - df["last_purchase_date"]).dt.days

    # Calculate lifecycle ratio (produto individual)
    df["lifecycle_ratio"] = np.where(
        (df["avg_days_between_purchases"].notna()) &
        (df["avg_days_between_purchases"] > 0) &
        (df["days_since_last"].notna()),
        df["days_since_last"] / df["avg_days_between_purchases"],
        np.nan,
    )

    # ── Category fallback for products without individual lifecycle ──────────
    has_category_data = (
        product_desc_df is not None and not product_desc_df.empty
        and category_lifecycle_df is not None and not category_lifecycle_df.empty
        and category_last_purchase_df is not None and not category_last_purchase_df.empty
    )
    if has_category_data:
        no_individual = df["lifecycle_ratio"].isna()
        if no_individual.any():
            desc_map = dict(zip(product_desc_df["product_id"], product_desc_df["description"]))
            df["product_category"] = df["product_id"].map(
                lambda pid: extract_product_category(desc_map.get(pid, ""))
            )
            df = df.merge(
                category_lifecycle_df[["product_category", "category_avg_lifecycle"]],
                on="product_category", how="left",
            )
            df = df.merge(
                category_last_purchase_df[["customer_id", "product_category", "last_category_purchase_date"]],
                on=["customer_id", "product_category"], how="left",
            )
            cat_days = (today - df["last_category_purchase_date"]).dt.days
            has_cat = (
                df["category_avg_lifecycle"].notna() &
                (df["category_avg_lifecycle"] > 0) &
                cat_days.notna()
            )
            # Fill lifecycle_ratio with category ratio where individual is missing
            cat_ratio = np.where(has_cat, cat_days / df["category_avg_lifecycle"], np.nan)
            df["lifecycle_ratio"] = df["lifecycle_ratio"].fillna(pd.Series(cat_ratio, index=df.index))
            # Use lower sample_size for category fallback (less reliable)
            df["sample_size"] = df["sample_size"].fillna(
                df["category_avg_lifecycle"].notna().astype(int) * 5
            )

            filled = no_individual.sum() - df["lifecycle_ratio"].isna().sum()
            if filled > 0:
                LOG.info(f"Category lifecycle fallback applied to {filled:,} offers without individual lifecycle")

            # Cleanup category columns
            for col in ["product_category", "category_avg_lifecycle", "last_category_purchase_date"]:
                if col in df.columns:
                    df = df.drop(columns=[col])

    # Discount factor using a sigmoid-like curve, weighted by reliability.
    #
    # Sigmoid: factor = 1 / (1 + exp(-5 * (ratio - 0.7)))
    #   ratio >= 1.0 => factor ~1.0 (no penalty, due for repurchase)
    #   ratio = 0.5  => factor ~0.5 (50% penalty)
    #   ratio = 0.0  => factor ~0.2 (80% penalty)
    #
    # Reliability weighting: products with low sample_size get a weaker discount
    # because we don't trust the lifecycle estimate. reliability in [0, 1]:
    #   sample_size=5  => reliability ~0.54 (partial trust)
    #   sample_size=20 => reliability ~1.0  (full trust)
    # Final: discount = 1 - reliability * (1 - sigmoid)
    has_lifecycle = df["lifecycle_ratio"].notna()
    discount_factor = np.ones(len(df))
    if has_lifecycle.any():
        ratio = df.loc[has_lifecycle, "lifecycle_ratio"].values
        sigmoid = 1.0 / (1.0 + np.exp(-5.0 * (ratio - 0.7)))

        sample_sz = df.loc[has_lifecycle, "sample_size"].fillna(0).values
        reliability = np.clip(np.log1p(sample_sz) / np.log1p(20), 0.0, 1.0)

        discount_factor[has_lifecycle] = 1.0 - reliability * (1.0 - sigmoid)

    # ── Uncertainty cap: produtos sem lifecycle data não podem ter score > 0.75 ──
    no_lifecycle = ~has_lifecycle
    if no_lifecycle.any():
        # Aplica cap de 0.75 ao discount_factor para produtos sem dados de lifecycle.
        # Isso reflete a incerteza: sem dados de ciclo de vida, não podemos afirmar
        # que o cliente está pronto para recompra.
        UNCERTAINTY_CAP = 0.75
        discount_factor[no_lifecycle] = UNCERTAINTY_CAP
        capped_count = no_lifecycle.sum()
        LOG.info(
            f"Uncertainty cap ({UNCERTAINTY_CAP:.0%}): applied to {capped_count:,} offers "
            f"without lifecycle data (individual or category)"
        )

    original_score = df["score"].copy()
    df["score_original"] = original_score
    df["score"] = original_score * discount_factor
    df["lifecycle_discount"] = discount_factor

    # Log statistics
    discounted = (discount_factor < 0.95) & has_lifecycle.values
    if discounted.any():
        avg_discount = 1.0 - discount_factor[discounted].mean()
        LOG.info(
            f"Lifecycle discount applied to {discounted.sum():,} offers "
            f"(avg penalty: {avg_discount*100:.1f}%)"
        )
    else:
        LOG.info("Lifecycle discount: no offers penalized (all clients are due for repurchase).")

    # Re-rank within each (customer_id, strategy) group based on adjusted score
    df = df.sort_values(["customer_id", "strategy", "score"], ascending=[True, True, False])
    df["rank"] = df.groupby(["customer_id", "strategy"]).cumcount() + 1

    # Drop auxiliary columns (keep lifecycle_ratio and lifecycle_discount for debug)
    cols_to_drop = [
        c for c in ["avg_days_between_purchases", "sample_size", "distinct_customers",
                     "last_purchase_date", "days_since_last",
                     "score_original"]
        if c in df.columns
    ]
    df = df.drop(columns=cols_to_drop)

    return df


# ===========================================================================
# LIFECYCLE HARD FILTER
# ===========================================================================

def apply_lifecycle_hard_filter(
    reco_df: pd.DataFrame,
    lifecycle_df: pd.DataFrame,
    last_purchase_df: pd.DataFrame,
    min_ratio: float = 0.6,
    fallback_days: int = 90,
    category_lifecycle_df: pd.DataFrame = None,
    category_last_purchase_df: pd.DataFrame = None,
    product_desc_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Remove ofertas cujo ciclo de vida indica compra muito recente.

    Filtro em duas camadas:
    1. **Produto individual**: usa lifecycle_ratio do produto específico.
    2. **Categoria (fallback)**: se o produto não tem lifecycle individual,
       ou se o cliente comprou outro produto da mesma categoria recentemente,
       usa o lifecycle médio da categoria como referência.

    Regras:
    - MANTER se nunca comprou este produto NEM nada da mesma categoria
    - MANTER se lifecycle_ratio >= min_ratio (produto individual)
    - MANTER se sem lifecycle individual E category_ratio >= min_ratio
    - REMOVER se lifecycle_ratio < min_ratio (comprou recentemente)
    - REMOVER se sem lifecycle individual E category_ratio < min_ratio
    - REMOVER se sem lifecycle individual E sem lifecycle de categoria
      E days_since_last < fallback_days
    """
    original_count = len(reco_df)

    if lifecycle_df.empty and last_purchase_df.empty:
        LOG.info("No lifecycle or last-purchase data available. Skipping hard filter.")
        return reco_df

    today = pd.Timestamp.now()

    # Merge lifecycle data (produto individual)
    df = reco_df.merge(
        lifecycle_df[["product_id", "avg_days_between_purchases"]],
        on="product_id",
        how="left",
    ) if not lifecycle_df.empty else reco_df.assign(avg_days_between_purchases=np.nan)

    # Merge last purchase date per (customer, product) pair
    df = df.merge(
        last_purchase_df[["customer_id", "product_id", "last_purchase_date"]],
        on=["customer_id", "product_id"],
        how="left",
    )

    # Calculate days since last purchase (produto individual)
    df["days_since_last"] = (today - df["last_purchase_date"]).dt.days

    # Calculate lifecycle ratio (produto individual)
    has_lifecycle = (
        df["avg_days_between_purchases"].notna() &
        (df["avg_days_between_purchases"] > 0) &
        df["days_since_last"].notna()
    )
    df["lifecycle_ratio"] = np.where(
        has_lifecycle,
        df["days_since_last"] / df["avg_days_between_purchases"],
        np.nan,
    )

    # ── Categoria fallback ────────────────────────────────────────────────────
    has_category_data = (
        product_desc_df is not None and not product_desc_df.empty
        and category_lifecycle_df is not None and not category_lifecycle_df.empty
        and category_last_purchase_df is not None and not category_last_purchase_df.empty
    )

    if has_category_data:
        # Derive category for each product in offers
        desc_map = dict(zip(product_desc_df["product_id"], product_desc_df["description"]))
        df["product_category"] = df["product_id"].map(
            lambda pid: extract_product_category(desc_map.get(pid, ""))
        )

        # Merge category lifecycle
        df = df.merge(
            category_lifecycle_df[["product_category", "category_avg_lifecycle"]],
            on="product_category",
            how="left",
        )

        # Merge category last purchase (most recent purchase of ANY product in same category)
        df = df.merge(
            category_last_purchase_df[["customer_id", "product_category", "last_category_purchase_date"]],
            on=["customer_id", "product_category"],
            how="left",
        )

        # Calculate category-level days since last and ratio
        df["cat_days_since_last"] = (today - df["last_category_purchase_date"]).dt.days
        has_cat_lifecycle = (
            df["category_avg_lifecycle"].notna() &
            (df["category_avg_lifecycle"] > 0) &
            df["cat_days_since_last"].notna()
        )
        df["category_ratio"] = np.where(
            has_cat_lifecycle,
            df["cat_days_since_last"] / df["category_avg_lifecycle"],
            np.nan,
        )
    else:
        df["category_ratio"] = np.nan
        df["cat_days_since_last"] = np.nan

    # ── Build keep mask (produto individual + categoria) ──────────────────
    never_bought_product = df["last_purchase_date"].isna()
    never_bought_category = df["cat_days_since_last"].isna() if has_category_data else pd.Series(True, index=df.index)
    has_ratio = df["lifecycle_ratio"].notna()
    has_cat_ratio = df["category_ratio"].notna()
    no_lifecycle_data = df["avg_days_between_purchases"].isna() | (df["avg_days_between_purchases"] <= 0)

    # Category blocks: cliente comprou algo da mesma categoria recentemente
    # Aplica-se TAMBÉM quando produto tem lifecycle individual (cross-product substitution)
    category_blocks = has_cat_ratio & (df["category_ratio"] < min_ratio)

    keep = (
        # Nunca comprou este produto NEM nada da mesma categoria
        (never_bought_product & never_bought_category) |
        # Produto individual: ratio OK E categoria nao bloqueia
        (has_ratio & (df["lifecycle_ratio"] >= min_ratio) & ~category_blocks) |
        # Produto sem lifecycle individual, mas com lifecycle de categoria: ratio de categoria OK
        (no_lifecycle_data & has_cat_ratio & (df["category_ratio"] >= min_ratio)) |
        # Sem lifecycle individual NEM de categoria: fallback por dias
        (no_lifecycle_data & ~has_cat_ratio & never_bought_product) |
        (no_lifecycle_data & ~has_cat_ratio & df["days_since_last"].notna() &
         (df["days_since_last"] >= fallback_days))
    )
    # O filtro de categoria só se aplica como fallback quando não há lifecycle individual.

    filtered_count = (~keep).sum()
    df = df[keep].copy()

    LOG.info(
        f"Lifecycle hard filter (min_ratio={min_ratio}): "
        f"removed {filtered_count:,} offers out of {original_count:,} "
        f"({filtered_count/max(original_count,1)*100:.1f}%)"
    )

    # Re-rank within each (customer_id, strategy) group
    df = df.sort_values(["customer_id", "strategy", "score"], ascending=[True, True, False])
    df["rank"] = df.groupby(["customer_id", "strategy"]).cumcount() + 1

    # Drop auxiliary columns
    cols_to_drop = [
        c for c in ["avg_days_between_purchases", "last_purchase_date",
                     "days_since_last", "lifecycle_ratio",
                     "product_category", "category_avg_lifecycle",
                     "last_category_purchase_date", "cat_days_since_last",
                     "category_ratio"]
        if c in df.columns
    ]
    df = df.drop(columns=cols_to_drop)

    return df


# ===========================================================================
# SCORE NORMALIZATION (PERCENTILE FALLBACK)
# ===========================================================================

def normalize_scores_percentile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza scores para percentil (0-1) dentro de cada estratégia.

    Garante distribuição significativa mesmo quando o modelo raw produz
    scores concentrados (e.g., 87% com score=1.0 após retrain).

    O ranking relativo é preservado: o produto com maior score raw
    continua com maior score normalizado.
    """
    for strategy in df['strategy'].unique():
        mask = df['strategy'] == strategy
        raw = df.loc[mask, 'score']
        # rank pct: 0.0 = pior, 1.0 = melhor
        df.loc[mask, 'score'] = raw.rank(pct=True, method='average')

    # Re-rank (garante consistência após normalização)
    df = df.sort_values(['customer_id', 'strategy', 'score'], ascending=[True, True, False])
    df['rank'] = df.groupby(['customer_id', 'strategy']).cumcount() + 1
    return df


# ===========================================================================
# FALSE DISCOVERY RATE (FDR) CONTROL
# ===========================================================================

def apply_fdr_filter(
    df_offers: pd.DataFrame,
    fdr_threshold: float = 0.20,
) -> pd.DataFrame:
    """
    Filtra ofertas controlando False Discovery Rate via Benjamini-Hochberg.

    Cada oferta e uma hipotese implicita: "O Cliente X comprara o Produto Y
    nos proximos 30 dias." Com milhares de ofertas simultaneas, o problema
    de multiplas comparacoes e real.

    O procedimento BH controla o FDR (proporcao esperada de falsos positivos
    entre as rejeicoes), sendo menos conservador que Bonferroni.

    NOTA IMPORTANTE: O controle de FDR requer p-values derivados de
    probabilidades calibradas. Atualmente usamos `1 - score_raw` como
    proxy de p-value, onde score_raw e o percentil normalizado. Isso e uma
    aproximacao grosseira. Para que o FDR funcione corretamente, e necessario
    implementar calibracao de scores (Platt scaling ou isotonic regression)
    como etapa futura.

    Parametros
    ----------
    df_offers     : DataFrame com coluna 'score_raw' (percentil normalizado)
    fdr_threshold : threshold de FDR (default: 0.20 = 20%)
                    Valores tipicos: 0.05 (conservador), 0.20 (equilibrado),
                    0.30 (agressivo)

    Retorna
    -------
    DataFrame filtrado contendo apenas ofertas que passaram no teste BH
    """
    from statsmodels.stats.multitest import multipletests

    if df_offers.empty:
        return df_offers

    score_col = "score_raw" if "score_raw" in df_offers.columns else "score"
    scores = df_offers[score_col].values

    # Proxy de p-value: 1 - score_raw (quanto menor o score, maior o p-value)
    # CAVEAT: score_raw e percentil, nao probabilidade calibrada. Resultados
    # devem ser interpretados com cautela ate implementacao de calibracao.
    p_values = 1.0 - scores

    # Clipa p-values para evitar 0.0 ou 1.0 exatos (BH requer p > 0)
    p_values = np.clip(p_values, 1e-10, 1.0 - 1e-10)

    rejected, p_corrected, _, _ = multipletests(
        p_values, alpha=fdr_threshold, method="fdr_bh"
    )

    before_count = len(df_offers)
    df_filtered = df_offers[rejected].copy()
    after_count = len(df_filtered)
    removed = before_count - after_count

    LOG.info(
        f"FDR filter (Benjamini-Hochberg, threshold={fdr_threshold:.0%}): "
        f"kept {after_count:,} of {before_count:,} offers "
        f"(removed {removed:,}, {removed/max(before_count,1)*100:.1f}%)"
    )

    if after_count > 0:
        # Re-rank within each (customer_id, strategy) after filtering
        df_filtered = df_filtered.sort_values(
            ["customer_id", "strategy", "score"], ascending=[True, True, False]
        )
        df_filtered["rank"] = df_filtered.groupby(
            ["customer_id", "strategy"]
        ).cumcount() + 1

    return df_filtered


# ===========================================================================
# GERACAO — MODELO A
# ===========================================================================

def run_model_a(pg, top_n: int, only_ever_bought: bool = True, canonical_map: dict = None) -> pd.DataFrame:
    """Carrega historico + modelo A e gera recomendacoes."""
    LOG.info("── Modelo A (LightGBM ranker) ──────────────────────────────────")
    df_history = load_history_a(pg, history_days=A_HISTORY_DAYS)

    # Consolidar duplicatas sob canonical_id
    if canonical_map:
        before_products = df_history['product_id'].nunique()
        df_history['product_id'] = df_history['product_id'].map(
            lambda pid: canonical_map.get(pid, pid)
        )
        after_products = df_history['product_id'].nunique()
        LOG.info(f"History (A) consolidated to canonical products: {before_products:,} -> {after_products:,}")

    model = load_model_a()
    reco_df = generate_recommendations(
        model, df_history, top_n=top_n, only_ever_bought=only_ever_bought,
    )
    LOG.info(f"Modelo A: {len(reco_df):,} ofertas para {reco_df['customer_id'].nunique():,} clientes")
    return reco_df


# ===========================================================================
# GERAÇÃO — MODELO B
# ===========================================================================

def run_model_b(pg, top_n: int, include_ever_bought: bool = True, canonical_map: dict = None) -> pd.DataFrame:
    """Carrega artefatos do Modelo B + historico e gera recomendacoes colaborativas."""
    LOG.info("── Modelo B (SVD colaborativo) ─────────────────────────────────")
    artifacts = load_model_artifacts()
    df_history = load_history_b(pg, history_days=B_HISTORY_DAYS)

    # Consolidar duplicatas sob canonical_id
    if canonical_map:
        before_products = df_history['product_id'].nunique()
        df_history['product_id'] = df_history['product_id'].map(
            lambda pid: canonical_map.get(pid, pid)
        )
        after_products = df_history['product_id'].nunique()
        LOG.info(f"History (B) consolidated to canonical products: {before_products:,} -> {after_products:,}")

    neighbors = compute_customer_similarity(
        artifacts["user_embeddings"],
        artifacts["customer_ids"],
        k_neighbors=DEFAULT_K_NEIGHBORS,
    )
    reco_df = generate_collaborative_recommendations(
        df_history, neighbors, top_n=top_n, include_ever_bought=include_ever_bought,
    )
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

    has_score_raw = "score_raw" in df.columns

    rows = [
        (
            batch_id,
            int(row["customer_id"]),
            int(row["product_id"]),
            str(row["strategy"]),
            float(row["score"]),
            float(row["score_raw"]) if has_score_raw else float(row["score"]),
            int(row["rank"]),
            generated_at,
            expires_at,
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO reco.offers
            (offer_batch_id, customer_id, product_id, strategy, score, score_raw, rank, generated_at, expires_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

def main(
    strategy: str = "both",
    top_n: int = 10,
    dry_run: bool = False,
    lifecycle_min_ratio: float = 0.6,
    lifecycle_min_samples: int = 5,
    lifecycle_min_customers: int = 3,
    include_never_bought: bool = False,
    fdr_threshold: Optional[float] = None,
) -> None:
    LOG.info("=" * 70)
    LOG.info(
        f"GENERATE OFFERS | strategy={strategy} | top_n={top_n} | "
        f"dry_run={dry_run} | lifecycle_min_ratio={lifecycle_min_ratio} | "
        f"lifecycle_min_samples={lifecycle_min_samples} | "
        f"lifecycle_min_customers={lifecycle_min_customers} | "
        f"include_never_bought={include_never_bought} | "
        f"fdr_threshold={fdr_threshold}"
    )
    LOG.info("=" * 70)

    pg = get_pg_conn()

    batch_id    = str(uuid.uuid4())
    generated_at = datetime.now(tz=timezone.utc)
    expires_at   = generated_at + timedelta(days=OFFER_EXPIRY_DAYS)

    LOG.info(f"Batch ID     : {batch_id}")
    LOG.info(f"Generated at : {generated_at.isoformat()}")
    LOG.info(f"Expires at   : {expires_at.isoformat()}")

    # ── Refresh materialized view com dados mais recentes ─────────────────────
    LOG.info("Refreshing product lifecycle materialized view...")
    try:
        with pg.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY reco.product_lifecycle")
        pg.commit()
        LOG.info("Lifecycle view refreshed.")
    except Exception as exc:
        LOG.warning(f"Could not refresh reco.product_lifecycle: {exc}. Continuing with stale data.")
        try:
            pg.rollback()
        except Exception:
            pass

    # ── Load canonical product mapping ────────────────────────────────────────
    try:
        canonical_df = pd.read_sql(
            "SELECT product_id, canonical_id FROM reco.product_canonical",
            pg
        )
        canonical_map = dict(zip(canonical_df['product_id'], canonical_df['canonical_id']))
        remapped_count = sum(1 for k, v in canonical_map.items() if k != v)
        LOG.info(f"Canonical mapping loaded: {len(canonical_map):,} products, "
                 f"{remapped_count:,} remapped")
    except Exception as exc:
        LOG.warning(f"Could not load reco.product_canonical: {exc}. Proceeding without canonical mapping.")
        canonical_map = None

    # Pre-load lifecycle and last-purchase data (used by hard filter + sigmoid discount)
    lifecycle_df = load_lifecycle_data(pg)
    lifecycle_df = filter_unreliable_lifecycle(
        lifecycle_df,
        min_sample_size=lifecycle_min_samples,
        min_distinct_customers=lifecycle_min_customers,
    )
    last_purchase_df = load_last_purchase_per_customer_product(pg)
    LOG.info(f"Last-purchase pairs loaded: {len(last_purchase_df):,}")

    # ── Apply canonical mapping to lifecycle and last-purchase data ────────────
    if canonical_map:
        # Consolidate last_purchase_df: remap product_id, then keep MAX date per pair
        last_purchase_df['product_id'] = last_purchase_df['product_id'].map(
            lambda pid: canonical_map.get(pid, pid)
        )
        before_lp = len(last_purchase_df)
        last_purchase_df = last_purchase_df.groupby(
            ['customer_id', 'product_id'], as_index=False
        )['last_purchase_date'].max()
        LOG.info(f"Last-purchase canonical consolidation: {before_lp:,} -> {len(last_purchase_df):,} pairs")

        # Consolidate lifecycle_df: remap product_id, then aggregate duplicates
        if not lifecycle_df.empty:
            lifecycle_df['product_id'] = lifecycle_df['product_id'].map(
                lambda pid: canonical_map.get(pid, pid)
            )
            before_lc = len(lifecycle_df)
            lifecycle_df = lifecycle_df.groupby('product_id', as_index=False).agg({
                'avg_days_between_purchases': 'mean',
                'median_days_between_purchases': 'mean',
                'lifecycle_tier': 'first',
                'sample_size': 'sum',
                'distinct_customers': 'sum',
            })
            LOG.info(f"Lifecycle canonical consolidation: {before_lc:,} -> {len(lifecycle_df):,} products")

    # ── Load category-level lifecycle data (fallback for products without individual lifecycle)
    category_lifecycle_df = load_category_lifecycle(pg)
    category_last_purchase_df = load_last_purchase_per_customer_category(pg)
    product_desc_df = load_product_descriptions(pg)

    # Determine only_ever_bought from include_never_bought flag
    only_ever_bought = not include_never_bought

    frames: List[pd.DataFrame] = []

    try:
        if strategy in ("a", "both"):
            frames.append(run_model_a(pg, top_n=top_n, only_ever_bought=only_ever_bought, canonical_map=canonical_map))

        if strategy in ("b", "both"):
            frames.append(run_model_b(pg, top_n=top_n, include_ever_bought=True, canonical_map=canonical_map))

        if not frames:
            LOG.error("Nenhuma estrategia selecionada.")
            sys.exit(1)

        df_all = pd.concat(frames, ignore_index=True)
        LOG.info(f"Total de ofertas geradas: {len(df_all):,} ({df_all['customer_id'].nunique():,} clientes unicos)")

        # ── Step 1: Hard filter — remove ofertas com lifecycle_ratio < min_ratio ──
        df_all = apply_lifecycle_hard_filter(
            df_all, lifecycle_df, last_purchase_df,
            min_ratio=lifecycle_min_ratio,
            category_lifecycle_df=category_lifecycle_df,
            category_last_purchase_df=category_last_purchase_df,
            product_desc_df=product_desc_df,
        )

        # ── Step 2: Never-bought filter — remove produtos nunca comprados ─────────
        if not include_never_bought:
            before_nb = len(df_all)
            df_all = df_all.merge(
                last_purchase_df[["customer_id", "product_id"]].drop_duplicates(),
                on=["customer_id", "product_id"],
                how="inner",
            )
            removed_nb = before_nb - len(df_all)
            LOG.info(
                f"Never-bought filter: removed {removed_nb:,} offers "
                f"(products the customer never purchased)"
            )
            # Re-rank after removal
            if removed_nb > 0:
                df_all = df_all.sort_values(
                    ["customer_id", "strategy", "score"], ascending=[True, True, False]
                )
                df_all["rank"] = df_all.groupby(["customer_id", "strategy"]).cumcount() + 1

        # ── Step 3: Normalize scores by percentile BEFORE lifecycle discount ──
        LOG.info(
            f"Score distribution before normalization: "
            f"min={df_all['score'].min():.4f}, "
            f"median={df_all['score'].median():.4f}, "
            f"max={df_all['score'].max():.4f}"
        )
        df_all = normalize_scores_percentile(df_all)
        LOG.info(
            f"Score distribution after normalization: "
            f"min={df_all['score'].min():.4f}, "
            f"median={df_all['score'].median():.4f}, "
            f"max={df_all['score'].max():.4f}"
        )

        # ── Step 4: Save normalized score before lifecycle discount ──────────
        df_all["score_raw"] = df_all["score"].copy()

        # ── Step 5: Sigmoid discount — TERMINAL: penaliza score final ────────
        df_all = apply_lifecycle_discount(
            df_all, lifecycle_df, last_purchase_df,
            category_lifecycle_df=category_lifecycle_df,
            category_last_purchase_df=category_last_purchase_df,
            product_desc_df=product_desc_df,
        )
        LOG.info(
            f"Score distribution after lifecycle discount: "
            f"min={df_all['score'].min():.4f}, "
            f"median={df_all['score'].median():.4f}, "
            f"max={df_all['score'].max():.4f}"
        )

        # ── Step 6: FDR filter — controle de False Discovery Rate ────────
        if fdr_threshold is not None and fdr_threshold > 0:
            df_all = apply_fdr_filter(df_all, fdr_threshold=fdr_threshold)

        LOG.info(f"Final offers after all filters: {len(df_all):,}")

        if dry_run:
            LOG.info("[DRY RUN] Sem gravacao no banco. Amostra:")
            print(df_all.head(10).to_string(index=False))
            return

        inserted = persist_offers(pg, df_all, batch_id, generated_at, expires_at)
        LOG.info(f"Concluido. {inserted:,} ofertas salvas em reco.offers.")

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
        help="Gera as ofertas mas NAO grava no banco -- mostra amostra",
    )
    parser.add_argument(
        "--lifecycle-min-ratio",
        type=float,
        default=0.6,
        help="Ratio minimo de ciclo de vida para manter oferta (default: 0.6)",
    )
    parser.add_argument(
        "--lifecycle-min-samples",
        type=int,
        default=5,
        help="Amostra minima de intervalos para confiar no lifecycle (default: 5)",
    )
    parser.add_argument(
        "--lifecycle-min-customers",
        type=int,
        default=3,
        help="Clientes distintos minimos para confiar no lifecycle (default: 3)",
    )
    parser.add_argument(
        "--include-never-bought",
        action="store_true",
        default=False,
        help="Incluir produtos que o cliente nunca comprou (por padrao, apenas recompra)",
    )
    parser.add_argument(
        "--fdr-threshold",
        type=float,
        default=None,
        help=(
            "Threshold de False Discovery Rate (Benjamini-Hochberg). "
            "Se informado, filtra ofertas controlando FDR. "
            "Valores tipicos: 0.05 (conservador), 0.20 (equilibrado), 0.30 (agressivo). "
            "NOTA: requer scores calibrados para resultados precisos; "
            "atualmente usa percentil como proxy."
        ),
    )
    args = parser.parse_args()
    main(
        strategy=args.strategy,
        top_n=args.top_n,
        dry_run=args.dry_run,
        lifecycle_min_ratio=args.lifecycle_min_ratio,
        lifecycle_min_samples=args.lifecycle_min_samples,
        lifecycle_min_customers=args.lifecycle_min_customers,
        include_never_bought=args.include_never_bought,
        fdr_threshold=args.fdr_threshold,
    )
