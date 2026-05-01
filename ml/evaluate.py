"""
ml/evaluate.py
==============
Módulo de avaliação offline dos modelos de recomendação.

RESPONSABILIDADE
  Este módulo centraliza TODAS as métricas de avaliação. Importado por
  modelo_a_ranker.py e modelo_b_colaborativo.py para garantir consistência
  na comparação entre estratégias.

MÉTRICAS IMPLEMENTADAS
  ┌─────────────────┬─────────────────────────────────────────────────────┐
  │ Precision@K     │ Fração dos top-K recomendados que são relevantes    │
  │ Recall@K        │ Fração dos itens relevantes capturados no top-K     │
  │ NDCG@K          │ Qualidade da ordenação com desconto logarítmico     │
  │ MAP@K           │ Média ponderada da precisão para toda a lista       │
  │ HitRate@K       │ % de clientes com ao menos 1 acerto no top-K       │
  ├─────────────────┼─────────────────────────────────────────────────────┤
  │ AUC-ROC         │ Área sob a curva ROC (para avaliação binária)       │
  │ Confusion Matrix│ Verdadeiros/Falsos Positivos e Negativos            │
  │ Classification  │ Relatório completo: precision, recall, F1 por classe│
  └─────────────────┴─────────────────────────────────────────────────────┘

USO
  from ml.evaluate import evaluate_ranking, evaluate_binary_classifier
  from ml.evaluate import print_evaluation_report

REFERÊNCIAS
  • Koren et al. (2009) — Matrix Factorization Techniques for Recommender Systems
  • Manning et al. (2008) — Introduction to Information Retrieval (Capítulo 8)
"""

import logging
import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

LOG = logging.getLogger("ml.evaluate")


# ===========================================================================
# SEÇÃO 1 — MÉTRICAS DE RANKING (Para avaliação dos dois modelos)
# ===========================================================================

def precision_at_k(recommended: List, relevant: List, k: int) -> float:
    """
    Precision@K — fração dos top-K que são relevantes.

    Pergunta: "Dos K itens que recomendamos, quantos o cliente comprou?"

    Parâmetros
    ----------
    recommended : lista de IDs recomendados, ORDENADOS por score (melhor primeiro)
    relevant    : lista de IDs que o cliente comprou no período de teste
    k           : tamanho do corte

    Retorna
    -------
    float em [0, 1] — 1.0 é perfeito

    Exemplo
    -------
    >>> precision_at_k([1, 2, 3, 4, 5], relevant=[2, 5, 10], k=5)
    0.4  # 2 acertos (prod 2 e 5) em 5 recomendações
    """
    if k <= 0:
        raise ValueError("k deve ser positivo")

    top_k = recommended[:k]
    relevant_set = set(relevant)
    hits = sum(1 for item in top_k if item in relevant_set)
    return hits / k


def recall_at_k(recommended: List, relevant: List, k: int) -> float:
    """
    Recall@K — fração dos itens relevantes capturados no top-K.

    Pergunta: "Dos produtos que o cliente comprou, quantos estavam nos K sugeridos?"

    Retorna 0.0 se relevant for vazio (cliente sem compras no período de teste).
    """
    if not relevant:
        return 0.0
    if k <= 0:
        raise ValueError("k deve ser positivo")

    top_k = set(recommended[:k])
    relevant_set = set(relevant)
    return len(top_k & relevant_set) / len(relevant_set)


def average_precision_at_k(recommended: List, relevant: List, k: int) -> float:
    """
    Average Precision@K (AP@K) — base do MAP@K.

    Calcula a precisão em cada posição onde há um acerto, depois tira a média.
    Penaliza recomendações relevantes colocadas em posições ruins.

    AP@K = (1/|R|) * sum_{i=1}^{K} P@i * rel(i)
    onde rel(i) = 1 se o item na posição i é relevante, 0 caso contrário.
    """
    if not relevant:
        return 0.0

    relevant_set = set(relevant)
    hits = 0
    score = 0.0

    for i, item in enumerate(recommended[:k], start=1):
        if item in relevant_set:
            hits += 1
            # P@i no ponto de acerto
            score += hits / i

    # Normaliza pelo tamanho do conjunto relevante (não pelo K)
    return score / min(len(relevant_set), k)


def ndcg_at_k(recommended: List, relevant: List, k: int) -> float:
    """
    Normalized Discounted Cumulative Gain @K (NDCG@K).

    Mede qualidade da ordenação — acertos no topo valem mais que no final.

    DCG@K  = sum_{i=1}^{K} rel_i / log2(i + 1)
    IDCG@K = DCG de um ranking perfeito (todos relevantes no topo)
    NDCG@K = DCG@K / IDCG@K

    Retorna valor em [0, 1].
    """
    if not relevant or k <= 0:
        return 0.0

    relevant_set = set(relevant)

    # DCG real — desconto pelo log2 da posição
    dcg = 0.0
    for i, item in enumerate(recommended[:k], start=1):
        if item in relevant_set:
            dcg += 1.0 / math.log2(i + 1)

    # IDCG — ranking ideal: todos os relevantes no topo
    n_ideal = min(len(relevant_set), k)
    idcg = sum(1.0 / math.log2(i + 1) for i in range(1, n_ideal + 1))

    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def hit_rate_at_k(
    recommendations_per_customer: Dict[int, List],
    relevant_per_customer: Dict[int, List],
    k: int,
) -> float:
    """
    Hit Rate@K — percentual de clientes com ao menos 1 acerto no top-K.

    Métrica de negócio: "Quantos clientes receberam ao menos 1 sugestão útil?"

    Parâmetros
    ----------
    recommendations_per_customer : {customer_id: [product_id_1, ...]} ordenados por score
    relevant_per_customer        : {customer_id: [product_id_comprado_1, ...]} do período de teste
    k                            : tamanho do corte

    Retorna
    -------
    float em [0, 1]
    """
    customers_com_reco = [c for c in recommendations_per_customer if c in relevant_per_customer]
    if not customers_com_reco:
        return 0.0

    hits = sum(
        1
        for c in customers_com_reco
        if any(
            item in set(relevant_per_customer[c])
            for item in recommendations_per_customer[c][:k]
        )
    )
    return hits / len(customers_com_reco)


def map_at_k(
    recommendations_per_customer: Dict[int, List],
    relevant_per_customer: Dict[int, List],
    k: int,
) -> float:
    """
    Mean Average Precision@K (MAP@K) — média de AP@K sobre todos os clientes.

    Combina precisão e ordenação em uma única métrica. É a mais informativa
    para comparar dois modelos de ranking.
    """
    customers = [c for c in recommendations_per_customer if c in relevant_per_customer]
    if not customers:
        return 0.0

    ap_scores = [
        average_precision_at_k(recommendations_per_customer[c], relevant_per_customer[c], k)
        for c in customers
    ]
    return float(np.mean(ap_scores))


# ===========================================================================
# SEÇÃO 2 — AVALIAÇÃO AGREGADA DE RANKING
# ===========================================================================

def evaluate_ranking(
    recommendations_per_customer: Dict[int, List],
    relevant_per_customer: Dict[int, List],
    k_values: List[int] = (5, 10, 20),
) -> pd.DataFrame:
    """
    Avalia um modelo de ranking para múltiplos valores de K.

    Retorna um DataFrame com todas as métricas por K, pronto para exibição
    ou gravação em reco.evaluation_runs.

    Parâmetros
    ----------
    recommendations_per_customer : dict {customer_id: [product_ids ordenados]}
    relevant_per_customer        : dict {customer_id: [product_ids comprados no período de teste]}
    k_values                     : lista de K para avaliar (ex.: [5, 10, 20])

    Retorna
    -------
    pd.DataFrame com colunas: [k, precision, recall, ndcg, map, hit_rate, n_customers]
    """
    # Interseção de clientes com recomendações E com compras no teste
    customers = [c for c in recommendations_per_customer if c in relevant_per_customer]
    n = len(customers)
    LOG.info(f"Avaliando {n} clientes com recomendações e compras no período de teste.")

    resultados = []
    for k in k_values:
        precisions = [
            precision_at_k(recommendations_per_customer[c], relevant_per_customer[c], k)
            for c in customers
        ]
        recalls = [
            recall_at_k(recommendations_per_customer[c], relevant_per_customer[c], k)
            for c in customers
        ]
        ndcgs = [
            ndcg_at_k(recommendations_per_customer[c], relevant_per_customer[c], k)
            for c in customers
        ]
        ap_scores = [
            average_precision_at_k(recommendations_per_customer[c], relevant_per_customer[c], k)
            for c in customers
        ]
        hr = hit_rate_at_k(recommendations_per_customer, relevant_per_customer, k)

        resultados.append({
            "k":           k,
            "precision":   float(np.mean(precisions)),
            "recall":      float(np.mean(recalls)),
            "ndcg":        float(np.mean(ndcgs)),
            "map":         float(np.mean(ap_scores)),
            "hit_rate":    hr,
            "n_customers": n,
        })

    return pd.DataFrame(resultados)


# ===========================================================================
# SEÇÃO 3 — AVALIAÇÃO BINÁRIA (Para Modelo A — LightGBM)
# ===========================================================================

def evaluate_binary_classifier(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float = 0.5,
    class_names: List[str] = ("nao_comprou", "comprou"),
) -> Dict:
    """
    Avaliação completa de um classificador binário (comprou / não comprou).

    Calcula e exibe:
    - Matriz de confusão (valores absolutos e percentuais)
    - AUC-ROC
    - Precision, Recall, F1-Score por classe
    - Acurácia geral

    Parâmetros
    ----------
    y_true        : array de rótulos verdadeiros (0 ou 1)
    y_pred_proba  : array de probabilidades previstas para a classe positiva (1)
    threshold     : ponto de corte para classificação (padrão: 0.5)
    class_names   : nomes das classes para exibição

    Retorna
    -------
    dict com todas as métricas calculadas
    """
    # Converte probabilidades em classes com o threshold definido
    y_pred = (y_pred_proba >= threshold).astype(int)

    # --- Matriz de Confusão ---
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()

    LOG.info("=" * 60)
    LOG.info("AVALIAÇÃO DO CLASSIFICADOR BINÁRIO")
    LOG.info("=" * 60)
    LOG.info(f"Threshold de classificação: {threshold:.2f}")
    LOG.info("")
    LOG.info("MATRIZ DE CONFUSÃO:")
    LOG.info(f"              Previsto: NÃO   Previsto: SIM")
    LOG.info(f"Real: NÃO      TN={tn:>8,}   FP={fp:>8,}")
    LOG.info(f"Real: SIM      FN={fn:>8,}   TP={tp:>8,}")
    LOG.info("")

    # Percentuais em relação ao total
    total = tn + fp + fn + tp
    LOG.info("MATRIZ DE CONFUSÃO (% do total):")
    LOG.info(f"  TN (Verdadeiro Negativo): {tn/total*100:.1f}% — acertou que NÃO compraria")
    LOG.info(f"  FP (Falso Positivo):      {fp/total*100:.1f}% — previu compra, não comprou (custo: mensagem desnecessária)")
    LOG.info(f"  FN (Falso Negativo):      {fn/total*100:.1f}% — previu não comprar, mas comprou (custo: oportunidade perdida)")
    LOG.info(f"  TP (Verdadeiro Positivo): {tp/total*100:.1f}% — acertou que compraria")
    LOG.info("")

    # --- AUC-ROC ---
    # AUC mede a capacidade de ordenação independente do threshold.
    # Muito mais informativa que acurácia em datasets desbalanceados.
    try:
        auc = roc_auc_score(y_true, y_pred_proba)
        LOG.info(f"AUC-ROC: {auc:.4f}  (>0.70 = bom | >0.80 = ótimo | >0.90 = excelente)")
    except ValueError as e:
        LOG.warning(f"Não foi possível calcular AUC-ROC: {e}")
        auc = None

    # --- Relatório de Classificação ---
    report = classification_report(
        y_true, y_pred,
        target_names=class_names,
        output_dict=True,
    )
    LOG.info("RELATÓRIO DE CLASSIFICAÇÃO:")
    LOG.info(classification_report(y_true, y_pred, target_names=class_names))

    # --- Métricas Derivadas ---
    # Precision (PPV): dos que previmos que comprariam, quantos compraram?
    precision_pos = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    # Recall (Sensibilidade): dos que compraram, quantos capturamos?
    recall_pos    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    # Especificidade: dos que NÃO compraram, quantos identificamos corretamente?
    specificity   = tn / (tn + fp) if (tn + fp) > 0 else 0.0
    # F1: média harmônica de precision e recall
    f1_pos = 2 * precision_pos * recall_pos / (precision_pos + recall_pos) if (precision_pos + recall_pos) > 0 else 0.0

    LOG.info(f"Precision (classe positiva): {precision_pos:.4f}")
    LOG.info(f"Recall    (classe positiva): {recall_pos:.4f}")
    LOG.info(f"Especificidade:              {specificity:.4f}")
    LOG.info(f"F1-Score  (classe positiva): {f1_pos:.4f}")
    LOG.info("=" * 60)

    return {
        "confusion_matrix":  cm,
        "tn": tn, "fp": fp, "fn": fn, "tp": tp,
        "auc_roc":           auc,
        "precision_pos":     precision_pos,
        "recall_pos":        recall_pos,
        "specificity":       specificity,
        "f1_pos":            f1_pos,
        "accuracy":          (tn + tp) / total,
        "classification_report": report,
        "threshold":         threshold,
    }


def find_optimal_threshold(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    metric: str = "f1-score",
) -> Tuple[float, float]:
    """
    Encontra o threshold ótimo de classificação por busca em grade.

    Para recomendação, geralmente queremos maximizar recall (não perder
    oportunidades de venda), mas isso depende do custo de negócio:
    - Alto recall → mais sugestões, mais mensagens, custo maior
    - Alta precision → menos sugestões, maior taxa de conversão esperada

    Parâmetros
    ----------
    y_true       : rótulos verdadeiros
    y_pred_proba : probabilidades previstas
    metric       : 'f1-score' | 'recall' | 'precision' — métrica a maximizar
                     (chaves do classification_report do sklearn)

    Retorna
    -------
    (best_threshold, best_score)
    """
    thresholds = np.arange(0.1, 0.9, 0.01)
    best_threshold = 0.5
    best_score = 0.0

    for thresh in thresholds:
        y_pred = (y_pred_proba >= thresh).astype(int)
        report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)

        # Extrai a métrica da classe positiva (comprou = 1)
        pos_key = "1" if "1" in report else list(report.keys())[1]
        score = report.get(pos_key, {}).get(metric, 0.0)

        if score > best_score:
            best_score = score
            best_threshold = thresh

    LOG.info(f"Threshold ótimo ({metric}): {best_threshold:.2f} → {metric}={best_score:.4f}")
    return float(best_threshold), float(best_score)


# ===========================================================================
# SEÇÃO 4 — RELATÓRIO FINAL FORMATADO
# ===========================================================================

def print_ranking_report(
    df_metrics: pd.DataFrame,
    strategy_name: str,
) -> None:
    """
    Exibe o relatório de ranking de forma formatada no log.

    Parâmetros
    ----------
    df_metrics    : DataFrame retornado por evaluate_ranking()
    strategy_name : nome da estratégia para identificação no log
    """
    LOG.info("=" * 70)
    LOG.info(f"AVALIAÇÃO DE RANKING — Estratégia: {strategy_name.upper()}")
    LOG.info("=" * 70)
    LOG.info(f"{'K':>4} {'Precision':>10} {'Recall':>10} {'NDCG':>10} {'MAP':>10} {'HitRate':>10}")
    LOG.info("-" * 60)

    for _, row in df_metrics.iterrows():
        LOG.info(
            f"{int(row['k']):>4} "
            f"{row['precision']:>10.4f} "
            f"{row['recall']:>10.4f} "
            f"{row['ndcg']:>10.4f} "
            f"{row['map']:>10.4f} "
            f"{row['hit_rate']:>10.4f}"
        )

    LOG.info("-" * 60)
    LOG.info(f"Clientes avaliados: {int(df_metrics['n_customers'].iloc[0]):,}")
    LOG.info("=" * 70)


def save_metrics_to_db(
    pg,
    df_metrics: pd.DataFrame,
    strategy: str,
    notes: str = "",
    auc_roc: Optional[float] = None,
    hit_rate_at_k: Optional[float] = None,
) -> None:
    """
    Persiste as métricas de avaliação em reco.evaluation_runs.

    Esta tabela é a fonte de verdade para comparar versões de modelos ao longo
    do tempo. Permite identificar regressões ao re-treinar.

    Parâmetros
    ----------
    pg             : conexão psycopg2
    df_metrics     : DataFrame de evaluate_ranking()
    strategy       : nome da estratégia ('modelo_a_ranker', 'modelo_b_colaborativo', ...)
    notes          : observações livres (ex.: versão, hiperparâmetros usados)
    auc_roc        : AUC-ROC do classificador binário (opcional, apenas Modelo A)
    hit_rate_at_k  : Hit Rate@K global (opcional; se None, extrai do DataFrame)
    """
    with pg.cursor() as cur:
        for _, row in df_metrics.iterrows():
            # Usa hit_rate do DataFrame se disponivel, senao o parametro global
            row_hit_rate = row.get("hit_rate") if "hit_rate" in row.index else hit_rate_at_k
            cur.execute(
                """
                INSERT INTO reco.evaluation_runs
                    (strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k,
                     n_customers, notes, auc_roc, hit_rate_at_k)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    strategy,
                    int(row["k"]),
                    float(row["precision"]),
                    float(row["recall"]),
                    float(row["ndcg"]),
                    float(row["map"]),
                    int(row["n_customers"]),
                    notes,
                    float(auc_roc) if auc_roc is not None else None,
                    float(row_hit_rate) if row_hit_rate is not None else None,
                ),
            )
    pg.commit()
    LOG.info(f"Métricas salvas em reco.evaluation_runs para strategy='{strategy}'")


# ===========================================================================
# SECTION 5 — CONVERSION METRICS (Real-world feedback)
# ===========================================================================

def conversion_rate_at_k(
    pg,
    strategy: Optional[str] = None,
    k: int = 10,
    batch_id: Optional[str] = None,
) -> Dict:
    """
    Taxa de conversao real das top-K ofertas enviadas.

    Mede: das ofertas com rank <= K que foram avaliadas, quantas converteram?
    """
    filters = ["o.rank <= %s"]
    params = [k]
    if strategy:
        filters.append("o.strategy = %s")
        params.append(strategy)
    if batch_id:
        filters.append("o.offer_batch_id = %s::uuid")
        params.append(batch_id)

    where = " AND ".join(filters)

    with pg.cursor() as cur:
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT o.offer_id) AS total_offers,
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted = TRUE) AS converted,
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted IS NOT NULL) AS evaluated
            FROM reco.offers o
            LEFT JOIN reco.offer_outcomes oo ON oo.offer_id = o.offer_id
            WHERE {where}
        """, params)
        row = cur.fetchone()

    total, converted, evaluated = row
    rate = round(converted * 100.0 / evaluated, 2) if evaluated > 0 else 0.0

    result = {
        "k": k,
        "total_offers": total,
        "evaluated": evaluated,
        "converted": converted,
        "conversion_rate": rate,
    }
    LOG.info(f"Conversion Rate @{k}: {rate}% ({converted}/{evaluated})")
    return result


def incremental_lift(
    pg,
    strategy: Optional[str] = None,
    window_days: int = 30,
) -> Dict:
    """
    Lift incremental (cohort-matched): compara taxa de conversao de ofertas
    vs taxa de compra organica dos MESMOS clientes para produtos NAO oferecidos.

    Abordagem cohort-matched:
      - Numerador: taxa de conversao das ofertas (avaliadas pelo feedback loop)
      - Denominador: taxa de compra dos MESMOS clientes que receberam ofertas,
        para produtos que NAO foram oferecidos, na MESMA janela temporal.

    Isso elimina o vies de seleçao (clientes que recebem ofertas podem ser
    naturalmente mais ativos) e o denominador astronomico do CROSS JOIN.

    lift = (conversion_rate_offers) / (organic_purchase_rate_cohort)
    lift > 1.0 = ofertas estao gerando vendas incrementais

    Tambem calcula intervalo de confianca binomial (Wilson) para o lift.
    """
    strategy_filter = ""
    params = [window_days]
    if strategy:
        strategy_filter = "AND o.strategy = %s"
        params.append(strategy)

    with pg.cursor() as cur:
        # Offer conversion rate
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted = TRUE) AS offer_converted,
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted IS NOT NULL) AS offer_evaluated
            FROM reco.offers o
            JOIN reco.offer_outcomes oo ON oo.offer_id = o.offer_id
            WHERE (o.generated_at + make_interval(days => %s))::date <= CURRENT_DATE
            {strategy_filter}
        """, params)
        offer_row = cur.fetchone()
        offer_converted, offer_evaluated = offer_row

        # Organic rate (cohort-matched):
        # Para os MESMOS clientes que receberam ofertas, conta compras de
        # produtos que NAO foram oferecidos, na mesma janela temporal.
        # Denominador = total de pares (cliente, produto_nao_oferecido) distintos
        # que esses clientes poderiam ter comprado (produtos ativos nao oferecidos).
        cur.execute(f"""
            WITH offered_customers AS (
                SELECT DISTINCT o.customer_id
                FROM reco.offers o
                JOIN reco.offer_outcomes oo ON oo.offer_id = o.offer_id
                WHERE (o.generated_at + make_interval(days => %s))::date <= CURRENT_DATE
                {strategy_filter}
            ),
            offered_pairs AS (
                SELECT DISTINCT o.customer_id, o.product_id
                FROM reco.offers o
                WHERE EXISTS (SELECT 1 FROM offered_customers oc WHERE oc.customer_id = o.customer_id)
            ),
            organic_purchases AS (
                SELECT DISTINCT oi.customer_id, oi.product_id
                FROM cur.order_items oi
                JOIN offered_customers oc ON oc.customer_id = oi.customer_id
                WHERE oi.sale_date >= CURRENT_DATE - make_interval(days => %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM offered_pairs op
                      WHERE op.customer_id = oi.customer_id
                        AND op.product_id = oi.product_id
                  )
            ),
            organic_denominator AS (
                SELECT COUNT(DISTINCT (oc.customer_id, p.product_id)) AS potential_organic_pairs
                FROM offered_customers oc
                CROSS JOIN cur.products p
                WHERE p.active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM offered_pairs op
                      WHERE op.customer_id = oc.customer_id
                        AND op.product_id = p.product_id
                  )
            )
            SELECT
                (SELECT COUNT(*) FROM organic_purchases) AS organic_purchases,
                (SELECT potential_organic_pairs FROM organic_denominator) AS potential_pairs
        """, params + [window_days])
        organic_row = cur.fetchone()

    offer_rate = offer_converted / offer_evaluated if offer_evaluated > 0 else 0
    organic_purchases, potential_pairs = organic_row
    organic_rate = organic_purchases / potential_pairs if potential_pairs > 0 else 0
    lift = offer_rate / organic_rate if organic_rate > 0 else float('inf') if offer_rate > 0 else 0

    # Intervalo de confianca binomial (Wilson) para as duas taxas
    offer_ci_low, offer_ci_high = None, None
    organic_ci_low, organic_ci_high = None, None
    try:
        from statsmodels.stats.proportion import proportion_confint
        if offer_evaluated > 0:
            offer_ci_low, offer_ci_high = proportion_confint(
                offer_converted, offer_evaluated, alpha=0.05, method="wilson"
            )
        if potential_pairs > 0:
            organic_ci_low, organic_ci_high = proportion_confint(
                organic_purchases, potential_pairs, alpha=0.05, method="wilson"
            )
    except ImportError:
        LOG.warning("statsmodels nao disponivel; CI nao calculado para incremental_lift.")

    result = {
        "offer_conversion_rate": round(offer_rate * 100, 2),
        "organic_purchase_rate": round(organic_rate * 100, 4),
        "lift": round(lift, 2),
        "offer_converted": offer_converted,
        "offer_evaluated": offer_evaluated,
        "organic_purchases": organic_purchases,
        "organic_potential_pairs": potential_pairs,
        "offer_ci_95": (
            round(offer_ci_low * 100, 2) if offer_ci_low is not None else None,
            round(offer_ci_high * 100, 2) if offer_ci_high is not None else None,
        ),
        "organic_ci_95": (
            round(organic_ci_low * 100, 4) if organic_ci_low is not None else None,
            round(organic_ci_high * 100, 4) if organic_ci_high is not None else None,
        ),
    }
    LOG.info(f"Incremental Lift (cohort-matched): {lift:.2f}x "
             f"(offer={offer_rate*100:.2f}% vs organic={organic_rate*100:.4f}%)")
    return result


def model_comparison_by_conversion(pg) -> List[Dict]:
    """
    Comparativo Modelo A vs Modelo B por conversao real.

    Retorna metricas de conversao separadas por estrategia.

    NOTA: Como ambos os modelos operam sobre a mesma populacao de clientes,
    esta comparacao e observacional e NAO estabelece causalidade.
    Para comparacao causal, implementar A/B testing com experiment_assignments.
    """
    with pg.cursor() as cur:
        cur.execute("""
            SELECT
                o.strategy,
                COUNT(DISTINCT o.offer_id)::int AS total_offers,
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted IS NOT NULL)::int AS evaluated,
                COUNT(DISTINCT oo.offer_id) FILTER (WHERE oo.converted = TRUE)::int AS converted,
                ROUND(AVG(o.score) * 100, 1)::float AS avg_score_pct,
                COALESCE(SUM(oo.total_value) FILTER (WHERE oo.converted = TRUE), 0)::float AS total_value
            FROM reco.offers o
            LEFT JOIN reco.offer_outcomes oo ON oo.offer_id = o.offer_id
            GROUP BY o.strategy
            ORDER BY o.strategy
        """)
        rows = cur.fetchall()

    results = []
    for row in rows:
        strategy, total, evaluated, converted, avg_score, total_value = row
        rate = round(converted * 100.0 / evaluated, 2) if evaluated > 0 else 0
        results.append({
            "strategy": strategy,
            "total_offers": total,
            "evaluated": evaluated,
            "converted": converted,
            "conversion_rate": rate,
            "avg_score_pct": avg_score,
            "total_converted_value": total_value,
        })
        LOG.info(f"  {strategy}: {rate}% conversion ({converted}/{evaluated}), value=R${total_value:,.2f}")

    return results


# ===========================================================================
# SECTION 6 — TESTES ESTATISTICOS (Inferencia para A/B testing)
# ===========================================================================

def compare_conversion_rates(
    n_a: int,
    conv_a: int,
    n_b: int,
    conv_b: int,
    alpha: float = 0.05,
) -> Dict:
    """
    Two-proportion z-test para comparar taxas de conversao entre dois grupos.

    Exemplo de uso: comparar Modelo A vs Modelo B, ou Treatment vs Control
    em um A/B test.

    Usa statsmodels.stats.proportion.proportions_ztest para o teste bicaudal
    e Wilson confidence intervals (mais robustos que Wald para proporcoes
    pequenas ou proximas de 0/1).

    Parametros
    ----------
    n_a    : total de ofertas avaliadas no grupo A
    conv_a : ofertas convertidas no grupo A
    n_b    : total de ofertas avaliadas no grupo B
    conv_b : ofertas convertidas no grupo B
    alpha  : nivel de significancia (padrao: 0.05)

    Retorna
    -------
    dict com:
        rate_a, rate_b        : taxas de conversao
        diff                  : diferenca (rate_a - rate_b)
        z_stat, p_value       : estatistica z e p-value bicaudal
        significant           : True se p_value < alpha
        ci_a, ci_b            : Wilson CI 95% para cada grupo
        ci_diff               : CI aproximado para a diferenca
    """
    from statsmodels.stats.proportion import proportions_ztest, proportion_confint

    rate_a = conv_a / n_a if n_a > 0 else 0.0
    rate_b = conv_b / n_b if n_b > 0 else 0.0

    # Two-proportion z-test (bicaudal)
    count = np.array([conv_a, conv_b])
    nobs = np.array([n_a, n_b])

    if n_a == 0 or n_b == 0:
        LOG.warning("Um dos grupos tem tamanho 0; teste nao pode ser realizado.")
        return {
            "rate_a": rate_a, "rate_b": rate_b, "diff": rate_a - rate_b,
            "z_stat": None, "p_value": None, "significant": None,
            "ci_a": (None, None), "ci_b": (None, None), "ci_diff": (None, None),
        }

    z_stat, p_value = proportions_ztest(count, nobs, alternative="two-sided")

    # Wilson confidence intervals para cada grupo
    ci_a_low, ci_a_high = proportion_confint(conv_a, n_a, alpha=alpha, method="wilson")
    ci_b_low, ci_b_high = proportion_confint(conv_b, n_b, alpha=alpha, method="wilson")

    # CI aproximado para a diferenca (Newcombe-Wilson)
    diff = rate_a - rate_b
    se_diff = np.sqrt(
        (ci_a_high - ci_a_low) ** 2 / 4 + (ci_b_high - ci_b_low) ** 2 / 4
    )
    from scipy.stats import norm
    z_crit = norm.ppf(1 - alpha / 2)
    ci_diff_low = diff - z_crit * se_diff
    ci_diff_high = diff + z_crit * se_diff

    significant = bool(p_value < alpha)

    LOG.info(
        f"Two-proportion z-test: rate_A={rate_a:.4f} vs rate_B={rate_b:.4f} | "
        f"diff={diff:+.4f} | z={z_stat:.3f} | p={p_value:.4f} | "
        f"significativo={'SIM' if significant else 'NAO'} (alpha={alpha})"
    )

    return {
        "rate_a": round(rate_a, 6),
        "rate_b": round(rate_b, 6),
        "diff": round(diff, 6),
        "z_stat": round(float(z_stat), 4),
        "p_value": round(float(p_value), 6),
        "significant": significant,
        "ci_a": (round(ci_a_low, 6), round(ci_a_high, 6)),
        "ci_b": (round(ci_b_low, 6), round(ci_b_high, 6)),
        "ci_diff": (round(ci_diff_low, 6), round(ci_diff_high, 6)),
    }


def bootstrap_ranking_ci(
    reco_per_customer: Dict[int, List],
    relevant_per_customer: Dict[int, List],
    k: int,
    metric_fn,
    n_boot: int = 1000,
    alpha: float = 0.05,
) -> Dict:
    """
    Bootstrap confidence interval para metricas de ranking, reamostrando clientes.

    Metricas de ranking (Precision@K, NDCG@K, MAP@K, HitRate@K) nao tem
    distribuicao analitica conhecida. Bootstrap CI reamostra clientes com
    reposicao e calcula a distribuicao empirica da metrica agregada.

    IMPORTANTE: Reamostra clientes (nao ofertas individuais) para preservar
    a correlacao intra-cliente (multiplas ofertas por cliente sao dependentes).

    Parametros
    ----------
    reco_per_customer     : {customer_id: [product_ids recomendados]}
    relevant_per_customer : {customer_id: [product_ids comprados]}
    k                     : corte para a metrica
    metric_fn             : funcao que calcula a metrica por cliente
                            assinatura: metric_fn(recommended, relevant, k) -> float
    n_boot                : numero de iteracoes bootstrap (padrao: 1000)
    alpha                 : nivel de significancia para o CI (padrao: 0.05)

    Retorna
    -------
    dict com:
        mean       : media da metrica
        ci_lower   : limite inferior do CI
        ci_upper   : limite superior do CI
        std        : desvio padrao bootstrap
        n_customers: numero de clientes usados
    """
    # Clientes em comum entre recomendacoes e ground truth
    customers = np.array([
        c for c in reco_per_customer if c in relevant_per_customer
    ])
    n = len(customers)

    if n == 0:
        LOG.warning("Nenhum cliente em comum para bootstrap CI.")
        return {"mean": 0.0, "ci_lower": 0.0, "ci_upper": 0.0, "std": 0.0, "n_customers": 0}

    rng = np.random.default_rng(seed=42)
    boot_scores = np.zeros(n_boot)

    for b in range(n_boot):
        # Reamostra clientes com reposicao
        sample_customers = rng.choice(customers, size=n, replace=True)
        scores = [
            metric_fn(reco_per_customer[c], relevant_per_customer[c], k)
            for c in sample_customers
        ]
        boot_scores[b] = np.mean(scores)

    # Percentile CI
    ci_lower = float(np.percentile(boot_scores, 100 * alpha / 2))
    ci_upper = float(np.percentile(boot_scores, 100 * (1 - alpha / 2)))
    mean_val = float(np.mean(boot_scores))
    std_val = float(np.std(boot_scores))

    LOG.info(
        f"Bootstrap CI ({1-alpha:.0%}): mean={mean_val:.4f} "
        f"[{ci_lower:.4f}, {ci_upper:.4f}] (n_boot={n_boot}, n_customers={n})"
    )

    return {
        "mean": round(mean_val, 6),
        "ci_lower": round(ci_lower, 6),
        "ci_upper": round(ci_upper, 6),
        "std": round(std_val, 6),
        "n_customers": n,
    }


def required_sample_size(
    baseline_rate: float,
    mde: float,
    alpha: float = 0.05,
    power: float = 0.80,
) -> Dict:
    """
    Power analysis: calcula tamanho de amostra necessario para um A/B test.

    Responde: "Quantos clientes preciso em cada grupo para detectar uma
    diferenca de `mde` pontos percentuais na taxa de conversao, com
    significancia `alpha` e poder `power`?"

    Usa o teste de proporcoes (NormalIndPower) do statsmodels.

    Parametros
    ----------
    baseline_rate : taxa de conversao do grupo controle (ex.: 0.05 = 5%)
    mde           : minima diferenca detectavel em pontos absolutos (ex.: 0.02 = 2pp)
    alpha         : nivel de significancia (padrao: 0.05)
    power         : poder estatistico desejado (padrao: 0.80)

    Retorna
    -------
    dict com:
        sample_size_per_group : clientes necessarios POR GRUPO
        total_sample_size     : total para dois grupos
        effect_size           : tamanho do efeito (Cohen's h)
        baseline_rate         : taxa baseline usada
        mde                   : MDE usada
    """
    from statsmodels.stats.power import NormalIndPower

    # Cohen's h para duas proporcoes
    p1 = baseline_rate
    p2 = baseline_rate + mde
    effect_size = 2 * (np.arcsin(np.sqrt(p2)) - np.arcsin(np.sqrt(p1)))

    analysis = NormalIndPower()
    n_per_group = analysis.solve_power(
        effect_size=effect_size,
        alpha=alpha,
        power=power,
        alternative="two-sided",
    )
    n_per_group = int(np.ceil(n_per_group))

    LOG.info(
        f"Power analysis: baseline={baseline_rate:.2%}, MDE={mde:.2%}, "
        f"alpha={alpha}, power={power} => {n_per_group:,} clientes/grupo "
        f"({2*n_per_group:,} total)"
    )

    return {
        "sample_size_per_group": n_per_group,
        "total_sample_size": 2 * n_per_group,
        "effect_size": round(float(effect_size), 4),
        "baseline_rate": baseline_rate,
        "mde": mde,
        "alpha": alpha,
        "power": power,
    }
