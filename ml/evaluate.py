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
    metric: str = "f1",
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
    metric       : 'f1' | 'recall' | 'precision' — métrica a maximizar

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
) -> None:
    """
    Persiste as métricas de avaliação em reco.evaluation_runs.

    Esta tabela é a fonte de verdade para comparar versões de modelos ao longo
    do tempo. Permite identificar regressões ao re-treinar.

    Parâmetros
    ----------
    pg         : conexão psycopg2
    df_metrics : DataFrame de evaluate_ranking()
    strategy   : nome da estratégia ('modelo_a_ranker', 'modelo_b_colaborativo', ...)
    notes      : observações livres (ex.: versão, hiperparâmetros usados)
    """
    with pg.cursor() as cur:
        for _, row in df_metrics.iterrows():
            cur.execute(
                """
                INSERT INTO reco.evaluation_runs
                    (strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k, n_customers, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                ),
            )
    pg.commit()
    LOG.info(f"Métricas salvas em reco.evaluation_runs para strategy='{strategy}'")
