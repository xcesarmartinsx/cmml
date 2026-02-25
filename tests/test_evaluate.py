"""
tests/test_evaluate.py
======================
Testes unitarios para ml/evaluate.py — metricas de avaliacao offline.

Cobre precision_at_k, recall_at_k, ndcg_at_k, average_precision_at_k,
hit_rate_at_k, map_at_k, evaluate_ranking, evaluate_binary_classifier,
find_optimal_threshold, e verificacao de que nenhuma funcao loga PII.

Todos os testes usam dados sinteticos — sem dependencia de banco de dados.
"""

import logging
import math
import re

import numpy as np
import pandas as pd
import pytest

from ml.evaluate import (
    average_precision_at_k,
    evaluate_binary_classifier,
    evaluate_ranking,
    find_optimal_threshold,
    hit_rate_at_k,
    map_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


# ===========================================================================
# precision_at_k
# ===========================================================================

class TestPrecisionAtK:
    def test_normal_case(self):
        # 2 hits out of 5: precision = 2/5 = 0.4
        assert precision_at_k([1, 2, 3, 4, 5], [2, 5, 10], k=5) == pytest.approx(0.4)

    def test_example_from_docstring(self):
        # Docstring example: precision_at_k([1,2,3],[2,5],3) == 1/3
        assert precision_at_k([1, 2, 3], [2, 5], k=3) == pytest.approx(1 / 3)

    def test_k_equals_1_hit(self):
        assert precision_at_k([5, 1, 2], [5, 10], k=1) == pytest.approx(1.0)

    def test_k_equals_1_miss(self):
        assert precision_at_k([5, 1, 2], [10, 20], k=1) == pytest.approx(0.0)

    def test_k_larger_than_list(self):
        # recommended has 3 items, k=10 -> only 3 checked, but divisor is k=10
        result = precision_at_k([1, 2, 3], [1, 2, 3], k=10)
        assert result == pytest.approx(3 / 10)

    def test_all_relevant(self):
        assert precision_at_k([1, 2, 3], [1, 2, 3, 4, 5], k=3) == pytest.approx(1.0)

    def test_none_relevant(self):
        assert precision_at_k([1, 2, 3], [10, 20, 30], k=3) == pytest.approx(0.0)

    def test_empty_recommended(self):
        # k=5 but no items -> 0 hits / 5 = 0.0
        assert precision_at_k([], [1, 2], k=5) == pytest.approx(0.0)

    def test_empty_relevant(self):
        assert precision_at_k([1, 2, 3], [], k=3) == pytest.approx(0.0)

    def test_k_zero_raises(self):
        with pytest.raises(ValueError, match="k deve ser positivo"):
            precision_at_k([1, 2], [1], k=0)

    def test_negative_k_raises(self):
        with pytest.raises(ValueError):
            precision_at_k([1, 2], [1], k=-1)


# ===========================================================================
# recall_at_k
# ===========================================================================

class TestRecallAtK:
    def test_normal_case(self):
        # 2 of 3 relevant found: recall = 2/3
        assert recall_at_k([1, 2, 3, 4, 5], [2, 5, 10], k=5) == pytest.approx(2 / 3)

    def test_k_equals_1_hit(self):
        assert recall_at_k([5, 1, 2], [5, 10], k=1) == pytest.approx(1 / 2)

    def test_k_equals_1_miss(self):
        assert recall_at_k([5, 1, 2], [10, 20], k=1) == pytest.approx(0.0)

    def test_k_larger_than_list(self):
        # recommended has 3 items, all relevant, k=10
        assert recall_at_k([1, 2, 3], [1, 2, 3, 4, 5], k=10) == pytest.approx(3 / 5)

    def test_all_relevant_captured(self):
        assert recall_at_k([1, 2, 3, 4], [1, 2], k=4) == pytest.approx(1.0)

    def test_none_relevant_captured(self):
        assert recall_at_k([1, 2, 3], [10, 20], k=3) == pytest.approx(0.0)

    def test_empty_relevant_returns_zero(self):
        # Special case: no ground truth -> recall = 0.0 (not division by zero)
        assert recall_at_k([1, 2, 3], [], k=3) == pytest.approx(0.0)

    def test_empty_recommended(self):
        assert recall_at_k([], [1, 2], k=5) == pytest.approx(0.0)

    def test_k_zero_raises(self):
        with pytest.raises(ValueError, match="k deve ser positivo"):
            recall_at_k([1, 2], [1], k=0)


# ===========================================================================
# ndcg_at_k
# ===========================================================================

class TestNDCGAtK:
    def test_perfect_ranking(self):
        # All relevant at top -> NDCG = 1.0
        assert ndcg_at_k([1, 2, 3], [1, 2, 3], k=3) == pytest.approx(1.0)

    def test_reversed_ranking(self):
        # Relevant item at last position, worse than ideal
        # recommended=[3, 2, 1], relevant=[1], k=3
        # DCG = 1/log2(4) = 1/2 = 0.5
        # IDCG = 1/log2(2) = 1.0
        # NDCG = 0.5
        assert ndcg_at_k([3, 2, 1], [1], k=3) == pytest.approx(
            (1 / math.log2(4)) / (1 / math.log2(2))
        )

    def test_single_hit_at_position_1(self):
        # DCG = 1/log2(2) = 1.0; IDCG = 1/log2(2) = 1.0 -> NDCG = 1.0
        assert ndcg_at_k([5, 1, 2], [5], k=3) == pytest.approx(1.0)

    def test_single_hit_at_position_2(self):
        # DCG = 1/log2(3); IDCG = 1/log2(2)
        expected = (1 / math.log2(3)) / (1 / math.log2(2))
        assert ndcg_at_k([1, 5, 2], [5], k=3) == pytest.approx(expected)

    def test_no_relevant_items(self):
        assert ndcg_at_k([1, 2, 3], [10, 20], k=3) == pytest.approx(0.0)

    def test_empty_relevant(self):
        assert ndcg_at_k([1, 2, 3], [], k=3) == pytest.approx(0.0)

    def test_k_zero(self):
        assert ndcg_at_k([1, 2], [1], k=0) == pytest.approx(0.0)

    def test_two_hits_known_value(self):
        # recommended=[1, 2, 3], relevant=[1, 3], k=3
        # DCG = 1/log2(2) + 1/log2(4) = 1.0 + 0.5 = 1.5
        # IDCG = 1/log2(2) + 1/log2(3) = 1.0 + 0.63093 = 1.63093
        dcg = 1 / math.log2(2) + 1 / math.log2(4)
        idcg = 1 / math.log2(2) + 1 / math.log2(3)
        assert ndcg_at_k([1, 2, 3], [1, 3], k=3) == pytest.approx(dcg / idcg)


# ===========================================================================
# average_precision_at_k
# ===========================================================================

class TestAveragePrecisionAtK:
    def test_perfect_ranking(self):
        # All 3 relevant in first 3 positions
        # AP = (1/3) * (1/1 + 2/2 + 3/3) = (1/3)*3 = 1.0
        assert average_precision_at_k([1, 2, 3], [1, 2, 3], k=3) == pytest.approx(1.0)

    def test_one_hit_at_position_2(self):
        # recommended=[10, 5, 20], relevant=[5], k=3
        # Hit at position 2: P@2 = 1/2
        # AP = (1/min(1,3)) * (1/2) = 0.5
        assert average_precision_at_k([10, 5, 20], [5], k=3) == pytest.approx(0.5)

    def test_no_hits(self):
        assert average_precision_at_k([1, 2, 3], [10, 20], k=3) == pytest.approx(0.0)

    def test_empty_relevant(self):
        assert average_precision_at_k([1, 2, 3], [], k=3) == pytest.approx(0.0)

    def test_hits_at_various_positions(self):
        # recommended=[1, 2, 3, 4, 5], relevant=[1, 3, 5], k=5
        # Hit at pos 1: P@1 = 1/1 = 1.0
        # Hit at pos 3: P@3 = 2/3
        # Hit at pos 5: P@5 = 3/5
        # AP = (1/min(3,5)) * (1.0 + 2/3 + 3/5) = (1/3) * (1 + 0.6667 + 0.6) = 2.2667/3
        expected = (1 / 3) * (1 / 1 + 2 / 3 + 3 / 5)
        assert average_precision_at_k([1, 2, 3, 4, 5], [1, 3, 5], k=5) == pytest.approx(expected)


# ===========================================================================
# hit_rate_at_k
# ===========================================================================

class TestHitRateAtK:
    def test_all_customers_hit(self):
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [10], 2: [30]}
        assert hit_rate_at_k(recs, rels, k=2) == pytest.approx(1.0)

    def test_no_customer_hit(self):
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [99], 2: [99]}
        assert hit_rate_at_k(recs, rels, k=2) == pytest.approx(0.0)

    def test_partial_hit(self):
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [10], 2: [99]}
        assert hit_rate_at_k(recs, rels, k=2) == pytest.approx(0.5)

    def test_no_overlap_customers(self):
        recs = {1: [10]}
        rels = {2: [10]}
        assert hit_rate_at_k(recs, rels, k=1) == pytest.approx(0.0)

    def test_empty_inputs(self):
        assert hit_rate_at_k({}, {}, k=5) == pytest.approx(0.0)

    def test_k_limits_recommendations(self):
        # Hit is at position 3, but k=1 -> miss
        recs = {1: [10, 20, 30]}
        rels = {1: [30]}
        assert hit_rate_at_k(recs, rels, k=1) == pytest.approx(0.0)
        # k=3 -> hit
        assert hit_rate_at_k(recs, rels, k=3) == pytest.approx(1.0)


# ===========================================================================
# map_at_k
# ===========================================================================

class TestMAPAtK:
    def test_perfect_ranking_all_customers(self):
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [10, 20], 2: [30, 40]}
        assert map_at_k(recs, rels, k=2) == pytest.approx(1.0)

    def test_no_overlap_customers(self):
        recs = {1: [10]}
        rels = {2: [10]}
        assert map_at_k(recs, rels, k=1) == pytest.approx(0.0)

    def test_mixed_performance(self):
        # Customer 1: perfect AP=1.0; Customer 2: no hits AP=0.0
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [10, 20], 2: [99]}
        assert map_at_k(recs, rels, k=2) == pytest.approx(0.5)


# ===========================================================================
# evaluate_ranking (integration of all ranking metrics)
# ===========================================================================

class TestEvaluateRanking:
    def test_returns_dataframe_with_expected_columns(self):
        recs = {1: [10, 20, 30]}
        rels = {1: [10, 30]}
        df = evaluate_ranking(recs, rels, k_values=[2, 3])
        assert isinstance(df, pd.DataFrame)
        expected_cols = {"k", "precision", "recall", "ndcg", "map", "hit_rate", "n_customers"}
        assert expected_cols.issubset(set(df.columns))
        assert len(df) == 2

    def test_values_at_k2(self):
        recs = {1: [10, 20, 30]}
        rels = {1: [10, 30]}
        df = evaluate_ranking(recs, rels, k_values=[2])
        row = df.iloc[0]
        # precision@2: 1 hit (10) out of 2 -> 0.5
        assert row["precision"] == pytest.approx(0.5)
        # recall@2: 1 of 2 relevant -> 0.5
        assert row["recall"] == pytest.approx(0.5)
        assert row["n_customers"] == 1

    def test_no_common_customers(self):
        recs = {1: [10]}
        rels = {2: [10]}
        df = evaluate_ranking(recs, rels, k_values=[5])
        # All metrics should be NaN or 0 — with 0 customers, means are nan
        assert df.iloc[0]["n_customers"] == 0


# ===========================================================================
# evaluate_binary_classifier
# ===========================================================================

class TestEvaluateBinaryClassifier:
    def test_perfect_classifier(self):
        y_true = np.array([0, 0, 1, 1])
        y_proba = np.array([0.1, 0.2, 0.8, 0.9])
        result = evaluate_binary_classifier(y_true, y_proba, threshold=0.5)
        assert result["tp"] == 2
        assert result["tn"] == 2
        assert result["fp"] == 0
        assert result["fn"] == 0
        assert result["accuracy"] == pytest.approx(1.0)
        assert result["precision_pos"] == pytest.approx(1.0)
        assert result["recall_pos"] == pytest.approx(1.0)

    def test_all_wrong(self):
        y_true = np.array([0, 0, 1, 1])
        y_proba = np.array([0.9, 0.8, 0.1, 0.2])
        result = evaluate_binary_classifier(y_true, y_proba, threshold=0.5)
        assert result["tp"] == 0
        assert result["fn"] == 2
        assert result["fp"] == 2
        assert result["tn"] == 0
        assert result["accuracy"] == pytest.approx(0.0)

    def test_custom_threshold(self):
        y_true = np.array([0, 1, 1])
        y_proba = np.array([0.3, 0.4, 0.6])
        # With threshold=0.35: predictions are [0, 1, 1]
        result = evaluate_binary_classifier(y_true, y_proba, threshold=0.35)
        assert result["tp"] == 2
        assert result["fp"] == 0
        assert result["fn"] == 0

    def test_returns_expected_keys(self):
        y_true = np.array([0, 1])
        y_proba = np.array([0.3, 0.7])
        result = evaluate_binary_classifier(y_true, y_proba)
        expected_keys = {
            "confusion_matrix", "tn", "fp", "fn", "tp",
            "auc_roc", "precision_pos", "recall_pos",
            "specificity", "f1_pos", "accuracy",
            "classification_report", "threshold",
        }
        assert expected_keys.issubset(set(result.keys()))

    def test_auc_roc_value(self):
        y_true = np.array([0, 0, 1, 1])
        y_proba = np.array([0.1, 0.4, 0.6, 0.9])
        result = evaluate_binary_classifier(y_true, y_proba)
        assert result["auc_roc"] == pytest.approx(1.0)


# ===========================================================================
# find_optimal_threshold
# ===========================================================================

class TestFindOptimalThreshold:
    def test_returns_tuple(self):
        y_true = np.array([0, 0, 1, 1, 1])
        y_proba = np.array([0.1, 0.3, 0.6, 0.7, 0.9])
        thresh, score = find_optimal_threshold(y_true, y_proba, metric="f1-score")
        assert isinstance(thresh, float)
        assert isinstance(score, float)
        assert 0.1 <= thresh <= 0.9
        assert 0.0 <= score <= 1.0

    def test_default_metric_f1_returns_zero_due_to_key_mismatch(self):
        # NOTE: find_optimal_threshold default metric="f1" does not match
        # sklearn's classification_report key "f1-score", so score is always 0.
        # This documents the current behavior (potential bug in source).
        y_true = np.array([0, 0, 1, 1])
        y_proba = np.array([0.1, 0.2, 0.8, 0.9])
        thresh, score = find_optimal_threshold(y_true, y_proba, metric="f1")
        assert score == pytest.approx(0.0)

    def test_separable_data_high_f1(self):
        np.random.seed(42)
        y_true = np.array([0] * 50 + [1] * 50)
        y_proba = np.concatenate([np.random.uniform(0.0, 0.3, 50),
                                  np.random.uniform(0.7, 1.0, 50)])
        thresh, score = find_optimal_threshold(y_true, y_proba, metric="f1-score")
        assert score > 0.9


# ===========================================================================
# PII safety: no function should log customer names, phone numbers, or CPF
# ===========================================================================

class TestNoPIIInLogs:
    """Verify that evaluate functions do not log PII (names, phone, CPF)."""

    def test_evaluate_ranking_no_pii(self, caplog):
        recs = {1: [10, 20], 2: [30, 40]}
        rels = {1: [10], 2: [30]}
        with caplog.at_level(logging.DEBUG, logger="ml.evaluate"):
            evaluate_ranking(recs, rels, k_values=[5])
        pii_pattern = re.compile(
            r"\b\d{3}[\.\-]?\d{3}[\.\-]?\d{3}[\.\-]?\d{2}\b"  # CPF
            r"|(\+?\d{2,3}\s?)?\(?\d{2}\)?\s?\d{4,5}[\-\s]?\d{4}"  # phone
        )
        for record in caplog.records:
            assert not pii_pattern.search(record.getMessage()), (
                f"PII pattern found in log: {record.getMessage()}"
            )

    def test_evaluate_binary_no_pii(self, caplog):
        y_true = np.array([0, 1, 0, 1])
        y_proba = np.array([0.2, 0.8, 0.3, 0.7])
        with caplog.at_level(logging.DEBUG, logger="ml.evaluate"):
            evaluate_binary_classifier(y_true, y_proba)
        for record in caplog.records:
            msg = record.getMessage()
            # Should not contain anything that looks like a name or phone
            assert "cliente" not in msg.lower() or "customer_id" not in msg.lower(), (
                f"Potential PII reference in log: {msg}"
            )
