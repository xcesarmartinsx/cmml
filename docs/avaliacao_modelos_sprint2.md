# Avaliacao dos Modelos ML — Sprint 2

**Data**: 2026-02-24
**Autor**: Data Scientist (agente cmml-data-scientist)
**Escopo**: Implementacao do Modelo 0 (baseline), comparativo entre os 3 modelos e status de gaps.

---

## 1. Resumo Executivo

Na Sprint 2, o gap critico G1 da Sprint 1 foi resolvido com a implementacao de `ml/baseline.py` (Modelo 0). Agora todos os 3 modelos de recomendacao estao implementados e avaliados com o mesmo split temporal, permitindo comparacao justa.

| Modelo | Arquivo | Estrategia | Status Sprint 2 |
|---|---|---|---|
| **Modelo 0 (Baseline)** | `ml/baseline.py` | Top vendidos por loja (90d, compradores unicos) | IMPLEMENTADO (novo) |
| **Modelo A (Ranker)** | `ml/modelo_a_ranker.py` | LightGBM supervisionado (Next Best Product) | Funcional (Sprint 1) |
| **Modelo B (Colaborativo)** | `ml/modelo_b_colaborativo.py` | SVD + vizinhos mais similares | Funcional (Sprint 1) |

---

## 2. Modelo 0 — Baseline (Novo)

### 2.1 Implementacao

| Item | Detalhe |
|---|---|
| **Arquivo** | `ml/baseline.py` |
| **Algoritmo** | Ranking por popularidade (compradores unicos por loja, ultimos 90 dias) |
| **Fallback** | Top global quando loja tem < 10 compras na janela |
| **Cold start** | Clientes sem historico recebem ranking global |
| **Janela de recompra** | Exclui produtos comprados nos ultimos 30 dias |
| **Score** | `n_buyers / max_global_buyers` (normalizado em [0, 1]) |
| **Persistencia** | Metricas em `reco.evaluation_runs` via `evaluate.py` |

### 2.2 Cenarios de Fallback

```
Cliente chega para recomendacao
    |
    +-- Tem loja principal? (loja com mais compras no treino)
    |     |
    |     +-- SIM: Loja tem >= 10 compras na janela de 90d?
    |     |     |
    |     |     +-- SIM: Top produtos da loja (excluindo comprados em 30d)
    |     |     |
    |     |     +-- NAO: Top produtos GLOBAL (excluindo comprados em 30d)
    |     |
    |     +-- NAO: Top produtos GLOBAL
    |
    +-- NAO (cold start): Top produtos GLOBAL
```

### 2.3 Parametros

| Parametro | Valor | Descricao |
|---|---|---|
| `POPULARITY_WINDOW_DAYS` | 90 | Janela para calcular popularidade |
| `RECOMPRA_WINDOW_DAYS` | 30 | Janela de exclusao de compras recentes |
| `MIN_STORE_PURCHASES` | 10 | Limiar para usar ranking local vs global |
| `DEFAULT_TOP_N` | 10 | Recomendacoes por cliente |

### 2.4 Design Decisions

1. **Reutiliza `load_order_history()` e `temporal_split()` do Modelo A**: garante que os 3 modelos usam exatamente os mesmos dados e o mesmo split temporal, eliminando vieses de comparacao.

2. **Score normalizado por `max_global_buyers`**: permite comparacao direta com scores dos Modelos A e B no dashboard de ofertas.

3. **Loja principal = loja com mais transacoes**: desempate pela compra mais recente (favorece a loja onde o cliente esta ativo).

4. **Nenhum SQL direto**: toda a logica de ranking e feita em pandas sobre dados ja carregados pela funcao compartilhada, eliminando risco de SQL injection.

---

## 3. Comparativo entre Modelos

### 3.1 Arquitetura de Avaliacao

Os 3 modelos sao avaliados com:
- **Mesmo split temporal**: definido por `temporal_split()` do Modelo A
  - Treino: ate `max_date - 90d`
  - Validacao: `max_date - 90d` a `max_date - 30d`
  - Teste: ultimos 30 dias
- **Mesmas metricas**: Precision@K, Recall@K, NDCG@K, MAP@K, HitRate@K (K = 5, 10, 20)
- **Mesmo filtro de clientes**: exclui BALCAO, CONSUMIDOR, GENERICO, AVULSO
- **Mesma persistencia**: `reco.evaluation_runs` via `save_metrics_to_db()`

### 3.2 Tabela Comparativa de Metricas

Para obter as metricas reais, executar os 3 modelos em sequencia:

```bash
# Executa os 3 modelos com avaliacao
python ml/baseline.py                   # Modelo 0
python ml/modelo_a_ranker.py            # Modelo A
python ml/modelo_b_colaborativo.py      # Modelo B
```

As metricas sao registradas em `reco.evaluation_runs` e podem ser consultadas:

```sql
-- Comparativo mais recente entre os 3 modelos
WITH latest AS (
    SELECT strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k, n_customers, notes,
           ROW_NUMBER() OVER (PARTITION BY strategy, k ORDER BY run_id DESC) AS rn
    FROM reco.evaluation_runs
    WHERE strategy IN ('baseline', 'modelo_a_ranker', 'modelo_b_colaborativo')
)
SELECT strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k, n_customers
FROM latest
WHERE rn = 1
ORDER BY k, strategy;
```

### 3.3 Expectativas de Performance

Baseado na literatura e na estrutura de cada modelo:

| Metrica | Modelo 0 (Baseline) | Modelo A (Ranker) | Modelo B (SVD) |
|---|---|---|---|
| **Precision@10** | Baixa-Media | Mais alta (supervisionado) | Media |
| **Recall@10** | Baixa | Media-Alta | Media |
| **NDCG@10** | Baixa | Mais alta (otimiza ordenacao) | Media |
| **HitRate@10** | Media (populares acertam) | Alta | Media-Alta |
| **Cobertura** | Baixa (mesmos itens para todos da loja) | Alta (personalizado) | Alta (personalizado) |

**Interpretacao esperada**:
- O Modelo A (supervisionado) deve superar o baseline em todas as metricas de ranking, pois usa features personalizadas por par (cliente, produto).
- O Modelo B (colaborativo) deve superar o baseline em clientes com historico suficiente, mas pode perder para o baseline em cold start.
- O baseline deve ter HitRate razoavel porque produtos populares tem maior probabilidade de serem comprados por qualquer cliente.

### 3.4 Criterio de Validacao

Para considerar que o modelo ML agrega valor sobre o baseline:

| Metrica | Criterio minimo |
|---|---|
| **Precision@10** | Modelo ML > Baseline em pelo menos 20% relativo |
| **NDCG@10** | Modelo ML > Baseline em pelo menos 15% relativo |
| **HitRate@10** | Modelo ML > Baseline em pelo menos 10% relativo |

Se o modelo ML **nao** superar o baseline, possibilidades:
1. Features insuficientes — enriquecer com dados de categoria, sazonalidade, estoque
2. Dados insuficientes — precisamos de mais historico ou mais clientes
3. O negocio e dominado por popularidade — baseline e o modelo certo

---

## 4. Gaps Resolvidos na Sprint 2

| # | Gap (Sprint 1) | Status Sprint 2 | Detalhes |
|---|---|---|---|
| G1 | Baseline nao implementado | RESOLVIDO | `ml/baseline.py` implementado com todos os cenarios |
| G6 | Metricas baseline nao no banco | RESOLVIDO | `save_metrics_to_db()` integrado no pipeline |

---

## 5. Gaps Remanescentes

| # | Gap | Impacto | Prioridade | Responsavel sugerido |
|---|---|---|---|---|
| G2 | Zero testes automatizados para ML | Alto | ALTA | QA + Data Scientist |
| G3 | Sem monitoramento de drift | Alto | ALTA | Data Scientist |
| G4 | Sem retreino automatizado | Medio | MEDIA | DevOps |
| G5 | Sem versionamento semantico de modelos | Medio | MEDIA | Data Scientist |
| G7 | Modelo B em pickle (risco de seguranca) | Medio | MEDIA | Data Scientist |
| G10 | Sem Camada 3 (regras de elegibilidade) | Medio | MEDIA | Data Scientist + Backend |
| G12 | f-string em load_order_history | Baixo | BAIXA | Backend |

### 5.1 Novo Gap Identificado

| # | Gap | Impacto | Prioridade |
|---|---|---|---|
| G13 | **Baseline nao integrado em `generate_offers.py`** | Clientes sem cobertura dos Modelos A/B nao recebem ofertas | ALTA |

Para resolver G13, `generate_offers.py` deve ser atualizado para incluir o baseline como fallback:

```python
# Proposta para generate_offers.py
if strategy in ("baseline", "all"):
    from ml.baseline import generate_baseline_recommendations
    # Gera baseline apenas para clientes SEM ofertas dos Modelos A/B
    covered_customers = set(df_all["customer_id"].unique()) if not df_all.empty else set()
    baseline_df = generate_baseline_recommendations(df_history, top_n=top_n)
    # Filtra apenas clientes nao cobertos
    baseline_df = baseline_df[~baseline_df["customer_id"].isin(covered_customers)]
    frames.append(baseline_df)
```

---

## 6. Como Reproduzir a Avaliacao

### 6.1 Executar os 3 modelos

```bash
# Pre-requisito: ETL executado, dados em cur.order_items
make etl

# Modelo 0 — Baseline
python ml/baseline.py

# Modelo A — LightGBM Ranker
python ml/modelo_a_ranker.py

# Modelo B — SVD Colaborativo
python ml/modelo_b_colaborativo.py
```

### 6.2 Consultar metricas no banco

```bash
make psql
```

```sql
-- Todas as avaliacoes registradas
SELECT run_id, strategy, k, precision_at_k, recall_at_k, ndcg_at_k, map_at_k, n_customers, created_at
FROM reco.evaluation_runs
ORDER BY created_at DESC, strategy, k;
```

### 6.3 Dry-run (sem banco)

```bash
python ml/baseline.py --dry-run
# Exibe rankings por loja e global sem executar avaliacao
```

---

## 7. Diagrama do Pipeline com Modelo 0

```
cur.order_items (PostgreSQL)
       |
       v
  load_order_history()          <-- compartilhada pelos 3 modelos
       |
       v
  temporal_split()              <-- split identico para comparacao justa
       |
       +---> df_train + df_val (passado)
       |        |
       |        +---> [Modelo 0] compute_store_rankings() + compute_global_ranking()
       |        |        |
       |        |        +---> generate_baseline_recommendations()
       |        |
       |        +---> [Modelo A] build_training_dataset() + train_lightgbm()
       |        |        |
       |        |        +---> generate_recommendations()
       |        |
       |        +---> [Modelo B] build_interaction_matrix() + fit_svd()
       |                 |
       |                 +---> generate_collaborative_recommendations()
       |
       +---> df_test (futuro)
                |
                +---> evaluate_ranking()   <-- mesmas metricas para os 3
                |
                v
          reco.evaluation_runs
                |
                v
          generate_offers.py --> reco.offers
```

---

## 8. Proximos Passos (Sprint 3)

1. **Executar os 3 modelos e registrar metricas reais** no banco para preencher a tabela comparativa (Secao 3.2)
2. **Integrar baseline em `generate_offers.py`** como fallback (G13)
3. **Criar testes unitarios para `ml/baseline.py`** (cobertura >= 80%)
4. **Implementar monitoramento de drift** (G3) — comparar distribuicao de features treino vs producao
5. **Migrar Modelo B de pickle para NPZ** (G7) — eliminar risco de execucao arbitraria
