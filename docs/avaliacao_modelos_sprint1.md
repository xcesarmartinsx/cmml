# Avaliacao dos Modelos ML — Sprint 1

**Data**: 2026-02-23
**Autor**: Data Scientist (agente cmml-data-scientist)
**Escopo**: Inventario completo dos modelos existentes, analise de riscos e gaps para producao.

---

## 1. Inventario dos Modelos Existentes

### 1.1 Modelo A — Ranker Supervisionado (LightGBM)

| Item | Detalhe |
|---|---|
| **Arquivo** | `ml/modelo_a_ranker.py` |
| **Artefato salvo** | `app/ml/models/classification/modelo_a_ranker_v1.lgb` (261 KB) |
| **Algoritmo** | LightGBM (gradient boosting), classificacao binaria |
| **Objetivo** | Prever se um cliente comprara um produto nos proximos 30 dias |
| **Label** | Binario: 1 = comprou no periodo de teste, 0 = nao comprou |
| **Data estimada de treinamento** | 2026-02-23 (baseado no timestamp do arquivo) |
| **Formato de persistencia** | Nativo LightGBM (.lgb) — adequado, sem dados de treino embutidos |

**Features utilizadas (14 features):**

| Grupo | Feature | Descricao |
|---|---|---|
| Cliente | `purchase_count_90d` | Total de compras nos ultimos 90 dias |
| Cliente | `avg_ticket_90d` | Ticket medio nos ultimos 90 dias |
| Cliente | `days_since_last_purchase` | Recencia (dias desde ultima compra) |
| Cliente | `customer_repeat_rate` | Fracao de produtos comprados mais de uma vez |
| Produto | `product_popularity_30d` | Compradores unicos nos ultimos 30 dias |
| Produto | `product_volume_90d` | Volume vendido nos ultimos 90 dias |
| Produto | `product_revenue_90d` | Receita nos ultimos 90 dias |
| Produto | `product_total_buyers` | Total de compradores (historico completo) |
| Produto | `product_repeat_rate` | Fracao de compradores que recomprou |
| Produto | `product_avg_repurchase_days` | Intervalo medio de recompra |
| Interacao | `bought_before` | Cliente ja comprou este produto? (contagem) |
| Interacao | `days_since_last_product_purchase` | Dias desde a ultima compra deste produto |
| Interacao | `avg_qty_per_purchase` | Quantidade media por compra do par |
| Interacao | `avg_value_per_purchase` | Valor medio por compra do par |
| Derivada | `lifecycle_ratio` | Fracao do ciclo de recompra ja decorrida |

**Hiperparametros:**

| Parametro | Valor |
|---|---|
| `learning_rate` | 0.05 |
| `num_leaves` | 63 |
| `min_child_samples` | 50 |
| `feature_fraction` | 0.8 |
| `bagging_fraction` | 0.8 |
| `lambda_l1` / `lambda_l2` | 0.1 / 0.1 |
| `scale_pos_weight` | Dinamico (sqrt do ratio de desbalanceamento) |
| `max_rounds` | 1000 (com early stopping em 50 rounds) |

---

### 1.2 Modelo B — Filtragem Colaborativa (SVD)

| Item | Detalhe |
|---|---|
| **Arquivo** | `ml/modelo_b_colaborativo.py` |
| **Artefato salvo** | `app/ml/models/classification/modelo_b_colaborativo_v1.pkl` (7.4 MB) |
| **Algoritmo** | Truncated SVD (scipy.sparse.linalg.svds) |
| **Objetivo** | Recomendar produtos comprados por clientes similares |
| **Score** | Proporcao de vizinhos que compraram o produto |
| **Data estimada de treinamento** | 2026-02-23 (baseado no timestamp do arquivo) |
| **Formato de persistencia** | Pickle (.pkl) — contem embeddings, indices e metadados |

**Componentes do modelo:**

| Componente | Descricao |
|---|---|
| Matriz de interacao | Usuario x Produto, ponderada por log1p(contagem de compras) |
| Embeddings de clientes | U * sqrt(Sigma), shape (n_users x n_factors) |
| Embeddings de produtos | Vt.T * sqrt(Sigma), shape (n_products x n_factors) |
| Similaridade | Cosseno entre embeddings normalizados |
| Vizinhos | Top-K clientes mais similares (padrao K=50) |
| Recomendacao | Produtos populares entre vizinhos, nao comprados recentemente |

**Parametros:**

| Parametro | Valor padrao |
|---|---|
| `n_factors` | 64 |
| `k_neighbors` | 50 |
| `top_n` | 10 |
| `history_days` | 1825 (~5 anos) |

---

### 1.3 Modelo 0 — Baseline (NAO EXISTE)

| Item | Status |
|---|---|
| **Arquivo esperado** | `ml/baseline.py` |
| **Status** | **NAO IMPLEMENTADO** |
| **Documentacao** | Descrito em `docs/07_ml_recomendacao.md` como "top vendidos por loja" |

O que precisa ser implementado:
- Top produtos vendidos por loja nos ultimos 90 dias
- Exclusao de produtos comprados recentemente pelo cliente (janela 30 dias)
- Fallback para top global quando loja nao tem dados suficientes
- Cold start: clientes novos recebem top da loja mais proxima ou top global

---

## 2. Analise de Data Leakage

### 2.1 Split Temporal — CORRETO

Ambos os modelos usam split temporal rigoroso:

- **Modelo A**: split em 3 partes (treino / validacao / teste) usando datas de venda.
  - Treino: ate `max_date - 90d`
  - Validacao: `max_date - 90d` a `max_date - 30d`
  - Teste: `max_date - 30d` a `max_date`
  - Early stopping usa ultimos 30 dias do treino (nao o teste) — **correto e bem implementado**.

- **Modelo B**: split em 2 partes (treino / teste).
  - Treino: ate `max_date - 30d`
  - Teste: ultimos 30 dias

**Veredicto: NENHUM split aleatorio encontrado. Risco de leakage temporal BAIXO.**

### 2.2 Features — Analise de Leakage

| Feature | Risco | Justificativa |
|---|---|---|
| `purchase_count_90d` | BAIXO | Calculada sobre `df_train` apenas |
| `avg_ticket_90d` | BAIXO | Calculada sobre `df_train` apenas |
| `days_since_last_purchase` | BAIXO | Referencia `reference_date` do treino |
| `product_popularity_30d` | BAIXO | Janela de 30d dentro do treino |
| `bought_before` | BAIXO | Historico de treino apenas |
| `days_since_last_product_purchase` | BAIXO | Ultima compra no treino |
| `lifecycle_ratio` | BAIXO | Derivada de features ja seguras |
| `customer_repeat_rate` | BAIXO | Calculada sobre treino |
| `product_repeat_rate` | BAIXO | Calculada sobre treino |
| `product_avg_repurchase_days` | BAIXO | Calculada sobre treino |

**Veredicto: Features construidas corretamente sobre dados passados (df_train). Nao ha leakage de features.**

### 2.3 Amostragem Negativa (Modelo A)

A funcao `build_training_dataset` exclui dos negativos tanto os produtos comprados no treino quanto no periodo de label (positivos). Isso e correto — nao ha contaminacao entre positivos e negativos.

**Ponto de atencao**: os negativos sao amostrados aleatoriamente (rng seed=42), o que pode gerar viés de popularidade (produtos raros tem a mesma chance de aparecer como negativo que populares). Isso e aceitavel para MVP, mas em versao futura considerar negative sampling ponderado pela popularidade.

---

## 3. Status do Pipeline de Avaliacao

### 3.1 `ml/evaluate.py` — COMPLETO E FUNCIONAL

O modulo implementa:
- Metricas de ranking: Precision@K, Recall@K, NDCG@K, MAP@K, HitRate@K
- Avaliacao binaria: AUC-ROC, Matriz de Confusao, Classification Report
- Busca de threshold otimo por grid search (F1, recall ou precision)
- Relatorio formatado no log
- Persistencia em `reco.evaluation_runs` (SQL parametrizado — correto)

**Qualidade**: implementacao solida, formulas corretas, bem documentada. As metricas sao consistentes com a literatura (Manning 2008, Koren 2009).

### 3.2 `ml/generate_offers.py` — COMPLETO E FUNCIONAL

Pipeline de geracao de ofertas:
- Carrega ambos os modelos treinados
- Gera recomendacoes para toda a base de clientes
- Persiste em `reco.offers` com batch_id UUID e rastreabilidade
- Suporta dry-run
- Idempotente via `ON CONFLICT DO NOTHING`

---

## 4. Gaps Identificados (o que falta para producao)

### 4.1 Gaps Criticos (Bloqueadores)

| # | Gap | Impacto | Prioridade |
|---|---|---|---|
| G1 | **Baseline (Modelo 0) nao implementado** | Sem fallback para cold start e sem referencia para medir ganho dos modelos ML | ALTA |
| G2 | **Zero testes automatizados** | Regressoes silenciosas em qualquer alteracao de feature ou pipeline | ALTA |
| G3 | **Sem monitoramento de drift** | Modelo pode degradar silenciosamente apos mudancas no comportamento de compra | ALTA |
| G4 | **Sem retreino automatizado** | Modelo fica obsoleto, requer intervencao manual para retreinar | MEDIA |

### 4.2 Gaps Importantes (Nao-bloqueadores)

| # | Gap | Impacto | Prioridade |
|---|---|---|---|
| G5 | **Sem versao semantica dos modelos** | Dificil rastrear qual versao gerou quais ofertas. Os nomes `_v1.lgb` / `_v1.pkl` sao manuais | MEDIA |
| G6 | **Metricas baseline nao registradas no banco** | Nao ha registro historico das metricas de avaliacao do Modelo 0 para comparacao | MEDIA |
| G7 | **Modelo B armazena dados via pickle** | Risco de seguranca (execucao arbitraria via pickle) e fragilidade entre versoes Python | MEDIA |
| G8 | **category_affinity desabilitada** | Feature `category_affinity` removida porque `GRUPOID` e NULL no ERP. Se o dado for corrigido, reativar | BAIXA |
| G9 | **Modelo B: similaridade densa em memoria** | `cosine_similarity` calcula matriz N x N completa. Para >100k clientes, precisa de Faiss/pgvector | BAIXA |
| G10 | **Sem Camada 3 (regras de elegibilidade)** | Diversificacao por categoria, mix seguro/novo e filtro de estoque nao implementados | MEDIA |
| G11 | **Sem feedback loop** | Modelos nao aprendem com o resultado real das ofertas enviadas | BAIXA (Fase 2) |
| G12 | **Query SQL com f-string** | `load_order_history` em ambos os modelos usa f-string para `cutoff_date` (nao e input externo, mas viola boas praticas) | BAIXA |

---

## 5. Analise de Seguranca e LGPD

| Verificacao | Status | Detalhe |
|---|---|---|
| Modelos nao contem dados de treino | OK | Modelo A salva apenas arvores (.lgb). Modelo B salva embeddings numericos, nao dados brutos |
| Logs sem PII | OK | Logs mostram apenas contagens e IDs numericos, nunca nomes, telefones ou CPF |
| SQL parametrizado em evaluate.py | OK | `save_metrics_to_db` usa `%s` placeholders |
| SQL com f-string em load_order_history | RISCO BAIXO | `cutoff_date` e gerado internamente (nao vem de input externo), mas melhor parametrizar |
| Filtro de clientes genericos | OK | Exclui BALCAO, CONSUMIDOR, GENERICO, AVULSO — evita PII de clientes reais em logs |

---

## 6. Recomendacoes Priorizadas

### Prioridade 1 — Fazer AGORA

1. **Implementar `ml/baseline.py`** (Modelo 0)
   - Top vendidos por loja (90 dias), excluindo comprados recentemente
   - Registrar metricas no banco via `evaluate_ranking` + `save_metrics_to_db`
   - Essencial como referencia: "o ML e melhor que simplesmente recomendar os mais vendidos?"

2. **Criar testes unitarios para `ml/evaluate.py`**
   - Testar cada metrica com exemplos sinteticos conhecidos
   - Garantir que `precision_at_k([1,2,3], [2,5], 3) == 1/3`
   - Cobertura minima: todas as funcoes de metricas

3. **Criar testes de integracao para o pipeline ML**
   - Testar `build_training_dataset` com dados sinteticos
   - Verificar que o split temporal nao contamina treino com dados futuros
   - Testar que features sao calculadas apenas sobre `df_train`

### Prioridade 2 — Proximo Sprint

4. **Implementar monitoramento de drift**
   - Comparar distribuicao de features do treino vs producao (PSI / KS-test)
   - Comparar metricas de avaliacao ao longo do tempo (alertar se AUC cair >5%)
   - Adicionar tabela `reco.model_health` com metricas periodicas

5. **Parametrizar queries SQL**
   - Substituir f-strings em `load_order_history` por queries parametrizadas
   - Mesmo que o input seja interno, e boa pratica defensiva

6. **Registrar modelo com metadados de versao**
   - Criar `reco.model_registry` com: versao, data de treino, metricas, hiperparametros, hash do artefato
   - Permitir rollback para versao anterior se nova versao degradar

### Prioridade 3 — Roadmap

7. **Implementar Camada 3 (regras de elegibilidade)** — diversificacao, filtro de estoque
8. **Migrar Modelo B de pickle para formato seguro** (ONNX, safetensors ou NPZ)
9. **Adicionar retreino automatizado** via Makefile / cron / Airflow
10. **Feedback loop** — rastrear conversao das ofertas enviadas

---

## 7. Proximos Passos Tecnicos

1. Implementar `ml/baseline.py` e executar avaliacao com split temporal identico aos modelos A e B
2. Comparar Precision@10, Recall@10, NDCG@10 e HitRate@10 entre Modelo 0, A e B
3. Criar suite de testes em `tests/test_evaluate.py` com pelo menos 10 casos de teste
4. Documentar metricas baseline de todos os 3 modelos na tabela `reco.evaluation_runs`
5. Definir criterio de "go/no-go" para producao: Modelo ML precisa superar Modelo 0 em pelo menos X% em Precision@10

---

## Anexo: Diagrama do Pipeline ML

```
cur.order_items (PostgreSQL)
       |
       v
  load_order_history()
       |
       v
  temporal_split()
       |
       +---> df_train (passado)
       |        |
       |        +---> build_customer_features()
       |        +---> build_product_features()
       |        +---> build_interaction_features()
       |        +---> build_training_dataset() [Modelo A]
       |        +---> build_interaction_matrix() + fit_svd() [Modelo B]
       |
       +---> df_test (futuro)
                |
                +---> evaluate_ranking() / evaluate_binary_classifier()
                |
                v
          reco.evaluation_runs (metricas)
                |
                v
          generate_offers.py --> reco.offers
```
