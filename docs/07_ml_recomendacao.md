# 07 — ML e Recomendação de Produtos

## Resumo

Descreve a estratégia de recomendação em 3 camadas (candidatos → ranking → regras), o Modelo 0 (fallback), as métricas de avaliação offline e o roadmap de feedback loop.

> **Status atual**: nenhum script ML implementado no repositório. Este documento define o design alvo (MVP).

---

## Arquitetura de 3 Camadas

```
┌────────────────────────────────────────────────────────────────┐
│  CAMADA 1: Candidate Generation                                │
│  Objetivo: Gerar 200–2000 candidatos por cliente               │
│  Prioridade: COBERTURA (não perder itens relevantes)           │
│  Custo: Baixo (heurísticas + similaridade)                     │
├────────────────────────────────────────────────────────────────┤
│  CAMADA 2: Ranking                                             │
│  Objetivo: Ordenar candidatos por score de relevância          │
│  Prioridade: PRECISÃO (top-N ser de alta qualidade)            │
│  Custo: Médio (modelo supervisionado ou heurística avançada)   │
├────────────────────────────────────────────────────────────────┤
│  CAMADA 3: Regras de Elegibilidade                             │
│  Objetivo: Filtros de negócio que o modelo não conhece         │
│  Prioridade: CONFIABILIDADE (não recomendar absurdos)          │
│  Custo: Baixo (queries SQL)                                    │
└────────────────────────────────────────────────────────────────┘
```

---

## Modelo 0 — Fallback / Baseline

O Modelo 0 funciona sem ML e deve ser implementado primeiro. Garante que todos os clientes recebam alguma recomendação mesmo sem histórico ou enquanto os modelos mais sofisticados não estão prontos.

### Estratégia

| Cenário | Fallback |
|---|---|
| Cliente com histórico | Top vendidos na sua loja principal, excluindo já comprados recentemente |
| Cliente novo (cold start) | Top vendidos global / na loja mais próxima |
| Produto novo (cold start) | Recomendar junto com os mais vendidos da mesma categoria |
| Modelo falhou | Top 10 global (nunca deixar lista vazia) |

### Implementação (proposto: `ml/baseline.py`)

```sql
-- Top produtos por loja (último 90 dias):
SELECT
    s.store_id,
    s.product_id,
    COUNT(DISTINCT s.customer_id) AS n_buyers,
    SUM(s.quantity)               AS total_qty,
    RANK() OVER (
        PARTITION BY s.store_id
        ORDER BY COUNT(DISTINCT s.customer_id) DESC
    ) AS rank
FROM cur.order_items s
WHERE s.sale_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY s.store_id, s.product_id;

-- Para um cliente específico — excluir já comprados (janela 30 dias):
WITH cliente_recente AS (
    SELECT DISTINCT product_id
    FROM cur.order_items
    WHERE customer_id = :customer_id
      AND sale_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT tp.product_id, tp.rank, tp.n_buyers
FROM reco.top_por_loja tp
WHERE tp.store_id = :store_id_do_cliente
  AND tp.product_id NOT IN (SELECT product_id FROM cliente_recente)
ORDER BY tp.rank
LIMIT 10;
```

---

## Camada 1 — Candidate Generation

### Estratégia A: Histórico do Próprio Cliente (Next Best Product)

Gera candidatos baseado no padrão individual:

```sql
-- Produtos comprados por clientes similares (mas não por este cliente):
SELECT DISTINCT oi2.product_id
FROM cur.order_items oi1
JOIN cur.order_items oi2
    ON oi1.customer_id != oi2.customer_id
    AND oi1.product_id = oi2.product_id  -- mesmo produto como "ponte"
WHERE oi1.customer_id = :customer_id
  AND oi2.product_id NOT IN (
      SELECT DISTINCT product_id FROM cur.order_items
      WHERE customer_id = :customer_id
  )
LIMIT 2000;
```

### Estratégia B: Colaborativo (pgvector)

Representa cada cliente como vetor de compras e busca os K vizinhos mais próximos:

```sql
-- Habilitar extensão:
CREATE EXTENSION IF NOT EXISTS vector;

-- Tabela de embeddings de clientes [Proposto]:
CREATE TABLE reco.customer_embeddings (
    customer_id   BIGINT PRIMARY KEY,
    embedding     vector(512),   -- dimensão definida pelo modelo
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Índice para busca aproximada de vizinhos (ivfflat):
CREATE INDEX ON reco.customer_embeddings
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Buscar os 50 clientes mais similares:
SELECT customer_id, embedding <=> :query_embedding AS distance
FROM reco.customer_embeddings
ORDER BY distance
LIMIT 50;
```

O embedding pode ser construído como:
- **Bag of words de produtos**: vetor binário ou TF-IDF de produtos comprados
- **Matrix Factorization** (SVD/ALS): vetores latentes de usuário
- **Word2Vec de produtos**: sequência temporal de compras

---

## Camada 2 — Ranking

### Ranker Supervisionado (MVP: LightGBM / XGBoost)

**Label**: comprou o produto nos próximos 30 dias? (1 = sim, 0 = não)

**Features sugeridas:**

| Feature | Tipo | Fonte |
|---|---|---|
| `customer_purchase_count_90d` | Numérica | `cur.order_items` |
| `customer_avg_ticket` | Numérica | `cur.order_items` |
| `product_popularity_30d` | Numérica | `cur.order_items` |
| `customer_last_purchase_days` | Numérica | `cur.order_items` |
| `product_bought_by_customer_before` | Binária | `cur.order_items` |
| `category_affinity_score` | Numérica | Calculada |
| `days_since_last_purchase_of_product` | Numérica | `cur.order_items` |
| `store_product_rank` | Numérica | `reco.top_por_loja` |

**Treinamento:**

```python
# [Proposto] ml/ranking.py (esboço)
import lightgbm as lgb

# Split temporal (NUNCA aleatório — vazamento de dados temporal):
# Treino: compras antes de T-30 dias
# Validação: compras entre T-30 e T-7 dias
# Teste: compras nos últimos 7 dias

train_data = lgb.Dataset(X_train, label=y_train)
val_data = lgb.Dataset(X_val, label=y_val)

params = {
    "objective": "binary",
    "metric": "auc",
    "learning_rate": 0.05,
    "num_leaves": 31,
}

model = lgb.train(params, train_data,
                  valid_sets=[val_data],
                  num_boost_round=200,
                  callbacks=[lgb.early_stopping(20)])

# Salvar modelo:
model.save_model("app/ml/models/classification/ranker_v1.lgb")
```

### Ranker Simples (Heurística — antes do modelo supervisionado)

Se não houver dados de validação suficientes, usar score heurístico:

```python
score = (
    0.4 * frequency_score      # frequência de compra pelo cliente
  + 0.3 * recency_inverse       # inverso do tempo desde última compra
  + 0.2 * popularity_score      # popularidade global do produto
  + 0.1 * category_affinity     # afinidade com categorias do cliente
)
```

---

## Camada 3 — Regras de Elegibilidade

Filtros aplicados **após** o ranking, antes de salvar em `reco.sugestoes`:

```python
# [Proposto] ml/apply_rules.py

def apply_rules(candidates_df, customer_id, config):
    """
    candidates_df: DataFrame com colunas [customer_id, product_id, score, rank]
    Retorna DataFrame filtrado.
    """
    # Regra 1: sem estoque (se tabela de estoque disponível)
    # candidates_df = candidates_df[candidates_df['in_stock'] == True]

    # Regra 2: não recomendar item comprado recentemente
    recompra_window = config.get('recompra_window_days', 30)
    candidates_df = candidates_df[
        candidates_df['days_since_last_purchase'] > recompra_window
    ]

    # Regra 3: produto descontinuado
    candidates_df = candidates_df[candidates_df['active'] == True]

    # Regra 4: diversificação por categoria (max 3 por categoria no top-10)
    candidates_df = diversify(candidates_df, by='category', max_per_group=3)

    # Regra 5: mix seguro + novo (70% já conhecidos, 30% descoberta)
    known = candidates_df[candidates_df['bought_before'] == True].head(7)
    new   = candidates_df[candidates_df['bought_before'] == False].head(3)
    return pd.concat([known, new]).sort_values('score', ascending=False)


def diversify(df, by, max_per_group):
    """Limita itens por grupo no top-N."""
    result = []
    counts = {}
    for _, row in df.iterrows():
        group = row[by]
        counts[group] = counts.get(group, 0) + 1
        if counts[group] <= max_per_group:
            result.append(row)
    return pd.DataFrame(result)
```

### Lista de Regras de Negócio

| Regra | Fonte | Implementação |
|---|---|---|
| Sem estoque | Tabela de estoque ERP | Filtro: `in_stock = TRUE` |
| Produto descontinuado | Tabela de produtos ERP | Filtro: `active = TRUE` |
| Janela de recompra (30 dias) | Configurável via `.env` | `days_since_last_purchase > 30` |
| Fora do sortimento da loja | Tabela de estoque por loja | Filtro por loja |
| Max 3 por categoria no top-10 | Regra de diversificação | `diversify()` |
| Mix 70% seguro / 30% novo | Regra de serendipidade | Split de listas |
| Margem mínima | Tabela de produtos (se disponível) | Filtro: `margin > threshold` |

---

## Avaliação Offline

### Métricas

| Métrica | Descrição | Fórmula (simplificada) |
|---|---|---|
| `Precision@K` | Fração de recomendações relevantes no top-K | `relevantes_no_top_K / K` |
| `Recall@K` | Fração de itens relevantes capturados no top-K | `relevantes_no_top_K / total_relevantes` |
| `NDCG@K` | Qualidade da ordenação, com desconto por posição | DCG normalizado |
| `MAP@K` | Média de Average Precision para todos os clientes | Média de AP@K |
| `Hit Rate@K` | Percentual de clientes com pelo menos 1 acerto no top-K | `hits / total_clientes` |

### Split Temporal (OBRIGATÓRIO)

```
Histórico completo: 2020-01-01 → 2025-12-16

Treino:     2020-01-01 → 2025-09-30   (≈ 70%)
Validação:  2025-10-01 → 2025-11-30   (para tuning)
Teste:      2025-12-01 → 2025-12-16   (avaliação final)

NUNCA usar split aleatório — cria vazamento temporal.
```

### Implementação (proposto: `ml/evaluate.py`)

```python
# [Proposto] ml/evaluate.py (esboço)
def precision_at_k(recommended, relevant, k):
    top_k = set(recommended[:k])
    relevant_set = set(relevant)
    return len(top_k & relevant_set) / k

def recall_at_k(recommended, relevant, k):
    top_k = set(recommended[:k])
    relevant_set = set(relevant)
    if not relevant_set:
        return 0.0
    return len(top_k & relevant_set) / len(relevant_set)
```

---

## Roadmap: Feedback Loop e WhatsApp

> Estas funcionalidades estão **fora do escopo do MVP** e devem ser implementadas em fases futuras.

### Feedback Loop (Fase 2)

Registrar eventos por oferta enviada:

```sql
-- [Proposto] Tabela de eventos de feedback:
CREATE TABLE reco.offer_events (
    event_id        BIGSERIAL   PRIMARY KEY,
    customer_id     BIGINT      REFERENCES cur.customers(customer_id),
    product_id      BIGINT      REFERENCES cur.products(product_id),
    strategy        TEXT,
    sent_at         TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    read_at         TIMESTAMPTZ,
    clicked_at      TIMESTAMPTZ,
    purchased_at    TIMESTAMPTZ,
    revenue_attr    NUMERIC(18,2)  -- receita atribuída ao clique
);
```

Usar para:
1. Calcular CTR e conversão por estratégia
2. Re-treinar modelos com feedback real
3. Detectar fadiga (cliente parou de abrir mensagens) e pausar envios

### Integração WhatsApp (Fase 3)

- **API**: WhatsApp Business API (Meta) ou provedor (Twilio, Zenvia, Take Blip)
- **Template de mensagem**: pré-aprovado pela Meta
- **Cadência**: não mais que 1 mensagem/semana por cliente (evitar spam)
- **Opt-out**: respeitar LGPD — manter lista de descadastre
- **Atribuição**: rastrear compra no ERP dentro de 7 dias do envio

---

## Próximos Passos

1. Implementar `ml/baseline.py` — Modelo 0 com top vendidos por loja.
2. Criar tabelas `reco.*` no PostgreSQL (ver [`docs/03_modelagem_dados.md`](03_modelagem_dados.md)).
3. Fazer EDA no notebook `notebooks/eda.ipynb` para entender distribuição de compras.
4. Implementar `ml/evaluate.py` com as métricas Precision@K e Recall@K.
5. Validar o Modelo 0 contra o split temporal antes de avançar para modelos supervisionados.
