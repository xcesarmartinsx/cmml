-- =============================================================================
-- sql/ddl/02_curated.sql
-- Camada curada (cur.*) e tabela de avaliação (reco.evaluation_runs).
--
-- cur.products     : catálogo limpo com chave de negócio do ERP
-- cur.order_items  : fato de compras com FK implícita para cur.products
-- reco.evaluation_runs : métricas de avaliação dos modelos ML
--
-- Idempotente: seguro para re-executar (CREATE ... IF NOT EXISTS).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- cur.products
-- Produtos ativos do ERP com metadados para features do modelo.
-- Nota: category (= group_id) está NULL para todos os produtos neste cliente —
--       os campos category_diversity e category_affinity do Modelo A ficarão
--       zerados, mas o treinamento é válido com as demais features.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cur.products (
    -- Identificador do produto no sistema de origem (ERP).
    product_id    BIGINT       NOT NULL,
    -- Nome do sistema de origem — permite múltiplos ERPs no futuro.
    source_system TEXT         NOT NULL,
    -- Descrição/nome do produto para exibição nas recomendações.
    description   TEXT,
    -- Categoria/grupo do produto (= GRUPOID do ERP). NULL = sem categoria.
    category      BIGINT,
    -- Unidade de medida (UN, KG, etc.).
    unit          TEXT,
    -- Produtos inativos são excluídos das recomendações.
    active        BOOLEAN      NOT NULL,
    -- Timestamp da última carga nesta tabela.
    loaded_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, source_system)
);

-- Índice em active — ambos os modelos filtram WHERE p.active = TRUE.
CREATE INDEX IF NOT EXISTS idx_cur_products_active
    ON cur.products (active);

-- ---------------------------------------------------------------------------
-- cur.order_items
-- Histórico de compras curado: base de feature engineering e treinamento.
-- Filtros aplicados na carga (ver sql/dml/01_stg_to_cur.sql):
--   • apenas produtos ativos
--   • sale_date entre 2000-01-01 e CURRENT_DATE (remove datas inválidas)
--   • quantity > 0 e total_value >= 0
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cur.order_items (
    -- Identificador do pedido/nota no ERP.
    order_id_src  TEXT          NOT NULL,
    -- Identificador do cliente (= customer_id_src de stg.sales).
    customer_id   BIGINT        NOT NULL,
    -- Identificador do produto (= product_id_src de stg.sales).
    product_id    BIGINT        NOT NULL,
    -- Identificador da loja (= store_id_src de stg.sales). Pode ser NULL.
    store_id      BIGINT,
    -- Sistema de origem — consistente com cur.products.
    source_system TEXT          NOT NULL,
    -- Data da venda — eixo temporal para features de recência e watermark.
    sale_date     DATE          NOT NULL,
    -- Quantidade vendida — feature de intensidade de preferência.
    quantity      NUMERIC(18,4) NOT NULL,
    -- Valor total da linha (preço × quantidade) — proxy de importância.
    total_value   NUMERIC(18,2) NOT NULL,
    -- Timestamp da carga.
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id_src, product_id, source_system)
);

-- Índice por customer_id — feature engineering lê o histórico por cliente.
CREATE INDEX IF NOT EXISTS idx_cur_oi_customer
    ON cur.order_items (customer_id);

-- Índice por product_id — feature engineering lê popularidade do produto.
CREATE INDEX IF NOT EXISTS idx_cur_oi_product
    ON cur.order_items (product_id);

-- Índice por sale_date — filtros temporais (cutoff, janelas de 30/90 dias).
CREATE INDEX IF NOT EXISTS idx_cur_oi_date
    ON cur.order_items (sale_date);

-- Índice composto (customer_id, sale_date DESC) — queries de recência por cliente.
CREATE INDEX IF NOT EXISTS idx_cur_oi_customer_date
    ON cur.order_items (customer_id, sale_date DESC);

-- ---------------------------------------------------------------------------
-- reco.evaluation_runs
-- Métricas de avaliação persistidas pelos modelos via ml/evaluate.py.
-- Cada linha = (estratégia, K) com precision@K, recall@K, ndcg@K, map@K.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reco.evaluation_runs (
    -- Identificador sequencial da linha de métricas.
    run_id         BIGSERIAL    PRIMARY KEY,
    -- Nome da estratégia / modelo (ex.: 'modelo_a_ranker', 'modelo_b_svd').
    strategy       TEXT         NOT NULL,
    -- Valor de K para o qual as métricas foram calculadas (5, 10, 20).
    k              INTEGER      NOT NULL,
    -- Precisão: fração dos top-K recomendados que são relevantes.
    precision_at_k NUMERIC(6,4),
    -- Recall: fração dos itens relevantes capturados nos top-K.
    recall_at_k    NUMERIC(6,4),
    -- NDCG: ganho cumulativo descontado normalizado — penaliza rankings ruins.
    ndcg_at_k      NUMERIC(6,4),
    -- MAP: média da precisão média — métrica composta de ranking.
    map_at_k       NUMERIC(6,4),
    -- Número de clientes avaliados neste run.
    n_customers    INTEGER,
    -- Anotações livres (ex.: parâmetros usados, flags especiais).
    notes          TEXT,
    -- Timestamp da avaliação.
    evaluated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- reco.offers
-- Tabela de ofertas geradas pelos modelos — com rastreabilidade de envio.
-- Cada execução de ml/generate_offers.py cria um novo offer_batch_id (UUID).
-- A coluna sent_via_whatsapp_at é preenchida quando a oferta for enviada
-- via WhatsApp (Phase 3). Enquanto NULL, a oferta ainda não foi disparada.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reco.offers (
    -- PK sequencial.
    offer_id             BIGSERIAL    PRIMARY KEY,
    -- UUID gerado por execução — agrupa todas as ofertas de um mesmo batch.
    offer_batch_id       UUID         NOT NULL,
    -- ID do cliente no ERP (= cur.order_items.customer_id = stg.customers.customer_id_src).
    customer_id          BIGINT       NOT NULL,
    -- ID do produto no ERP (= cur.products.product_id).
    product_id           BIGINT       NOT NULL,
    -- Modelo que gerou a recomendação: 'modelo_a_ranker' ou 'modelo_b_colaborativo'.
    strategy             TEXT         NOT NULL,
    -- Score de relevância previsto pelo modelo (usado para ordenação, não é probabilidade calibrada).
    score                NUMERIC(6,4) NOT NULL,
    -- Posição no ranking desta oferta dentro da lista do cliente (1 = melhor).
    rank                 INTEGER      NOT NULL,
    -- Quando a oferta foi gerada (timestamp do batch).
    generated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Validade da oferta (NULL = sem expiração; default = 30 dias após geração).
    expires_at           TIMESTAMPTZ  NULL,
    -- Preenchido quando a mensagem WhatsApp for enviada (Phase 3). NULL = não enviado.
    sent_via_whatsapp_at TIMESTAMPTZ  NULL,
    -- Evita duplicatas dentro do mesmo batch (um produto por cliente por batch).
    UNIQUE (offer_batch_id, customer_id, product_id)
);

-- Índice para buscar ofertas de um cliente (cronológico reverso).
CREATE INDEX IF NOT EXISTS idx_reco_offers_customer
    ON reco.offers (customer_id, generated_at DESC);

-- Índice para agrupar/filtrar por batch.
CREATE INDEX IF NOT EXISTS idx_reco_offers_batch
    ON reco.offers (offer_batch_id);

-- ---------------------------------------------------------------------------
-- Adiciona coluna auc_roc à tabela de avaliação (métricas de classificação).
-- ---------------------------------------------------------------------------
ALTER TABLE reco.evaluation_runs
  ADD COLUMN IF NOT EXISTS auc_roc NUMERIC(6,4);

-- ---------------------------------------------------------------------------
-- Adiciona coluna hit_rate_at_k à tabela de avaliação.
-- Hit Rate@K = percentual de clientes com ao menos 1 acerto no top-K.
-- ---------------------------------------------------------------------------
ALTER TABLE reco.evaluation_runs
  ADD COLUMN IF NOT EXISTS hit_rate_at_k NUMERIC(6,4);
