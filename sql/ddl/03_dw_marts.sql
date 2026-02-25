-- =============================================================================
-- sql/ddl/03_dw_marts.sql
--
-- Tabelas mart do Data Warehouse (dw.*) para o Dashboard Visão 360°.
--
-- PROPÓSITO
--   Estas tabelas armazenam dados PRÉ-AGREGADOS gerados pelo ETL
--   etl/load_dw_marts.py a partir das camadas stg.* e cur.*.
--   Elas NÃO são fonte de verdade — são derivados otimizados para leitura
--   pelo dashboard (baixa latência, sem joins em tempo de consulta).
--
-- ESTRATÉGIA DE ATUALIZAÇÃO POR TABELA
--   mart_revenue_daily    → UPSERT incremental (por sale_date)
--                           Só os dias novos/alterados são recalculados.
--   mart_product_ranking  → FULL-REFRESH a cada execução
--                           TRUNCATE + INSERT em transação única.
--   mart_customer_summary → FULL-REFRESH a cada execução
--   mart_state_summary    → FULL-REFRESH a cada execução
--   mart_refresh_log      → APPEND-ONLY (log de auditoria, nunca apagado)
--
-- IDEMPOTÊNCIA
--   Todas as instruções usam CREATE TABLE IF NOT EXISTS e
--   CREATE INDEX IF NOT EXISTS — seguro executar múltiplas vezes.
--
-- EXECUÇÃO
--   psql -h $PG_HOST -U $PG_USER -d $PG_DB -f sql/ddl/03_dw_marts.sql
--
-- DEPENDÊNCIAS
--   Requer que o schema 'dw' já exista (criado em 00_schemas.sql).
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. dw.mart_revenue_daily
--    Agrega faturamento, pedidos e clientes únicos por dia de venda.
--    Usado pelos gráficos de série temporal e sazonalidade do dashboard.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.mart_revenue_daily (

    -- Chave primária: um registro por dia de venda
    date             DATE          NOT NULL,

    -- Campos calendário derivados de 'date' — evitam recálculo em cada query
    year             SMALLINT      NOT NULL,          -- ex.: 2024
    quarter          SMALLINT      NOT NULL,          -- 1 = Jan-Mar … 4 = Out-Dez
    month            SMALLINT      NOT NULL,          -- 1 = Janeiro … 12 = Dezembro
    week_of_year     SMALLINT      NOT NULL,          -- semana ISO 8601 (1–53)
    day_of_week      SMALLINT      NOT NULL,          -- 0 = Domingo … 6 = Sábado

    -- Métricas de venda do dia
    total_revenue    NUMERIC(16,2) NOT NULL DEFAULT 0, -- soma de cur.order_items.total_value
    total_orders     INTEGER       NOT NULL DEFAULT 0, -- count(DISTINCT order_id_src)
    total_items      INTEGER       NOT NULL DEFAULT 0, -- count(*) de linhas de item
    total_qty        NUMERIC(14,3) NOT NULL DEFAULT 0, -- soma de cur.order_items.quantity
    unique_customers INTEGER       NOT NULL DEFAULT 0, -- count(DISTINCT customer_id) no dia
    unique_products  INTEGER       NOT NULL DEFAULT 0, -- count(DISTINCT product_id) no dia

    -- Controle de atualização: quando esta linha foi processada pelo ETL
    refreshed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (date)
);

-- Comentários de tabela e colunas (visíveis no pgAdmin / \d+)
COMMENT ON TABLE  dw.mart_revenue_daily IS
    'Agrega métricas de venda por dia. Atualizado incrementalmente por load_dw_marts.py.';
COMMENT ON COLUMN dw.mart_revenue_daily.date         IS 'Data de venda (sale_date de cur.order_items).';
COMMENT ON COLUMN dw.mart_revenue_daily.quarter      IS 'Trimestre: 1=Jan-Mar, 2=Abr-Jun, 3=Jul-Set, 4=Out-Dez.';
COMMENT ON COLUMN dw.mart_revenue_daily.total_revenue IS 'Soma de total_value em reais.';
COMMENT ON COLUMN dw.mart_revenue_daily.total_orders  IS 'Pedidos distintos (order_id_src) no dia.';
COMMENT ON COLUMN dw.mart_revenue_daily.unique_customers IS 'Clientes distintos que compraram no dia.';
COMMENT ON COLUMN dw.mart_revenue_daily.refreshed_at  IS 'Timestamp da última atualização pelo ETL.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. dw.mart_product_ranking
--    Métricas acumuladas de desempenho de cada produto ao longo de todo
--    o histórico disponível em cur.order_items.
--    Full-refresh — recalcula tudo na íntegra a cada execução do ETL.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.mart_product_ranking (

    -- Chave natural do produto (product_id de cur.products)
    product_id       TEXT          NOT NULL,

    -- Atributos descritivos do produto (de cur.products)
    description      TEXT,                            -- nome/descrição do produto
    unit             TEXT,                            -- unidade de medida (UN, KG, CX…)
    active           BOOLEAN,                         -- TRUE = produto ativo no ERP

    -- Métricas de vendas acumuladas (período total do histórico)
    total_revenue    NUMERIC(16,2) NOT NULL DEFAULT 0, -- faturamento total acumulado
    total_qty        NUMERIC(14,3) NOT NULL DEFAULT 0, -- quantidade total vendida
    order_count      INTEGER       NOT NULL DEFAULT 0, -- número de pedidos distintos
    unique_customers INTEGER       NOT NULL DEFAULT 0, -- clientes únicos que compraram

    -- Janela temporal das vendas do produto
    first_sale_date  DATE,                            -- data da primeira venda
    last_sale_date   DATE,                            -- data da venda mais recente

    -- Posições no ranking (1 = melhor)
    revenue_rank     INTEGER,                         -- posição por faturamento acumulado
    qty_rank         INTEGER,                         -- posição por quantidade vendida

    -- Controle de atualização
    refreshed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (product_id)
);

COMMENT ON TABLE  dw.mart_product_ranking IS
    'Ranking acumulado de produtos por faturamento e volume. Full-refresh por load_dw_marts.py.';
COMMENT ON COLUMN dw.mart_product_ranking.revenue_rank IS 'Posição ordinal por total_revenue DESC (1 = produto mais lucrativo).';
COMMENT ON COLUMN dw.mart_product_ranking.qty_rank     IS 'Posição ordinal por total_qty DESC (1 = produto mais vendido em quantidade).';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. dw.mart_customer_summary
--    Visão lifetime de cada cliente: frequência, recência e valor monetário.
--    Serve como base para análise RFM (Recency / Frequency / Monetary)
--    e segmentação de clientes.
--    Full-refresh a cada execução do ETL.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.mart_customer_summary (

    -- Chave natural do cliente (customer_id de cur.order_items)
    customer_id      TEXT          NOT NULL,

    -- Dados geográficos obtidos via JOIN com stg.customers
    city             TEXT,                            -- cidade do cliente
    state            TEXT,                            -- estado (UF de 2 letras)

    -- Métricas de ciclo de vida (Lifetime Value)
    first_purchase   DATE,                            -- data da primeira compra
    last_purchase    DATE,                            -- data da compra mais recente
    total_orders     INTEGER       NOT NULL DEFAULT 0, -- total de pedidos distintos
    total_revenue    NUMERIC(16,2) NOT NULL DEFAULT 0, -- receita total gerada pelo cliente
    total_qty        NUMERIC(14,3) NOT NULL DEFAULT 0, -- quantidade total comprada
    unique_products  INTEGER       NOT NULL DEFAULT 0, -- variedade de produtos adquiridos
    avg_ticket       NUMERIC(12,2),                   -- ticket médio por pedido
    active_span_days INTEGER,                         -- dias entre 1ª e última compra

    -- Controle de atualização
    refreshed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (customer_id)
);

COMMENT ON TABLE  dw.mart_customer_summary IS
    'Resumo lifetime por cliente (RFM). Full-refresh por load_dw_marts.py.';
COMMENT ON COLUMN dw.mart_customer_summary.active_span_days IS
    'Diferença em dias entre first_purchase e last_purchase. NULL se só 1 pedido.';
COMMENT ON COLUMN dw.mart_customer_summary.avg_ticket IS
    'total_revenue / total_orders. NULL se total_orders = 0.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. dw.mart_state_summary
--    Faturamento, base de clientes e pedidos agregados por estado (UF).
--    Alimenta a seção geográfica do dashboard.
--    Full-refresh a cada execução do ETL.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.mart_state_summary (

    -- Chave: sigla do estado (UF)
    state            TEXT          NOT NULL,          -- ex.: 'CE', 'SP', 'RN'

    -- Métricas agregadas do estado
    total_customers  INTEGER       NOT NULL DEFAULT 0, -- clientes únicos com pelo menos 1 venda
    total_orders     INTEGER       NOT NULL DEFAULT 0, -- pedidos totais de clientes do estado
    total_revenue    NUMERIC(16,2) NOT NULL DEFAULT 0, -- receita total do estado
    avg_ticket       NUMERIC(12,2),                   -- ticket médio dos pedidos do estado

    -- Controle de atualização
    refreshed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (state)
);

COMMENT ON TABLE  dw.mart_state_summary IS
    'Resumo de vendas por estado (UF). Full-refresh por load_dw_marts.py.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. dw.mart_refresh_log
--    Log de execução de cada carga das tabelas mart.
--    APPEND-ONLY — nunca apagado, fornece histórico completo de execuções.
--    Separado de etl.load_batches para não poluir o log das cargas stg/cur.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.mart_refresh_log (

    -- Identificador auto-incremental desta execução
    refresh_id      BIGSERIAL     PRIMARY KEY,

    -- Nome da tabela mart atualizada (ex.: 'dw.mart_revenue_daily')
    mart_name       TEXT          NOT NULL,

    -- Estado da execução: running → success | failed
    status          TEXT          NOT NULL
                    CHECK (status IN ('running', 'success', 'failed')),

    -- Métricas da execução
    rows_processed  INTEGER,                          -- linhas inseridas/atualizadas
    watermark_from  DATE,                             -- início do intervalo processado
    watermark_to    DATE,                             -- fim do intervalo processado

    -- Detalhes de falha (NULL em caso de sucesso)
    error_message   TEXT,

    -- Timestamps de início e fim da execução
    started_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ                       -- NULL enquanto status = 'running'
);

COMMENT ON TABLE  dw.mart_refresh_log IS
    'Log append-only de execuções de carga dos marts DW. Nunca apagar.';
COMMENT ON COLUMN dw.mart_refresh_log.watermark_from IS
    'Data inicial do incremento processado. NULL para full-refresh.';
COMMENT ON COLUMN dw.mart_refresh_log.finished_at IS
    'Timestamp de encerramento. NULL se execução ainda em andamento (status=running).';


-- ─────────────────────────────────────────────────────────────────────────────
-- Índices — criados com IF NOT EXISTS para idempotência total
-- ─────────────────────────────────────────────────────────────────────────────

-- Índice composto para filtros de período (year + month) no dashboard
CREATE INDEX IF NOT EXISTS idx_mart_rev_year_month
    ON dw.mart_revenue_daily (year, month);

-- Índice para filtro por trimestre (análise quarterly)
CREATE INDEX IF NOT EXISTS idx_mart_rev_year_quarter
    ON dw.mart_revenue_daily (year, quarter);

-- Produtos ordenados por faturamento (query de ranking DESC)
CREATE INDEX IF NOT EXISTS idx_mart_prod_revenue
    ON dw.mart_product_ranking (total_revenue DESC);

-- Produtos ordenados por quantidade (ranking alternativo)
CREATE INDEX IF NOT EXISTS idx_mart_prod_qty
    ON dw.mart_product_ranking (total_qty DESC);

-- Clientes filtrados por estado (mapa geográfico)
CREATE INDEX IF NOT EXISTS idx_mart_cust_state
    ON dw.mart_customer_summary (state);

-- Log de refresh filtrado por mart (monitoramento de execuções recentes)
CREATE INDEX IF NOT EXISTS idx_mart_log_name_started
    ON dw.mart_refresh_log (mart_name, started_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verificação final: lista as tabelas DW criadas/existentes
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    schemaname || '.' || tablename                                    AS tabela,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS tamanho
FROM   pg_tables
WHERE  schemaname = 'dw'
ORDER  BY tablename;
