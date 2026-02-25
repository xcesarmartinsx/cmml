-- =============================================================================
-- sql/ddl/01_staging.sql
-- Cria as tabelas de controle ETL (schema etl) e as tabelas de staging
-- (schema stg) que recebem os dados brutos extraídos do ERP GP.
--
-- PRÉ-REQUISITO: 00_schemas.sql já executado.
--
-- IDEMPOTENTE: use IF NOT EXISTS em toda criação — seguro reexecutar.
--
-- Execução:
--   psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -f sql/ddl/01_staging.sql
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMA etl — Controle interno do pipeline
-- Criado automaticamente por etl.common.ensure_etl_control(), mas centralizado
-- aqui para que o DDL formal exista em repositório e seja versionável.
-- ─────────────────────────────────────────────────────────────────────────────

-- Registra o watermark mais recente por dataset.
-- Uma linha por dataset (ex.: 'stg.sales', 'stg.customers').
-- O ETL lê last_ts para saber a partir de quando extrair na próxima execução.
CREATE TABLE IF NOT EXISTS etl.load_control (
    -- Nome lógico do dataset — coincide com o nome da tabela destino (ex.: 'stg.sales').
    dataset_name   TEXT        NOT NULL,
    -- Último timestamp carregado com sucesso — base do filtro incremental.
    last_ts        TIMESTAMPTZ NULL,
    -- Último ID carregado com sucesso — alternativa ao last_ts para fontes sem coluna de data.
    last_id        BIGINT      NULL,
    -- Quando este watermark foi atualizado pela última vez.
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Chave primária: um watermark por dataset.
    PRIMARY KEY (dataset_name)
);

-- Histórico completo de execuções — uma linha por run de cada dataset.
-- Permite auditar: "o que foi carregado, quando, quantas linhas, e com qual resultado".
-- Referenciado por stg.* via batch_id para rastreabilidade de linhagem.
CREATE TABLE IF NOT EXISTS etl.load_batches (
    -- Identificador único do batch gerado automaticamente.
    batch_id        BIGSERIAL    NOT NULL,
    -- Dataset que este batch pertence (ex.: 'stg.sales').
    dataset_name    TEXT         NOT NULL,
    -- Momento em que o batch foi aberto (início da execução).
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Momento em que o batch foi fechado (fim da execução, sucesso ou falha).
    finished_at     TIMESTAMPTZ  NULL,
    -- Estado do batch: 'running' durante a execução, 'success' ou 'failed' ao fechar.
    status          TEXT         NOT NULL DEFAULT 'running',
    -- Início da janela de dados extraída neste batch (inclusivo, com overlap).
    watermark_from  TIMESTAMPTZ  NULL,
    -- Fim da janela de dados extraída (novo watermark em caso de sucesso).
    watermark_to    TIMESTAMPTZ  NULL,
    -- Quantos dias de overlap foram usados para capturar late-arriving data.
    overlap_days    INT          NOT NULL DEFAULT 0,
    -- Quantas linhas foram lidas da fonte.
    rows_extracted  BIGINT       NOT NULL DEFAULT 0,
    -- Quantas linhas foram efetivamente inseridas/atualizadas no destino.
    rows_inserted   BIGINT       NOT NULL DEFAULT 0,
    -- Quantas linhas foram puladas (já existiam no destino — ON CONFLICT DO NOTHING).
    rows_skipped    BIGINT       NOT NULL DEFAULT 0,
    -- Mensagem de erro em caso de falha (stacktrace completo).
    error_message   TEXT         NULL,
    -- Hostname da máquina que executou o batch — útil em ambientes multi-nó.
    host_name       TEXT         NULL,
    -- Chave primária do batch.
    PRIMARY KEY (batch_id)
);

-- Índice para busca por dataset e data de execução — usado em consultas de auditoria.
CREATE INDEX IF NOT EXISTS idx_load_batches_dataset_started
    ON etl.load_batches (dataset_name, started_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMA stg — Staging (dados brutos normalizados do ERP)
-- ─────────────────────────────────────────────────────────────────────────────

-- ── stg.stores ──
-- Lojas/filiais extraídas do ERP. Deve ser carregada ANTES de stg.sales
-- porque as lojas são dimensão de stg.sales (store_id_src).
CREATE TABLE IF NOT EXISTS stg.stores (
    -- ID original da loja no ERP — chave de negócio.
    store_id_src    BIGINT       NOT NULL,
    -- Sistema de origem (literal 'sqlserver_gp' — gravado pelo ETL).
    source_system   TEXT         NOT NULL,
    -- Nome da loja/filial.
    name            TEXT         NULL,
    -- Cidade onde a loja está localizada — usada para segmentação regional.
    city            TEXT         NULL,
    -- Estado (UF) — 2 letras, usada em regras de recomendação por região.
    state           CHAR(2)      NULL,
    -- Indica se a loja está ativa — lojas inativas não devem receber recomendações.
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    -- ID do batch que carregou/atualizou este registro — rastreabilidade de linhagem.
    batch_id        BIGINT       NOT NULL REFERENCES etl.load_batches(batch_id),
    -- Momento em que o registro foi extraído da fonte.
    extracted_at    TIMESTAMPTZ  NOT NULL,
    -- Data em que a loja foi vista pela primeira vez neste pipeline — nunca sobrescrever.
    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Última vez que este registro foi atualizado no staging.
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- PK composta: loja é única por ID + sistema de origem (suporta múltiplos ERPs).
    PRIMARY KEY (store_id_src, source_system)
);

-- ── stg.products ──
-- Catálogo de produtos extraídos do ERP. Carregada antes de stg.sales.
CREATE TABLE IF NOT EXISTS stg.products (
    -- ID original do produto no ERP — chave de negócio.
    product_id_src  BIGINT       NOT NULL,
    -- Sistema de origem.
    source_system   TEXT         NOT NULL,
    -- Descrição do produto (nome exibido na recomendação).
    description     TEXT         NULL,
    -- ID do grupo/categoria principal — usado para diversificação por categoria no top-N.
    group_id        BIGINT       NULL,
    -- ID do subgrupo/subcategoria — granularidade adicional para regras de negócio.
    subgroup_id     BIGINT       NULL,
    -- Unidade de medida (ex.: 'UN', 'KG', 'CX') — pode filtrar recomendações.
    unit            TEXT         NULL,
    -- Produto ativo = pode ser recomendado; inativo = excluir do top-N.
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    -- ID do batch que carregou este registro.
    batch_id        BIGINT       NOT NULL REFERENCES etl.load_batches(batch_id),
    -- Momento da extração da fonte.
    extracted_at    TIMESTAMPTZ  NOT NULL,
    -- Data em que o produto foi visto pela primeira vez — nunca sobrescrever.
    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Última atualização do registro no staging.
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- PK composta.
    PRIMARY KEY (product_id_src, source_system)
);

-- Índice para busca rápida por categoria (usado na diversificação do top-N).
CREATE INDEX IF NOT EXISTS idx_stg_products_group
    ON stg.products (group_id);

-- ── stg.customers ──
-- Clientes extraídos do ERP. Carregada antes de stg.sales.
-- LGPD: CPF/CNPJ nunca armazenado em plain text — apenas hash SHA-256.
CREATE TABLE IF NOT EXISTS stg.customers (
    -- ID original do cliente no ERP — chave de negócio.
    customer_id_src BIGINT       NOT NULL,
    -- Sistema de origem.
    source_system   TEXT         NOT NULL,
    -- Nome do cliente — usado em personalização e cold-start por segmento.
    name            TEXT         NULL,
    -- Cidade do cliente — base de recomendações regionais e cold-start por localidade.
    city            TEXT         NULL,
    -- Estado (UF) do cliente.
    state           CHAR(2)      NULL,
    -- Tipo de documento: 'PF' (CPF) ou 'PJ' (CNPJ) — derivado do tamanho do doc.
    document_type   TEXT         NULL,
    -- Hash SHA-256 do CPF/CNPJ limpo — usado somente para deduplicação (LGPD).
    hash_document   TEXT         NULL,
    -- Cliente ativo = elegível para receber recomendações.
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    -- Telefone fixo principal (E.FONE1 do ERP GP).
    phone           TEXT         NULL,
    -- Telefone SMS/celular — preferencial para WhatsApp (E.FONESMS do ERP GP).
    mobile          TEXT         NULL,
    -- ID do batch de carga.
    batch_id        BIGINT       NOT NULL REFERENCES etl.load_batches(batch_id),
    -- Momento da extração.
    extracted_at    TIMESTAMPTZ  NOT NULL,
    -- Data da primeira vez que o cliente foi carregado — nunca sobrescrever (histórico de entrada).
    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Última atualização.
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- PK composta.
    PRIMARY KEY (customer_id_src, source_system)
);

-- Índice para busca por cidade/estado em segmentação e cold-start regional.
CREATE INDEX IF NOT EXISTS idx_stg_customers_city_state
    ON stg.customers (city, state);

-- ── stg.sales ──
-- Fato de compras extraído de MOVIMENTO_DIA (TIPO=1) do ERP.
-- É a base de todo o sistema de recomendação: "quem comprou o quê e quando".
CREATE TABLE IF NOT EXISTS stg.sales (
    -- Número do documento/pedido no ERP — parte da chave de negócio.
    order_id_src    TEXT         NOT NULL,
    -- ID do produto comprado — parte da chave de negócio.
    product_id_src  BIGINT       NOT NULL,
    -- Sistema de origem — parte da chave (suporta múltiplos ERPs).
    source_system   TEXT         NOT NULL,
    -- ID do cliente que comprou — chave estrangeira lógica para stg.customers.
    customer_id_src BIGINT       NOT NULL,
    -- ID da loja onde a venda ocorreu — chave estrangeira lógica para stg.stores.
    store_id_src    BIGINT       NULL,
    -- Data da venda — base do watermark incremental e do filtro temporal do modelo.
    sale_date       DATE         NOT NULL,
    -- Quantidade vendida — usada para ponderar score de preferência (frequência * volume).
    quantity        NUMERIC(18,4) NOT NULL,
    -- Valor total da linha do pedido — pode ser usado como proxy de importância.
    total_value     NUMERIC(18,2) NOT NULL,
    -- ID do batch de carga — rastreabilidade completa de cada linha.
    batch_id        BIGINT       NOT NULL REFERENCES etl.load_batches(batch_id),
    -- Momento da extração da fonte.
    extracted_at    TIMESTAMPTZ  NOT NULL,
    -- Momento da inserção no staging.
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- PK composta: garante unicidade da linha de venda e viabiliza ON CONFLICT DO NOTHING.
    PRIMARY KEY (order_id_src, product_id_src, source_system)
);

-- Índice para queries de "histórico de compras por cliente" (base do modelo colaborativo).
CREATE INDEX IF NOT EXISTS idx_stg_sales_customer
    ON stg.sales (customer_id_src);

-- Índice para consultas temporais (janela de N dias para feature de recência).
CREATE INDEX IF NOT EXISTS idx_stg_sales_date
    ON stg.sales (sale_date);

-- Índice para filtrar vendas de um batch específico (validações pós-carga).
CREATE INDEX IF NOT EXISTS idx_stg_sales_batch
    ON stg.sales (batch_id);

-- Índice composto para queries de "itens comprados por cliente em período" (modelo colaborativo).
CREATE INDEX IF NOT EXISTS idx_stg_sales_customer_date
    ON stg.sales (customer_id_src, sale_date DESC);
