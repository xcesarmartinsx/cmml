-- =============================================================================
-- sql/dml/01_stg_to_cur.sql
-- Transforma stg.* → cur.*
--
-- Execução: segura para re-rodar (ON CONFLICT ... DO UPDATE / DO NOTHING).
-- Ordem obrigatória: cur.products deve ser populado ANTES de cur.order_items
-- (o INSERT de order_items faz JOIN em cur.products para filtrar inativos).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. cur.products
-- Cópia direta de stg.products com rename das colunas para o modelo canônico.
-- Inclui produtos inativos — o filtro active=TRUE é aplicado em cur.order_items
-- e nos próprios modelos, preservando o histórico completo no catálogo.
-- ---------------------------------------------------------------------------
INSERT INTO cur.products
    (product_id, source_system, description, category, unit, active)
SELECT
    -- Chave de negócio do produto no ERP.
    product_id_src   AS product_id,
    -- Sistema de origem.
    source_system,
    -- Nome/descrição para exibição.
    description,
    -- Categoria = GRUPOID. Atualmente NULL para todos os produtos neste cliente.
    group_id         AS category,
    -- Unidade de medida.
    unit,
    -- Status ativo/inativo.
    active
FROM stg.products
ON CONFLICT (product_id, source_system) DO UPDATE SET
    -- Atualiza atributos mutáveis; nunca sobrescreve a PK.
    description = EXCLUDED.description,
    category    = EXCLUDED.category,
    unit        = EXCLUDED.unit,
    active      = EXCLUDED.active,
    loaded_at   = now();

-- ---------------------------------------------------------------------------
-- 2. cur.order_items
-- Fato de compras com filtros de qualidade aplicados:
--   a) JOIN cur.products (active=TRUE) — remove itens de produtos descontinuados
--   b) sale_date BETWEEN '2000-01-01' AND CURRENT_DATE — remove as 4 linhas com
--      ano 2404 (erro de digitação no ERP) e datas anteriores ao ERP
--   c) quantity > 0 — remove devoluções/estornos e linhas inválidas
--   d) total_value >= 0 — remove valores negativos (créditos/ajustes)
-- ---------------------------------------------------------------------------
INSERT INTO cur.order_items
    (order_id_src, customer_id, product_id, store_id, source_system,
     sale_date, quantity, total_value)
SELECT
    -- Identificador do pedido/nota no ERP.
    s.order_id_src,
    -- ID do cliente (renomeado de customer_id_src para o modelo canônico).
    s.customer_id_src   AS customer_id,
    -- ID do produto (renomeado de product_id_src).
    s.product_id_src    AS product_id,
    -- ID da loja (pode ser NULL se não preenchido no ERP).
    s.store_id_src      AS store_id,
    -- Sistema de origem.
    s.source_system,
    -- Data da venda — eixo temporal do modelo.
    s.sale_date,
    -- Quantidade vendida.
    s.quantity,
    -- Valor total da linha.
    s.total_value
FROM stg.sales s
-- JOIN garante que só entram produtos que existem em cur.products e estão ativos.
JOIN cur.products p
    ON  p.product_id    = s.product_id_src
    AND p.source_system = s.source_system
    AND p.active        = TRUE
-- Remove datas inválidas: ano 2404 (erro de digitação) e anterior ao ERP.
WHERE s.sale_date BETWEEN '2000-01-01' AND CURRENT_DATE
  -- Remove devoluções/estornos (quantidade zero ou negativa).
  AND s.quantity    > 0
  -- Remove valores negativos (ajustes de crédito).
  AND s.total_value >= 0
ON CONFLICT (order_id_src, product_id, source_system) DO NOTHING;
