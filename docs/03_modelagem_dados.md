# 03 — Modelagem de Dados

## Resumo

Descreve as entidades do ERP GP, as regras de identidade, o dicionário de dados extraído do código e das queries, e as convenções de schemas no PostgreSQL.

---

## Entidades do ERP GP (SQL Server)

### Tabela Principal: `dbo.MOVIMENTO_DIA`

Fato de compras — base de toda a modelagem de recomendação.

| Coluna ERP | Alias ETL | Tipo (inferido) | Descrição |
|---|---|---|---|
| `NUMDOCUMENTO` | `order_id_src` | VARCHAR/INT | Número do documento/pedido no ERP |
| `ENTIDADEID_CLIENTE` | `customer_id_src` | INT/BIGINT | **PK do cliente no ERP** (chave de identidade) |
| `PRODUTOID` | `product_id_src` | INT/BIGINT | Identificador do produto no ERP |
| `DATA` | `sale_date` | DATE/DATETIME | Data da venda |
| `QUANTIDADE` | `quantity` | DECIMAL/INT | Quantidade vendida |
| `VALORTOTAL` | `total_value` | DECIMAL | Valor total da linha do pedido |
| `ENTIDADEID_LOJA` | `store_id_src` | INT/BIGINT | Identificador da loja |
| — | `source_system` | VARCHAR | Literal `'sqlserver_gp'` (adicionado pelo ETL) |

**Filtro obrigatório na extração:**
```sql
WHERE TIPO = 1                          -- somente vendas (não devoluções ou outros tipos)
  AND ENTIDADEID_CLIENTE IS NOT NULL    -- exclui vendas sem cliente identificado
  AND PRODUTOID IS NOT NULL             -- exclui registros sem produto
  AND DATA IS NOT NULL                  -- exclui datas nulas
```

**Query oficial de extração:**
```sql
SELECT
    NUMDOCUMENTO            AS order_id_src,
    ENTIDADEID_CLIENTE      AS customer_id_src,
    PRODUTOID               AS product_id_src,
    DATA                    AS sale_date,
    QUANTIDADE              AS quantity,
    VALORTOTAL              AS total_value,
    ENTIDADEID_LOJA         AS store_id_src,
    'sqlserver_gp'          AS source_system
FROM dbo.MOVIMENTO_DIA
WHERE TIPO = 1
  AND ENTIDADEID_CLIENTE IS NOT NULL
  AND PRODUTOID IS NOT NULL
  AND DATA IS NOT NULL;
```

### Outras Tabelas ERP (Não Confirmadas — Proposto)

> As tabelas abaixo **não foram confirmadas** no repositório (sem queries no código). São propostas baseadas no padrão de ERPs GP e no backup disponível.

| Tabela ERP (estimada) | Entidade | Colunas esperadas |
|---|---|---|
| `dbo.ENTIDADE` | Clientes e lojas | `ENTIDADEID`, `NOME`, `CPF_CNPJ`, `CIDADE`, `UF` |
| `dbo.PRODUTO` | Produtos | `PRODUTOID`, `DESCRICAO`, `GRUPOID`, `SUBGRUPOID`, `UNIDADE` |
| `dbo.GRUPO` | Categoria de produto | `GRUPOID`, `DESCRICAO` |
| `dbo.ESTOQUE` | Estoque atual | `PRODUTOID`, `LOJAID`, `QUANTIDADE` |

**Ação necessária**: confirmar nomes reais das tabelas consultando o banco restaurado:
```sql
-- Listar tabelas do ERP:
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;
```

---

## Regras de Identidade e Deduplicação

### Cliente

- **Chave primária ERP**: `ENTIDADEID_CLIENTE` — considerada autoritativa.
- **Risco de duplicata**: clientes podem ter múltiplos registros se foram recadastrados.
- **Estratégia proposta**:
  1. Usar `ENTIDADEID_CLIENTE` como `customer_id_src` no staging.
  2. Na camada curada (`cur.customers`), criar `customer_id` interno (sequencial no Postgres).
  3. Tabela de mapeamento `cur.customer_id_map (customer_id_src, source_system, customer_id)` para resolver duplicatas por CPF/CNPJ quando disponível.

### Produto

- **Chave primária ERP**: `PRODUTOID` — considerada autoritativa.
- **Estratégia proposta**: criar `product_id` interno no staging; mapear via `cur.product_id_map`.

### Pedido

- **Chave composta**: `(NUMDOCUMENTO, ENTIDADEID_LOJA)` — o número do documento pode se repetir entre lojas.
- **Atenção**: confirmar se `NUMDOCUMENTO` é único por loja ou global no ERP.

---

## Schemas e Tabelas no PostgreSQL

### Schema `etl` — Controle Interno

**Existente** (criado automaticamente pelo `etl/common.py`):

```sql
-- Criado por: etl.common.ensure_etl_control()
CREATE TABLE etl.load_control (
    dataset_name  TEXT PRIMARY KEY,
    last_ts       TIMESTAMPTZ NULL,       -- último timestamp carregado (incremental por data)
    last_id       BIGINT NULL,            -- último ID carregado (incremental por ID)
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Schema `stg` — Staging (Proposto)

Dados brutos extraídos do ERP. Sem transformações. Preserva a estrutura original.

```sql
-- [Proposto] sql/ddl/01_staging.sql

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.sales (
    -- Chave de identidade da linha no staging
    order_id_src    TEXT        NOT NULL,
    customer_id_src BIGINT      NOT NULL,
    product_id_src  BIGINT      NOT NULL,
    store_id_src    BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    -- Dados de negócio
    sale_date       DATE        NOT NULL,
    quantity        NUMERIC(18,4) NOT NULL,
    total_value     NUMERIC(18,2) NOT NULL,
    -- Metadados ETL
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- PK composta para UPSERT seguro
    PRIMARY KEY (order_id_src, product_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id_src BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    name            TEXT,
    document        TEXT,       -- CPF ou CNPJ (mascarar em produção — LGPD)
    city            TEXT,
    state           CHAR(2),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS stg.products (
    product_id_src  BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    description     TEXT,
    group_id        BIGINT,     -- categoria
    subgroup_id     BIGINT,     -- subcategoria
    unit            TEXT,
    active          BOOLEAN,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS stg.stores (
    store_id_src    BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    name            TEXT,
    city            TEXT,
    state           CHAR(2),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id_src, source_system)
);
```

### Schema `cur` — Curado (Proposto)

Entidades consolidadas com chaves internas do projeto.

```sql
-- [Proposto] sql/ddl/02_curated.sql

CREATE SCHEMA IF NOT EXISTS cur;

CREATE TABLE IF NOT EXISTS cur.customers (
    customer_id     BIGSERIAL   PRIMARY KEY,
    customer_id_src BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    name            TEXT,
    city            TEXT,
    state           CHAR(2),
    first_purchase  DATE,
    last_purchase   DATE,
    total_orders    INT         DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS cur.products (
    product_id      BIGSERIAL   PRIMARY KEY,
    product_id_src  BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    description     TEXT,
    category        TEXT,
    subcategory     TEXT,
    unit            TEXT,
    active          BOOLEAN     DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS cur.stores (
    store_id        BIGSERIAL   PRIMARY KEY,
    store_id_src    BIGINT      NOT NULL,
    source_system   TEXT        NOT NULL,
    name            TEXT,
    city            TEXT,
    state           CHAR(2),
    UNIQUE (store_id_src, source_system)
);

CREATE TABLE IF NOT EXISTS cur.order_items (
    order_id_src    TEXT        NOT NULL,
    customer_id     BIGINT      NOT NULL REFERENCES cur.customers(customer_id),
    product_id      BIGINT      NOT NULL REFERENCES cur.products(product_id),
    store_id        BIGINT      NOT NULL REFERENCES cur.stores(store_id),
    sale_date       DATE        NOT NULL,
    quantity        NUMERIC(18,4) NOT NULL,
    total_value     NUMERIC(18,2) NOT NULL,
    source_system   TEXT        NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id_src, product_id, source_system)
);

-- Índices para performance de queries de recomendação:
CREATE INDEX IF NOT EXISTS idx_order_items_customer ON cur.order_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product  ON cur.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_date     ON cur.order_items(sale_date);
CREATE INDEX IF NOT EXISTS idx_order_items_store    ON cur.order_items(store_id);
```

### Schema `reco` — Recomendações (Proposto)

```sql
-- [Proposto] sql/ddl/03_reco.sql

CREATE SCHEMA IF NOT EXISTS reco;

-- Sugestões finais por cliente (top-N filtrado)
CREATE TABLE IF NOT EXISTS reco.sugestoes (
    customer_id     BIGINT      NOT NULL REFERENCES cur.customers(customer_id),
    rank            SMALLINT    NOT NULL,  -- posição 1 a N
    product_id      BIGINT      NOT NULL REFERENCES cur.products(product_id),
    score           FLOAT       NOT NULL,  -- score do ranker
    strategy        TEXT        NOT NULL,  -- 'baseline', 'collaborative', 'ranking'
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ,           -- validade da sugestão
    PRIMARY KEY (customer_id, rank)
);

-- Índice para busca por cliente:
CREATE INDEX IF NOT EXISTS idx_sugestoes_customer ON reco.sugestoes(customer_id);

-- Métricas de avaliação offline [Proposto]
CREATE TABLE IF NOT EXISTS reco.evaluation_runs (
    run_id          BIGSERIAL   PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    strategy        TEXT        NOT NULL,
    k               INT         NOT NULL,
    precision_at_k  FLOAT,
    recall_at_k     FLOAT,
    ndcg_at_k       FLOAT,
    map_at_k        FLOAT,
    n_customers     INT,
    notes           TEXT
);
```

---

## Convenções de Nomenclatura

| Padrão | Exemplo | Descrição |
|---|---|---|
| `*_src` | `customer_id_src` | ID original da fonte (ERP) |
| `*_id` | `customer_id` | ID interno do projeto (sequencial Postgres) |
| `loaded_at` | `loaded_at` | Timestamp de carga ETL |
| `updated_at` | `updated_at` | Timestamp de atualização |
| `source_system` | `'sqlserver_gp'` | Nome lógico da fonte (para rastreabilidade) |
| Schema `stg.*` | `stg.sales` | Dados brutos, sem transformação |
| Schema `cur.*` | `cur.customers` | Dados limpos e consolidados |
| Schema `reco.*` | `reco.sugestoes` | Saída dos modelos |
| Schema `etl.*` | `etl.load_control` | Controle interno do ETL |

---

## Próximos Passos

1. Restaurar o backup `GP_CASADASREDES161225.BAK` e confirmar nomes reais das tabelas ERP.
2. Criar os arquivos DDL em `sql/ddl/` com os scripts acima.
3. Confirmar a unicidade de `NUMDOCUMENTO` dentro do ERP (por loja ou global).
4. Avaliar se `ENTIDADEID_CLIENTE` é suficiente para deduplicação ou se é necessário cruzar com CPF/CNPJ.
5. Definir política de mascaramento de dados pessoais (ver [`docs/09_seguranca_e_compliance.md`](09_seguranca_e_compliance.md)).
