# 02 — Arquitetura do Sistema

## Resumo

Este documento descreve a arquitetura atual (estado real do repositório) e o design alvo para o MVP, incluindo diagramas textuais, responsabilidade de cada componente e estrutura de pastas.

---

## Estado Atual do Repositório

O repositório conta com:
- `etl/common.py` — infraestrutura de ETL (conexões, watermark, carga)
- `.env` — configuração de ambiente
- `docker/sqlserver/backup/` — backup do banco ERP
- Estrutura de diretórios planejada para `app/` (vazia)

**Não implementado ainda**: docker-compose, DDLs do Postgres, scripts ETL individuais, scripts ML, testes.

---

## Diagrama: Fluxo Atual

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FONTE: SQL Server (container sqlserver_gp, porta 1433)                  │
│                                                                          │
│  Banco: GP_CASADASREDES                                                  │
│  Schema: dbo                                                             │
│  Tabela principal: MOVIMENTO_DIA (fato de compras, TIPO=1)               │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │
                              │  ODBC Driver 18 for SQL Server
                              │  pyodbc — extração em chunks (FETCH_CHUNK=5000)
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  ETL (Python — etl/common.py)                                            │
│                                                                          │
│  1. get_mssql_conn()    → abre conexão com SQL Server                    │
│  2. get_watermark()     → lê último watermark (last_ts/last_id)          │
│  3. mssql_fetch_iter()  → extrai dados em chunks                         │
│  4. pg_copy_upsert_stg() → COPY temp table → UPSERT staging              │
│  5. set_watermark()     → atualiza watermark (carga incremental)         │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │
                              │  psycopg2 — COPY (bulk) + INSERT ON CONFLICT
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  DESTINO: PostgreSQL (container reco-postgres, porta 5432)               │
│                                                                          │
│  Banco: cmml                                                             │
│                                                                          │
│  schema etl:                                                             │
│    └─ load_control  ← controle de watermark por dataset                  │
│                                                                          │
│  schema stg: [Proposto — DDL não existe ainda]                           │
│    ├─ stg.sales     ← fato de compras (MOVIMENTO_DIA)                    │
│    ├─ stg.customers ← clientes (ENTIDADE)                                │
│    ├─ stg.products  ← produtos                                           │
│    └─ stg.stores    ← lojas                                              │
│                                                                          │
│  schema cur: [Proposto]                                                  │
│    ├─ cur.customers ← clientes deduplicados                              │
│    ├─ cur.products  ← produtos normalizados                              │
│    ├─ cur.orders    ← pedidos                                            │
│    ├─ cur.order_items ← itens de pedido                                  │
│    └─ cur.stores    ← lojas                                              │
│                                                                          │
│  schema reco: [Proposto]                                                 │
│    ├─ reco.candidates    ← candidatos por cliente (200-2000)             │
│    ├─ reco.ranked        ← candidatos rankeados                          │
│    └─ reco.sugestoes     ← top-N final por cliente                       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Diagrama: Fluxo Alvo MVP — Pipeline de Recomendação

```
                        ┌─────────────┐
                        │  ERP (MSSQL) │
                        └──────┬──────┘
                               │ ETL diário (cron/Airflow)
                               ▼
                    ┌──────────────────────┐
                    │   stg.* (staging)    │
                    │   dados brutos 1:1   │
                    └──────────┬───────────┘
                               │ transformação/limpeza
                               ▼
                    ┌──────────────────────┐
                    │   cur.* (curated)    │
                    │  entidades limpas    │
                    └────┬─────────────────┘
                         │
          ┌──────────────┼──────────────────────┐
          │              │                       │
          ▼              ▼                       ▼
   ┌─────────────┐ ┌──────────────┐    ┌──────────────────┐
   │  Modelo 0   │ │  Candidate   │    │  Feature Store   │
   │  (Baseline) │ │  Generation  │    │  (cur.features)  │
   │  top vendas │ │  colaborativo│    │  [Proposto]      │
   │  por loja   │ │  (pgvector?) │    └────────┬─────────┘
   └──────┬──────┘ └──────┬───────┘             │
          │               │ 200-2000 candidatos  │
          └───────┬────────┘                     │
                  │                              │
                  ▼                              │
        ┌─────────────────┐                      │
        │    Ranker        │◄─────────────────────┘
        │  (Modelo A)      │  features cliente+produto
        │  score por item  │
        └────────┬─────────┘
                 │ top-N candidatos rankeados
                 ▼
        ┌─────────────────────────────────┐
        │  Regras de Elegibilidade         │
        │  - sem estoque? → remove         │
        │  - comprado recentemente? → pula │
        │  - descontinuado? → remove       │
        │  - fora do sortimento? → remove  │
        │  - diversificação cat./marca     │
        └──────────────┬──────────────────┘
                       │ top-N filtrado
                       ▼
              ┌────────────────┐
              │ reco.sugestoes │
              │  top-10/cliente│
              └───────┬────────┘
                      │
                      ▼
              [Aplicação / API]
              [WhatsApp — roadmap]
```

---

## Componentes e Containers

### Existentes (inferidos do contexto — docker-compose não encontrado no repo)

| Container | Imagem | Porta | Função |
|---|---|---|---|
| `sqlserver_gp` | `mcr.microsoft.com/mssql/server:2022-latest` | 1433:1433 | Banco ERP (fonte) |
| `reco-postgres` | `pgvector/pgvector:pg16` | 5432:5432 | Banco destino + vetores |
| `reco-pgadmin` | `dpage/pgadmin4:latest` | 5050:80 | Interface web Postgres |

> **Nota**: O arquivo `docker-compose.yml` **não foi encontrado** no repositório. Os containers precisam ser definidos. Ver [`docs/05_ambiente_e_configuracao.md`](05_ambiente_e_configuracao.md) para o template proposto.

### ETL (Python)

| Módulo | Status | Função |
|---|---|---|
| `etl/common.py` | Implementado | Conexões, watermark, COPY+UPSERT |
| `etl/load_sales.py` | Proposto | Carga do fato de compras |
| `etl/load_customers.py` | Proposto | Carga de clientes |
| `etl/load_products.py` | Proposto | Carga de produtos |
| `etl/load_stores.py` | Proposto | Carga de lojas |

### ML (Python)

| Módulo | Status | Função |
|---|---|---|
| `ml/baseline.py` | Proposto | Modelo 0 — mais vendidos |
| `ml/candidate_generation.py` | Proposto | Geração de candidatos |
| `ml/ranking.py` | Proposto | Ranker supervisionado |
| `ml/apply_rules.py` | Proposto | Filtros de elegibilidade |
| `ml/evaluate.py` | Proposto | Métricas offline |

---

## Estrutura de Pastas Proposta

```
cmml/
├── .env                      ← segredos (NÃO versionar)
├── .env.example              ← template seguro
├── .gitignore                ← [Proposto]
├── Makefile                  ← [Proposto] comandos principais
├── requirements.txt          ← [Proposto] dependências Python
├── docker-compose.yml        ← [Proposto] orquestração de containers
├── README.md
│
├── app/                      ← aplicação futura (roadmap)
│   ├── control/
│   ├── view/
│   ├── controller/
│   ├── model/
│   └── ml/
│       └── models/
│           ├── regression/
│           └── classification/
│
├── docker/
│   └── sqlserver/
│       └── backup/
│           └── GP_CASADASREDES161225.BAK   ← [mover para storage externo]
│
├── docs/                     ← documentação
│   ├── 01_visao_geral.md
│   ├── 02_arquitetura.md
│   ├── 03_modelagem_dados.md
│   ├── 04_etl_pipeline.md
│   ├── 05_ambiente_e_configuracao.md
│   ├── 06_execucao_operacional.md
│   ├── 07_ml_recomendacao.md
│   ├── 08_contribuicao.md
│   ├── 09_seguranca_e_compliance.md
│   ├── 10_faq.md
│   └── inventario_repo.md
│
├── etl/                      ← pipeline de extração e carga
│   ├── common.py             ← infraestrutura (implementado)
│   ├── load_sales.py         ← [Proposto]
│   ├── load_customers.py     ← [Proposto]
│   ├── load_products.py      ← [Proposto]
│   └── load_stores.py        ← [Proposto]
│
├── ml/                       ← modelos e recomendação [Proposto]
│   ├── baseline.py
│   ├── candidate_generation.py
│   ├── ranking.py
│   ├── apply_rules.py
│   └── evaluate.py
│
├── notebooks/                ← EDA e experimentação [Proposto]
│   ├── eda.ipynb
│   └── model_evaluation.ipynb
│
├── sql/                      ← DDLs e queries [Proposto]
│   ├── ddl/
│   │   ├── 00_schemas.sql
│   │   ├── 01_staging.sql
│   │   ├── 02_curated.sql
│   │   └── 03_reco.sql
│   └── queries/
│       └── extract_sales.sql
│
├── logs/                     ← [Proposto] logs de execução
└── tests/                    ← [Proposto] testes automatizados
    ├── test_common.py
    └── test_etl_integration.py
```

---

## Decisões de Design

| Decisão | Justificativa |
|---|---|
| COPY + UPSERT no Postgres | COPY é o método mais rápido para bulk insert; UPSERT garante idempotência (reprocessamento seguro) |
| Extração em chunks (5000 linhas) | Evita estouro de memória em queries grandes do SQL Server |
| Watermark por dataset | Permite carga incremental independente por entidade |
| Schema separados (stg/cur/reco) | Separação de responsabilidades; facilita debugging e reprocessamento |
| pgvector no Postgres | Permite calcular similaridade coseno diretamente no banco para candidate generation colaborativo |
| python-dotenv | Evita hardcode de credenciais; facilita deploy em múltiplos ambientes |

---

## Próximos Passos

1. Criar `docker-compose.yml` com os 3 containers (ver [`docs/05_ambiente_e_configuracao.md`](05_ambiente_e_configuracao.md)).
2. Criar scripts DDL para os schemas `stg`, `cur`, `reco` (ver [`docs/03_modelagem_dados.md`](03_modelagem_dados.md)).
3. Implementar `etl/load_sales.py` usando `etl/common.py` como base (ver [`docs/04_etl_pipeline.md`](04_etl_pipeline.md)).
4. Implementar `ml/baseline.py` para ter recomendações imediatas sem ML (ver [`docs/07_ml_recomendacao.md`](07_ml_recomendacao.md)).
