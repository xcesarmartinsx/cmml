# Inventário do Repositório

> **Gerado em**: 2026-02-20
> **Caminho base**: `/home/gameserver/projects/cmml`

## Resumo

O repositório está no estágio inicial de desenvolvimento. Contém a infraestrutura base de ETL e uma estrutura de diretórios planejada para a aplicação, mas a maioria dos componentes ainda não foi implementada.

---

## Arquivos Existentes

| Caminho | Tipo | Tamanho | Descrição |
|---|---|---|---|
| `.env` | Configuração | ~500 B | Variáveis de ambiente (placeholders; NÃO versionar) |
| `etl/common.py` | Python | ~8 KB | Infraestrutura ETL: conexões, watermark, COPY+UPSERT |
| `docker/sqlserver/backup/GP_CASADASREDES161225.BAK` | Binário | 6,2 GB | Backup do banco ERP GP (SQL Server 2022), data 16/12/2025 |

---

## Diretórios Existentes (vazios)

| Caminho | Propósito planejado |
|---|---|
| `app/control/` | Camada de controle da aplicação web |
| `app/view/` | Camada de view/templates |
| `app/controller/` | Controllers da aplicação |
| `app/model/` | Modelos da aplicação |
| `app/ml/models/regression/` | Modelos de regressão treinados |
| `app/ml/models/classification/` | Modelos de classificação treinados |

---

## Arquivos Ausentes (Proposto — devem ser criados)

### Infraestrutura / DevOps

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `docker-compose.yml` | ALTA | Definição dos containers (SQL Server, Postgres, pgAdmin) |
| `.gitignore` | ALTA | Excluir `.env`, `*.BAK`, `__pycache__`, etc. |
| `requirements.txt` | ALTA | Dependências Python (`pyodbc`, `psycopg2`, `python-dotenv`, etc.) |
| `Makefile` | MÉDIA | Comandos operacionais (up/down/etl/reco/test) |
| `.env.example` | ALTA | Template seguro de configuração (criado nesta documentação) |

### ETL

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `etl/load_sales.py` | ALTA | Extração e carga do fato de compras (MOVIMENTO_DIA) |
| `etl/load_customers.py` | ALTA | Extração e carga de clientes (ENTIDADE) |
| `etl/load_products.py` | ALTA | Extração e carga de produtos |
| `etl/load_stores.py` | MÉDIA | Extração e carga de lojas |

### SQL / DDL

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `sql/ddl/00_schemas.sql` | ALTA | Criação dos schemas (`etl`, `stg`, `cur`, `reco`) |
| `sql/ddl/01_staging.sql` | ALTA | Tabelas de staging (`stg.*`) |
| `sql/ddl/02_curated.sql` | ALTA | Tabelas curadas (`cur.*`) |
| `sql/ddl/03_reco.sql` | ALTA | Tabelas de recomendação (`reco.*`) |

### ML

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `ml/baseline.py` | ALTA | Modelo 0: mais vendidos por loja/categoria |
| `ml/candidate_generation.py` | MÉDIA | Geração de candidatos (colaborativo) |
| `ml/ranking.py` | MÉDIA | Ranker supervisionado |
| `ml/apply_rules.py` | MÉDIA | Filtros de elegibilidade |
| `ml/evaluate.py` | MÉDIA | Precision@K, Recall@K, NDCG |
| `notebooks/eda.ipynb` | BAIXA | Exploração dos dados |
| `notebooks/model_evaluation.ipynb` | BAIXA | Avaliação offline dos modelos |

### Aplicação

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `app/api.py` | BAIXA (roadmap) | API REST para consumo das recomendações |
| `app/whatsapp.py` | BAIXA (roadmap) | Integração WhatsApp para envio de ofertas |

### Testes

| Arquivo | Prioridade | Descrição |
|---|---|---|
| `tests/test_common.py` | MÉDIA | Testes unitários do ETL |
| `tests/test_etl_integration.py` | MÉDIA | Testes de integração ETL |

---

## Conteúdo do `etl/common.py` — Funções Implementadas

| Função | Descrição |
|---|---|
| `get_pg_conn()` | Abre conexão com PostgreSQL via variáveis de ambiente |
| `get_mssql_conn()` | Abre conexão com SQL Server via ODBC Driver 18 |
| `ensure_etl_control(pg)` | Cria schema `etl` e tabela `etl.load_control` se não existirem |
| `get_watermark(pg, dataset)` | Lê `last_ts` e `last_id` do controle incremental |
| `set_watermark(pg, dataset, ...)` | Atualiza watermark após carga bem-sucedida |
| `mssql_fetch_iter(conn, sql, ...)` | Extrai dados em chunks (iterador de dicts) |
| `pg_copy_upsert_stg(pg, table, ...)` | Carrega dados via COPY + UPSERT com detecção automática de PK |

---

## Observações Críticas

1. **Backup de 6.2 GB no repositório**: o arquivo `docker/sqlserver/backup/GP_CASADASREDES161225.BAK` não deve ser versionado no Git (adicionar ao `.gitignore` e usar Git LFS ou armazenamento externo).
2. **Sem docker-compose**: o ambiente de desenvolvimento não pode ser reproduzido sem o `docker-compose.yml`.
3. **Sem DDL**: as tabelas no PostgreSQL não podem ser criadas sem os scripts SQL.
4. **Sem testes**: não há cobertura de testes automatizados.
5. **ODBC Driver 18**: o `common.py` usa "ODBC Driver 18 for SQL Server" — documentação deve refletir isso (não versão 17).

---

## Próximos Passos

1. Criar `.gitignore` e remover o `.BAK` do histórico git (se adicionado).
2. Criar `docker-compose.yml` para reproduzir o ambiente localmente.
3. Criar `requirements.txt` com as dependências Python.
4. Criar os scripts DDL para os schemas do PostgreSQL.
5. Implementar os scripts ETL individuais (`load_sales.py`, etc.).
6. Ver [`docs/06_execucao_operacional.md`](06_execucao_operacional.md) para rotina operacional proposta.
