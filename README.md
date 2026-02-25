# CMML — Pipeline de Dados e Recomendação de Produtos

![CI](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml/badge.svg)
![Security](https://github.com/<OWNER>/<REPO>/actions/workflows/security.yml/badge.svg)

> **Status**: Em desenvolvimento (MVP)
> **Ultima atualizacao**: ver historico git
>
> **Nota**: Substitua `<OWNER>/<REPO>` nas badges acima pelo caminho real do repositorio GitHub.

## Visão Geral

O CMML é uma esteira de dados e ML para **recomendação personalizada de produtos**, construída sobre dados do ERP GP (SQL Server) carregados em PostgreSQL. O sistema gera sugestões de compra para cada cliente usando duas estratégias complementares:

- **Estratégia A — Ranking / Next Best Product**: personalização baseada no histórico individual do cliente.
- **Estratégia B — Colaborativo**: "clientes similares compraram" — descobre itens que o cliente ainda não conhece.

```
ERP (SQL Server)
      │
      │  ODBC / pyodbc
      ▼
  [ ETL — extract ]
      │
      ▼
  stg.* (staging)          ← dados brutos, 1:1 com a fonte
      │
      ▼
  cur.* (curated)          ← entidades limpas e consolidadas
      │
      ├──► [ Candidate Generation ]  → 200-2000 candidatos/cliente
      │
      ├──► [ Ranking ]               → top-N ordenado por score
      │
      ├──► [ Regras de Elegibilidade ]  → filtros estoque/janela/loja
      │
      └──► reco.sugestoes            ← tabela final consumida pela aplicação
```

Documentação completa em [`docs/`](docs/).

---

## Quickstart

### 1. Pré-requisitos

| Componente | Versão mínima |
|---|---|
| Docker + Docker Compose | 24.x |
| Python | 3.10+ |
| ODBC Driver 18 for SQL Server | — |
| `unixODBC` | — |

Instalar dependências Python:

```bash
cd /home/gameserver/projects/cmml
pip install -r requirements.txt   # [Proposto] — ver docs/05_ambiente_e_configuracao.md
```

### 2. Configurar ambiente

```bash
cp .env.example .env
# edite .env e preencha as credenciais reais
nano .env
```

Variáveis obrigatórias:

```
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
MSSQL_HOST, MSSQL_PORT, MSSQL_DB, MSSQL_USER, MSSQL_PASSWORD
```

Ver [`docs/05_ambiente_e_configuracao.md`](docs/05_ambiente_e_configuracao.md) para descrição completa.

### 3. Subir containers

```bash
# [Proposto] — docker-compose ainda não existe no repo
docker compose up -d

# Verificar saúde:
docker ps
docker logs reco-postgres
docker logs sqlserver_gp
```

Ver [`docs/05_ambiente_e_configuracao.md`](docs/05_ambiente_e_configuracao.md) para detalhes de rede e volumes.

### 4. Restaurar backup do ERP (SQL Server)

```bash
# O backup está em:
docker/sqlserver/backup/GP_CASADASREDES161225.BAK   # 6.2 GB

# [Proposto] restaurar via sqlcmd:
docker exec -it sqlserver_gp /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_PASSWORD" \
  -Q "RESTORE DATABASE [GP_CASADASREDES] FROM DISK='/var/opt/mssql/backup/GP_CASADASREDES161225.BAK' WITH REPLACE, RECOVERY"
```

### 5. Criar schemas no PostgreSQL

```bash
# [Proposto] — scripts DDL ainda não existem no repo
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f sql/ddl/00_schemas.sql
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f sql/ddl/01_staging.sql
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f sql/ddl/02_curated.sql
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f sql/ddl/03_reco.sql
```

### 6. Rodar ETL

```bash
# [Proposto] — scripts ETL individuais ainda não existem
python etl/load_sales.py          # fato de compras (MOVIMENTO_DIA)
python etl/load_customers.py      # entidades/clientes
python etl/load_products.py       # produtos
```

O módulo [`etl/common.py`](etl/common.py) contém toda a infraestrutura de conexão, controle incremental e carga.

### 7. Gerar recomendações

```bash
# [Proposto]
python ml/candidate_generation.py   # gera candidatos
python ml/ranking.py                # aplica ranker
python ml/apply_rules.py            # filtros de elegibilidade
```

---

## Estrutura de Diretórios

```
cmml/
├── .env                        ← variáveis de ambiente (NÃO versionar)
├── .env.example                ← template seguro para versionamento
├── Makefile                    ← [Proposto] comandos principais
├── requirements.txt            ← [Proposto] dependências Python
├── README.md                   ← este arquivo
├── app/                        ← [Proposto] aplicação web/API futura
│   ├── control/
│   ├── view/
│   ├── controller/
│   ├── model/
│   └── ml/
│       └── models/
│           ├── regression/
│           └── classification/
├── docker/
│   └── sqlserver/
│       └── backup/
│           └── GP_CASADASREDES161225.BAK   ← backup do ERP (6.2 GB)
├── docs/                       ← documentação completa
├── etl/
│   ├── common.py               ← infraestrutura ETL (conexões, upsert, watermark)
│   └── [scripts individuais — Proposto]
├── ml/                         ← [Proposto] scripts ML
└── sql/                        ← [Proposto] DDLs e queries
```

---

## Arquitetura Resumida

Ver [`docs/02_arquitetura.md`](docs/02_arquitetura.md) para diagramas completos.

**Pipeline de dados:**
1. `stg.*` — dados brutos extraídos do ERP, sem transformação
2. `cur.*` — entidades consolidadas e limpas
3. `reco.*` — saída final do modelo para a aplicação

**Modelos:**
- **Modelo 0 (fallback)**: mais vendidos por loja/categoria — sem ML, funciona para cold start
- **Modelo B (colaborativo)**: usuário-usuário ou item-item via pgvector embeddings
- **Modelo A (ranker)**: ordena candidatos por score supervisionado (features cliente + produto)

---

## Troubleshooting

### Erro de conexão ODBC com SQL Server

```
[Microsoft][ODBC Driver 18 for SQL Server]SSL Provider ...
```

Solução: confirme `TrustServerCertificate=yes` no DSN (já implementado em `etl/common.py`).

### Porta 1433 não acessível

```bash
# Verificar se o container está rodando:
docker ps | grep sqlserver
# Verificar logs:
docker logs sqlserver_gp 2>&1 | tail -20
```

### Erro de driver ODBC não encontrado

```bash
# Listar drivers instalados:
odbcinst -q -d
# Driver esperado: "ODBC Driver 18 for SQL Server"
# Instalar no Ubuntu: ver docs/05_ambiente_e_configuracao.md
```

### PostgreSQL — permissão negada

```bash
# Verificar usuário e banco:
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "\dn"
```

### Variáveis de ambiente não carregadas

```bash
# O ETL usa python-dotenv — garantir que o .env existe na raiz:
ls -la /home/gameserver/projects/cmml/.env
```

---

## Links da Documentação

| Documento | Descrição |
|---|---|
| [01 — Visão Geral](docs/01_visao_geral.md) | Contexto de negócio, escopo, glossário |
| [02 — Arquitetura](docs/02_arquitetura.md) | Diagramas, componentes, fluxo |
| [03 — Modelagem de Dados](docs/03_modelagem_dados.md) | ERP, entidades, dicionário de dados |
| [04 — ETL Pipeline](docs/04_etl_pipeline.md) | Extract/Transform/Load, incremental, validações |
| [05 — Ambiente e Configuração](docs/05_ambiente_e_configuracao.md) | Docker, .env, ODBC, portas |
| [06 — Execução Operacional](docs/06_execucao_operacional.md) | Rotina, monitoramento, backup |
| [07 — ML e Recomendação](docs/07_ml_recomendacao.md) | Modelos, métricas, roadmap |
| [08 — Contribuição](docs/08_contribuicao.md) | Git flow, padrões, PR |
| [09 — Segurança e Compliance](docs/09_seguranca_e_compliance.md) | LGPD, credenciais, riscos |
| [10 — FAQ](docs/10_faq.md) | Perguntas frequentes |
| [Inventário do Repo](docs/inventario_repo.md) | Lista completa de arquivos |
