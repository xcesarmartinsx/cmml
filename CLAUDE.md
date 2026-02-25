# CMML — Contexto do Projeto

## O que é

Pipeline de dados e ML para **recomendação personalizada de produtos**, construído sobre o ERP GP (SQL Server). O sistema extrai dados via ODBC, carrega em PostgreSQL e gera sugestões de compra por cliente usando duas estratégias: ranking LightGBM supervisionado (Modelo A) e filtragem colaborativa SVD (Modelo B).

## Arquitetura

```
ERP GP (SQL Server:1433)
      │  ODBC / pyodbc
      ▼
  etl/*.py  ──►  stg.*  ──►  cur.*  ──►  reco.sugestoes
                                │
                                └──►  dw.mart_*  (analytics / KPIs)
                                              │
                                        FastAPI (api:8001)
                                    /api/business/*   ←─ Business 360° (port 3001)
                                    /api/recommendations/* ←─ Ofertas (port 3001)
                                    /api/evaluation-runs  ←─ Dashboard ML (port 3000)
```

## Serviços e Portas

| Serviço        | Container        | Porta | Tecnologia               |
|----------------|------------------|-------|--------------------------|
| SQL Server     | sqlserver_gp     | 1433  | SQL Server 2022          |
| PostgreSQL     | reco-postgres    | 5432  | pgvector/pg16            |
| API            | reco-api         | 8001  | FastAPI + uvicorn        |
| Dashboard ML   | reco-dashboard   | 3000  | React 18 + Vite + Nginx  |
| Business 360°  | reco-business    | 3001  | React 18 + Vite + Nginx  |
| pgAdmin        | reco-pgadmin     | 5050  | pgAdmin 4                |

## Status: MVP Funcional — Riscos Críticos Abertos

1. **Zero autenticação** — todos os endpoints da API são públicos, incluindo dados de clientes (PII)
2. **Senhas hardcoded** no `docker-compose.yml` (`SA_PASSWORD`, `POSTGRES_PASSWORD`, `PGADMIN_DEFAULT_PASSWORD`)
3. **CORS irrestrito** — `allow_origins=["*"]` em `app/api/main.py`
4. **PII exposta sem controle** — telefone e nome de clientes retornados sem autenticação
5. **Zero testes automatizados** — sem pytest, sem CI/CD pipeline

## Time de Agentes

| Agente             | Papel                                | Subagent Name        |
|--------------------|--------------------------------------|----------------------|
| Team Leader        | Orquestração, backlog, governança    | *(sessão principal)* |
| Data Scientist     | ML, features, avaliação, drift       | cmml-data-scientist  |
| Backend Engineer   | FastAPI, ETL, PostgreSQL, segurança  | cmml-backend         |
| Frontend Engineer  | React, UX, autenticação frontend     | cmml-frontend        |
| QA & Security      | Testes, OWASP, LGPD                  | cmml-qa-security     |
| DevOps / SRE       | Docker, CI/CD, backup, observab.     | cmml-devops          |

## Definition of Done

- [ ] Código revisado por ≥1 agente do domínio relevante
- [ ] Testes unitários para lógica nova (cobertura ≥80% nos módulos alterados)
- [ ] Nenhum secret hardcoded (senha, token, chave)
- [ ] Endpoint com PII exige autenticação ou dado mascarado
- [ ] Logs sem dado sensível (nome, telefone, CPF)
- [ ] Documentação em `docs/` atualizada se houver mudança de comportamento

## Regras de Segurança & LGPD (INEGOCIÁVEIS)

1. **Nunca hardcode** senhas, tokens ou chaves — usar variáveis de ambiente
2. **SQL sempre parametrizado** — nunca f-string com input vindo de fora do código
3. **PII protegida** — qualquer endpoint que retorne nome, telefone ou customer_id requer autenticação
4. **SA proibido em produção** — criar usuário dedicado com menor privilégio no SQL Server
5. **Logs mascarados** — não logar CPF, telefone, nome completo de clientes
6. **LGPD** — dados pessoais só coletados/armazenados com base legal documentada; política de retenção definida

## Comandos Úteis (Makefile)

```bash
make up       # sobe todos os containers
make down     # para e remove containers
make etl      # executa pipeline ETL completo (incremental)
make reco     # gera recomendações (baseline + ML)
make test     # roda testes automatizados
make ddl      # aplica DDLs no PostgreSQL
make psql     # abre psql no container postgres
make status   # status de todos os containers
```

## Mapa de Arquivos Críticos

| Domínio   | Arquivos                                                                     |
|-----------|------------------------------------------------------------------------------|
| API       | `app/api/main.py`, `app/api/routers/business.py`, `app/api/routers/recommendations.py` |
| ETL       | `etl/common.py`, `etl/load_*.py`, `etl/run_all.sh`, `etl/load_dw_marts.py` |
| ML        | `ml/modelo_a_ranker.py`, `ml/modelo_b_colaborativo.py`, `ml/generate_offers.py`, `ml/evaluate.py` |
| SQL       | `sql/ddl/00_schemas.sql` … `sql/ddl/03_dw_marts.sql`, `sql/dml/01_stg_to_cur.sql` |
| Frontend  | `app/business/src/`, `app/dashboard/src/`                                    |
| Infra     | `../docker-compose.yml`, `app/*/Dockerfile`, `app/*/nginx.conf`, `Makefile` |
| Docs      | `docs/01_visao_geral.md` … `docs/10_faq.md`, `docs/09_seguranca_e_compliance.md` |
| Config    | `.env` (nunca commitar), `.env.example`, `.claude/settings.local.json`       |

## Schemas PostgreSQL

| Schema | Propósito                                                 |
|--------|-----------------------------------------------------------|
| `etl`  | Controle de batches, watermarks, log de carga            |
| `stg`  | Dados brutos do ERP (stores, products, customers, sales) |
| `cur`  | Entidades limpas e consolidadas                          |
| `reco` | Sugestões geradas, evaluation_runs, metadados ML         |
| `dw`   | Marts de analytics (revenue, produtos, clientes, UF)     |
