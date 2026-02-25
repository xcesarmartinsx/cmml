# Auditoria OWASP Top 10 -- Sprint 1

**Data**: 2026-02-23
**Auditor**: QA & Security Agent (cmml-qa-security)
**Escopo**: Endpoints FastAPI (`app/api/`), ETL (`etl/common.py`, `etl/load_*.py`), logs (`logs/`)

---

## Resumo Executivo

| Severidade | Quantidade |
|------------|-----------|
| Critical   | 2         |
| High       | 3         |
| Medium     | 3         |
| Low        | 2         |

---

## Findings

### F-01 -- Zero Autenticacao em Endpoints com PII

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **CRITICAL** |
| **OWASP**      | A01:2021 -- Broken Access Control / A07:2021 -- Identification and Authentication Failures |
| **Arquivo**    | `app/api/routers/recommendations.py` (endpoint `/api/recommendations/offers`), `app/api/routers/business.py` (endpoints `/api/business/top-customers`, `/api/business/customer-share`) |

**Descricao**: Todos os endpoints da API sao publicos. Nao existe nenhum mecanismo de autenticacao (JWT, API key, OAuth, session). Qualquer pessoa com acesso de rede pode consultar:

- `GET /api/recommendations/offers` -- retorna `customer_name`, `phone`, `mobile`, `contact` (telefone formatado), `customer_id`
- `GET /api/business/top-customers` -- retorna `customer_name`, `phone`
- `GET /api/business/customer-share` -- retorna `customer_name`

Isso expoe dados pessoais (PII) de clientes sem nenhum controle.

**Evidencia**: `recommendations.py` linhas 116-126 retornam `sc.name AS customer_name`, `sc.mobile`, `sc.phone`, `COALESCE(sc.mobile, sc.phone) AS contact`. `business.py` linhas 632-656 retornam `MAX(sc.name) AS customer_name`, `MAX(COALESCE(sc.mobile, sc.phone)) AS phone`.

**Recomendacao**:
1. Implementar autenticacao JWT com middleware FastAPI
2. Exigir token valido em todos os endpoints que retornam PII
3. Criar roles (admin, viewer) com niveis de acesso distintos

---

### F-02 -- CORS Irrestrito (allow_origins=["*"] + allow_credentials=True)

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **CRITICAL** |
| **OWASP**      | A05:2021 -- Security Misconfiguration |
| **Arquivo**    | `app/api/main.py` linhas 15-21 |

**Descricao**: A configuracao CORS permite requisicoes de qualquer origem (`allow_origins=["*"]`) combinado com `allow_credentials=True`. Essa combinacao:

- Permite que qualquer site malicioso faca requisicoes autenticadas ao backend
- Quando autenticacao for implementada, cookies/tokens de sessao serao enviados automaticamente para qualquer dominio
- Viola a RFC 6454 -- navegadores modernos bloqueiam `credentials: true` com `origin: *`, mas a configuracao demonstra falta de controle de seguranca

**Evidencia**:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # qualquer origem
    allow_credentials=True,     # envia cookies
    allow_methods=["*"],        # qualquer metodo HTTP
    allow_headers=["*"],        # qualquer header
)
```

**Recomendacao**:
1. Restringir `allow_origins` para os dominios dos frontends: `["http://localhost:3000", "http://localhost:3001"]`
2. Avaliar se `allow_credentials=True` e realmente necessario
3. Limitar `allow_methods` para `["GET"]` (API somente leitura)
4. Limitar `allow_headers` para os necessarios

---

### F-03 -- PII Exposta nos Response Bodies sem Mascaramento

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **HIGH** |
| **OWASP**      | A02:2021 -- Cryptographic Failures (dados sensiveis expostos) |
| **Arquivo**    | `app/api/routers/recommendations.py` linhas 111-126, `app/api/routers/business.py` linhas 632-656, 661-682 |

**Descricao**: Telefones de clientes (fixo e celular) sao retornados em texto claro nos responses JSON. O endpoint `/api/recommendations/offers` retorna tres campos de telefone simultaneamente: `contact`, `mobile` e `phone`. O endpoint `/api/business/top-customers` retorna `phone`.

Mesmo com autenticacao, telefones devem ser mascarados para usuarios sem privilegio especifico (ex: exibir apenas `(**) *****-1234`).

**Recomendacao**:
1. Implementar funcao de mascaramento de telefone no backend
2. Retornar telefone completo apenas para role `admin` ou `operador_whatsapp`
3. Para demais usuarios, retornar telefone mascarado

---

### F-04 -- Senha Padrao Hardcoded nos Defaults de Conexao

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **HIGH** |
| **OWASP**      | A07:2021 -- Identification and Authentication Failures |
| **Arquivo**    | `app/api/main.py` linhas 29-36, `app/api/routers/recommendations.py` linhas 23-30, `app/api/routers/business.py` linhas 43-55 |

**Descricao**: A funcao `get_db()` / `_get_db()` tem senha padrao `"reco"` como fallback:

```python
password=os.getenv("PG_PASSWORD", "reco"),
```

Se a variavel de ambiente nao estiver definida, a aplicacao conecta com uma senha trivial. Alem disso, a senha padrao esta repetida em 3 arquivos diferentes, violando DRY e aumentando risco de inconsistencia.

**Recomendacao**:
1. Remover defaults de senha -- falhar se `PG_PASSWORD` nao estiver definido
2. Centralizar a funcao de conexao em um unico modulo
3. Usar `os.environ["PG_PASSWORD"]` (lanca KeyError) em vez de `os.getenv` com default

---

### F-05 -- Credenciais no .env.example

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **HIGH** |
| **OWASP**      | A07:2021 -- Identification and Authentication Failures |
| **Arquivo**    | `.env.example` linhas 31, 47, 73 |

**Descricao**: O arquivo `.env.example` contem senhas reais/funcionais em vez de placeholders:

- `PG_PASSWORD=reco` (linha 31)
- `MSSQL_PASSWORD=Strong!Passw0rd` (linha 47)
- `PGADMIN_DEFAULT_PASSWORD=admin123` (linha 73)

Essas senhas sao as mesmas usadas nos containers, fazendo do `.env.example` um arquivo de credenciais que esta versionado no Git.

**Recomendacao**:
1. Substituir valores por placeholders: `PG_PASSWORD=CHANGE_ME_IN_PRODUCTION`
2. Documentar que os valores devem ser alterados antes do deploy

---

### F-06 -- Uso de Conta SA (Super Admin) no SQL Server

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **MEDIUM** |
| **OWASP**      | A01:2021 -- Broken Access Control |
| **Arquivo**    | `.env.example` linha 44, `etl/common.py` (get_mssql_conn) |

**Descricao**: O ETL conecta no SQL Server usando a conta `sa` (system administrator). Se a conexao for comprometida ou houver SQL injection na fonte, o atacante tera acesso total ao SQL Server, incluindo:

- DROP de qualquer tabela/database
- CREATE LOGIN (criar novos usuarios)
- Acesso a `xp_cmdshell` (execucao de comandos no SO)

**Evidencia**: Logs confirmam uso: `SQL Server : sa@127.0.0.1:1433/master` (arquivo `logs/etl_20260221_140416.log` linha 8).

**Recomendacao**:
1. Criar usuario dedicado com apenas SELECT nas tabelas do ERP necessarias
2. Revogar acesso de `sa` a qualquer aplicacao

---

### F-07 -- Credenciais de Conexao Logadas em Arquivo

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **MEDIUM** |
| **OWASP**      | A09:2021 -- Security Logging and Monitoring Failures |
| **Arquivo**    | `etl/run_all.sh` linhas 165-166, logs em `logs/etl_*.log` |

**Descricao**: O script `run_all.sh` loga o usuario e host de conexao nos arquivos de log:

```
Banco PG   : reco@127.0.0.1:5432/reco
SQL Server : sa@127.0.0.1:1433/master
```

Embora senhas nao estejam diretamente nos logs, o nome de usuario `sa` e os hosts/portas facilitam um ataque se os logs forem vazados. Confirma tambem que `sa` esta em uso.

**Recomendacao**:
1. Nao logar nomes de usuario de banco
2. Se necessario para debug, usar nivel DEBUG (nao INFO) e nao gravar em arquivo

---

### F-08 -- f-string com Interpolacao de Variavel em SQL (Risco Parcial)

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **MEDIUM** |
| **OWASP**      | A03:2021 -- Injection |
| **Arquivo**    | `app/api/routers/business.py` linhas 174-192, 273-337, 393-416 e `app/api/routers/recommendations.py` linhas 111-157 |

**Descricao**: Diversos endpoints usam f-strings para montar SQL com clausulas WHERE e ORDER BY dinamicas. Exemplos:

- `business.py` monta `{where_clause}` via f-string (linhas 174, 273, etc.)
- `business.py` monta `ORDER BY {order_col}` via f-string (linhas 414, 437)
- `recommendations.py` monta `{strategy_filter}` via f-string (linha 154)

**Analise de risco**:
- Os valores interpolados (`where_clause`, `order_col`, `strategy_filter`) sao construidos internamente pelo servidor a partir de valores validados:
  - `order_col` vem de um dicionario fixo (`sort_col_map`) -- nao ha SQL injection
  - `where_clause` e montada com `%s` parametrizado -- seguro
  - `strategy_filter` usa `%s` parametrizado -- seguro
  - `granularity` e validado por regex no Query -- seguro
- **Nao ha SQL injection exploravel atualmente**, mas o padrao de f-string e fragil: uma refatoracao futura que nao siga o padrao pode introduzir vulnerabilidade.

**Recomendacao**:
1. Documentar claramente que valores interpolados em f-strings NUNCA devem conter input do usuario
2. Considerar uso de query builder para reduzir risco de refatoracao insegura

---

### F-09 -- Ausencia de Rate Limiting

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **LOW** |
| **OWASP**      | A05:2021 -- Security Misconfiguration |
| **Arquivo**    | `app/api/main.py` (aplicacao inteira) |

**Descricao**: Nenhum mecanismo de rate limiting esta implementado. Um atacante pode fazer milhares de requests por segundo, causando:

- Sobrecarga no PostgreSQL (cada request abre uma conexao)
- Exfiltracao massiva de dados de clientes via `/api/recommendations/offers` com paginacao

**Recomendacao**:
1. Adicionar middleware de rate limiting (ex: `slowapi`)
2. Limitar a 60 req/min por IP para endpoints com PII

---

### F-10 -- Ausencia de Connection Pooling

| Campo        | Valor |
|--------------|-------|
| **Severidade** | **LOW** |
| **OWASP**      | N/A (disponibilidade) |
| **Arquivo**    | `app/api/main.py`, `app/api/routers/business.py`, `app/api/routers/recommendations.py` |

**Descricao**: Cada request HTTP abre e fecha uma conexao PostgreSQL (`psycopg2.connect()`). Sem connection pool, sob carga a API pode esgotar as conexoes do PostgreSQL ou ficar lenta.

**Recomendacao**:
1. Usar `psycopg2.pool.ThreadedConnectionPool` ou migrar para `asyncpg`
2. Centralizar em um unico modulo de conexao

---

## Analise de Dependencias

### requirements.txt (`app/api/requirements.txt`)

| Pacote | Versao | CVEs Diretos | Observacao |
|--------|--------|-------------|------------|
| `fastapi` | 0.115.6 | Nenhum CVE direto no core | Versao atual e 0.128.7; considerar upgrade para patches de seguranca |
| `uvicorn[standard]` | 0.34.0 | Nenhum CVE conhecido | -- |
| `psycopg2-binary` | 2.9.10 | Nenhum CVE direto | `psycopg2-binary` embute libpq e OpenSSL; versoes anteriores tiveram CVEs em libs bundled |

**Recomendacao geral**: Nenhuma vulnerabilidade critica conhecida nas dependencias diretas. Manter dependencias atualizadas e rodar `pip-audit` ou `safety check` periodicamente.

---

## Analise LGPD

### Dados Pessoais Identificados nos Endpoints

| Dado Pessoal | Endpoint | Campo no Response | Risco |
|-------------|----------|-------------------|-------|
| Nome do cliente | `/api/recommendations/offers` | `customer_name` | Exposto sem auth |
| Nome do cliente | `/api/business/top-customers` | `customer_name` | Exposto sem auth |
| Nome do cliente | `/api/business/customer-share` | `customer_name` | Exposto sem auth |
| Telefone celular | `/api/recommendations/offers` | `mobile` | Exposto sem auth |
| Telefone fixo | `/api/recommendations/offers` | `phone` | Exposto sem auth |
| Telefone (tratado) | `/api/recommendations/offers` | `contact` | Exposto sem auth |
| Telefone | `/api/business/top-customers` | `phone` | Exposto sem auth |
| ID do cliente | `/api/recommendations/offers` | `customer_id` | Exposto sem auth |

### Dados Pessoais no ETL / Banco

| Dado | Tabela | Tratamento LGPD |
|------|--------|----------------|
| Nome | `stg.customers.name` | Armazenado em plain text |
| Telefone fixo | `stg.customers.phone` | Armazenado em plain text |
| Telefone celular | `stg.customers.mobile` | Armazenado em plain text |
| CPF/CNPJ | `stg.customers.hash_document` | Hasheado com SHA-256 (ADEQUADO) |
| Historico de compras | `cur.order_items` | Pseudonimizado via `customer_id` |
| Cidade/Estado | `stg.customers` | Armazenado em plain text |

### Riscos LGPD

1. **Exposicao sem base legal de acesso**: endpoints publicos permitem acesso irrestrito a nome e telefone de clientes, sem consentimento e sem relacao de necessidade.
2. **Ausencia de politica de retencao**: sem rotina de expurgo de dados antigos.
3. **Ausencia de mecanismo de exclusao**: sem endpoint/rotina para atender direito de exclusao (Art. 18 LGPD).
4. **Ponto positivo**: CPF/CNPJ esta adequadamente hasheado no pipeline ETL (`load_customers.py` usa SHA-256).

---

## Analise de Logs -- PII

### Arquivos analisados

- `logs/etl_20260221_135435.log`
- `logs/etl_20260221_140416.log`
- `logs/etl_20260221_140647.log`

### Resultado

**Nenhuma PII encontrada nos logs existentes.** Os logs contem apenas:
- Metadados de execucao (timestamps, nomes de scripts)
- Metricas de carga (contagem de linhas, status)
- Mensagens de erro tecnicas (nomes de tabelas SQL Server)
- Credenciais parciais: usuario `sa` e `reco` logados com host/porta (F-07)

### Risco potencial nos scripts ETL

O codigo em `etl/common.py` e `etl/load_*.py` nao loga valores de clientes diretamente. Porem, nao existe nenhum filtro de protecao que impeca PII de ser logada acidentalmente (ex: se um futuro `LOG.debug(row)` for adicionado, telefones e nomes serao gravados no log).

---

## Matriz de Priorizacao

| # | Finding | Severidade | Esforco | Prioridade |
|---|---------|-----------|---------|-----------|
| F-01 | Zero autenticacao | Critical | Alto | P0 -- Bloqueia deploy |
| F-02 | CORS irrestrito | Critical | Baixo | P0 -- Correcao imediata |
| F-05 | Senhas em .env.example | High | Baixo | P1 -- Correcao imediata |
| F-03 | PII nos responses | High | Medio | P1 -- Junto com F-01 |
| F-04 | Senha default hardcoded | High | Baixo | P1 -- Refatoracao |
| F-06 | Conta SA no SQL Server | Medium | Medio | P2 |
| F-07 | Credenciais nos logs | Medium | Baixo | P2 |
| F-08 | f-string em SQL | Medium | Medio | P2 -- Documentar |
| F-09 | Sem rate limiting | Low | Medio | P3 |
| F-10 | Sem connection pool | Low | Medio | P3 |
