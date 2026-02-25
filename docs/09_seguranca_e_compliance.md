# 09 — Segurança e Compliance

## Resumo

Riscos identificados no repositório, diretrizes LGPD aplicáveis ao projeto e práticas de segurança recomendadas.

---

## Riscos Identificados no Repositório

### RISCO 1 — Arquivo `.env` no repositório

**Status**: O arquivo `.env` existe no repositório com credenciais placeholder (`SUA_SENHA_AQUI`).

**Risco**: Se credenciais reais forem colocadas no `.env` e o arquivo for comitado acidentalmente, as senhas ficam expostas no histórico Git.

**Ação imediata**:
```bash
# Adicionar ao .gitignore:
echo ".env" >> .gitignore
echo "*.BAK" >> .gitignore
echo "__pycache__/" >> .gitignore
echo "logs/" >> .gitignore
echo "*.log" >> .gitignore

# Se o .env já foi comitado, remover do histórico:
git rm --cached .env
git commit -m "chore: remove .env from tracking"
```

### RISCO 2 — Backup de 6.2 GB no repositório

**Arquivo**: `docker/sqlserver/backup/GP_CASADASREDES161225.BAK`

**Risco**: Contém dados reais do ERP (clientes, produtos, vendas). Se o repositório for público ou clonado por pessoas não autorizadas, há exposição de dados sensíveis.

**Ação**:
1. Remover do repositório Git.
2. Armazenar em local seguro com controle de acesso (S3 privado, NAS corporativo).
3. Documentar onde o backup está armazenado.

```bash
# Remover arquivo grande do histórico (se necessário):
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch docker/sqlserver/backup/GP_CASADASREDES161225.BAK' \
  --prune-empty --tag-name-filter cat -- --all

# Alternativa mais moderna (git-filter-repo):
pip install git-filter-repo
git filter-repo --path docker/sqlserver/backup/GP_CASADASREDES161225.BAK --invert-paths
```

### RISCO 3 — Usuário `sa` no SQL Server

O `.env` usa `MSSQL_USER=sa` (super-administrador do SQL Server).

**Ação**:
1. Criar um usuário dedicado com permissões apenas de leitura nas tabelas necessárias:
```sql
-- No SQL Server:
CREATE LOGIN cmml_etl WITH PASSWORD = 'senha_segura';
CREATE USER cmml_etl FOR LOGIN cmml_etl;

-- Permissão de leitura apenas nas tabelas necessárias:
GRANT SELECT ON dbo.MOVIMENTO_DIA TO cmml_etl;
GRANT SELECT ON dbo.ENTIDADE TO cmml_etl;
GRANT SELECT ON dbo.PRODUTO TO cmml_etl;
```
2. Atualizar `MSSQL_USER=cmml_etl` no `.env`.

### RISCO 4 — Usuário `postgres` (super-usuário) no PostgreSQL

**Ação**: Criar usuário com permissões mínimas:
```sql
-- No PostgreSQL:
CREATE USER cmml_app WITH PASSWORD 'senha_segura';
GRANT CONNECT ON DATABASE cmml TO cmml_app;
GRANT USAGE ON SCHEMA stg, cur, reco, etl TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stg TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO cmml_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA stg, etl TO cmml_app;
```

---

## LGPD — Lei Geral de Proteção de Dados

O projeto processa **dados pessoais de clientes** (nome, CPF/CNPJ, histórico de compras).

### Dados pessoais identificados

| Dado | Tabela | Sensibilidade |
|---|---|---|
| Nome do cliente | `stg.customers`, `cur.customers` | Pessoal |
| CPF / CNPJ | `stg.customers` | Pessoal (sensível para PF) |
| Histórico de compras | `stg.sales`, `cur.order_items` | Comportamental |
| Localização (cidade/estado) | `stg.customers`, `cur.customers` | Pessoal |

### Obrigações LGPD relevantes

1. **Finalidade**: Dados usados exclusivamente para recomendação de produtos. Não compartilhar com terceiros sem consentimento.
2. **Minimização**: Coletar apenas os campos necessários para a recomendação. Não coletar CPF/CNPJ se não for necessário para identificação.
3. **Segurança**: Criptografar dados em repouso e em trânsito. Controle de acesso por função (RBAC).
4. **Retenção**: Definir período máximo de retenção dos dados históricos (ex.: 3 anos). Implementar rotina de expurgo.
5. **Direito de exclusão**: Implementar rotina para excluir dados de um cliente específico ao receber solicitação.
6. **Anonimização**: Dados usados para ML devem ser anonimizados (pseudonimizar `customer_id` em vez de usar nome/CPF).

### Pseudonimização (proposta)

```sql
-- Na camada curada, nunca expor dados pessoais brutos:
-- cur.customers usa customer_id (sequencial interno), não CPF/nome
-- Relatórios de ML usam customer_id, não dados identificáveis
```

---

## Boas Práticas de Segurança

| Prática | Status | Ação |
|---|---|---|
| `.env` no `.gitignore` | Ausente | Adicionar imediatamente |
| `.env.example` sem segredos | Implementado (nesta documentação) | — |
| Usuário de banco com mínimos privilégios | Ausente | Criar usuários dedicados |
| TLS/SSL nas conexões de banco | Parcial (TrustServerCertificate=yes) | Avaliar uso de cert em prod |
| Logs sem dados pessoais | Não verificável | Garantir no código |
| Backup criptografado | Não verificável | Usar `pg_dump` com `-E` ou criptografia no storage |
| Rotação de senhas | Não implementado | Definir política (90 dias) |
| Auditoria de acesso ao banco | Não implementado | Habilitar `pgaudit` no Postgres |

---

## Usuários de Banco com Menor Privilégio

### PostgreSQL

Três roles estão disponíveis:

| Role           | Propósito                        | Permissões                                                    |
|----------------|----------------------------------|---------------------------------------------------------------|
| `reco`         | Superusuário do banco (owner)    | ALL — usar apenas para DDL e manutenção                       |
| `cmml_app`     | ETL + pipeline ML                | CONNECT + USAGE + ALL em tabelas/sequences (etl,stg,cur,reco,ml,dw) |
| `cmml_readonly`| API FastAPI (somente leitura)    | CONNECT + USAGE + SELECT em cur, reco, dw, stg, etl           |

**Como criar:**
```bash
# Substituir as senhas antes de executar:
psql -h $PG_HOST -p $PG_PORT -U postgres -d reco -f sql/ddl/00_users_pg.sql
```

Após criar, atualizar `.env`:
```
PG_APP_USER=cmml_app
PG_APP_PASSWORD=<senha_definida>
PG_READONLY_USER=cmml_readonly
PG_READONLY_PASSWORD=<senha_definida>
```

### SQL Server

| Login       | Propósito      | Permissões                                          |
|-------------|----------------|-----------------------------------------------------|
| `sa`        | Administrador  | ALL — usar apenas para manutenção e backup          |
| `cmml_etl`  | Pipeline ETL   | SELECT em dbo.MOVIMENTO_DIA, dbo.ENTIDADE, dbo.PRODUTO |

**Como criar:**
```bash
# Substituir a senha antes de executar:
sqlcmd -S localhost,1433 -U sa -P '<SA_PASSWORD>' -i sql/ddl/00_users_mssql.sql
```

Após criar, atualizar `.env`:
```
MSSQL_ETL_USER=cmml_etl
MSSQL_ETL_PASSWORD=<senha_definida>
```

---

## Próximos Passos

1. ~~Criar `.gitignore` e remover `.env` do rastreamento Git.~~ (Implementado)
2. Mover o backup `.BAK` para storage externo seguro.
3. ~~Criar usuários de banco com privilégios mínimos.~~ (Implementado — `sql/ddl/00_users_pg.sql` e `sql/ddl/00_users_mssql.sql`)
4. Consultar jurídico sobre base legal para processamento dos dados (consentimento ou legítimo interesse).
5. Definir política de retenção e rotina de expurgo de dados antigos.
