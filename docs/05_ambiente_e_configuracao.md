# 05 — Ambiente e Configuração

## Resumo

Pré-requisitos, configuração de variáveis de ambiente, portas, volumes e instruções passo a passo para subir o ambiente de desenvolvimento e produção.

---

## Pré-requisitos

| Componente | Versão mínima | Como verificar |
|---|---|---|
| Ubuntu / Debian | 22.04+ | `lsb_release -a` |
| Docker Engine | 24.x | `docker --version` |
| Docker Compose | V2 (plugin) | `docker compose version` |
| Python | 3.10+ | `python3 --version` |
| pip | 23+ | `pip --version` |
| unixODBC | qualquer | `odbcinst --version` |
| ODBC Driver 18 for SQL Server | 18.x | `odbcinst -q -d` |

---

## Instalação do ODBC Driver 18 (Ubuntu)

```bash
# Instalar dependências
sudo apt-get update
sudo apt-get install -y curl gnupg2 unixodbc unixodbc-dev

# Adicionar repositório Microsoft
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Instalar driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Verificar instalação:
odbcinst -q -d
# Deve aparecer: [ODBC Driver 18 for SQL Server]
```

---

## Dependências Python

Crie o arquivo `requirements.txt` (proposto — não existe no repo):

```txt
pyodbc>=5.0.0
psycopg2-binary>=2.9.0
python-dotenv>=1.0.0
pandas>=2.0.0
numpy>=1.24.0
scikit-learn>=1.3.0
scipy>=1.11.0
pgvector>=0.2.0
```

Instalar:

```bash
cd /home/gameserver/projects/cmml
pip install -r requirements.txt
```

---

## Variáveis de Ambiente

### Arquivo `.env` (existente no repo — com placeholders)

Copie o template e preencha:

```bash
cp .env.example .env
nano .env   # ou: vim .env
```

### Referência completa das variáveis

| Variável | Obrigatória | Valor padrão | Descrição |
|---|---|---|---|
| `PG_HOST` | Sim | `127.0.0.1` | Host do PostgreSQL |
| `PG_PORT` | Sim | `5432` | Porta do PostgreSQL |
| `PG_DB` | Sim | `cmml` | Nome do banco de destino |
| `PG_USER` | Sim | `postgres` | Usuário do PostgreSQL |
| `PG_PASSWORD` | Sim | — | Senha do PostgreSQL |
| `MSSQL_HOST` | Sim | `127.0.0.1` | Host do SQL Server |
| `MSSQL_PORT` | Sim | `1433` | Porta do SQL Server |
| `MSSQL_DB` | Sim | `GP_CASADASREDES` | Banco de dados ERP |
| `MSSQL_USER` | Sim | `sa` | Usuário do SQL Server |
| `MSSQL_PASSWORD` | Sim | — | Senha do SQL Server |
| `MSSQL_SCHEMA` | Não | `dbo` | Schema do ERP no SQL Server |
| `ERP_SOURCE` | Não | `ERP_GP` | Nome lógico da fonte (rastreabilidade) |
| `FETCH_CHUNK` | Não | `5000` | Linhas por chunk de extração |
| `PGADMIN_DEFAULT_EMAIL` | Não | — | Login do pgAdmin |
| `PGADMIN_DEFAULT_PASSWORD` | Não | — | Senha do pgAdmin |
| `RECO_CANDIDATE_SIZE` | Não | `500` | Candidatos por cliente (ML) |
| `RECO_TOP_N` | Não | `10` | Top-N de recomendações |
| `RECO_RECOMPRA_WINDOW_DAYS` | Não | `30` | Janela de recompra (dias) |

### Exportar variáveis manualmente (opcional)

```bash
# Carregar .env no shell atual:
export $(grep -v '^#' .env | xargs)

# Verificar:
echo $PG_DB
echo $MSSQL_DB
```

> O Python usa `python-dotenv` (`load_dotenv()`) e carrega o `.env` automaticamente.
> Não é necessário exportar manualmente ao rodar scripts Python.

---

## Docker Compose (Proposto)

O arquivo `docker-compose.yml` **não existe no repositório**. Template proposto:

```yaml
# docker-compose.yml [Proposto]
services:

  sqlserver_gp:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: sqlserver_gp
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "${MSSQL_PASSWORD}"
      MSSQL_PID: Developer
    ports:
      - "1433:1433"
    volumes:
      - sqlserver_data:/var/opt/mssql
      - ./docker/sqlserver/backup:/var/opt/mssql/backup:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "/opt/mssql-tools18/bin/sqlcmd",
             "-S", "localhost", "-U", "sa",
             "-P", "${MSSQL_PASSWORD}",
             "-Q", "SELECT 1",
             "-No"]
      interval: 30s
      timeout: 10s
      retries: 5

  reco-postgres:
    image: pgvector/pgvector:pg16
    container_name: reco-postgres
    environment:
      POSTGRES_DB: "${PG_DB}"
      POSTGRES_USER: "${PG_USER}"
      POSTGRES_PASSWORD: "${PG_PASSWORD}"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d ${PG_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  reco-pgadmin:
    image: dpage/pgadmin4:latest
    container_name: reco-pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: "${PGADMIN_DEFAULT_EMAIL}"
      PGADMIN_DEFAULT_PASSWORD: "${PGADMIN_DEFAULT_PASSWORD}"
    ports:
      - "5050:80"
    depends_on:
      - reco-postgres
    restart: unless-stopped

volumes:
  sqlserver_data:
  postgres_data:
```

### Subir os containers

```bash
cd /home/gameserver/projects/cmml

# Subir em background:
docker compose up -d

# Verificar status:
docker compose ps

# Ver logs:
docker compose logs -f reco-postgres
docker compose logs -f sqlserver_gp
```

### Parar os containers

```bash
docker compose down          # para sem remover volumes
docker compose down -v       # para E remove volumes (⚠ apaga dados)
```

---

## Portas e Serviços

| Serviço | Container | Porta Host | Porta Container | Protocolo |
|---|---|---|---|---|
| SQL Server (ERP) | `sqlserver_gp` | 1433 | 1433 | TCP |
| PostgreSQL | `reco-postgres` | 5432 | 5432 | TCP |
| pgAdmin | `reco-pgadmin` | 5050 | 80 | HTTP |

Acessar pgAdmin:
- URL: `http://localhost:5050`
- Login: valor de `PGADMIN_DEFAULT_EMAIL` no `.env`

---

## Restaurar Backup do SQL Server

O backup `GP_CASADASREDES161225.BAK` (6.2 GB) está em `docker/sqlserver/backup/`.

```bash
# Restaurar via sqlcmd dentro do container:
docker exec -it sqlserver_gp /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_PASSWORD" \
  -Q "RESTORE DATABASE [GP_CASADASREDES] \
      FROM DISK = '/var/opt/mssql/backup/GP_CASADASREDES161225.BAK' \
      WITH REPLACE, RECOVERY, STATS = 10"

# Verificar restauração:
docker exec -it sqlserver_gp /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_PASSWORD" \
  -Q "SELECT name, state_desc FROM sys.databases WHERE name = 'GP_CASADASREDES'"
```

---

## Volumes e Paths Relevantes

| Path no host | Path no container | Conteúdo |
|---|---|---|
| `./docker/sqlserver/backup/` | `/var/opt/mssql/backup/` | Backup .BAK do ERP |
| `sqlserver_data` (volume docker) | `/var/opt/mssql/` | Dados do SQL Server |
| `postgres_data` (volume docker) | `/var/lib/postgresql/data/` | Dados do PostgreSQL |
| `./logs/` | — | Logs dos scripts Python |

---

## Verificar Conexões Manualmente

```bash
# Testar PostgreSQL:
psql -h 127.0.0.1 -p 5432 -U "$PG_USER" -d "$PG_DB" -c "SELECT version();"

# Testar SQL Server via sqlcmd:
docker exec -it sqlserver_gp /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_PASSWORD" \
  -Q "SELECT @@VERSION"

# Testar conexão Python (ODBC):
python3 -c "
import os; from dotenv import load_dotenv; load_dotenv()
from etl.common import get_pg_conn, get_mssql_conn
pg = get_pg_conn(); print('Postgres OK')
ms = get_mssql_conn(); print('SQL Server OK')
"
```

---

## Próximos Passos

1. Criar o arquivo `docker-compose.yml` com o template acima.
2. Criar o arquivo `requirements.txt`.
3. Adicionar `.env` ao `.gitignore`.
4. Documentar procedure de backup do PostgreSQL (ver [`docs/06_execucao_operacional.md`](06_execucao_operacional.md)).
