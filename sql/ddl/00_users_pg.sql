-- =============================================================================
-- sql/ddl/00_users_pg.sql
-- Cria usuários PostgreSQL com menor privilégio para o projeto CMML.
--
-- PRÉ-REQUISITO: 00_schemas.sql já executado (schemas etl, stg, cur, reco, ml, dw).
--
-- IDEMPOTENTE: usa DO $$ BEGIN ... EXCEPTION WHEN duplicate_object ... END $$
--
-- Execução (como superusuário postgres ou reco):
--   psql -h $PG_HOST -p $PG_PORT -U postgres -d $PG_DB -f sql/ddl/00_users_pg.sql
--
-- Variáveis esperadas (substituir antes de executar ou usar \set):
--   :pg_app_password      — senha do cmml_app
--   :pg_readonly_password  — senha do cmml_readonly
--
-- Exemplo com psql:
--   psql -v pg_app_password="'senha_app'" -v pg_readonly_password="'senha_ro'" \
--        -h localhost -U postgres -d reco -f sql/ddl/00_users_pg.sql
--
-- Ou substitua manualmente 'CHANGE_ME_APP' e 'CHANGE_ME_READONLY' abaixo.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. cmml_app — usuário da aplicação (ETL + ML pipeline)
--    Permissões: CONNECT + USAGE em todos os schemas + ALL em tabelas + sequences
-- ─────────────────────────────────────────────────────────────────────────────

DO $$ BEGIN
    CREATE ROLE cmml_app WITH LOGIN PASSWORD 'CHANGE_ME_APP';
EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE 'Role cmml_app already exists, skipping creation.';
END $$;

-- Permissão de conexão ao banco
GRANT CONNECT ON DATABASE reco TO cmml_app;

-- USAGE nos schemas que o pipeline precisa acessar
GRANT USAGE ON SCHEMA etl TO cmml_app;
GRANT USAGE ON SCHEMA stg TO cmml_app;
GRANT USAGE ON SCHEMA cur TO cmml_app;
GRANT USAGE ON SCHEMA reco TO cmml_app;
GRANT USAGE ON SCHEMA ml TO cmml_app;
GRANT USAGE ON SCHEMA dw TO cmml_app;

-- ALL PRIVILEGES em tabelas existentes
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stg TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cur TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA reco TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ml TO cmml_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO cmml_app;

-- USAGE em sequences (necessário para BIGSERIAL como etl.load_batches.batch_id)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA etl TO cmml_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA stg TO cmml_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA cur TO cmml_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA reco TO cmml_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA ml TO cmml_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA dw TO cmml_app;

-- Default privileges para tabelas e sequences criadas no futuro
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT ALL PRIVILEGES ON TABLES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT ALL PRIVILEGES ON TABLES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA cur GRANT ALL PRIVILEGES ON TABLES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA reco GRANT ALL PRIVILEGES ON TABLES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml GRANT ALL PRIVILEGES ON TABLES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT ALL PRIVILEGES ON TABLES TO cmml_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT USAGE ON SEQUENCES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT USAGE ON SEQUENCES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA cur GRANT USAGE ON SEQUENCES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA reco GRANT USAGE ON SEQUENCES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml GRANT USAGE ON SEQUENCES TO cmml_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT USAGE ON SEQUENCES TO cmml_app;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. cmml_readonly — usuário somente leitura (API FastAPI)
--    Permissões: CONNECT + USAGE em cur, reco, dw + SELECT apenas
-- ─────────────────────────────────────────────────────────────────────────────

DO $$ BEGIN
    CREATE ROLE cmml_readonly WITH LOGIN PASSWORD 'CHANGE_ME_READONLY';
EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE 'Role cmml_readonly already exists, skipping creation.';
END $$;

-- Permissão de conexão ao banco
GRANT CONNECT ON DATABASE reco TO cmml_readonly;

-- USAGE nos schemas que a API precisa ler
GRANT USAGE ON SCHEMA cur TO cmml_readonly;
GRANT USAGE ON SCHEMA reco TO cmml_readonly;
GRANT USAGE ON SCHEMA dw TO cmml_readonly;
GRANT USAGE ON SCHEMA stg TO cmml_readonly;
GRANT USAGE ON SCHEMA etl TO cmml_readonly;

-- SELECT em tabelas existentes
GRANT SELECT ON ALL TABLES IN SCHEMA cur TO cmml_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA reco TO cmml_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA dw TO cmml_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA stg TO cmml_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA etl TO cmml_readonly;

-- Default privileges para tabelas criadas no futuro
ALTER DEFAULT PRIVILEGES IN SCHEMA cur GRANT SELECT ON TABLES TO cmml_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA reco GRANT SELECT ON TABLES TO cmml_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT ON TABLES TO cmml_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT SELECT ON TABLES TO cmml_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT SELECT ON TABLES TO cmml_readonly;


-- ─────────────────────────────────────────────────────────────────────────────
-- Verificação
-- ─────────────────────────────────────────────────────────────────────────────

SELECT rolname, rolcanlogin
FROM   pg_roles
WHERE  rolname IN ('cmml_app', 'cmml_readonly')
ORDER  BY rolname;
