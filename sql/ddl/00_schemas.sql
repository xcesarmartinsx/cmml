-- =============================================================================
-- sql/ddl/00_schemas.sql
-- Cria todos os schemas do projeto reco.
--
-- ORDEM IMPORTA: etl deve existir antes de stg, porque stg.* tem FK para
-- etl.load_batches. Rode este arquivo ANTES de 01_staging.sql.
--
-- Execução:
--   psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -f sql/ddl/00_schemas.sql
-- =============================================================================

-- Cria o schema de controle interno do ETL (watermarks, batches, auditoria).
-- Deve existir antes de qualquer tabela stg.* que referencie etl.load_batches.
CREATE SCHEMA IF NOT EXISTS etl;

-- Cria o schema de staging: dados brutos extraídos do ERP sem transformação pesada.
-- Cada tabela aqui corresponde 1:1 a uma entidade do SQL Server.
CREATE SCHEMA IF NOT EXISTS stg;

-- Cria o schema curado: entidades consolidadas com chaves internas do projeto.
-- É a camada que alimenta o modelo de ML — dados limpos e com IDs estáveis.
CREATE SCHEMA IF NOT EXISTS cur;

-- Cria o schema de recomendação: saída dos modelos (top-N por cliente).
-- Lido pela aplicação/API para servir sugestões ao usuário final.
CREATE SCHEMA IF NOT EXISTS reco;

-- Cria o schema de ML: feature store, registro de treinos e sugestões brutas.
-- Separado de reco para isolar artefatos de modelo dos resultados finais.
CREATE SCHEMA IF NOT EXISTS ml;

-- Confirma execução com listagem dos schemas criados.
SELECT schema_name
FROM   information_schema.schemata
WHERE  schema_name IN ('etl', 'stg', 'cur', 'reco', 'ml')
ORDER  BY schema_name;
