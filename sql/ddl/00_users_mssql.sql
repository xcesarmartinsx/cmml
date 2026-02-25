-- =============================================================================
-- sql/ddl/00_users_mssql.sql
-- Cria usuário SQL Server com menor privilégio para o ETL do CMML.
--
-- CONTEXTO:
--   O pipeline ETL precisa apenas de SELECT nas tabelas do ERP GP:
--     - dbo.MOVIMENTO_DIA (vendas)
--     - dbo.ENTIDADE (clientes)
--     - dbo.PRODUTO (produtos)
--     - dbo.ENTIDADE_LOJA (lojas/filiais)
--
--   Atualmente o ETL usa o usuário 'sa' (super-administrador), o que viola
--   o princípio de menor privilégio. Este script cria o usuário cmml_etl
--   com permissões mínimas.
--
-- EXECUÇÃO:
--   1. Conectar ao SQL Server como 'sa' ou outro sysadmin:
--      sqlcmd -S localhost,1433 -U sa -P '<SA_PASSWORD>' -i sql/ddl/00_users_mssql.sql
--
--   2. Ou via Azure Data Studio / SSMS conectado ao container sqlserver_gp.
--
--   3. Após criar, atualizar .env:
--      MSSQL_USER=cmml_etl
--      MSSQL_PASSWORD=<senha definida abaixo>
--
-- IMPORTANTE: Substitua 'CHANGE_ME_ETL' por uma senha segura antes de executar.
-- =============================================================================

USE master;
GO

-- Cria o login no nível do servidor (se não existir)
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'cmml_etl')
BEGIN
    CREATE LOGIN cmml_etl WITH PASSWORD = 'CHANGE_ME_ETL',
        DEFAULT_DATABASE = GP_CASADASREDES,
        CHECK_EXPIRATION = OFF,
        CHECK_POLICY = ON;
    PRINT 'Login cmml_etl created.';
END
ELSE
    PRINT 'Login cmml_etl already exists, skipping.';
GO

-- Alterna para o banco do ERP
USE GP_CASADASREDES;
GO

-- Cria o usuário no banco de dados (se não existir)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'cmml_etl')
BEGIN
    CREATE USER cmml_etl FOR LOGIN cmml_etl;
    PRINT 'User cmml_etl created in GP_CASADASREDES.';
END
ELSE
    PRINT 'User cmml_etl already exists in GP_CASADASREDES, skipping.';
GO

-- Concede SELECT apenas nas tabelas necessárias para o ETL
GRANT SELECT ON dbo.MOVIMENTO_DIA TO cmml_etl;
GRANT SELECT ON dbo.ENTIDADE TO cmml_etl;
GRANT SELECT ON dbo.PRODUTO TO cmml_etl;
GRANT SELECT ON dbo.ENTIDADE_LOJA TO cmml_etl;
GO

PRINT 'Permissions granted to cmml_etl: SELECT on MOVIMENTO_DIA, ENTIDADE, PRODUTO, ENTIDADE_LOJA.';
GO
