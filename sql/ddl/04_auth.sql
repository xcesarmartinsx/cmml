-- =============================================================================
-- sql/ddl/04_auth.sql
-- Tabela de usuários para autenticação da API.
-- Idempotente: seguro para re-executar (CREATE ... IF NOT EXISTS).
-- =============================================================================

CREATE TABLE IF NOT EXISTS reco.users (
    user_id       BIGSERIAL    PRIMARY KEY,
    username      VARCHAR(64)  UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name     VARCHAR(255),
    role          VARCHAR(20)  NOT NULL DEFAULT 'commercial'
                  CHECK (role IN ('admin', 'commercial')),
    is_active     BOOLEAN      DEFAULT TRUE,
    created_at    TIMESTAMPTZ  DEFAULT now(),
    updated_at    TIMESTAMPTZ  DEFAULT now()
);
