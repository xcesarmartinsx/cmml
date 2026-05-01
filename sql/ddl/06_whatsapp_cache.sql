-- =============================================================================
-- sql/ddl/06_whatsapp_cache.sql
-- Cache de validacao WhatsApp via Evolution API.
--
-- Idempotente: seguro para re-executar.
-- =============================================================================

CREATE TABLE IF NOT EXISTS reco.whatsapp_cache (
    phone_number    TEXT        NOT NULL PRIMARY KEY,
    has_whatsapp    BOOLEAN     NOT NULL,
    jid             TEXT,
    validated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_whatsapp_cache_validated
    ON reco.whatsapp_cache (validated_at);

COMMENT ON TABLE reco.whatsapp_cache IS
    'Cache de resultados de validacao WhatsApp via Evolution API. TTL: 30 dias.';
