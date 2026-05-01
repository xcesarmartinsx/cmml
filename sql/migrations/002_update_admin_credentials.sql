-- sql/migrations/002_update_admin_credentials.sql
-- Migration idempotente: promove 'cesarmartins' a admin e desativa 'admin' genérico.
-- Segurança: apenas o bcrypt hash é armazenado, nunca a senha em texto plano.
-- =============================================================================

-- Promove cesarmartins para admin e redefine senha
UPDATE reco.users
SET
    role          = 'admin',
    password_hash = '$2b$12$VqRiHqDPxUZi6lne77AnI..kP1lrOa.FYOcfowQbX300cTigC38jK',
    updated_at    = now()
WHERE username = 'cesarmartins';

-- Desativa usuário 'admin' genérico (mantido para histórico de auditoria)
UPDATE reco.users
SET is_active = FALSE, updated_at = now()
WHERE username = 'admin';
