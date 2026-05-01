-- sql/migrations/001_add_user_role.sql
-- Migration idempotente: adicionar coluna role em reco.users
-- =============================================================================

ALTER TABLE reco.users
  ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'commercial';

ALTER TABLE reco.users
  DROP CONSTRAINT IF EXISTS users_role_check;

ALTER TABLE reco.users
  ADD CONSTRAINT users_role_check CHECK (role IN ('admin', 'commercial'));

-- Garantir que o usuário admin existente tenha role = 'admin'
UPDATE reco.users SET role = 'admin' WHERE username = 'admin';
