-- =============================================================================
-- sql/ddl/07_experiments.sql
-- Schema para A/B testing e experiment assignment de clientes.
--
-- reco.experiment_assignments : assignment de clientes a grupos experimentais
--
-- Proposta documentada na auditoria de teste de hipoteses (2026-04-21).
-- Integracao com generate_offers.py sera implementada em fase futura.
--
-- Idempotente: seguro para re-executar (CREATE ... IF NOT EXISTS).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- reco.experiment_assignments
-- Atribui cada cliente a um grupo experimental para A/B testing.
-- Cada par (experiment_name, customer_id) e unico: um cliente pertence a
-- exatamente um grupo por experimento.
--
-- Grupos tipicos:
--   'treatment_a'  : recebe ofertas do Modelo A
--   'treatment_b'  : recebe ofertas do Modelo B
--   'control'      : nao recebe ofertas (baseline causal)
--   'holdout'      : recebe ofertas aleatorias (baseline de ranking)
--
-- Integracao futura com generate_offers.py:
--   1. Antes de gerar ofertas, consultar:
--        SELECT customer_id, group_name
--        FROM reco.experiment_assignments
--        WHERE experiment_name = %s
--   2. Filtrar clientes conforme assignment:
--        - control     => excluir do batch de ofertas
--        - treatment_a => gerar apenas com Modelo A
--        - treatment_b => gerar apenas com Modelo B
--   3. Medir ATE (Average Treatment Effect):
--        ATE = taxa_conversao(treatment) - taxa_conversao(control)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reco.experiment_assignments (
    assignment_id   BIGSERIAL    PRIMARY KEY,
    experiment_name TEXT         NOT NULL,
    customer_id     BIGINT       NOT NULL,
    group_name      TEXT         NOT NULL,
    assigned_at     TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (experiment_name, customer_id)
);

-- Indice para buscar todos os clientes de um experimento por grupo.
CREATE INDEX IF NOT EXISTS idx_experiment_assignments_experiment_group
    ON reco.experiment_assignments (experiment_name, group_name);
