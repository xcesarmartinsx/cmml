-- =============================================================================
-- sql/ddl/05_feedback.sql
-- Ciclo de retroalimentacao: rastreamento de conversao de ofertas.
--
-- reco.offer_outcomes : resultado de cada oferta (converteu ou nao)
-- reco.feedback_runs  : log de execucoes do cross-reference automatico
--
-- Idempotente: seguro para re-executar (CREATE ... IF NOT EXISTS).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- reco.offer_outcomes
-- Resultado de cada oferta: converteu (virou venda) ou nao.
-- Preenchido automaticamente pelo cross-reference (ml/feedback_loop.py)
-- ou manualmente via import de planilha Excel.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reco.offer_outcomes (
    outcome_id        BIGSERIAL PRIMARY KEY,
    offer_id          BIGINT NOT NULL REFERENCES reco.offers(offer_id),
    converted         BOOLEAN NOT NULL DEFAULT FALSE,
    conversion_date   DATE NULL,
    conversion_source TEXT NOT NULL CHECK (conversion_source IN ('automatic','manual')),
    order_id_src      TEXT NULL,
    quantity          NUMERIC(18,4) NULL,
    total_value       NUMERIC(18,2) NULL,
    notes             TEXT NULL,
    matched_by        TEXT NULL,
    matched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (offer_id, order_id_src)
);

CREATE INDEX IF NOT EXISTS idx_offer_outcomes_offer
    ON reco.offer_outcomes(offer_id);

CREATE INDEX IF NOT EXISTS idx_offer_outcomes_converted
    ON reco.offer_outcomes(converted, conversion_source);

-- ---------------------------------------------------------------------------
-- reco.feedback_runs
-- Log de execucoes do engine de cross-reference (ml/feedback_loop.py).
-- Cada linha = uma execucao do motor de matching.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reco.feedback_runs (
    run_id                 BIGSERIAL PRIMARY KEY,
    offer_batch_id         UUID NULL,
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at            TIMESTAMPTZ NULL,
    status                 TEXT NOT NULL DEFAULT 'running',
    conversion_window_days INT NOT NULL DEFAULT 30,
    offers_evaluated       INT NOT NULL DEFAULT 0,
    offers_converted       INT NOT NULL DEFAULT 0,
    conversion_rate        NUMERIC(5,2) NULL,
    triggered_by           TEXT NOT NULL,
    error_message          TEXT NULL
);
