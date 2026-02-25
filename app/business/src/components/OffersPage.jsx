/**
 * OffersPage.jsx
 * ---------------
 * Página de Ofertas — agrega todos os relatórios relacionados ao
 * pipeline de recomendações ML.
 *
 * Seções:
 *   1. KPI Cards       — total de ofertas, clientes alcançados, score médio, % já compraram
 *   2. Funil de Scores — distribuição de ofertas por faixa de probabilidade (Modelo A vs B)
 *   3. Ciclo de Vida   — top 50 produtos nas ofertas com dados de recompra
 *   4. Lista de Ofertas — tabela interativa com filtros e paginação
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import OffersTable           from './OffersTable.jsx'
import ProductLifecycleTable from './ProductLifecycleTable.jsx'

// ── KPI Card simples (inline, sem dependência do componente global) ────────────
function StatCard({ label, value, sub, color = 'var(--purple)' }) {
  return (
    <div className="kpi-card" style={{ '--kpi-color': color }}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value" style={{ fontSize: 22 }}>{value}</div>
      {sub && <div className="kpi-sub">{sub}</div>}
    </div>
  )
}

// ── Funil de scores ───────────────────────────────────────────────────────────
function ScoreFunnel({ funnel, nModeloA, nModeloB }) {
  const maxTotal = Math.max(...funnel.map(f => f.total), 1)

  return (
    <div className="chart-card" style={{ padding: '20px 24px' }}>
      <div className="chart-title" style={{ marginBottom: 4 }}>Funil de Oportunidades</div>
      <div className="chart-subtitle" style={{ marginBottom: 18 }}>
        Distribuição de ofertas por faixa de probabilidade
        · <span style={{ color: 'var(--blue)' }}>Modelo A: {nModeloA?.toLocaleString('pt-BR')}</span>
        &nbsp;·&nbsp;
        <span style={{ color: 'var(--purple)' }}>Modelo B: {nModeloB?.toLocaleString('pt-BR')}</span>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        {funnel.map(bracket => (
          <div key={bracket.label} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>

            {/* Label da faixa */}
            <div style={{ width: 52, textAlign: 'right', fontSize: 12, fontWeight: 700, color: bracket.color, flexShrink: 0 }}>
              {bracket.label}
            </div>

            {/* Barra composta: Modelo A + Modelo B */}
            <div style={{ flex: 1, height: 22, background: 'var(--bg)', borderRadius: 4, overflow: 'hidden', display: 'flex' }}>
              {/* Modelo A */}
              <div style={{
                width:      `${(bracket.modelo_a / maxTotal) * 100}%`,
                background: 'var(--blue)',
                opacity:    0.85,
                transition: 'width 0.4s ease',
              }} title={`Modelo A: ${bracket.modelo_a?.toLocaleString('pt-BR')}`} />
              {/* Modelo B */}
              <div style={{
                width:      `${(bracket.modelo_b / maxTotal) * 100}%`,
                background: 'var(--purple)',
                opacity:    0.75,
                transition: 'width 0.4s ease',
              }} title={`Modelo B: ${bracket.modelo_b?.toLocaleString('pt-BR')}`} />
            </div>

            {/* Números */}
            <div style={{ width: 150, fontSize: 11, color: 'var(--text-muted)', flexShrink: 0 }}>
              <span style={{ color: 'var(--text)', fontWeight: 600 }}>
                {bracket.total?.toLocaleString('pt-BR')}
              </span>
              {' '}ofertas
              <span style={{ marginLeft: 6, fontSize: 10 }}>({bracket.pct}%)</span>
            </div>

            {/* Detalhe A vs B */}
            <div style={{ width: 170, fontSize: 10, color: 'var(--text-muted)', flexShrink: 0 }}>
              <span style={{ color: 'var(--blue)' }}>A: {bracket.modelo_a?.toLocaleString('pt-BR')}</span>
              {' · '}
              <span style={{ color: 'var(--purple)' }}>B: {bracket.modelo_b?.toLocaleString('pt-BR')}</span>
            </div>

          </div>
        ))}
      </div>

      {/* Legenda */}
      <div style={{ display: 'flex', gap: 16, marginTop: 14, paddingTop: 12, borderTop: '1px solid var(--border)' }}>
        <span style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 5 }}>
          <span style={{ width: 12, height: 12, borderRadius: 2, background: 'var(--blue)', display: 'inline-block' }} />
          Modelo A (LightGBM)
        </span>
        <span style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 5 }}>
          <span style={{ width: 12, height: 12, borderRadius: 2, background: 'var(--purple)', display: 'inline-block' }} />
          Modelo B (SVD Colaborativo)
        </span>
      </div>
    </div>
  )
}

// ── Componente principal ──────────────────────────────────────────────────────
export default function OffersPage() {
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    apiFetch('/api/recommendations/summary')
      .then(r => r.json())
      .then(d => { setSummary(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const fmtDate = iso =>
    iso ? new Date(iso).toLocaleDateString('pt-BR', {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    }) : '—'

  return (
    <main className="main">

      {/* ── 1. KPI Cards ─────────────────────────────────────────────────── */}
      <section>
        <h2 className="section-title offers-accent">
          Resumo do Batch Atual
          {summary?.generated_at && (
            <span style={{ fontSize: 11, fontWeight: 400, color: 'var(--text-muted)', marginLeft: 8 }}>
              Gerado em {fmtDate(summary.generated_at)}
            </span>
          )}
        </h2>

        {loading ? (
          <div style={{ display: 'flex', gap: 14 }}>
            {[...Array(4)].map((_, i) => (
              <div key={i} className="kpi-card" style={{ flex: 1, '--kpi-color': 'var(--purple)', minHeight: 90 }}>
                <div style={{ height: 12, background: 'var(--border)', borderRadius: 4, width: '60%', marginBottom: 8 }} />
                <div style={{ height: 22, background: 'var(--border)', borderRadius: 4, width: '80%' }} />
              </div>
            ))}
          </div>
        ) : (
          <div className="kpi-grid">
            <StatCard
              label="Total de Ofertas"
              value={summary?.total_offers?.toLocaleString('pt-BR') ?? '—'}
              sub={`Modelo A: ${summary?.n_modelo_a?.toLocaleString('pt-BR') ?? '—'} · B: ${summary?.n_modelo_b?.toLocaleString('pt-BR') ?? '—'}`}
              color="var(--purple)"
            />
            <StatCard
              label="Clientes Alcançados"
              value={summary?.n_customers?.toLocaleString('pt-BR') ?? '—'}
              sub="Com ao menos 1 oferta"
              color="var(--blue)"
            />
            <StatCard
              label="Score Médio"
              value={summary?.avg_score_pct != null ? `${summary.avg_score_pct}%` : '—'}
              sub="Probabilidade média de compra"
              color="#16a34a"
            />
            <StatCard
              label="Já Compraram o Produto"
              value={summary?.pct_bought_before != null ? `${summary.pct_bought_before}%` : '—'}
              sub="Das ofertas, cliente tem histórico"
              color="var(--amber)"
            />
          </div>
        )}
      </section>

      {/* ── 2. Funil de Scores ───────────────────────────────────────────── */}
      {!loading && summary?.funnel?.length > 0 && (
        <section>
          <h2 className="section-title offers-accent">Distribuição por Score</h2>
          <ScoreFunnel
            funnel={summary.funnel}
            nModeloA={summary.n_modelo_a}
            nModeloB={summary.n_modelo_b}
          />
        </section>
      )}

      {/* ── 3. Ciclo de Vida dos Produtos ────────────────────────────────── */}
      <section>
        <h2 className="section-title offers-accent">Ciclo de Vida dos Produtos</h2>
        <ProductLifecycleTable />
      </section>

      {/* ── 4. Lista de Ofertas ──────────────────────────────────────────── */}
      <section>
        <h2 className="section-title offers-accent">Lista de Ofertas</h2>
        <OffersTable />
      </section>

    </main>
  )
}
