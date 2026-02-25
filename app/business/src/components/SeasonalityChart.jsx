/**
 * SeasonalityChart.jsx
 * --------------------
 * Exibe a sazonalidade mensal: faturamento médio por mês do ano (Jan–Dez).
 *
 * Usa a média do faturamento mensal ao longo dos anos do período filtrado,
 * eliminando o viés de ter mais ou menos anos na amostra.
 *
 * Destaca automaticamente o mês de maior e menor faturamento médio.
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro
 *   yearTo   {number}  Ano final do filtro
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
} from 'recharts'

function fmtBRL(v) {
  if (v == null) return '—'
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(2)}M`
  if (v >= 1_000)     return `R$ ${(v / 1_000).toFixed(0)}K`
  return `R$ ${v.toFixed(0)}`
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const avg   = payload[0]?.value
  const years = payload[0]?.payload?.years_count

  return (
    <div className="tooltip">
      <div className="tooltip-label">{label}</div>
      <div className="tooltip-row"><span>Média mensal</span><strong>{fmtBRL(avg)}</strong></div>
      <div className="tooltip-row"><span>Anos na amostra</span><strong>{years}</strong></div>
    </div>
  )
}

export default function SeasonalityChart({ yearFrom, yearTo }) {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams()
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/seasonality?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [yearFrom, yearTo])

  if (loading) return <div className="chart-card"><div className="empty">Carregando…</div></div>

  // Encontra o mês de maior e menor faturamento para destacar nas barras.
  const maxVal = data.length ? Math.max(...data.map(d => d.avg_revenue || 0)) : 0
  const minVal = data.length ? Math.min(...data.map(d => d.avg_revenue || 0)) : 0

  // Média geral: linha de referência horizontal.
  const overallAvg = data.length
    ? data.reduce((s, d) => s + (d.avg_revenue || 0), 0) / data.length
    : 0

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <div className="chart-title">Sazonalidade Mensal</div>
          <div className="chart-subtitle">Faturamento médio por mês · linha tracejada = média geral</div>
        </div>
      </div>

      {data.length === 0 ? (
        <div className="empty">Nenhum dado para o período.</div>
      ) : (
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={data} margin={{ top: 8, right: 12, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey="month_name" tick={{ fontSize: 11 }} />
            <YAxis tickFormatter={fmtBRL} tick={{ fontSize: 10 }} width={58} />
            <Tooltip content={<CustomTooltip />} />

            {/* Linha de referência da média geral */}
            <ReferenceLine
              y={overallAvg}
              stroke="#94a3b8"
              strokeDasharray="4 3"
              strokeWidth={1.5}
            />

            <Bar dataKey="avg_revenue" radius={[4, 4, 0, 0]}>
              {data.map((entry, i) => {
                // Destaca mês de pico (verde escuro) e vale (vermelho claro).
                const isMax = entry.avg_revenue === maxVal
                const isMin = entry.avg_revenue === minVal
                return (
                  <Cell
                    key={i}
                    fill={isMax ? '#059669' : isMin ? '#fca5a5' : '#10b981'}
                    fillOpacity={isMax ? 1 : isMin ? 0.9 : 0.75}
                  />
                )
              })}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}

      {/* Legenda de destaques */}
      {data.length > 0 && (
        <div style={{ display: 'flex', gap: 16, marginTop: 10, fontSize: 11, color: 'var(--text-muted)', flexWrap: 'wrap' }}>
          <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 2, background: '#059669' }} />
            Pico: {data.find(d => d.avg_revenue === maxVal)?.month_name}
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 2, background: '#fca5a5' }} />
            Vale: {data.find(d => d.avg_revenue === minVal)?.month_name}
          </span>
        </div>
      )}
    </div>
  )
}
