import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

export default function ConversionMetrics() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    apiFetch('/api/recommendations/feedback/summary')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading) return <div style={{ padding: 16, color: 'var(--muted)', fontSize: 13 }}>Carregando metricas...</div>
  if (!data || !data.evaluated) return null

  const cards = [
    { label: 'Taxa de Conversao', value: `${data.conversion_rate || 0}%`, color: 'var(--green)', sub: `${data.converted || 0} de ${data.evaluated || 0} avaliadas` },
    { label: 'Modelo A', value: `${data.modelo_a_rate || 0}%`, color: 'var(--blue)', sub: `${data.modelo_a_converted || 0} / ${data.modelo_a_evaluated || 0}` },
    { label: 'Modelo B', value: `${data.modelo_b_rate || 0}%`, color: 'var(--purple)', sub: `${data.modelo_b_converted || 0} / ${data.modelo_b_evaluated || 0}` },
    { label: 'Valor Convertido', value: data.total_converted_value > 0 ? `R$ ${Number(data.total_converted_value).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}` : 'R$ 0', color: 'var(--amber)', sub: `${data.pending || 0} pendentes` },
  ]

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 16 }}>
      {cards.map(c => (
        <div key={c.label} className="chart-card" style={{ padding: 16, borderLeft: `3px solid ${c.color}` }}>
          <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 4 }}>{c.label}</div>
          <div style={{ fontSize: 22, fontWeight: 700, color: c.color }}>{c.value}</div>
          <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>{c.sub}</div>
        </div>
      ))}
    </div>
  )
}
