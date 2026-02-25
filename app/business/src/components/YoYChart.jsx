/**
 * YoYChart.jsx
 * ------------
 * Gráfico de barras comparativo Ano a Ano (Year-over-Year).
 *
 * Exibe para cada ano:
 *   - Faturamento total (barras)
 *   - Variação % vs ano anterior (badge sobre cada barra)
 *   - Ticket médio (linha secundária)
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro
 *   yearTo   {number}  Ano final do filtro
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,            // colorização individual de cada barra
  LabelList,       // exibe variação % sobre cada barra
} from 'recharts'

// Formata monetário compacto.
function fmtBRL(v) {
  if (v == null) return '—'
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(2)}M`
  if (v >= 1_000)     return `R$ ${(v / 1_000).toFixed(0)}K`
  return `R$ ${v.toFixed(0)}`
}

// Tooltip customizado.
function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const rev    = payload.find(p => p.dataKey === 'revenue')?.value
  const ticket = payload.find(p => p.dataKey === 'avg_ticket')?.value
  const orders = payload.find(p => p.dataKey === 'orders')?.value
  const yoy    = payload[0]?.payload?.yoy_pct

  return (
    <div className="tooltip">
      <div className="tooltip-label">{label}</div>
      <div className="tooltip-row"><span>Faturamento</span><strong>{fmtBRL(rev)}</strong></div>
      <div className="tooltip-row"><span>Pedidos</span><strong>{Number(orders).toLocaleString('pt-BR')}</strong></div>
      <div className="tooltip-row"><span>Ticket médio</span><strong>R$ {Number(ticket).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></div>
      {yoy != null && (
        <div className="tooltip-row">
          <span>Variação YoY</span>
          <strong style={{ color: yoy >= 0 ? '#10b981' : '#f43f5e' }}>
            {yoy >= 0 ? '+' : ''}{yoy.toFixed(1)}%
          </strong>
        </div>
      )}
    </div>
  )
}

// Renderiza o label de variação % sobre cada barra.
function YoYLabel({ x, y, width, value }) {
  if (value == null) return null

  // Cor do texto: verde para positivo, vermelho para negativo.
  const color = value >= 0 ? '#059669' : '#e11d48'
  const sign  = value >= 0 ? '+' : ''

  return (
    <text
      x={x + width / 2}    // centraliza horizontalmente sobre a barra
      y={y - 6}             // posiciona acima do topo da barra
      textAnchor="middle"
      fill={color}
      fontSize={10}
      fontWeight={600}
      fontFamily="'JetBrains Mono', monospace"
    >
      {sign}{value.toFixed(1)}%
    </text>
  )
}

export default function YoYChart({ yearFrom, yearTo }) {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams()
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/yoy?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [yearFrom, yearTo])

  if (loading) return <div className="chart-card"><div className="empty">Carregando…</div></div>

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <div className="chart-title">Evolução Anual (YoY)</div>
          <div className="chart-subtitle">Barras = receita · Linha = ticket médio · % = crescimento vs ano anterior</div>
        </div>
      </div>

      {data.length === 0 ? (
        <div className="empty">Nenhum dado para o período.</div>
      ) : (
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={data} margin={{ top: 24, right: 50, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey="year" tick={{ fontSize: 12 }} />

            {/* Eixo esquerdo: faturamento */}
            <YAxis
              yAxisId="rev"
              tickFormatter={fmtBRL}
              tick={{ fontSize: 10 }}
              width={64}
            />

            {/* Eixo direito: ticket médio */}
            <YAxis
              yAxisId="ticket"
              orientation="right"
              tickFormatter={v => `R$ ${v >= 1000 ? (v/1000).toFixed(1)+'K' : v}`}
              tick={{ fontSize: 10 }}
              width={55}
            />

            <Tooltip content={<CustomTooltip />} />
            <Legend
              verticalAlign="top"
              height={28}
              formatter={v => v === 'revenue' ? 'Faturamento' : v === 'avg_ticket' ? 'Ticket médio' : v}
            />

            {/* Barras de faturamento — cor varia se YoY positivo ou negativo */}
            <Bar yAxisId="rev" dataKey="revenue" name="revenue" radius={[4, 4, 0, 0]}>
              {data.map((entry, i) => (
                <Cell
                  key={i}
                  fill={entry.yoy_pct == null || entry.yoy_pct >= 0 ? '#10b981' : '#f43f5e'}
                  fillOpacity={0.85}
                />
              ))}
              {/* Label de variação YoY sobre cada barra */}
              <LabelList dataKey="yoy_pct" content={<YoYLabel />} />
            </Bar>

            {/* Linha de ticket médio */}
            <Line
              yAxisId="ticket"
              type="monotone"
              dataKey="avg_ticket"
              name="avg_ticket"
              stroke="#8b5cf6"
              strokeWidth={2}
              dot={{ r: 3 }}
              activeDot={{ r: 5 }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
