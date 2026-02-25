/**
 * RevenueChart.jsx
 * ----------------
 * Gráfico de linha/área da evolução do faturamento ao longo do tempo.
 *
 * Suporta quatro granularidades via toggle:
 *   Diário | Mensal | Trimestral | Anual
 *
 * Cada ponto exibe:
 *   - Faturamento (área preenchida)
 *   - Pedidos (linha secundária no eixo direito)
 *   - Ticket médio (no tooltip)
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro
 *   yearTo   {number}  Ano final do filtro
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import {
  ComposedChart,   // gráfico composto: mistura Area + Line
  Area,            // área preenchida (faturamento)
  Line,            // linha simples (pedidos)
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'

// Formata valores monetários de forma compacta (R$ 1,2M, R$ 500K etc.)
function fmtBRL(v) {
  if (v == null) return '—'
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000)     return `R$ ${(v / 1_000).toFixed(0)}K`
  return `R$ ${v.toFixed(0)}`
}

// Formata números inteiros com separador de milhar.
function fmtNum(v) {
  if (v == null) return '—'
  return v.toLocaleString('pt-BR')
}

// Tooltip customizado exibido ao passar o mouse sobre um ponto.
function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  // Extrai os valores de cada série presente no ponto.
  const rev    = payload.find(p => p.dataKey === 'revenue')?.value
  const orders = payload.find(p => p.dataKey === 'orders')?.value
  const ticket = payload.find(p => p.dataKey === 'avg_ticket')?.value

  return (
    <div className="tooltip">
      <div className="tooltip-label">{label}</div>
      <div className="tooltip-row">
        <span>Faturamento</span>
        <strong>{fmtBRL(rev)}</strong>
      </div>
      {orders != null && (
        <div className="tooltip-row">
          <span>Pedidos</span>
          <strong>{fmtNum(orders)}</strong>
        </div>
      )}
      {ticket != null && (
        <div className="tooltip-row">
          <span>Ticket médio</span>
          <strong>R$ {Number(ticket).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong>
        </div>
      )}
    </div>
  )
}

export default function RevenueChart({ yearFrom, yearTo }) {
  // Estado do toggle de granularidade (diário | mensal | trimestral | anual).
  const [gran, setGran] = useState('monthly')

  // Dados retornados pela API para a granularidade e período selecionados.
  const [data, setData] = useState([])

  // Indicador de carregamento.
  const [loading, setLoading] = useState(true)

  // Busca os dados sempre que o período ou a granularidade mudam.
  useEffect(() => {
    setLoading(true)

    // Monta os query params dinamicamente.
    const params = new URLSearchParams({ granularity: gran })
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/revenue?${params}`)
      .then(r => r.json())
      .then(d => {
        setData(d)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [gran, yearFrom, yearTo])

  // Calcula o domínio do eixo Y principal para evitar zoom excessivo.
  const maxRev = data.length ? Math.max(...data.map(d => d.revenue || 0)) : 0

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <div className="chart-title">Faturamento ao Longo do Tempo</div>
          <div className="chart-subtitle">
            Área = receita · Linha = pedidos
          </div>
        </div>

        {/* Toggle de granularidade */}
        <div className="toggle-group">
          {[
            { key: 'daily',     label: 'Diário'    },
            { key: 'monthly',   label: 'Mensal'    },
            { key: 'quarterly', label: 'Trimestral'},
            { key: 'yearly',    label: 'Anual'     },
          ].map(({ key, label }) => (
            <button
              key={key}
              className={`toggle-btn ${gran === key ? 'active' : ''}`}
              onClick={() => setGran(key)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Estado de carregamento ou gráfico */}
      {loading ? (
        <div className="empty">Carregando…</div>
      ) : data.length === 0 ? (
        <div className="empty">Nenhum dado para o período selecionado.</div>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <ComposedChart
            data={data}
            margin={{ top: 8, right: 50, left: 10, bottom: gran === 'daily' ? 60 : 20 }}
          >
            <defs>
              {/* Gradiente de preenchimento da área */}
              <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#10b981" stopOpacity={0.25} />
                <stop offset="95%" stopColor="#10b981" stopOpacity={0.02} />
              </linearGradient>
            </defs>

            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />

            {/* Eixo X com os labels de período */}
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11 }}
              // Rotaciona os labels em granularidade diária para evitar sobreposição.
              angle={gran === 'daily' ? -45 : 0}
              textAnchor={gran === 'daily' ? 'end' : 'middle'}
              height={gran === 'daily' ? 70 : 30}
              // Reduz densidade de ticks para não poluir o eixo.
              interval={gran === 'daily' ? 30 : gran === 'monthly' ? 5 : 0}
            />

            {/* Eixo Y esquerdo: faturamento */}
            <YAxis
              yAxisId="rev"
              tickFormatter={fmtBRL}
              tick={{ fontSize: 10 }}
              width={60}
              domain={[0, maxRev * 1.1]}
            />

            {/* Eixo Y direito: pedidos */}
            <YAxis
              yAxisId="ord"
              orientation="right"
              tick={{ fontSize: 10 }}
              width={45}
              tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(0)}K` : v}
            />

            <Tooltip content={<CustomTooltip />} />

            <Legend
              verticalAlign="top"
              height={28}
              formatter={v => v === 'revenue' ? 'Faturamento' : 'Pedidos'}
            />

            {/* Área preenchida — faturamento (eixo esquerdo) */}
            <Area
              yAxisId="rev"
              type="monotone"
              dataKey="revenue"
              stroke="#10b981"
              strokeWidth={2}
              fill="url(#revGrad)"
              dot={false}
              activeDot={{ r: 4 }}
            />

            {/* Linha de pedidos (eixo direito) */}
            <Line
              yAxisId="ord"
              type="monotone"
              dataKey="orders"
              stroke="#3b82f6"
              strokeWidth={1.5}
              dot={false}
              activeDot={{ r: 4 }}
              strokeDasharray="4 3"
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
