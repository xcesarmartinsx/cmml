/**
 * CustomerShareChart.jsx
 * ----------------------
 * Gráfico de pizza (donut) exibindo a participação percentual de cada cliente
 * no faturamento total do período selecionado.
 *
 * Exibe os 9 maiores clientes individualmente e agrupa o restante como "Outros".
 * Destaca o maior cliente no cabeçalho do card.
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro (null = sem filtro)
 *   yearTo   {number}  Ano final do filtro (null = sem filtro)
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import {
  PieChart, Pie, Cell,
  Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'

// ── Paleta de cores para as fatias individuais ────────────────────────────────
// A primeira cor (vermelho) é aplicada ao maior cliente (normalmente BALCÃO).
const SLICE_COLORS = [
  '#ef4444', // 1º — vermelho (maior cliente)
  '#3b82f6', // 2º — azul
  '#f59e0b', // 3º — âmbar
  '#8b5cf6', // 4º — roxo
  '#06b6d4', // 5º — ciano
  '#ec4899', // 6º — rosa
  '#84cc16', // 7º — verde-limão
  '#f97316', // 8º — laranja
  '#14b8a6', // 9º — teal
]
// Cor neutra para a fatia "Outros" (clientes agrupados fora do top N).
const OTHERS_COLOR = '#94a3b8'

// ── Formatadores ──────────────────────────────────────────────────────────────

function fmtBRL(v) {
  if (v == null) return '—'
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(2)}M`
  if (v >= 1_000)     return `R$ ${(v / 1_000).toFixed(0)}K`
  return 'R$ ' + Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
}

// ── Tooltip personalizado ─────────────────────────────────────────────────────

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0].payload

  return (
    <div style={{
      background: 'var(--card)',
      border: '1px solid var(--border)',
      borderRadius: 8,
      padding: '10px 14px',
      fontSize: 12,
      maxWidth: 240,
    }}>
      {/* Nome do cliente */}
      <div style={{ fontWeight: 600, marginBottom: 6, color: 'var(--text)', wordBreak: 'break-word' }}>
        {d.customer_name}
      </div>

      {/* Receita */}
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, color: 'var(--text-muted)' }}>
        <span>Receita</span>
        <strong style={{ color: 'var(--text)' }}>{fmtBRL(d.revenue)}</strong>
      </div>

      {/* Share percentual */}
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, color: 'var(--text-muted)' }}>
        <span>Share</span>
        <strong style={{ color: payload[0].fill }}>{d.share_pct?.toFixed(2)}%</strong>
      </div>
    </div>
  )
}

// ── Legenda personalizada ─────────────────────────────────────────────────────

function CustomLegend({ payload }) {
  return (
    <ul style={{ listStyle: 'none', padding: 0, margin: 0, fontSize: 11 }}>
      {payload.map((entry, i) => (
        <li
          key={i}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            marginBottom: 5,
          }}
        >
          {/* Quadrado colorido da fatia */}
          <span style={{
            width: 10, height: 10,
            borderRadius: 2,
            background: entry.color,
            flexShrink: 0,
          }} />

          {/* Nome do cliente (truncado se muito longo) */}
          <span
            style={{
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              maxWidth: 150,
              color: 'var(--text)',
            }}
            title={entry.value}
          >
            {entry.value}
          </span>

          {/* Percentual alinhado à direita */}
          <span style={{ marginLeft: 'auto', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
            {entry.payload.share_pct?.toFixed(1)}%
          </span>
        </li>
      ))}
    </ul>
  )
}

// ── Componente principal ──────────────────────────────────────────────────────

export default function CustomerShareChart({ yearFrom, yearTo }) {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  // Busca o share de clientes sempre que o período de filtro muda.
  useEffect(() => {
    setLoading(true)

    const params = new URLSearchParams({ top: 9 })
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/customer-share?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [yearFrom, yearTo])

  // O maior cliente é sempre o primeiro item (API ordena por receita DESC).
  const topCustomer = data.find(d => !d.is_others)

  return (
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>

      {/* Cabeçalho do card */}
      <div
        className="chart-header"
        style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}
      >
        <div>
          <div className="chart-title">Participação por Cliente</div>
          <div className="chart-subtitle">Share de receita — top 9 clientes + Outros</div>
        </div>

        {/* Badge de destaque do maior cliente */}
        {topCustomer && (
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 2 }}>
              MAIOR CLIENTE
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: '#ef4444', lineHeight: 1 }}>
              {topCustomer.share_pct?.toFixed(1)}%
            </div>
            <div
              style={{
                fontSize: 10, color: 'var(--text-muted)',
                maxWidth: 120, overflow: 'hidden',
                textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }}
              title={topCustomer.customer_name}
            >
              {topCustomer.customer_name}
            </div>
          </div>
        )}
      </div>

      {/* Estado de carregamento */}
      {loading ? (
        <div className="empty" style={{ height: 280 }}>Carregando…</div>
      ) : data.length === 0 ? (
        <div className="empty" style={{ height: 200 }}>Sem dados para o período.</div>
      ) : (
        <div style={{ padding: '16px 20px' }}>
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={data}
                dataKey="revenue"
                nameKey="customer_name"
                // Centraliza o donut à esquerda para dar espaço à legenda
                cx="38%"
                cy="50%"
                innerRadius={60}
                outerRadius={110}
                paddingAngle={2}
              >
                {data.map((entry, i) => (
                  <Cell
                    key={i}
                    // "Outros" recebe cor neutra; demais recebem cores da paleta
                    fill={entry.is_others ? OTHERS_COLOR : SLICE_COLORS[i % SLICE_COLORS.length]}
                    stroke="none"
                  />
                ))}
              </Pie>

              <Tooltip content={<CustomTooltip />} />

              {/* Legenda vertical à direita com share% em cada item */}
              <Legend
                layout="vertical"
                align="right"
                verticalAlign="middle"
                content={<CustomLegend />}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
