/**
 * TopCustomers.jsx
 * ----------------
 * Lista os maiores clientes por faturamento, exibindo nome e telefone mascarado.
 *
 * Dados enriquecidos via JOIN com stg.customers no endpoint da API:
 *   customer_name → stg.customers.name
 *   phone         → stg.customers.mobile (prioritário) ou stg.customers.phone
 *
 * Telefone é mascarado para proteger PII: (**) *****-XXXX
 * Se o telefone não estiver cadastrado na base, exibe "—".
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro (null = sem filtro)
 *   yearTo   {number}  Ano final do filtro (null = sem filtro)
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

// ── Formatadores ──────────────────────────────────────────────────────────────

function fmtBRL(v) {
  if (v == null) return '—'
  return 'R$ ' + Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
}

function fmtNum(v) {
  if (v == null) return '—'
  return Number(v).toLocaleString('pt-BR')
}

/**
 * Mascara o número de telefone para proteger PII.
 * Exibe apenas os 4 últimos dígitos: (##) *****-XXXX
 * Retorna '—' se o valor for nulo ou vazio.
 */
function maskPhone(raw) {
  if (!raw || !raw.trim()) return '—'

  const digits = raw.replace(/\D/g, '')

  if (digits.length < 6) return '—'

  const lastFour = digits.slice(-4)
  return `(**) *****-${lastFour}`
}

// ── Ícone de telefone SVG inline ──────────────────────────────────────────────
function PhoneIcon() {
  return (
    <svg
      width="12" height="12" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round"
      strokeLinejoin="round" style={{ flexShrink: 0, opacity: 0.6 }}
    >
      <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 12a19.79 19.79 0 0 1-3-8.59A2 2 0 0 1 3.72 1.18h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L7.91 9a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7a2 2 0 0 1 1.72 2.01z" />
    </svg>
  )
}

// ── Componente principal ──────────────────────────────────────────────────────

export default function TopCustomers({ yearFrom, yearTo }) {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  // Busca o ranking sempre que o período de filtro muda.
  useEffect(() => {
    setLoading(true)

    const params = new URLSearchParams({ limit: 10 })
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/top-customers?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [yearFrom, yearTo])

  // Valor máximo de receita para normalizar as barras de progresso.
  const maxRev = data.length ? Math.max(...data.map(d => d.total_revenue || 0)) : 1

  return (
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>

      {/* Cabeçalho do card */}
      <div
        className="chart-header"
        style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}
      >
        <div>
          <div className="chart-title">Top Clientes</div>
          <div className="chart-subtitle">Por faturamento no período selecionado</div>
        </div>
      </div>

      {/* Estado de carregamento */}
      {loading ? (
        <div className="empty" style={{ height: 200 }}>Carregando…</div>
      ) : data.length === 0 ? (
        <div className="empty" style={{ height: 160 }}>Sem dados para o período.</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table className="rank-table">
            <thead>
              <tr>
                {/* Coluna de posição */}
                <th style={{ width: 32 }}>#</th>

                {/* Coluna de nome do cliente */}
                <th>Cliente</th>

                {/* Coluna de telefone */}
                <th>Telefone</th>

                {/* Coluna de barra de proporção */}
                <th style={{ minWidth: 90 }}>Proporção</th>

                {/* Colunas de métricas */}
                <th className="right">Receita</th>
                <th className="right">Pedidos</th>
                <th className="right">Ticket</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => {
                // Telefone formatado (exibe '—' se NULL ou vazio).
                const phoneDisplay = maskPhone(row.phone)

                return (
                  <tr key={i}>

                    {/* Posição ordinal */}
                    <td>
                      <span className="rank-num" style={{ fontSize: 11 }}>
                        #{i + 1}
                      </span>
                    </td>

                    {/* Nome do cliente */}
                    <td>
                      <span
                        className="mono"
                        style={{
                          fontSize: 12,
                          // Trunca nomes muito longos com reticências
                          display: 'block',
                          maxWidth: 220,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                        }}
                        title={row.customer_name || '—'}
                      >
                        {row.customer_name || '—'}
                      </span>
                    </td>

                    {/* Telefone com ícone indicativo */}
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                        {phoneDisplay !== '—' && <PhoneIcon />}
                        <span
                          className="mono"
                          style={{ fontSize: 12, color: phoneDisplay === '—' ? 'var(--text-muted)' : 'var(--text)' }}
                        >
                          {phoneDisplay}
                        </span>
                      </div>
                    </td>

                    {/* Barra de progresso proporcional ao 1º colocado */}
                    <td>
                      <div className="progress-bar-wrap">
                        <div className="progress-bar-bg">
                          <div
                            className="progress-bar-fill"
                            style={{
                              width: `${Math.min((row.total_revenue / maxRev) * 100, 100)}%`,
                              background: 'var(--purple)',
                            }}
                          />
                        </div>
                      </div>
                    </td>

                    {/* Métricas numéricas */}
                    <td className="right mono">{fmtBRL(row.total_revenue)}</td>
                    <td className="right mono">{fmtNum(row.total_orders)}</td>
                    <td className="right mono">{fmtBRL(row.avg_ticket)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
