/**
 * GeographicTable.jsx
 * -------------------
 * Tabela de distribuição de faturamento, pedidos e clientes por estado (UF).
 *
 * Inclui barra visual de share percentual de receita para cada estado.
 * Dados provenientes de dw.mart_state_summary (acumulado total, sem filtro de período).
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

function fmtBRL(v) {
  if (v == null) return '—'
  return 'R$ ' + Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
}

function fmtNum(v) {
  if (v == null) return '—'
  return Number(v).toLocaleString('pt-BR')
}

export default function GeographicTable() {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  // Busca os dados geográficos uma única vez (sem filtro de período disponível no mart).
  useEffect(() => {
    apiFetch('/api/business/geography')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  // Máximo de share para normalizar as barras (normalmente 100% mas pode variar com N/D).
  const maxShare = data.length ? Math.max(...data.map(d => d.revenue_share_pct || 0)) : 1

  return (
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>
      <div className="chart-header" style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}>
        <div>
          <div className="chart-title">Distribuição Geográfica</div>
          <div className="chart-subtitle">Faturamento acumulado total por estado (UF)</div>
        </div>
      </div>

      {loading ? (
        <div className="empty" style={{ height: 200 }}>Carregando…</div>
      ) : (
        <div className="geo-table-wrap">
          <table className="geo-table">
            <thead>
              <tr>
                <th>Estado</th>
                <th>Share (%)</th>
                <th style={{ textAlign: 'right' }}>Faturamento</th>
                <th style={{ textAlign: 'right' }}>Pedidos</th>
                <th style={{ textAlign: 'right' }}>Clientes</th>
                <th style={{ textAlign: 'right' }}>Ticket Médio</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={row.state}>
                  {/* Tag colorida do estado */}
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {/* Número de posição */}
                      <span style={{ fontSize: 10, color: 'var(--text-muted)', minWidth: 16 }}>
                        {i + 1}
                      </span>
                      <span className="state-tag">{row.state}</span>
                    </div>
                  </td>

                  {/* Barra de share percentual */}
                  <td>
                    <div className="progress-bar-wrap">
                      <div className="progress-bar-bg">
                        <div
                          className="progress-bar-fill"
                          style={{
                            width: `${Math.min((row.revenue_share_pct / maxShare) * 100, 100)}%`,
                          }}
                        />
                      </div>
                      <span className="progress-label">{row.revenue_share_pct?.toFixed(1)}%</span>
                    </div>
                  </td>

                  <td style={{ textAlign: 'right' }} className="mono">
                    {fmtBRL(row.total_revenue)}
                  </td>
                  <td style={{ textAlign: 'right' }} className="mono">
                    {fmtNum(row.total_orders)}
                  </td>
                  <td style={{ textAlign: 'right' }} className="mono">
                    {fmtNum(row.total_customers)}
                  </td>
                  <td style={{ textAlign: 'right' }} className="mono">
                    {row.avg_ticket != null
                      ? 'R$ ' + Number(row.avg_ticket).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
                      : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
