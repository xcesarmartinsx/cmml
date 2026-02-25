/**
 * ProductRanking.jsx
 * ------------------
 * Tabela interativa com o ranking dos produtos mais vendidos/lucrativos.
 *
 * Funcionalidades:
 *   - Ordenação clicável por qualquer coluna
 *   - Filtro de campo (receita | quantidade | pedidos | clientes)
 *   - Barra visual de progresso proporcional ao 1º colocado
 *   - Indicador de produto ativo/inativo
 *
 * Props:
 *   yearFrom {number}  Ano inicial do filtro
 *   yearTo   {number}  Ano final do filtro
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

// Medalhas para os 3 primeiros colocados.
const MEDALS = { 1: '🥇', 2: '🥈', 3: '🥉' }

function fmtBRL(v) {
  if (v == null) return '—'
  return 'R$ ' + Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
}

function fmtNum(v) {
  if (v == null) return '—'
  return Number(v).toLocaleString('pt-BR')
}

// Renderiza o indicador de rank com medalha para top-3.
function RankCell({ rank }) {
  if (rank <= 3) return <span title={`#${rank}`}>{MEDALS[rank]}</span>
  return <span className="rank-num">#{rank}</span>
}

// Barra de progresso proporcional relativa ao valor máximo da lista.
function ProgressBar({ value, max, color = 'var(--green)' }) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0
  return (
    <div className="progress-bar-wrap">
      <div className="progress-bar-bg">
        <div
          className="progress-bar-fill"
          style={{ width: `${pct}%`, background: color }}
        />
      </div>
    </div>
  )
}

export default function ProductRanking({ yearFrom, yearTo }) {
  const [data, setData]       = useState([])
  const [loading, setLoading] = useState(true)

  // Campo de ordenação e métrica principal exibida na barra.
  const [sortField, setSortField] = useState('revenue')
  const [sortDir,   setSortDir]   = useState('desc')

  // Busca o ranking sempre que o período ou a métrica de ordenação mudam.
  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams({ sort: sortField, limit: 30 })
    if (yearFrom) params.set('year_from', yearFrom)
    if (yearTo)   params.set('year_to',   yearTo)

    apiFetch(`/api/business/products?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [sortField, yearFrom, yearTo])

  // Ordena localmente para mudança rápida sem re-fetch (a API já ordenou a principal).
  function handleSort(field) {
    if (field === sortField) {
      // Inverte a direção se clicou na mesma coluna.
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      // Troca o campo: ativa novo fetch automático pelo useEffect.
      setSortField(field)
      setSortDir('desc')
    }
  }

  // Ordena os dados localmente (complementa a ordenação da API).
  const sorted = [...data].sort((a, b) => {
    const av = a[sortField] ?? 0
    const bv = b[sortField] ?? 0
    return sortDir === 'asc' ? av - bv : bv - av
  })

  // Valor máximo do campo atual (para normalizar as barras de progresso).
  const maxVal = sorted.length ? Math.max(...sorted.map(d => d[sortField] || 0)) : 1

  // Ícone de seta para indicar direção de ordenação.
  function SortIcon({ field }) {
    if (sortField !== field) return <span style={{ opacity: 0.3 }}>↕</span>
    return <span style={{ color: 'var(--green)' }}>{sortDir === 'asc' ? '↑' : '↓'}</span>
  }

  // Cabeçalho clicável.
  function Th({ field, children, className = '' }) {
    return (
      <th className={className} onClick={() => handleSort(field)}>
        {children} <SortIcon field={field} />
      </th>
    )
  }

  return (
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>
      {/* Cabeçalho com toggle de métrica */}
      <div className="chart-header" style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}>
        <div>
          <div className="chart-title">Ranking de Produtos</div>
          <div className="chart-subtitle">Top 30 · clique no cabeçalho para ordenar</div>
        </div>
        {/* Seletores rápidos de métrica principal */}
        <div className="toggle-group">
          {[
            { key: 'revenue',   label: 'Receita'   },
            { key: 'qty',       label: 'Qtd'       },
            { key: 'orders',    label: 'Pedidos'   },
            { key: 'customers', label: 'Clientes'  },
          ].map(({ key, label }) => (
            <button
              key={key}
              className={`toggle-btn ${sortField === key ? 'active' : ''}`}
              onClick={() => { setSortField(key); setSortDir('desc') }}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="empty" style={{ height: 200 }}>Carregando…</div>
      ) : (
        <div style={{ overflowX: 'auto', maxHeight: 480, overflowY: 'auto' }}>
          <table className="rank-table">
            <thead>
              <tr>
                <th style={{ width: 40 }}>#</th>
                <th>Produto</th>
                {/* Coluna de barra de progresso (sem ordenação — apenas visual) */}
                <th style={{ minWidth: 100 }}>Proporção</th>
                <Th field="total_revenue" className="right">Receita</Th>
                <Th field="total_qty"     className="right">Qtd</Th>
                <Th field="order_count"   className="right">Pedidos</Th>
                <Th field="unique_customers" className="right">Clientes</Th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((row, i) => {
                const rank = i + 1
                return (
                  <tr key={row.product_id}>
                    {/* Posição no ranking */}
                    <td><RankCell rank={rank} /></td>

                    {/* Nome do produto truncado + badge de inativo */}
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, maxWidth: 280 }}>
                        <span
                          style={{
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            fontSize: 12,
                          }}
                          title={row.description}
                        >
                          {row.description || row.product_id}
                        </span>
                        {/* Badge de produto inativo */}
                        {row.active === false && (
                          <span
                            style={{
                              fontSize: 9,
                              padding: '1px 5px',
                              background: '#fee2e2',
                              color: '#991b1b',
                              borderRadius: 3,
                              flexShrink: 0,
                            }}
                          >
                            inativo
                          </span>
                        )}
                      </div>
                    </td>

                    {/* Barra de progresso proporcional */}
                    <td>
                      <ProgressBar value={row[sortField] || 0} max={maxVal} />
                    </td>

                    <td className="right mono">{fmtBRL(row.total_revenue)}</td>
                    <td className="right mono">{fmtNum(row.total_qty)}</td>
                    <td className="right mono">{fmtNum(row.order_count)}</td>
                    <td className="right mono">{fmtNum(row.unique_customers)}</td>
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
