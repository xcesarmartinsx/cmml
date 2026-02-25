/**
 * ProductLifecycleTable.jsx
 * --------------------------
 * Tabela dos 50 principais produtos nas ofertas do batch atual,
 * enriquecida com dados de ciclo de vida (intervalo médio de recompra)
 * e valor médio unitário.
 *
 * Colunas: # | Produto | Tipo | Ciclo Médio | Taxa Recompra | Compradores | Ofertas | Preço Médio | Score Médio
 *
 * Todas as colunas são ordenáveis por clique no cabeçalho.
 * O cabeçalho também oferece botões de ordenação rápida por Valor (maior→menor / menor→maior).
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

// ── Badge de tipo de ciclo de vida ────────────────────────────────────────────
const LIFECYCLE_STYLE = {
  'Consumível':    { bg: '#eff6ff', color: '#1d4ed8', dot: '#3b82f6' },
  'Sazonal':       { bg: '#fffbeb', color: '#92400e', dot: '#f59e0b' },
  'Durável':       { bg: '#f0fdf4', color: '#15803d', dot: '#10b981' },
  'Sem histórico': { bg: '#f8fafc', color: '#64748b', dot: '#94a3b8' },
}

function LifecycleBadge({ type }) {
  const s = LIFECYCLE_STYLE[type] ?? LIFECYCLE_STYLE['Sem histórico']
  return (
    <span style={{
      display:      'inline-flex',
      alignItems:   'center',
      gap:          5,
      padding:      '3px 9px',
      borderRadius: '999px',
      fontSize:     11,
      fontWeight:   600,
      background:   s.bg,
      color:        s.color,
      whiteSpace:   'nowrap',
    }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: s.dot, flexShrink: 0 }} />
      {type}
    </span>
  )
}

// ── Formata dias de forma legível ─────────────────────────────────────────────
function fmtDays(d) {
  if (!d) return <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>—</span>
  if (d < 90)   return `${d} dias`
  if (d < 365)  return `${Math.round(d / 30)} meses`
  const years = Math.floor(d / 365)
  const rem   = Math.round((d % 365) / 30)
  return rem > 0 ? `${years} ano${years > 1 ? 's' : ''} ${rem}m` : `${years} ano${years > 1 ? 's' : ''}`
}

// ── Formata BRL compacto ──────────────────────────────────────────────────────
function fmtBRL(v) {
  if (!v && v !== 0) return '—'
  return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', minimumFractionDigits: 2 })
}

// ── Badge de score ────────────────────────────────────────────────────────────
function ScoreBadge({ pct }) {
  const color = pct >= 70 ? '#16a34a' : pct >= 40 ? '#d97706' : '#dc2626'
  return (
    <span style={{
      display:      'inline-block',
      padding:      '2px 8px',
      borderRadius: '999px',
      fontSize:     11,
      fontWeight:   700,
      color:        '#fff',
      background:   color,
    }}>
      {pct}%
    </span>
  )
}

// ── Barra de progresso inline ─────────────────────────────────────────────────
function ProgressBar({ pct, color = 'var(--purple)' }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 100 }}>
      <div style={{ flex: 1, height: 5, background: 'var(--border)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${Math.min(pct, 100)}%`, height: '100%', background: color, borderRadius: 3 }} />
      </div>
      <span style={{ fontSize: 11, color: 'var(--text-muted)', minWidth: 30, textAlign: 'right' }}>
        {pct.toFixed(1)}%
      </span>
    </div>
  )
}

// ── Botão de ordenação rápida ─────────────────────────────────────────────────
function QuickSortBtn({ label, col, sortKey, sortAsc, onSort, icon }) {
  const active = sortKey === col
  return (
    <button
      onClick={() => onSort(col)}
      style={{
        display:      'inline-flex',
        alignItems:   'center',
        gap:          4,
        padding:      '4px 10px',
        borderRadius: 6,
        fontSize:     11,
        fontWeight:   600,
        border:       `1px solid ${active ? 'var(--amber)' : 'var(--border)'}`,
        background:   active ? 'var(--amber-light)' : 'var(--surface)',
        color:        active ? '#92400e' : 'var(--text-muted)',
        cursor:       'pointer',
        whiteSpace:   'nowrap',
      }}
    >
      {icon} {label}
      {active && <span style={{ fontSize: 9 }}>{sortAsc ? '▲' : '▼'}</span>}
    </button>
  )
}

// ── Componente principal ──────────────────────────────────────────────────────
export default function ProductLifecycleTable() {
  const [data,    setData]    = useState([])
  const [loading, setLoading] = useState(true)
  const [sortKey, setSortKey] = useState('n_offers')
  const [sortAsc, setSortAsc] = useState(false)

  useEffect(() => {
    setLoading(true)
    apiFetch('/api/recommendations/product-lifecycle')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  function handleSort(key) {
    if (sortKey === key) setSortAsc(a => !a)
    else { setSortKey(key); setSortAsc(false) }
  }

  const sorted = [...data].sort((a, b) => {
    const va = a[sortKey] ?? 0
    const vb = b[sortKey] ?? 0
    const cmp = typeof va === 'string' ? va.localeCompare(vb) : va - vb
    return sortAsc ? cmp : -cmp
  })

  const maxOffers = Math.max(...data.map(r => r.n_offers), 1)
  const maxPrice  = Math.max(...data.map(r => r.avg_unit_price), 1)

  // Cabeçalho de coluna com indicador de ordenação
  function Th({ col, label, align = 'left', style = {} }) {
    const active = sortKey === col
    return (
      <th
        onClick={() => handleSort(col)}
        style={{ textAlign: align, cursor: 'pointer', userSelect: 'none', ...style }}
        title={`Ordenar por ${label}`}
      >
        {label}
        <span style={{ marginLeft: 4, opacity: active ? 1 : 0.3, fontSize: 10 }}>
          {active ? (sortAsc ? '▲' : '▼') : '▼'}
        </span>
      </th>
    )
  }

  return (
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>

      {/* Cabeçalho */}
      <div className="chart-header" style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}>
        <div>
          <div className="chart-title">Ciclo de Vida — Top 50 Produtos nas Ofertas</div>
          <div className="chart-subtitle">
            Intervalo médio de recompra e preço médio calculados do histórico de compras
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 10, alignItems: 'flex-end' }}>

          {/* Botões de ordenação rápida por valor */}
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <span style={{ fontSize: 11, color: 'var(--text-muted)', marginRight: 2 }}>Ordenar por valor:</span>
            <QuickSortBtn
              label="Maior valor"
              col="avg_unit_price"
              sortKey={sortKey}
              sortAsc={sortAsc}
              onSort={key => { setSortKey(key); setSortAsc(false) }}
              icon="↓"
            />
            <QuickSortBtn
              label="Menor valor"
              col="avg_unit_price"
              sortKey={sortKey}
              sortAsc={sortAsc}
              onSort={key => { setSortKey(key); setSortAsc(true) }}
              icon="↑"
            />
          </div>

          {/* Legenda de tipos */}
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
            {Object.entries(LIFECYCLE_STYLE).map(([type, s]) => (
              <span key={type} style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: 11, color: s.color }}>
                <span style={{ width: 8, height: 8, borderRadius: '50%', background: s.dot }} />
                {type}
              </span>
            ))}
          </div>

        </div>
      </div>

      {/* Tabela */}
      {loading ? (
        <div className="empty" style={{ height: 160 }}>Calculando ciclos de vida…</div>
      ) : data.length === 0 ? (
        <div className="empty" style={{ height: 120 }}>Sem dados disponíveis.</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table className="rank-table">
            <thead>
              <tr>
                <Th col="rank"                label="#"             style={{ width: 36 }} />
                <Th col="product_name"        label="Produto" />
                <Th col="lifecycle_type"      label="Tipo" />
                <Th col="avg_repurchase_days" label="Ciclo Médio"   align="right" />
                <Th col="repeat_rate_pct"     label="Taxa Recompra" />
                <Th col="total_buyers"        label="Compradores"   align="right" />
                <Th col="n_offers"            label="Ofertas"       align="right" />
                <Th col="avg_unit_price"      label="Preço Médio"   align="right" />
                <Th col="avg_score_pct"       label="Score Médio"   align="center" />
              </tr>
            </thead>
            <tbody>
              {sorted.map(row => (
                <tr key={row.product_id}>

                  {/* Rank */}
                  <td>
                    <span className="rank-num" style={{ fontSize: 11 }}>#{row.rank}</span>
                  </td>

                  {/* Nome do produto */}
                  <td style={{ maxWidth: 260 }}>
                    <span style={{ fontSize: 12, fontWeight: 500 }}>{row.product_name}</span>
                  </td>

                  {/* Badge de tipo */}
                  <td>
                    <LifecycleBadge type={row.lifecycle_type} />
                  </td>

                  {/* Ciclo médio de recompra */}
                  <td style={{ textAlign: 'right' }}>
                    <span className="mono" style={{ fontSize: 12 }}>
                      {fmtDays(row.avg_repurchase_days)}
                    </span>
                  </td>

                  {/* Taxa de recompra com barra */}
                  <td style={{ minWidth: 130 }}>
                    <ProgressBar pct={row.repeat_rate_pct} color="var(--purple)" />
                  </td>

                  {/* Total de compradores */}
                  <td style={{ textAlign: 'right' }}>
                    <span className="mono" style={{ fontSize: 12 }}>
                      {row.total_buyers.toLocaleString('pt-BR')}
                    </span>
                  </td>

                  {/* Ofertas no batch com barra relativa ao máximo */}
                  <td style={{ textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' }}>
                      <div style={{ width: 60, height: 4, background: 'var(--border)', borderRadius: 3, overflow: 'hidden' }}>
                        <div style={{
                          width:      `${(row.n_offers / maxOffers) * 100}%`,
                          height:     '100%',
                          background: 'var(--blue)',
                          borderRadius: 3,
                        }} />
                      </div>
                      <span className="mono" style={{ fontSize: 12 }}>
                        {row.n_offers.toLocaleString('pt-BR')}
                      </span>
                    </div>
                  </td>

                  {/* Preço médio com barra de calor */}
                  <td style={{ textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' }}>
                      <div style={{ width: 48, height: 4, background: 'var(--border)', borderRadius: 3, overflow: 'hidden' }}>
                        <div style={{
                          width:      `${(row.avg_unit_price / maxPrice) * 100}%`,
                          height:     '100%',
                          background: 'var(--amber)',
                          borderRadius: 3,
                        }} />
                      </div>
                      <span className="mono" style={{ fontSize: 12, fontWeight: 500, color: 'var(--text)' }}>
                        {fmtBRL(row.avg_unit_price)}
                      </span>
                    </div>
                  </td>

                  {/* Score médio */}
                  <td style={{ textAlign: 'center' }}>
                    <ScoreBadge pct={row.avg_score_pct} />
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
