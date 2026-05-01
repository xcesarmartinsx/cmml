/**
 * ProductLifecyclePage.jsx
 * -------------------------
 * Pagina dedicada ao ciclo de vida de TODOS os produtos ativos.
 *
 * Consulta o endpoint GET /api/recommendations/lifecycle com suporte a:
 *   - Busca por nome do produto (debounce 300ms)
 *   - Filtro por tier (short / medium / long)
 *   - Paginacao server-side (50 por pagina)
 *
 * Tambem exibe estatisticas agregadas via GET /api/recommendations/lifecycle/stats.
 *
 * Colunas: Produto | ID | Ciclo Medio | Ciclo Mediano | Tier | Amostra | Clientes
 */
import { useState, useEffect, useRef, useCallback } from 'react'
import { apiFetch } from '../api.js'

const PAGE_SIZE = 50

// -- Cores dos badges por tier ------------------------------------------------
const TIER_STYLE = {
  short:  { label: 'Curto',  bg: '#dcfce7', color: '#166534', dot: '#16a34a' },
  medium: { label: 'Medio',  bg: '#fef3c7', color: '#92400e', dot: '#d97706' },
  long:   { label: 'Longo',  bg: '#fee2e2', color: '#991b1b', dot: '#dc2626' },
}

function TierBadge({ tier }) {
  const s = TIER_STYLE[tier] || { label: tier || '—', bg: '#f1f5f9', color: '#64748b', dot: '#94a3b8' }
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
      {s.label}
    </span>
  )
}

// -- Badge de dias com cor contextual -----------------------------------------
function DaysBadge({ days }) {
  if (days == null || days === 0) {
    return <span style={{ color: 'var(--muted)', fontStyle: 'italic' }}>--</span>
  }
  const color =
    days < 90   ? '#16a34a' :   // verde
    days <= 365 ? '#d97706' :   // amarelo
                  '#dc2626'     // vermelho

  return (
    <span style={{
      display:      'inline-block',
      padding:      '2px 10px',
      borderRadius: '999px',
      fontSize:     12,
      fontWeight:   700,
      color:        '#fff',
      background:   color,
      whiteSpace:   'nowrap',
    }}>
      {days.toFixed(1)}d
    </span>
  )
}

// -- Stat card compacto para o header -----------------------------------------
function StatPill({ label, value, color = 'var(--purple)' }) {
  return (
    <div style={{
      display:      'flex',
      alignItems:   'center',
      gap:          8,
      padding:      '6px 14px',
      borderRadius: 8,
      background:   'var(--surface)',
      border:       '1px solid var(--border)',
    }}>
      <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0 }} />
      <div>
        <div style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.2 }}>{label}</div>
        <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text)', lineHeight: 1.3 }}>{value}</div>
      </div>
    </div>
  )
}

// -- Componente principal -----------------------------------------------------
export default function ProductLifecyclePage() {
  const [data,    setData]    = useState([])
  const [stats,   setStats]   = useState(null)
  const [loading, setLoading] = useState(true)
  const [page,    setPage]    = useState(1)
  const [search,  setSearch]  = useState('')
  const [tier,    setTier]    = useState('')

  // Debounce: armazena o valor "efetivo" da busca (aplicado apos 300ms)
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const timerRef = useRef(null)

  // Debounce handler
  const handleSearchChange = useCallback((value) => {
    setSearch(value)
    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => {
      setDebouncedSearch(value)
      setPage(1)
    }, 300)
  }, [])

  // Cleanup do timer ao desmontar
  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  // Carrega stats uma vez
  useEffect(() => {
    apiFetch('/api/recommendations/lifecycle/stats')
      .then(r => r.json())
      .then(d => setStats(d))
      .catch(() => {})
  }, [])

  // Carrega dados quando filtros ou pagina mudam
  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams({
      limit:  PAGE_SIZE,
      offset: (page - 1) * PAGE_SIZE,
    })
    if (debouncedSearch) params.set('search', debouncedSearch)
    if (tier)            params.set('tier', tier)

    apiFetch(`/api/recommendations/lifecycle?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => { setData([]); setLoading(false) })
  }, [debouncedSearch, tier, page])

  // Reseta pagina ao trocar tier
  useEffect(() => { setPage(1) }, [tier])

  // -- Paginacao ---------------------------------------------------------------
  // Como a API nao retorna total_count, inferimos se ha proxima pagina
  const hasNextPage = data.length === PAGE_SIZE
  const hasPrevPage = page > 1

  const btnStyle = (disabled) => ({
    padding:      '4px 12px',
    fontSize:     12,
    fontWeight:   600,
    borderRadius: 6,
    border:       '1px solid var(--border)',
    background:   'var(--surface)',
    color:        disabled ? 'var(--muted)' : 'var(--text)',
    cursor:       disabled ? 'default' : 'pointer',
    opacity:      disabled ? 0.5 : 1,
  })

  const selectStyle = {
    fontSize:     12,
    padding:      '6px 10px',
    borderRadius: 6,
    border:       '1px solid var(--border)',
    background:   'var(--surface)',
    color:        'var(--text)',
    cursor:       'pointer',
  }

  return (
    <main className="main">

      {/* -- Header com stats ------------------------------------------------- */}
      <section>
        <h2 className="section-title">Ciclo de Vida dos Produtos</h2>

        {stats && (
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
            <StatPill label="Total de produtos"  value={stats.total?.toLocaleString('pt-BR') ?? '—'}  color="var(--blue)" />
            <StatPill label="Curto (<90d)"        value={stats.count_short?.toLocaleString('pt-BR') ?? '—'}  color="#16a34a" />
            <StatPill label="Medio (90-365d)"     value={stats.count_medium?.toLocaleString('pt-BR') ?? '—'} color="#d97706" />
            <StatPill label="Longo (>365d)"       value={stats.count_long?.toLocaleString('pt-BR') ?? '—'}   color="#dc2626" />
          </div>
        )}
      </section>

      {/* -- Card com filtros e tabela ---------------------------------------- */}
      <section>
        <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>

          {/* Barra de filtros */}
          <div className="chart-header" style={{
            padding:      '16px 20px',
            borderBottom: '1px solid var(--border)',
            display:      'flex',
            alignItems:   'center',
            justifyContent: 'space-between',
            flexWrap:     'wrap',
            gap:          12,
          }}>
            <div>
              <div className="chart-title">Produtos com Ciclo de Vida Calculado</div>
              <div className="chart-subtitle">
                Dados calculados a partir dos intervalos de recompra no historico de vendas
              </div>
            </div>

            <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>

              {/* Campo de pesquisa */}
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <label style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>Pesquisar:</label>
                <input
                  type="text"
                  value={search}
                  onChange={e => handleSearchChange(e.target.value)}
                  placeholder="Nome do produto..."
                  style={{
                    fontSize:     12,
                    padding:      '6px 10px',
                    borderRadius: 6,
                    border:       '1px solid var(--border)',
                    background:   'var(--surface)',
                    color:        'var(--text)',
                    minWidth:     200,
                    outline:      'none',
                  }}
                />
              </div>

              {/* Filtro de tier */}
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <label style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>Tier:</label>
                <select
                  value={tier}
                  onChange={e => setTier(e.target.value)}
                  style={selectStyle}
                >
                  <option value="">Todos</option>
                  <option value="short">Curto (&lt;90d)</option>
                  <option value="medium">Medio (90-365d)</option>
                  <option value="long">Longo (&gt;365d)</option>
                </select>
              </div>

            </div>
          </div>

          {/* Tabela */}
          {loading ? (
            <div className="empty" style={{ height: 200 }}>Carregando ciclos de vida...</div>
          ) : data.length === 0 ? (
            <div className="empty" style={{ height: 160 }}>
              {debouncedSearch || tier
                ? 'Nenhum produto encontrado com os filtros selecionados.'
                : 'Sem dados de ciclo de vida disponíveis.'}
            </div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table className="rank-table" style={{ tableLayout: 'auto', width: '100%' }}>
                <thead>
                  <tr>
                    <th style={{ width: 32 }}>#</th>
                    <th style={{ minWidth: 280 }}>Produto</th>
                    <th style={{ textAlign: 'right' }}>ID</th>
                    <th style={{ textAlign: 'center' }}>Ciclo Medio (dias)</th>
                    <th style={{ textAlign: 'right' }}>Ciclo Mediano (dias)</th>
                    <th>Tier</th>
                    <th style={{ textAlign: 'right' }}>Amostra</th>
                    <th style={{ textAlign: 'right' }}>Clientes</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, i) => (
                    <tr key={row.product_id}>
                      {/* Posicao global */}
                      <td>
                        <span className="rank-num" style={{ fontSize: 11 }}>
                          #{(page - 1) * PAGE_SIZE + i + 1}
                        </span>
                      </td>

                      {/* Nome do produto — completo, sem truncar */}
                      <td style={{ fontSize: 12, fontWeight: 500 }}>
                        {row.product_name || '--'}
                      </td>

                      {/* ID */}
                      <td style={{ textAlign: 'right' }}>
                        <span className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                          {row.product_id}
                        </span>
                      </td>

                      {/* Ciclo medio com badge colorido */}
                      <td style={{ textAlign: 'center' }}>
                        <DaysBadge days={row.avg_days} />
                      </td>

                      {/* Ciclo mediano */}
                      <td style={{ textAlign: 'right' }}>
                        <span className="mono" style={{ fontSize: 12 }}>
                          {row.median_days != null && row.median_days > 0
                            ? `${row.median_days.toFixed(1)}d`
                            : <span style={{ color: 'var(--muted)', fontStyle: 'italic' }}>--</span>
                          }
                        </span>
                      </td>

                      {/* Tier badge */}
                      <td>
                        <TierBadge tier={row.tier} />
                      </td>

                      {/* Amostra (sample_size) */}
                      <td style={{ textAlign: 'right' }}>
                        <span className="mono" style={{ fontSize: 12 }}>
                          {row.sample_size?.toLocaleString('pt-BR') ?? '—'}
                        </span>
                      </td>

                      {/* Clientes distintos */}
                      <td style={{ textAlign: 'right' }}>
                        <span className="mono" style={{ fontSize: 12 }}>
                          {row.distinct_customers?.toLocaleString('pt-BR') ?? '—'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Paginacao */}
          {!loading && data.length > 0 && (
            <div style={{
              display:        'flex',
              alignItems:     'center',
              justifyContent: 'space-between',
              flexWrap:       'wrap',
              gap:            8,
              padding:        '10px 20px',
              borderTop:      '1px solid var(--border)',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={!hasPrevPage}
                  style={btnStyle(!hasPrevPage)}
                >
                  Anterior
                </button>

                <span style={{ fontSize: 12, color: 'var(--text)', fontWeight: 600, padding: '0 8px' }}>
                  Pagina {page}
                </span>

                <button
                  onClick={() => setPage(p => p + 1)}
                  disabled={!hasNextPage}
                  style={btnStyle(!hasNextPage)}
                >
                  Proxima
                </button>
              </div>

              <span style={{ fontSize: 11, color: 'var(--muted)' }}>
                Exibindo {((page - 1) * PAGE_SIZE + 1).toLocaleString('pt-BR')}
                {' '}a{' '}
                {((page - 1) * PAGE_SIZE + data.length).toLocaleString('pt-BR')}
                {' '}registros (pagina {page})
              </span>
            </div>
          )}

        </div>
      </section>
    </main>
  )
}
