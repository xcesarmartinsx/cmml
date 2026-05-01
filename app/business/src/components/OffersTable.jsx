/**
 * OffersTable.jsx
 * ---------------
 * Exibe a lista de ofertas geradas pelos modelos de recomendação.
 *
 * Colunas: Cliente | Produto | Relevância | Contato | WhatsApp
 *
 * O botão de WhatsApp não tem ação nesta versão (Phase 1).
 * O contato exibe o celular se disponível; caso contrário, o telefone fixo.
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'
import CustomerPurchaseModal from './CustomerPurchaseModal.jsx'
import ConversionMetrics from './ConversionMetrics.jsx'
import FeedbackUpload from './FeedbackUpload.jsx'

const PAGE_SIZE = 50

/**
 * Formata o número de telefone para exibição legível.
 * Ex: "11912345678" → "(11) 91234-5678"
 */
function formatPhone(raw) {
  if (!raw || !raw.trim()) return '—'
  const digits = raw.replace(/\D/g, '')
  if (digits.length < 10) return raw.trim()
  if (digits.length === 11) {
    return `(${digits.slice(0,2)}) ${digits.slice(2,7)}-${digits.slice(7)}`
  }
  if (digits.length === 10) {
    return `(${digits.slice(0,2)}) ${digits.slice(2,6)}-${digits.slice(6)}`
  }
  return raw.trim()
}

// ── Tooltip de produto com detalhes no hover ──────────────────────────────────
function ProductTooltip({ row }) {
  const [show, setShow] = useState(false)
  return (
    <span
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
      style={{ position: 'relative', cursor: 'default', display: 'block' }}
    >
      <span style={{
        fontSize: 12, display: 'block', overflow: 'hidden',
        textOverflow: 'ellipsis', whiteSpace: 'nowrap'
      }}>
        {row.product_name || `Produto #${row.product_id}`}
      </span>
      {show && (
        <div style={{
          position: 'absolute', bottom: '100%', left: 0, zIndex: 100,
          background: '#fff', border: '1px solid var(--border)',
          borderRadius: 8, padding: '8px 12px',
          boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
          whiteSpace: 'normal', fontSize: 12, minWidth: 280, maxWidth: 400,
          pointerEvents: 'none',
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4, wordBreak: 'break-word' }}>
            {row.product_name || `Produto #${row.product_id}`}
          </div>
          <div style={{ color: 'var(--muted)', fontSize: 11 }}>ID: {row.product_id}</div>
          <div style={{ color: 'var(--muted)', fontSize: 11 }}>
            Valor: {(row.avg_unit_price ?? 0) > 0
              ? row.avg_unit_price.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })
              : '—'}
          </div>
          <div style={{ color: 'var(--muted)', fontSize: 11 }}>
            Ultima compra: {row.last_purchase_date
              ? new Date(row.last_purchase_date).toLocaleDateString('pt-BR')
              : 'Nunca comprou'}
          </div>
        </div>
      )}
    </span>
  )
}

// ── Badge de probabilidade ─────────────────────────────────────────────────────
function ScoreBadge({ pct }) {
  const color =
    pct >= 50 ? '#16a34a' :   // verde — alta probabilidade
    pct >= 25 ? '#d97706' :   // âmbar — média probabilidade
                '#dc2626'     // vermelho — baixa probabilidade

  return (
    <span style={{
      display:       'inline-block',
      padding:       '2px 10px',
      borderRadius:  '999px',
      fontSize:      12,
      fontWeight:    700,
      color:         '#fff',
      background:    color,
      whiteSpace:    'nowrap',
    }}>
      {pct}%
    </span>
  )
}

// ── Ícone WhatsApp (SVG inline) ────────────────────────────────────────────────
function WhatsAppIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>
    </svg>
  )
}

// ── Badge de tipo de telefone (WPP confirmado / Celular / Sem WPP / Fixo) ────
function PhoneTypeBadge({ type }) {
  const badges = {
    whatsapp:      { label: 'WPP \u2713', bg: '#dcfce7', color: '#166534' },
    mobile:        { label: 'Celular',  bg: '#fef3c7', color: '#92400e' },
    mobile_no_wpp: { label: 'Sem WPP',  bg: '#fee2e2', color: '#991b1b' },
    landline:      { label: 'Fixo',     bg: '#f3f4f6', color: '#6b7280' },
  }
  const b = badges[type]
  if (!b) return null
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', borderRadius: 4,
      fontSize: 10, fontWeight: 600, marginLeft: 6,
      background: b.bg, color: b.color,
    }}>{b.label}</span>
  )
}

// ── Componente principal ───────────────────────────────────────────────────────
export default function OffersTable() {
  const [data,         setData]         = useState([])
  const [strategy,     setStrategy]     = useState('')
  const [minScore,     setMinScore]     = useState(0)
  const [lastPurchase, setLastPurchase] = useState('')  // '' | 'yes' | 'no'
  const [page,         setPage]         = useState(1)
  const [loading,      setLoading]      = useState(true)
  const [batchInfo,    setBatchInfo]    = useState(null)
  const [sortKeys,     setSortKeys]     = useState([
    { col: 'score', dir: 'desc' },
    { col: 'price', dir: 'desc' },
  ])
  const [selectedCustomer, setSelectedCustomer] = useState(null) // { id, name } | null
  const [exporting,        setExporting]        = useState(false)
  const [showFeedbackUpload, setShowFeedbackUpload] = useState(false)
  const [exportingFeedback, setExportingFeedback] = useState(false)

  // Carrega lista de batches disponíveis
  useEffect(() => {
    apiFetch('/api/recommendations/batches')
      .then(r => r.json())
      .then(d => {
        if (d.length > 0) setBatchInfo(d[0])
      })
      .catch(() => {})
  }, [])

  // Carrega ofertas quando estratégia muda (ordenação é client-side)
  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams({ limit: 5000 })
    if (strategy) params.set('strategy', strategy)

    apiFetch(`/api/recommendations/offers?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [strategy])

  async function handleExport() {
    setExporting(true)
    try {
      const params = new URLSearchParams()
      if (strategy) params.set('strategy', strategy)
      const resp = await apiFetch(`/api/recommendations/offers/export?${params}`)
      if (!resp.ok) throw new Error('Falha na exportação')
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      // Pega o nome do arquivo do header Content-Disposition, ou usa padrão
      const disposition = resp.headers.get('Content-Disposition') || ''
      const match = disposition.match(/filename="?([^"]+)"?/)
      a.download = match ? match[1] : `ofertas_${new Date().toISOString().slice(0,10)}.csv`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      console.error('Erro ao exportar:', e)
    } finally {
      setExporting(false)
    }
  }

  async function handleExportFeedback() {
    setExportingFeedback(true)
    try {
      const params = new URLSearchParams()
      if (strategy) params.set('strategy', strategy)
      const resp = await apiFetch(`/api/recommendations/offers/export-feedback?${params}`)
      if (!resp.ok) throw new Error('Falha na exportacao')
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      const disposition = resp.headers.get('Content-Disposition') || ''
      const match = disposition.match(/filename="?([^"]+)"?/)
      a.download = match ? match[1] : `feedback_ofertas_${new Date().toISOString().slice(0,10)}.xlsx`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      console.error('Erro ao exportar feedback:', e)
    } finally {
      setExportingFeedback(false)
    }
  }

  // Toggle de ordenação multi-coluna ao clicar na coluna
  function toggleSort(col) {
    setSortKeys(prev => {
      if (prev[0]?.col === col) {
        // Ja e primario: inverte direcao
        return [{ col, dir: prev[0].dir === 'desc' ? 'asc' : 'desc' }, ...prev.slice(1)]
      }
      // Promove para primario, antigo primario vira secundario
      const rest = prev.filter(s => s.col !== col)
      return [{ col, dir: 'desc' }, ...rest]
    })
  }

  // Seta indicadora de direcao (com indicador de prioridade)
  function sortArrow(col) {
    const idx = sortKeys.findIndex(s => s.col === col)
    if (idx < 0) return ''
    const arrow = sortKeys[idx].dir === 'desc' ? '\u2193' : '\u2191'
    if (idx === 0) return ` ${arrow}`
    return ` ${arrow}\u00B2`  // superscript 2 para indicar secundario
  }

  // Volta para pagina 1 quando qualquer filtro muda
  useEffect(() => { setPage(1) }, [strategy, minScore, lastPurchase, sortKeys])

  const fmtDate = iso =>
    iso ? new Date(iso).toLocaleDateString('pt-BR', {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    }) : '—'

  // ── Dados filtrados e paginados ─────────────────────────────────────────────
  const filtered = data.filter(row =>
    (row.score_pct ?? 0) >= minScore &&
    (lastPurchase === '' ||
      (lastPurchase === 'yes' ? row.last_purchase_date !== null : row.last_purchase_date === null))
  )

  // Ordenacao client-side multi-coluna
  const sorted = [...filtered].sort((a, b) => {
    for (const { col, dir } of sortKeys) {
      const mult = dir === 'desc' ? -1 : 1
      const va = col === 'score' ? (a.score_pct ?? 0) : (a.avg_unit_price ?? 0)
      const vb = col === 'score' ? (b.score_pct ?? 0) : (b.avg_unit_price ?? 0)
      if (va !== vb) return (va - vb) * mult
    }
    return 0
  })

  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE))
  const safePage   = Math.min(page, totalPages)
  const paged      = sorted.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE)

  // Botão de página simples
  const btnStyle = (disabled) => ({
    padding:      '4px 12px',
    fontSize:     12,
    fontWeight:   600,
    borderRadius: 6,
    border:       '1px solid var(--border)',
    background:   disabled ? 'var(--surface)' : 'var(--surface)',
    color:        disabled ? 'var(--muted)' : 'var(--text)',
    cursor:       disabled ? 'default' : 'pointer',
    opacity:      disabled ? 0.5 : 1,
  })

  return (
    <>
    <ConversionMetrics />
    <div className="chart-card" style={{ padding: 0, overflow: 'hidden' }}>

      {/* ── Cabeçalho ─────────────────────────────────────────────────────── */}
      <div className="chart-header" style={{ padding: '16px 20px', borderBottom: '1px solid var(--border)' }}>
        <div>
          <div className="chart-title">Lista de Ofertas</div>
          <div className="chart-subtitle">
            {batchInfo
              ? `Gerado em ${fmtDate(batchInfo.generated_at)} · ${batchInfo.n_customers?.toLocaleString('pt-BR')} clientes · ${batchInfo.n_offers?.toLocaleString('pt-BR')} ofertas`
              : 'Carregando informações do batch…'}
          </div>
        </div>

        {/* Filtros */}
        <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>

          {/* Filtro de estratégia */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <label style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>Modelo:</label>
            <select
              value={strategy}
              onChange={e => setStrategy(e.target.value)}
              style={{
                fontSize: 12, padding: '4px 8px', borderRadius: 6,
                border: '1px solid var(--border)', background: 'var(--surface)',
                color: 'var(--text)', cursor: 'pointer',
              }}
            >
              <option value="">Todos</option>
              <option value="modelo_a_ranker">Modelo A (LightGBM)</option>
              <option value="modelo_b_colaborativo">Modelo B (SVD)</option>
            </select>
          </div>

          {/* Filtro de % chance mínima */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <label style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>Relevância mín.:</label>
            <select
              value={minScore}
              onChange={e => setMinScore(Number(e.target.value))}
              style={{
                fontSize: 12, padding: '4px 8px', borderRadius: 6,
                border: '1px solid var(--border)', background: 'var(--surface)',
                color: 'var(--text)', cursor: 'pointer',
              }}
            >
              <option value={0}>Todas</option>
              <option value={30}>≥ 30%</option>
              <option value={40}>≥ 40%</option>
              <option value={50}>≥ 50%</option>
              <option value={60}>≥ 60%</option>
              <option value={70}>≥ 70%</option>
              <option value={80}>≥ 80%</option>
            </select>
          </div>

          {/* Filtro de última compra */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <label style={{ fontSize: 12, color: 'var(--muted)', whiteSpace: 'nowrap' }}>Última compra:</label>
            <select
              value={lastPurchase}
              onChange={e => setLastPurchase(e.target.value)}
              style={{
                fontSize: 12, padding: '4px 8px', borderRadius: 6,
                border: '1px solid var(--border)', background: 'var(--surface)',
                color: 'var(--text)', cursor: 'pointer',
              }}
            >
              <option value="">Todos</option>
              <option value="yes">Já comprou</option>
              <option value="no">Nunca comprou</option>
            </select>
          </div>

          {/* Exportar CSV */}
          <button
            onClick={handleExport}
            disabled={exporting || loading}
            title="Exportar toda a lista como CSV"
            style={{
              display:      'inline-flex',
              alignItems:   'center',
              gap:          6,
              padding:      '4px 14px',
              fontSize:     12,
              fontWeight:   600,
              borderRadius: 6,
              border:       '1px solid var(--border)',
              background:   exporting ? 'var(--surface)' : 'var(--purple)',
              color:        exporting ? 'var(--muted)' : '#fff',
              cursor:       exporting || loading ? 'not-allowed' : 'pointer',
              opacity:      exporting || loading ? 0.7 : 1,
              whiteSpace:   'nowrap',
              transition:   'opacity 0.2s',
            }}
          >
            {exporting ? 'Exportando…' : '↓ Exportar CSV'}
          </button>

          {/* Export Feedback Excel */}
          <button
            onClick={handleExportFeedback}
            disabled={exportingFeedback || loading}
            title="Exportar planilha para preenchimento de feedback"
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              padding: '4px 14px', fontSize: 12, fontWeight: 600, borderRadius: 6,
              border: '1px solid var(--border)',
              background: exportingFeedback ? 'var(--surface)' : 'var(--green)',
              color: exportingFeedback ? 'var(--muted)' : '#fff',
              cursor: exportingFeedback || loading ? 'not-allowed' : 'pointer',
              opacity: exportingFeedback || loading ? 0.7 : 1,
              whiteSpace: 'nowrap', transition: 'opacity 0.2s',
            }}
          >
            {exportingFeedback ? 'Exportando...' : 'Feedback Excel'}
          </button>

          {/* Import Feedback */}
          <button
            onClick={() => setShowFeedbackUpload(true)}
            title="Importar planilha de feedback preenchida"
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              padding: '4px 14px', fontSize: 12, fontWeight: 600, borderRadius: 6,
              border: '1px solid var(--border)',
              background: 'var(--amber)',
              color: '#fff', cursor: 'pointer',
              whiteSpace: 'nowrap', transition: 'opacity 0.2s',
            }}
          >
            Importar Feedback
          </button>

        </div>
      </div>

      {/* ── Tabela ────────────────────────────────────────────────────────── */}
      {loading ? (
        <div className="empty" style={{ height: 200 }}>Carregando ofertas…</div>
      ) : data.length === 0 ? (
        <div className="empty" style={{ height: 160 }}>Sem ofertas disponíveis.</div>
      ) : filtered.length === 0 ? (
        <div className="empty" style={{ height: 160 }}>Nenhuma oferta com os filtros selecionados.</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table className="rank-table" style={{ tableLayout: 'auto', width: '100%' }}>
            <thead>
              <tr>
                <th style={{ width: 32 }}>#</th>
                <th style={{ minWidth: 180 }}>Cliente</th>
                <th style={{ minWidth: 200 }}>Produto</th>
                <th
                  onClick={() => toggleSort('price')}
                  style={{ textAlign: 'right', cursor: 'pointer', userSelect: 'none' }}
                  title="Clique para ordenar por valor"
                >Valor{sortArrow('price')}</th>
                <th
                  onClick={() => toggleSort('score')}
                  style={{ textAlign: 'center', cursor: 'pointer', userSelect: 'none' }}
                  title="Clique para ordenar por probabilidade"
                >% Chance{sortArrow('score')}</th>
                <th>Última Compra</th>
                <th>Contato</th>
                <th>Modelo</th>
                <th style={{ textAlign: 'center' }}>Status</th>
                <th style={{ textAlign: 'center' }}>WhatsApp</th>
              </tr>
            </thead>
            <tbody>
              {paged.map((row, i) => (
                <tr key={row.offer_id}>
                  {/* Posição global (considerando página atual) */}
                  <td>
                    <span className="rank-num" style={{ fontSize: 11 }}>
                      #{(safePage - 1) * PAGE_SIZE + i + 1}
                    </span>
                  </td>

                  {/* Cliente — clicavel para abrir timeline de compras */}
                  <td style={{
                    minWidth:     180,
                    overflow:     'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace:   'nowrap',
                    paddingRight: 12,
                  }}>
                    <button
                      onClick={() => setSelectedCustomer({ id: row.customer_id, name: row.customer_name })}
                      title={row.customer_name || '—'}
                      style={{
                        background:      'none',
                        border:          'none',
                        padding:         0,
                        cursor:          'pointer',
                        fontSize:        13,
                        fontWeight:      500,
                        color:           'var(--purple)',
                        textDecoration:  'underline',
                        textDecorationStyle: 'dotted',
                        textUnderlineOffset: '3px',
                        textAlign:       'left',
                        maxWidth:        '100%',
                        overflow:        'hidden',
                        textOverflow:    'ellipsis',
                        whiteSpace:      'nowrap',
                        display:         'block',
                      }}
                    >
                      {row.customer_name || '—'}
                    </button>
                  </td>

                  {/* Produto — hover mostra tooltip com detalhes */}
                  <td style={{ minWidth: 200, paddingRight: 12 }}>
                    <ProductTooltip row={row} />
                  </td>

                  {/* Valor unitário médio */}
                  <td style={{ textAlign: 'right' }}>
                    <span className="mono" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                      {(row.avg_unit_price ?? 0) > 0
                        ? row.avg_unit_price.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })
                        : '—'}
                    </span>
                  </td>

                  {/* Badge de probabilidade */}
                  <td style={{ textAlign: 'center' }}>
                    <ScoreBadge pct={row.score_pct ?? 0} />
                  </td>

                  {/* Última compra deste produto por este cliente */}
                  <td>
                    <span className="mono" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                      {row.last_purchase_date
                        ? new Date(row.last_purchase_date).toLocaleDateString('pt-BR', {
                            day: '2-digit', month: '2-digit', year: 'numeric',
                          })
                        : <span style={{ color: 'var(--muted)', fontStyle: 'italic' }}>nunca comprou</span>
                      }
                    </span>
                  </td>

                  {/* Contato — celular preferencial, fallback para fixo */}
                  <td>
                    <span className="mono" style={{ fontSize: 12 }}>
                      {formatPhone(row.contact)}
                    </span>
                    <PhoneTypeBadge type={row.phone_type} />
                  </td>

                  {/* Estratégia */}
                  <td>
                    <span style={{ fontSize: 11, color: 'var(--muted)' }}>
                      {row.strategy === 'modelo_a_ranker' ? 'Modelo A' : 'Modelo B'}
                    </span>
                  </td>

                  {/* Status de conversao */}
                  <td style={{ textAlign: 'center' }}>
                    {row.converted === true ? (
                      <span style={{
                        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
                        fontSize: 11, fontWeight: 600, background: '#dcfce7', color: '#166534',
                      }}>Convertido</span>
                    ) : row.converted === false ? (
                      <span style={{
                        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
                        fontSize: 11, fontWeight: 600, background: '#fee2e2', color: '#991b1b',
                      }}>Nao convertido</span>
                    ) : (
                      <span style={{
                        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
                        fontSize: 11, fontWeight: 600, background: '#fef3c7', color: '#92400e',
                      }}>Pendente</span>
                    )}
                  </td>

                  {/* Botão WhatsApp (sem ação — Phase 1) */}
                  <td style={{ textAlign: 'center' }}>
                    <button
                      disabled
                      title="Envio via WhatsApp — disponível em breve"
                      style={{
                        display:        'inline-flex',
                        alignItems:     'center',
                        gap:            4,
                        padding:        '4px 10px',
                        borderRadius:   6,
                        fontSize:       12,
                        fontWeight:     600,
                        border:         'none',
                        background:     '#d1fae5',
                        color:          '#065f46',
                        cursor:         'not-allowed',
                        opacity:        0.7,
                        whiteSpace:     'nowrap',
                      }}
                    >
                      <WhatsAppIcon />
                      Enviar
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* ── Paginação + Rodapé ────────────────────────────────────────────── */}
      {!loading && sorted.length > 0 && (
        <div style={{
          display:       'flex',
          alignItems:    'center',
          justifyContent:'space-between',
          flexWrap:      'wrap',
          gap:           8,
          padding:       '10px 20px',
          borderTop:     '1px solid var(--border)',
        }}>

          {/* Controles de navegação */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={safePage === 1}
              style={btnStyle(safePage === 1)}
            >
              ← Anterior
            </button>

            {/* Números de página */}
            <div style={{ display: 'flex', gap: 4 }}>
              {Array.from({ length: totalPages }, (_, i) => i + 1)
                .filter(p => p === 1 || p === totalPages || Math.abs(p - safePage) <= 2)
                .reduce((acc, p, idx, arr) => {
                  if (idx > 0 && p - arr[idx - 1] > 1) acc.push('…')
                  acc.push(p)
                  return acc
                }, [])
                .map((p, idx) =>
                  p === '…' ? (
                    <span key={`ellipsis-${idx}`} style={{ fontSize: 12, color: 'var(--muted)', padding: '4px 2px' }}>…</span>
                  ) : (
                    <button
                      key={p}
                      onClick={() => setPage(p)}
                      style={{
                        ...btnStyle(false),
                        minWidth:   28,
                        background: p === safePage ? 'var(--purple)' : 'var(--surface)',
                        color:      p === safePage ? '#fff'          : 'var(--text)',
                        border:     p === safePage ? 'none'          : '1px solid var(--border)',
                      }}
                    >
                      {p}
                    </button>
                  )
                )
              }
            </div>

            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={safePage === totalPages}
              style={btnStyle(safePage === totalPages)}
            >
              Próxima →
            </button>
          </div>

          {/* Resumo */}
          <span style={{ fontSize: 11, color: 'var(--muted)' }}>
            {((safePage - 1) * PAGE_SIZE + 1).toLocaleString('pt-BR')}–{Math.min(safePage * PAGE_SIZE, sorted.length).toLocaleString('pt-BR')} de {sorted.length.toLocaleString('pt-BR')} ofertas filtradas · {data.length.toLocaleString('pt-BR')} total
          </span>

        </div>
      )}

      {/* Modal de timeline de compras */}
      {selectedCustomer && (
        <CustomerPurchaseModal
          customerId={selectedCustomer.id}
          customerName={selectedCustomer.name}
          onClose={() => setSelectedCustomer(null)}
        />
      )}

      {showFeedbackUpload && (
        <FeedbackUpload
          onClose={() => setShowFeedbackUpload(false)}
          onSuccess={() => {
            // Reload offers to show updated status
            setLoading(true)
            const params = new URLSearchParams({ limit: 500 })
            if (strategy) params.set('strategy', strategy)
            apiFetch(`/api/recommendations/offers?${params}`)
              .then(r => r.json())
              .then(d => { setData(d); setLoading(false) })
              .catch(() => setLoading(false))
          }}
        />
      )}
    </div>
    </>
  )
}
