/**
 * CustomerPurchaseModal.jsx
 * -------------------------
 * Modal de timeline de compras de um cliente.
 * Exibe as compras mais recentes no topo, mais antigas ao descer.
 *
 * Props:
 *   customerId   {number}   — id do cliente
 *   customerName {string}   — nome do cliente (exibido no cabeçalho)
 *   onClose      {function} — chamado ao fechar o modal
 */
import { useState, useEffect } from 'react'
import { apiFetch } from '../api.js'

function fmtDate(iso) {
  if (!iso) return '—'
  const [y, m, d] = iso.slice(0, 10).split('-')
  return `${d}/${m}/${y}`
}

function fmtCurrency(val) {
  if (val == null) return '—'
  return val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })
}

// Agrupa lista de itens por sale_date, mantendo a ordem original (desc)
function groupByDate(purchases) {
  const groups = []
  let current = null
  for (const item of purchases) {
    if (!current || current.date !== item.sale_date) {
      current = { date: item.sale_date, items: [] }
      groups.push(current)
    }
    current.items.push(item)
  }
  return groups
}

export default function CustomerPurchaseModal({ customerId, customerName, onClose }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  // Fecha com Escape
  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  // Carrega histórico
  useEffect(() => {
    setLoading(true)
    setError(null)
    apiFetch(`/api/business/customers/${customerId}/purchase-history`)
      .then(r => {
        if (!r.ok) throw new Error(`Erro ${r.status}`)
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [customerId])

  const groups = data ? groupByDate(data.purchases) : []

  return (
    /* Backdrop */
    <div
      onClick={onClose}
      style={{
        position:        'fixed',
        inset:           0,
        background:      'rgba(0,0,0,0.45)',
        zIndex:          1000,
        display:         'flex',
        alignItems:      'center',
        justifyContent:  'center',
        padding:         '24px 16px',
      }}
    >
      {/* Painel — para o clique não propagar para o backdrop */}
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background:   'var(--surface, #fff)',
          borderRadius: 12,
          boxShadow:    '0 8px 40px rgba(0,0,0,0.22)',
          width:        '100%',
          maxWidth:     560,
          maxHeight:    '80vh',
          display:      'flex',
          flexDirection:'column',
          overflow:     'hidden',
        }}
      >
        {/* Cabeçalho */}
        <div style={{
          display:        'flex',
          alignItems:     'center',
          justifyContent: 'space-between',
          padding:        '16px 20px',
          borderBottom:   '1px solid var(--border)',
          flexShrink:     0,
        }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15, color: 'var(--text)' }}>
              {customerName || `Cliente #${customerId}`}
            </div>
            {data && (
              <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>
                {data.total_purchases.toLocaleString('pt-BR')} item{data.total_purchases !== 1 ? 'ns' : ''} no histórico
              </div>
            )}
          </div>
          <button
            onClick={onClose}
            title="Fechar"
            style={{
              background:   'none',
              border:       'none',
              cursor:       'pointer',
              color:        'var(--muted)',
              fontSize:     20,
              lineHeight:   1,
              padding:      '4px 6px',
              borderRadius: 6,
            }}
          >
            ✕
          </button>
        </div>

        {/* Corpo com scroll */}
        <div style={{ overflowY: 'auto', padding: '20px 24px', flex: 1 }}>

          {loading && (
            <div style={{ textAlign: 'center', color: 'var(--muted)', padding: '40px 0', fontSize: 13 }}>
              Carregando histórico…
            </div>
          )}

          {error && (
            <div style={{ textAlign: 'center', color: '#dc2626', padding: '40px 0', fontSize: 13 }}>
              Não foi possível carregar o histórico.
            </div>
          )}

          {!loading && !error && groups.length === 0 && (
            <div style={{ textAlign: 'center', color: 'var(--muted)', padding: '40px 0', fontSize: 13 }}>
              Nenhuma compra registrada.
            </div>
          )}

          {/* Timeline */}
          {!loading && !error && groups.length > 0 && (
            <div style={{ position: 'relative', paddingLeft: 28 }}>
              {/* Linha vertical */}
              <div style={{
                position:   'absolute',
                left:       8,
                top:        6,
                bottom:     6,
                width:      2,
                background: 'var(--border)',
                borderRadius: 2,
              }} />

              {groups.map((group, gi) => (
                <div key={group.date} style={{ marginBottom: gi < groups.length - 1 ? 24 : 0 }}>

                  {/* Círculo + Data */}
                  <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                    <div style={{
                      position:     'absolute',
                      left:         2,
                      width:        14,
                      height:       14,
                      borderRadius: '50%',
                      background:   'var(--purple, #7c3aed)',
                      border:       '2px solid var(--surface, #fff)',
                      flexShrink:   0,
                    }} />
                    <span style={{
                      fontSize:   12,
                      fontWeight: 700,
                      color:      'var(--purple, #7c3aed)',
                    }}>
                      {fmtDate(group.date)}
                    </span>
                  </div>

                  {/* Itens do dia */}
                  <div style={{
                    background:   'var(--bg, #f8f9fa)',
                    borderRadius: 8,
                    border:       '1px solid var(--border)',
                    overflow:     'hidden',
                  }}>
                    {group.items.map((item, ii) => (
                      <div
                        key={`${item.order_id}-${ii}`}
                        style={{
                          display:       'flex',
                          justifyContent:'space-between',
                          alignItems:    'flex-start',
                          padding:       '8px 12px',
                          borderBottom:  ii < group.items.length - 1 ? '1px solid var(--border)' : 'none',
                          gap:           12,
                        }}
                      >
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{
                            fontSize:     12,
                            fontWeight:   500,
                            color:        'var(--text)',
                            whiteSpace:   'nowrap',
                            overflow:     'hidden',
                            textOverflow: 'ellipsis',
                          }}>
                            {item.product_name}
                          </div>
                          <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 2 }}>
                            Qtd: {item.quantity}
                          </div>
                        </div>
                        <div style={{
                          fontSize:   12,
                          fontWeight: 600,
                          color:      'var(--text)',
                          whiteSpace: 'nowrap',
                          flexShrink: 0,
                        }}>
                          {fmtCurrency(item.total_value)}
                        </div>
                      </div>
                    ))}
                  </div>

                </div>
              ))}
            </div>
          )}

        </div>
      </div>
    </div>
  )
}
