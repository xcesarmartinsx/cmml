import { useState } from 'react'

const STRATEGY_LABELS = {
  modelo_a_ranker: 'Modelo A',
  modelo_b_colaborativo: 'Modelo B',
}

function StrategyBadge({ strategy }) {
  const isA = strategy.includes('_a_')
  return (
    <span className={`strategy-badge ${isA ? 'strategy-a' : 'strategy-b'}`}>
      {STRATEGY_LABELS[strategy] || strategy}
    </span>
  )
}

export default function RunsTable({ runs }) {
  const [sortField, setSortField] = useState('evaluated_at')
  const [sortDir, setSortDir] = useState('desc')

  function handleSort(field) {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

  const sorted = [...runs].sort((a, b) => {
    let av = a[sortField]
    let bv = b[sortField]
    if (sortField === 'evaluated_at') {
      av = new Date(av)
      bv = new Date(bv)
    }
    if (av < bv) return sortDir === 'asc' ? -1 : 1
    if (av > bv) return sortDir === 'asc' ? 1 : -1
    return 0
  })

  function SortIcon({ field }) {
    if (sortField !== field) return <span style={{ opacity: 0.3 }}>↕</span>
    return <span>{sortDir === 'asc' ? '↑' : '↓'}</span>
  }

  function Th({ field, children, className = '' }) {
    return (
      <th
        className={className}
        onClick={() => handleSort(field)}
        style={{ cursor: 'pointer', userSelect: 'none' }}
      >
        {children} <SortIcon field={field} />
      </th>
    )
  }

  if (sorted.length === 0) {
    return (
      <div className="table-card">
        <div className="empty-chart">Nenhum registro encontrado.</div>
      </div>
    )
  }

  return (
    <div className="table-card">
      <div className="table-wrapper">
        <table className="runs-table">
          <thead>
            <tr>
              <Th field="evaluated_at">Data / Hora</Th>
              <Th field="strategy">Modelo</Th>
              <Th field="k" className="right">K</Th>
              <Th field="precision_at_k" className="right">Precision@K</Th>
              <Th field="recall_at_k" className="right">Recall@K</Th>
              <Th field="ndcg_at_k" className="right">NDCG@K</Th>
              <Th field="map_at_k" className="right">MAP@K</Th>
              <Th field="n_customers" className="right">Clientes</Th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((run) => (
              <tr key={run.run_id}>
                <td className="td-mono" style={{ fontSize: 12 }}>
                  {new Date(run.evaluated_at).toLocaleString('pt-BR')}
                </td>
                <td>
                  <StrategyBadge strategy={run.strategy} />
                </td>
                <td className="right td-mono">@{run.k}</td>
                <td className="right td-mono">
                  {run.precision_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono">
                  {run.recall_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono td-highlight">
                  {run.ndcg_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono">
                  {run.map_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono">
                  {run.n_customers?.toLocaleString('pt-BR') ?? '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
