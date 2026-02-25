const STRATEGY_LABELS = {
  modelo_a_ranker: 'Modelo A — Ranker (LightGBM)',
  modelo_b_colaborativo: 'Modelo B — Colaborativo (SVD)',
}

const STRATEGY_COLORS = {
  modelo_a_ranker: '#3b82f6',
  modelo_b_colaborativo: '#f97316',
}

function parseNotes(notes = '') {
  const extract = (pattern) => {
    const m = notes.match(pattern)
    return m ? m[1] : null
  }
  return {
    historyDays: extract(/history_days=(\d+)/),
    aucRoc: extract(/auc_roc=([\d.]+)/),
    nUsers: extract(/n_users=([\d,]+)/),
    nItems: extract(/n_items=([\d,]+)/),
    nFactors: extract(/n_factors=(\d+)/),
    kNeighbors: extract(/k_neighbors=(\d+)/),
    bestThreshold: extract(/best_threshold=([\d.]+)/),
  }
}

export default function ModelCard({ strategy, latestRunsByK }) {
  const color = STRATEGY_COLORS[strategy] || '#6b7280'
  const label = STRATEGY_LABELS[strategy] || strategy

  const sortedK = Object.entries(latestRunsByK).sort(
    ([a], [b]) => Number(a) - Number(b)
  )

  if (sortedK.length === 0) return null

  // Use K=10 for meta info, fallback to first available
  const metaRun =
    latestRunsByK[10] || latestRunsByK[Object.keys(latestRunsByK)[0]]
  const meta = parseNotes(metaRun?.notes)

  const lastEvaluatedAt = metaRun?.evaluated_at
    ? new Date(metaRun.evaluated_at).toLocaleString('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : '—'

  return (
    <div className="model-card" style={{ '--card-color': color }}>
      <div className="card-header">
        <div className="card-dot" style={{ backgroundColor: color }} />
        <h3>{label}</h3>
      </div>

      <div className="card-meta">
        <div className="meta-item">
          <span className="meta-label">Último treino</span>
          <span className="meta-value">{lastEvaluatedAt}</span>
        </div>
        <div className="meta-item">
          <span className="meta-label">Clientes avaliados</span>
          <span className="meta-value">
            {metaRun?.n_customers?.toLocaleString('pt-BR') ?? '—'}
          </span>
        </div>
        {meta.historyDays && (
          <div className="meta-item">
            <span className="meta-label">Janela histórica</span>
            <span className="meta-value">{meta.historyDays} dias</span>
          </div>
        )}
        {meta.aucRoc && (
          <div className="meta-item">
            <span className="meta-label">AUC-ROC</span>
            <span className="meta-value">
              {parseFloat(meta.aucRoc).toFixed(4)}
            </span>
          </div>
        )}
        {meta.nUsers && (
          <div className="meta-item">
            <span className="meta-label">Usuários</span>
            <span className="meta-value">{meta.nUsers}</span>
          </div>
        )}
        {meta.nItems && (
          <div className="meta-item">
            <span className="meta-label">Itens</span>
            <span className="meta-value">{meta.nItems}</span>
          </div>
        )}
        {meta.nFactors && (
          <div className="meta-item">
            <span className="meta-label">Fatores SVD</span>
            <span className="meta-value">{meta.nFactors}</span>
          </div>
        )}
      </div>

      <div className="card-metrics">
        <table className="metrics-table">
          <thead>
            <tr>
              <th>K</th>
              <th className="right">Precision</th>
              <th className="right">Recall</th>
              <th className="right">NDCG</th>
              <th className="right">MAP</th>
            </tr>
          </thead>
          <tbody>
            {sortedK.map(([k, run]) => (
              <tr key={k}>
                <td>
                  <strong>@{k}</strong>
                </td>
                <td className="right td-mono">
                  {run.precision_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono">
                  {run.recall_at_k?.toFixed(4) ?? '—'}
                </td>
                <td
                  className="right td-mono"
                  style={{ color, fontWeight: 700 }}
                >
                  {run.ndcg_at_k?.toFixed(4) ?? '—'}
                </td>
                <td className="right td-mono">
                  {run.map_at_k?.toFixed(4) ?? '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
