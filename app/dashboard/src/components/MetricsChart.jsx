import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { format } from 'date-fns'
import { ptBR } from 'date-fns/locale'

const STRATEGY_COLORS = {
  modelo_a_ranker: '#3b82f6',
  modelo_b_colaborativo: '#f97316',
}

const STRATEGY_LABELS = {
  modelo_a_ranker: 'Modelo A',
  modelo_b_colaborativo: 'Modelo B',
}

const METRIC_LABELS = {
  precision_at_k: 'Precision@K',
  recall_at_k: 'Recall@K',
  ndcg_at_k: 'NDCG@K',
  map_at_k: 'MAP@K',
  auc_roc: 'AUC-ROC',
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="custom-tooltip">
      <div className="label">{label}</div>
      {payload.map((p) => (
        <div key={p.dataKey} className="entry">
          <span className="dot" style={{ background: p.color }} />
          <span style={{ color: '#64748b', minWidth: 80 }}>{p.name}</span>
          <strong>{p.value?.toFixed(4)}</strong>
        </div>
      ))}
    </div>
  )
}

export default function MetricsChart({ runs, strategies, metric, k }) {
  const metricLabel = METRIC_LABELS[metric] || metric
  const isGlobalMetric = metric === 'auc_roc'

  // AUC-ROC is a global metric (not per-K), so use runs filtered by K but read auc_roc field
  // For @K metrics, filter to selected K as before
  const kRuns = isGlobalMetric
    ? runs.filter((r) => r.k === k)
    : runs.filter((r) => r.k === k)

  if (kRuns.length === 0) {
    return (
      <div className="empty-chart">
        Nenhum dado para K={k} com os filtros selecionados.
      </div>
    )
  }

  // Collect all unique timestamps per strategy
  const tsSet = new Set(kRuns.map((r) => r.evaluated_at))
  const timestamps = [...tsSet].sort()

  // Build chart data: one point per timestamp with value per strategy
  const chartData = timestamps.map((ts) => {
    const point = {
      ts,
      time: format(new Date(ts), 'dd/MM HH:mm', { locale: ptBR }),
    }
    for (const s of strategies) {
      const run = kRuns.find((r) => r.evaluated_at === ts && r.strategy === s)
      if (run) {
        const val = run[metric]
        if (val != null) point[s] = val
      }
    }
    return point
  })

  // Only show points where at least one strategy has data
  const validData = chartData.filter((d) =>
    strategies.some((s) => d[s] != null)
  )

  return (
    <div>
      <p className="chart-subtitle">
        Evolução de <strong>{metricLabel}</strong> ao longo das avaliações{isGlobalMetric ? '' : ` (K=${k})`}
      </p>
      <ResponsiveContainer width="100%" height={360}>
        <LineChart
          data={validData}
          margin={{ top: 10, right: 24, left: 0, bottom: 60 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis
            dataKey="time"
            tick={{ fontSize: 11, fontFamily: 'JetBrains Mono' }}
            angle={-40}
            textAnchor="end"
            height={70}
            interval={0}
          />
          <YAxis
            tick={{ fontSize: 11, fontFamily: 'JetBrains Mono' }}
            tickFormatter={(v) => v.toFixed(3)}
            domain={[0, 'auto']}
            width={52}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            verticalAlign="top"
            height={36}
            formatter={(value) => STRATEGY_LABELS[value] || value}
            iconType="circle"
          />
          {strategies.map((strategy) => (
            <Line
              key={strategy}
              type="monotone"
              dataKey={strategy}
              name={strategy}
              stroke={STRATEGY_COLORS[strategy] || '#6b7280'}
              strokeWidth={2.5}
              dot={{ r: 5, strokeWidth: 2 }}
              activeDot={{ r: 7 }}
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
