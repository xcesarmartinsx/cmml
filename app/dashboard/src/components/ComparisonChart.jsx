import {
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
} from 'recharts'
import { useState } from 'react'

const STRATEGY_COLORS = {
  modelo_a_ranker: '#3b82f6',
  modelo_b_colaborativo: '#f97316',
}

const STRATEGY_LABELS = {
  modelo_a_ranker: 'Modelo A',
  modelo_b_colaborativo: 'Modelo B',
}

const METRICS = ['precision_at_k', 'recall_at_k', 'ndcg_at_k', 'map_at_k']
const METRIC_LABELS = {
  precision_at_k: 'Precision',
  recall_at_k: 'Recall',
  ndcg_at_k: 'NDCG',
  map_at_k: 'MAP',
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="custom-tooltip">
      <div className="label">{label}</div>
      {payload.map((p) => (
        <div key={p.dataKey} className="entry">
          <span className="dot" style={{ background: p.color }} />
          <span style={{ color: '#64748b', minWidth: 80 }}>
            {STRATEGY_LABELS[p.dataKey] || p.dataKey}
          </span>
          <strong>{p.value?.toFixed(4)}</strong>
        </div>
      ))}
    </div>
  )
}

export default function ComparisonChart({ runs, strategies, kValues }) {
  const [selectedK, setSelectedK] = useState(
    kValues.includes(10) ? 10 : kValues[0] ?? 10
  )

  // Get latest run per strategy for selectedK
  const latestByStrategy = {}
  runs
    .filter((r) => r.k === selectedK)
    .sort((a, b) => new Date(b.evaluated_at) - new Date(a.evaluated_at))
    .forEach((r) => {
      if (!latestByStrategy[r.strategy]) latestByStrategy[r.strategy] = r
    })

  // Bar chart data: one bar-group per metric
  const barData = METRICS.map((metric) => {
    const point = { metric: METRIC_LABELS[metric] }
    for (const s of strategies) {
      const run = latestByStrategy[s]
      if (run) point[s] = run[metric]
    }
    return point
  })

  return (
    <div>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 16,
        }}
      >
        <p className="chart-subtitle" style={{ marginBottom: 0 }}>
          Comparação das <strong>métricas mais recentes</strong> por modelo
        </p>
        <div className="button-group">
          {kValues.map((k) => (
            <button
              key={k}
              className={`k-button ${selectedK === k ? 'active' : ''}`}
              onClick={() => setSelectedK(k)}
            >
              @{k}
            </button>
          ))}
        </div>
      </div>

      <ResponsiveContainer width="100%" height={300}>
        <BarChart
          data={barData}
          margin={{ top: 10, right: 24, left: 0, bottom: 10 }}
          barCategoryGap="30%"
          barGap={4}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis
            dataKey="metric"
            tick={{ fontSize: 13, fontWeight: 600 }}
          />
          <YAxis
            tick={{ fontSize: 11, fontFamily: 'JetBrains Mono' }}
            tickFormatter={(v) => v.toFixed(3)}
            width={52}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            verticalAlign="top"
            height={36}
            formatter={(v) => STRATEGY_LABELS[v] || v}
            iconType="circle"
          />
          {strategies.map((strategy) => (
            <Bar
              key={strategy}
              dataKey={strategy}
              name={strategy}
              fill={STRATEGY_COLORS[strategy] || '#6b7280'}
              radius={[4, 4, 0, 0]}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
