import { useState, useEffect, useCallback, useRef } from 'react'
import { Routes, Route } from 'react-router-dom'
import ModelCard from './components/ModelCard'
import MetricsChart from './components/MetricsChart'
import ComparisonChart from './components/ComparisonChart'
import RunsTable from './components/RunsTable'
import LoginPage from './components/LoginPage'
import PrivateRoute from './components/PrivateRoute'
import { logout, apiFetch, getUserRole, getToken, setToken } from './api.js'
import UsersPage from './components/UsersPage.jsx'
import './index.css'

const METRIC_OPTIONS = [
  { value: 'ndcg_at_k', label: 'NDCG@K' },
  { value: 'precision_at_k', label: 'Precision@K' },
  { value: 'recall_at_k', label: 'Recall@K' },
  { value: 'map_at_k', label: 'MAP@K' },
  { value: 'auc_roc', label: 'AUC-ROC' },
]

export default function App() {
  // SSO: ler token passado via URL ao vir do Business 360
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const tokenFromUrl = params.get('token')
    if (tokenFromUrl) {
      setToken(tokenFromUrl)
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [])

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/admin/users" element={<PrivateRoute><UsersPage /></PrivateRoute>} />
      <Route path="/*" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
    </Routes>
  )
}

function Dashboard() {
  const isAdmin = getUserRole() === 'admin'
  const [runs, setRuns] = useState([])
  const [strategies, setStrategies] = useState([])
  const [kValues, setKValues] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshing, setRefreshing] = useState(false)
  const [dataMeta, setDataMeta] = useState(null)
  const [adminMenuOpen, setAdminMenuOpen] = useState(false)
  const adminMenuRef = useRef(null)

  // Filters
  const [selectedStrategy, setSelectedStrategy] = useState('all')
  const [selectedK, setSelectedK] = useState(10)
  const [selectedMetric, setSelectedMetric] = useState('ndcg_at_k')

  const fetchData = useCallback(async (isRefresh = false) => {
    try {
      if (isRefresh) setRefreshing(true)
      else setLoading(true)
      setError(null)

      const [runsRes, strategiesRes, kRes] = await Promise.all([
        apiFetch('/api/evaluation-runs'),
        apiFetch('/api/strategies'),
        apiFetch('/api/k-values'),
      ])

      if (!runsRes.ok || !strategiesRes.ok || !kRes.ok) {
        throw new Error('Erro ao buscar dados da API')
      }

      const [runsData, strategiesData, kData] = await Promise.all([
        runsRes.json(),
        strategiesRes.json(),
        kRes.json(),
      ])

      setRuns(runsData)
      setStrategies(strategiesData)
      setKValues(kData)

      // Set default K to 10 if available
      if (kData.includes(10)) setSelectedK(10)
      else if (kData.length > 0) setSelectedK(kData[0])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  useEffect(() => {
    apiFetch('/api/business/meta')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setDataMeta(d) })
      .catch(() => null)
  }, [])

  useEffect(() => {
    function handleClickOutside(e) {
      if (adminMenuRef.current && !adminMenuRef.current.contains(e.target)) {
        setAdminMenuOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Build latestRunsByK per strategy (latest evaluated_at for each k)
  const latestRunsByStrategy = {}
  for (const s of strategies) {
    latestRunsByStrategy[s] = {}
    const stratRuns = runs.filter((r) => r.strategy === s)
    for (const k of kValues) {
      const kRuns = stratRuns
        .filter((r) => r.k === k)
        .sort((a, b) => new Date(b.evaluated_at) - new Date(a.evaluated_at))
      if (kRuns.length > 0) latestRunsByStrategy[s][k] = kRuns[0]
    }
  }

  // Filtered runs for table and charts
  const filteredRuns =
    selectedStrategy === 'all'
      ? runs
      : runs.filter((r) => r.strategy === selectedStrategy)

  const activeStrategies =
    selectedStrategy === 'all' ? strategies : [selectedStrategy]

  const lastEvaluatedAt =
    runs.length > 0
      ? new Date(
          Math.max(...runs.map((r) => new Date(r.evaluated_at)))
        ).toLocaleString('pt-BR')
      : '—'

  if (loading) {
    return (
      <div className="state-container">
        <div className="spinner" />
        <span>Carregando dados...</span>
      </div>
    )
  }

  if (error) {
    return (
      <div className="state-container">
        <div className="error-box">
          <span>⚠</span>
          <span>
            <strong>Erro ao carregar dados:</strong> {error}
          </span>
        </div>
      </div>
    )
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <div>
            <h1>CMML — Dashboard de Modelos ML</h1>
            <p>
              Monitoramento de performance e progressão dos modelos de
              recomendação
            </p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
            <div className="last-update">
              <span>Última avaliação</span>
              <strong>{lastEvaluatedAt}</strong>
            </div>
            {dataMeta?.last_sale_date && (
              <div className="last-update" style={{ borderLeft: '2px solid var(--purple)', paddingLeft: 10 }}>
                <span>Dados até</span>
                <strong style={{ color: 'var(--purple)' }}>
                  {new Date(dataMeta.last_sale_date + 'T12:00:00').toLocaleDateString('pt-BR', {
                    day: '2-digit', month: '2-digit', year: 'numeric',
                  })}
                </strong>
                <span style={{ fontSize: 11, color: '#64748b', marginTop: 1 }}>
                  Próx. atualização:{' '}
                  {new Date(new Date(dataMeta.last_sale_date + 'T12:00:00').getTime() + 86400000)
                    .toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' })}
                </span>
              </div>
            )}
            <button
              className="refresh-btn"
              onClick={() => fetchData(true)}
              disabled={refreshing}
            >
              {refreshing ? '↻ Atualizando…' : '↻ Atualizar'}
            </button>
            <button className="logout-btn" onClick={logout} title="Sair">Sair</button>
            {isAdmin && (
              <div style={{ position: 'relative' }} ref={adminMenuRef}>
                <button
                  className="refresh-btn"
                  style={{ marginLeft: 8 }}
                  onClick={() => setAdminMenuOpen(o => !o)}
                >
                  Administração ▾
                </button>
                {adminMenuOpen && (
                  <div style={{
                    position: 'absolute', right: 0, top: 'calc(100% + 4px)',
                    background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8,
                    boxShadow: '0 4px 20px rgba(0,0,0,0.12)', minWidth: 180, zIndex: 100,
                  }}>
                    <button
                      onClick={() => { setAdminMenuOpen(false); window.location.href = '/admin/users' }}
                      style={{
                        display: 'block', width: '100%', textAlign: 'left',
                        padding: '10px 16px', background: 'none', border: 'none',
                        cursor: 'pointer', fontSize: 14, color: '#374151',
                      }}
                      onMouseEnter={e => e.currentTarget.style.background = '#f8fafc'}
                      onMouseLeave={e => e.currentTarget.style.background = 'none'}
                    >
                      Usuários
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </header>

      <main className="main">
        {/* ── Model Summary Cards ── */}
        <section>
          <h2 className="section-title">Resumo do Último Treino</h2>
          <div className="cards-grid">
            {strategies.map((strategy) => (
              <ModelCard
                key={strategy}
                strategy={strategy}
                latestRunsByK={latestRunsByStrategy[strategy] || {}}
              />
            ))}
          </div>
        </section>

        {/* ── Filters ── */}
        <section>
          <div className="filters-card">
            <div className="filter-group">
              <label>Modelo</label>
              <select
                value={selectedStrategy}
                onChange={(e) => setSelectedStrategy(e.target.value)}
              >
                <option value="all">Todos os modelos</option>
                {strategies.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>

            <div className="filter-group">
              <label>K (gráfico de progressão)</label>
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

            <div className="filter-group">
              <label>Métrica</label>
              <select
                value={selectedMetric}
                onChange={(e) => setSelectedMetric(e.target.value)}
              >
                {METRIC_OPTIONS.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </section>

        {/* ── Progression Line Chart ── */}
        <section>
          <h2 className="section-title">Progressão de Qualidade</h2>
          <div className="chart-card">
            <MetricsChart
              runs={filteredRuns}
              strategies={activeStrategies}
              metric={selectedMetric}
              k={selectedK}
            />
          </div>
        </section>

        {/* ── Comparison Bar Chart ── */}
        <section>
          <h2 className="section-title">Comparação por Modelo</h2>
          <div className="chart-card">
            <ComparisonChart
              runs={runs}
              strategies={strategies}
              kValues={kValues}
            />
          </div>
        </section>

        {/* ── Data Table ── */}
        <section>
          <h2 className="section-title">
            Histórico de Avaliações
            <span
              style={{
                fontSize: 12,
                fontWeight: 400,
                color: '#64748b',
                marginLeft: 8,
              }}
            >
              ({filteredRuns.length} registros)
            </span>
          </h2>
          <RunsTable runs={filteredRuns} />
        </section>
      </main>
    </div>
  )
}
