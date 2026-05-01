/**
 * App.jsx — Plataforma de Análise e Recomendações
 * -------------------------------------------------
 * Componente raiz. Gerencia:
 *   1. Navegação entre páginas (Dashboard | Ofertas)
 *   2. Metadados e filtro de período (somente no Dashboard)
 *   3. KPIs consolidados do Dashboard
 */
import { useState, useEffect, useCallback } from 'react'
import { Routes, Route } from 'react-router-dom'
import KPICard           from './components/KPICard.jsx'
import RevenueChart      from './components/RevenueChart.jsx'
import YoYChart          from './components/YoYChart.jsx'
import SeasonalityChart  from './components/SeasonalityChart.jsx'
import ProductRanking    from './components/ProductRanking.jsx'
import GeographicTable   from './components/GeographicTable.jsx'
import TopCustomers      from './components/TopCustomers.jsx'
import CustomerShareChart from './components/CustomerShareChart.jsx'
import OffersPage        from './components/OffersPage.jsx'
import LoginPage         from './components/LoginPage.jsx'
import PrivateRoute      from './components/PrivateRoute.jsx'
import { logout, apiFetch, getUserRole, getToken } from './api.js'
import UsersPage from './components/UsersPage.jsx'
import ProductLifecyclePage from './components/ProductLifecyclePage.jsx'
import './index.css'

// ── Formatadores ──────────────────────────────────────────────────────────────
function fmtBRL(v) {
  if (v == null || isNaN(v)) return '—'
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(2)}M`
  if (v >= 1_000)     return `R$ ${(v / 1_000).toFixed(0)}K`
  return `R$ ${Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`
}

function fmtInt(v) {
  if (v == null) return '—'
  return Number(v).toLocaleString('pt-BR')
}

// ── Presets de período ────────────────────────────────────────────────────────
function buildPresets(maxYear) {
  return [
    { key: '5y',  label: '5 anos', from: maxYear - 4, to: maxYear },
    { key: '3y',  label: '3 anos', from: maxYear - 2, to: maxYear },
    { key: '1y',  label: '1 ano',  from: maxYear,     to: maxYear },
    { key: 'all', label: 'Tudo',   from: null,         to: null   },
  ]
}

const CURRENT_YEAR = new Date().getFullYear()

// ── Ícones de navegação (SVG inline, sem dependências) ────────────────────────
function IconDashboard({ active }) {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={active ? 2.2 : 1.8} strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </svg>
  )
}

function IconOffers({ active }) {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={active ? 2.2 : 1.8} strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 2L2 7l10 5 10-5-10-5z" />
      <path d="M2 17l10 5 10-5" />
      <path d="M2 12l10 5 10-5" />
    </svg>
  )
}

function IconLifecycle({ active }) {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={active ? 2.2 : 1.8} strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  )
}

// ── Componente principal (roteamento) ─────────────────────────────────────────
export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/*" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
    </Routes>
  )
}

// ── Dashboard (conteúdo protegido) ───────────────────────────────────────────
function Dashboard() {
  // ── Navegação ───────────────────────────────────────────────────────────────
  const [view, setView] = useState('dashboard')  // 'dashboard' | 'offers' | 'lifecycle' | 'users'
  const isAdmin = getUserRole() === 'admin'

  // ── Estado do Dashboard ─────────────────────────────────────────────────────
  const [meta,       setMeta]       = useState(null)
  const [kpis,       setKpis]       = useState(null)
  const [loading,    setLoading]    = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error,      setError]      = useState(null)

  // ── Filtro de período (apenas Dashboard) ────────────────────────────────────
  const [presets,  setPresets]  = useState(() => buildPresets(CURRENT_YEAR))
  const [preset,   setPreset]   = useState('5y')
  const [yearFrom, setYearFrom] = useState(CURRENT_YEAR - 4)
  const [yearTo,   setYearTo]   = useState(CURRENT_YEAR)

  function applyPreset(p) {
    setPreset(p.key)
    setYearFrom(p.from)
    setYearTo(p.to)
  }

  // ── Metadados ───────────────────────────────────────────────────────────────
  useEffect(() => {
    apiFetch('/api/business/meta')
      .then(r => r.json())
      .then(d => {
        setMeta(d)
        const newPresets = buildPresets(d.year_max)
        newPresets.find(p => p.key === 'all').from = d.year_min
        setPresets(newPresets)
        setPreset(prev => {
          const active = newPresets.find(p => p.key === prev)
          if (active) { setYearFrom(active.from); setYearTo(active.to) }
          return prev
        })
      })
      .catch(err => setError(err.message))
  }, [])

  // ── KPIs ────────────────────────────────────────────────────────────────────
  const fetchKpis = useCallback(async (isRefresh = false) => {
    try {
      if (isRefresh) setRefreshing(true)
      else           setLoading(true)
      setError(null)
      const params = new URLSearchParams()
      if (yearFrom) params.set('year_from', yearFrom)
      if (yearTo)   params.set('year_to',   yearTo)
      const res  = await apiFetch(`/api/business/kpis?${params}`)
      const data = await res.json()
      setKpis(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [yearFrom, yearTo])

  useEffect(() => { fetchKpis() }, [fetchKpis])

  const periodLabel =
    yearFrom && yearTo  ? `${yearFrom} – ${yearTo}`
    : yearFrom          ? `A partir de ${yearFrom}`
    : yearTo            ? `Até ${yearTo}`
    : 'Período completo'

  // ── Loading / Error (apenas no carregamento inicial do Dashboard) ───────────
  if (loading && !kpis && view === 'dashboard') {
    return (
      <div className="state-center">
        <div className="spinner" />
        <span>Carregando dados empresariais…</span>
      </div>
    )
  }

  if (error && !kpis && view === 'dashboard') {
    return (
      <div className="state-center">
        <div className="error-box">
          <span>⚠</span>
          <div>
            <strong>Erro ao carregar dados:</strong>
            <br />{error}
            <br /><small>Verifique se o serviço da API está rodando.</small>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="app">

      {/* ══ HEADER ══════════════════════════════════════════════════════════ */}
      <header className="header">
        <div className="header-inner">

          {/* ── Marca + Navegação ── */}
          <div className="header-left">
            <div className="header-brand">
              <h1>
                <span className="dot" style={{ background: view === 'offers' ? 'var(--purple)' : view === 'lifecycle' ? 'var(--amber)' : 'var(--green)' }} />
                {view === 'dashboard'
                  ? 'Visao 360 Empresarial'
                  : view === 'lifecycle'
                  ? 'Ciclo de Vida dos Produtos'
                  : 'Ofertas & Recomendacoes'}
              </h1>
              <p>{view === 'dashboard'
                ? 'Faturamento - Produtos - Clientes - Sazonalidade'
                : view === 'lifecycle'
                ? 'Intervalos de recompra - Tiers - Validacao de ofertas ML'
                : 'Modelos ML - Ciclo de Vida - Funil de Oportunidades'
              }</p>
            </div>

            {/* Abas de navegação */}
            <nav className="nav-tabs">
              <button
                className={`nav-tab ${view === 'dashboard' ? 'active' : ''}`}
                onClick={() => setView('dashboard')}
              >
                <IconDashboard active={view === 'dashboard'} />
                Dashboard
              </button>
              <button
                className={`nav-tab offers ${view === 'offers' ? 'active' : ''}`}
                onClick={() => setView('offers')}
              >
                <IconOffers active={view === 'offers'} />
                Ofertas
              </button>
              <button
                className={`nav-tab ${view === 'lifecycle' ? 'active' : ''}`}
                onClick={() => setView('lifecycle')}
              >
                <IconLifecycle active={view === 'lifecycle'} />
                Ciclo de Vida
              </button>
              {isAdmin && (
                <button
                  className={`nav-tab ${view === 'users' ? 'active' : ''}`}
                  onClick={() => setView('users')}
                >
                  Usuarios
                </button>
              )}
            </nav>
          </div>

          {/* ── Logout ── */}
          <button className="logout-btn" onClick={logout} title="Sair">Sair</button>
          {isAdmin && (
            <button
              className="nav-tab offers"
              style={{ marginLeft: 8 }}
              title="Abrir Dashboard ML como administrador"
              onClick={() => window.open(`http://localhost:3000/?token=${getToken()}`, '_blank')}
            >
              Dashboard ML
            </button>
          )}

          {/* ── Controles (apenas no Dashboard) ── */}
          {view === 'dashboard' && (
            <div className="header-controls">
              <div className="period-filters">
                {presets.map(p => (
                  <button
                    key={p.key}
                    className={`period-btn ${preset === p.key ? 'active' : ''}`}
                    onClick={() => applyPreset(p)}
                  >
                    {p.label}
                  </button>
                ))}
              </div>

              {meta?.last_refresh && (
                <div className="last-update">
                  <span>Dados atualizados em</span>
                  <strong>
                    {new Date(meta.last_refresh).toLocaleString('pt-BR', {
                      day: '2-digit', month: '2-digit', year: 'numeric',
                      hour: '2-digit', minute: '2-digit',
                    })}
                  </strong>
                </div>
              )}

              <button
                className="refresh-btn"
                onClick={() => fetchKpis(true)}
                disabled={refreshing}
              >
                {refreshing ? '↻ Atualizando…' : '↻ Atualizar'}
              </button>
            </div>
          )}

        </div>
      </header>

      {/* ══ CONTEÚDO ════════════════════════════════════════════════════════ */}

      {view === 'users' ? (
        <UsersPage />
      ) : view === 'lifecycle' ? (
        // ── Pagina de Ciclo de Vida ─────────────────────────────────────────
        <ProductLifecyclePage />
      ) : view === 'offers' ? (
        // ── Pagina de Ofertas ───────────────────────────────────────────────
        <OffersPage />
      ) : (
        // ── Dashboard Empresarial ───────────────────────────────────────────
        <main className="main">

          <section>
            <h2 className="section-title">Indicadores — {periodLabel}</h2>
            <div className="kpi-grid">
              <KPICard
                label="Faturamento Total"
                value={fmtBRL(kpis?.total_revenue)}
                sub={periodLabel}
                color="var(--green)"
                change={kpis?.yoy_growth}
                changeSub="YoY"
              />
              <KPICard
                label="Pedidos"
                value={fmtInt(kpis?.total_orders)}
                sub={`${fmtInt(kpis?.days_with_sales)} dias com vendas`}
                color="var(--blue)"
              />
              <KPICard
                label="Ticket Médio"
                value={kpis?.avg_ticket
                  ? 'R$ ' + Number(kpis.avg_ticket).toLocaleString('pt-BR', { minimumFractionDigits: 2 })
                  : '—'}
                sub="Por pedido"
                color="var(--purple)"
              />
              <KPICard
                label="Média Diária"
                value={kpis?.total_revenue && kpis?.days_with_sales
                  ? fmtBRL(kpis.total_revenue / kpis.days_with_sales)
                  : '—'}
                sub="Nos dias com venda"
                color="var(--amber)"
              />
              <KPICard
                label="Produtos no Catálogo"
                value={fmtInt(meta?.counts?.products)}
                sub="Histórico completo"
                color="#64748b"
              />
            </div>
          </section>

          <section>
            <h2 className="section-title">Evolução do Faturamento</h2>
            <RevenueChart yearFrom={yearFrom} yearTo={yearTo} />
          </section>

          <section>
            <h2 className="section-title">Análise Comparativa</h2>
            <div className="two-col">
              <YoYChart         yearFrom={yearFrom} yearTo={yearTo} />
              <SeasonalityChart yearFrom={yearFrom} yearTo={yearTo} />
            </div>
          </section>

          <section>
            <h2 className="section-title">Ranking de Produtos</h2>
            <ProductRanking yearFrom={yearFrom} yearTo={yearTo} />
          </section>

          <section>
            <h2 className="section-title">Clientes e Distribuição</h2>
            <div className="two-col">
              <CustomerShareChart yearFrom={yearFrom} yearTo={yearTo} />
              <TopCustomers       yearFrom={yearFrom} yearTo={yearTo} />
            </div>
            <div style={{ marginTop: 24 }}>
              <GeographicTable />
            </div>
          </section>

        </main>
      )}

    </div>
  )
}
