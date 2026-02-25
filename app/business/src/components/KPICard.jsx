/**
 * KPICard.jsx
 * -----------
 * Exibe um indicador-chave de performance (KPI) com:
 *   - Rótulo descritivo
 *   - Valor principal formatado
 *   - Texto secundário (ex.: período de referência)
 *   - Badge opcional de variação (↑ crescimento / ↓ queda)
 *
 * Props:
 *   label     {string}  Rótulo do KPI (ex.: "Faturamento Total")
 *   value     {string}  Valor formatado (ex.: "R$ 12,5M")
 *   sub       {string}  Texto auxiliar abaixo do valor
 *   color     {string}  Cor CSS da borda lateral (ex.: "#10b981")
 *   change    {number}  Variação percentual YoY (null = não exibe badge)
 *   changeSub {string}  Texto extra do badge (ex.: "vs ano anterior")
 */
export default function KPICard({ label, value, sub, color, change, changeSub }) {
  // Determina o tipo do badge com base no sinal da variação.
  const badgeClass =
    change === null || change === undefined ? null
    : change > 0  ? 'up'
    : change < 0  ? 'down'
    : 'flat'

  // Símbolo de seta para indicar direção da variação.
  const arrow = change > 0 ? '▲' : change < 0 ? '▼' : '—'

  return (
    <div
      className="kpi-card"
      // Injeta a cor via variável CSS para que o seletor .kpi-card use --kpi-color.
      style={{ '--kpi-color': color || 'var(--green)' }}
    >
      {/* Rótulo superior em caixa alta */}
      <div className="kpi-label">{label}</div>

      {/* Valor principal — fonte grande e negrito */}
      <div className="kpi-value">{value}</div>

      {/* Linha inferior: texto auxiliar + badge de variação */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        {sub && <span className="kpi-sub">{sub}</span>}

        {/* Badge de crescimento: só renderiza se change foi passado */}
        {badgeClass && (
          <span className={`kpi-badge ${badgeClass}`}>
            {arrow} {Math.abs(change).toFixed(1)}%
            {changeSub && <span style={{ fontWeight: 400, marginLeft: 3 }}>{changeSub}</span>}
          </span>
        )}
      </div>
    </div>
  )
}
