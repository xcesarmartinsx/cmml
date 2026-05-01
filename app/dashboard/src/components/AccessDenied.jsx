/**
 * AccessDenied.jsx — Tela exibida quando um usuario commercial
 * tenta acessar o Dashboard ML (exclusivo para admins).
 */
import { clearToken } from '../api.js'

export default function AccessDenied() {
  function handleBack() {
    clearToken()
    window.location.href = 'http://localhost:3001'
  }

  return (
    <div className="state-container" style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <div className="error-box" style={{ maxWidth: 420, textAlign: 'center', padding: '2rem' }}>
        <div style={{ fontSize: 48, marginBottom: 16 }}>🔒</div>
        <h2 style={{ marginBottom: 8 }}>Acesso Restrito</h2>
        <p style={{ marginBottom: 20, color: '#64748b' }}>
          Este painel é exclusivo para administradores.
        </p>
        <button
          onClick={handleBack}
          style={{
            background: 'var(--green, #22c55e)',
            color: '#fff',
            border: 'none',
            borderRadius: 6,
            padding: '0.6rem 1.4rem',
            cursor: 'pointer',
            fontWeight: 600,
          }}
        >
          Voltar ao Business 360°
        </button>
      </div>
    </div>
  )
}
