/**
 * LoginPage.jsx — Tela de autenticacao
 *
 * Envia credenciais para POST /api/auth/token.
 * Em sucesso, salva o token e redireciona para /.
 * Em erro, exibe mensagem generica (nao revela se usuario existe).
 */
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { setToken } from '../api.js'

const API_URL = import.meta.env.VITE_API_URL || ''

export default function LoginPage() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    setLoading(true)

    try {
      const res = await fetch(`${API_URL}/api/auth/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })

      if (!res.ok) {
        setError('Credenciais invalidas')
        return
      }

      const data = await res.json()
      setToken(data.access_token)
      navigate('/', { replace: true })
    } catch {
      setError('Erro ao conectar com o servidor')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-container">
      <form className="login-form" onSubmit={handleSubmit}>
        <div className="login-header">
          <h1>CMML</h1>
          <p>Visao 360 Empresarial</p>
        </div>

        {error && <div className="login-error">{error}</div>}

        <div className="login-field">
          <label htmlFor="username">Usuario</label>
          <input
            id="username"
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
            required
            disabled={loading}
          />
        </div>

        <div className="login-field">
          <label htmlFor="password">Senha</label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            required
            disabled={loading}
          />
        </div>

        <button type="submit" className="login-btn" disabled={loading}>
          {loading ? 'Entrando...' : 'Entrar'}
        </button>
      </form>
    </div>
  )
}
