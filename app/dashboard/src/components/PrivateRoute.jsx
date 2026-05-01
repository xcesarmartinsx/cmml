/**
 * PrivateRoute.jsx — Rota protegida com verificacao de autenticacao e role.
 *
 * - Sem token: redireciona para /login
 * - Token com role commercial: exibe AccessDenied (admin-only app)
 * - Token com role admin: renderiza children
 */
import { Navigate } from 'react-router-dom'
import { getToken, getUserRole } from '../api.js'
import AccessDenied from './AccessDenied'

export default function PrivateRoute({ children }) {
  if (!getToken()) {
    return <Navigate to="/login" replace />
  }
  const role = getUserRole()
  if (role && role !== 'admin') {
    return <AccessDenied />
  }
  return children
}
