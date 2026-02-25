/**
 * PrivateRoute.jsx — Protecao de rotas autenticadas
 *
 * Verifica se 'cmml_token' existe no sessionStorage.
 * Se nao existe, redireciona para /login.
 */
import { Navigate } from 'react-router-dom'
import { getToken } from '../api.js'

export default function PrivateRoute({ children }) {
  if (!getToken()) {
    return <Navigate to="/login" replace />
  }
  return children
}
