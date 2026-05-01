/**
 * api.js — Authenticated fetch wrapper
 *
 * Adds Authorization header to all requests.
 * On 401 response: clears token and redirects to /login.
 */

const TOKEN_KEY = 'cmml_token'

export function getToken() {
  return sessionStorage.getItem(TOKEN_KEY)
}

export function setToken(token) {
  sessionStorage.setItem(TOKEN_KEY, token)
}

export function clearToken() {
  sessionStorage.removeItem(TOKEN_KEY)
}

export function logout() {
  clearToken()
  window.location.href = '/login'
}

export async function apiFetch(url, options = {}) {
  const token = getToken()
  const headers = { ...options.headers }

  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }

  const response = await fetch(url, { ...options, headers })

  if (response.status === 401) {
    clearToken()
    window.location.href = '/login'
    throw new Error('Sessao expirada')
  }

  return response
}

function _decodeTokenPayload() {
  const token = getToken()
  if (!token) return null
  try {
    return JSON.parse(atob(token.split('.')[1]))
  } catch {
    return null
  }
}

export function getUserRole() {
  return _decodeTokenPayload()?.role || null
}

export function getUsername() {
  return _decodeTokenPayload()?.sub || null
}
