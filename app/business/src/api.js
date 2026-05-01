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

/**
 * Wrapper around fetch that injects the Bearer token
 * and handles 401 by redirecting to login.
 */
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

/**
 * Decodifica o payload do JWT (Base64, sem verificacao de assinatura).
 * Usado apenas para UX client-side — o servidor sempre valida o token.
 */
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

// ── Feedback endpoints ───────────────────────────────────────────────────────
export async function fetchFeedbackSummary() {
  const res = await apiFetch('/api/recommendations/feedback/summary')
  return res.json()
}

export async function fetchFeedbackRuns() {
  const res = await apiFetch('/api/recommendations/feedback/runs')
  return res.json()
}

export async function triggerFeedbackRun(windowDays = 30) {
  const params = new URLSearchParams({ window_days: windowDays })
  const res = await apiFetch(`/api/recommendations/feedback/run?${params}`, { method: 'POST' })
  return res.json()
}

export async function importFeedbackExcel(file) {
  const formData = new FormData()
  formData.append('file', file)
  const res = await apiFetch('/api/recommendations/feedback/import', {
    method: 'POST',
    body: formData,
  })
  return res.json()
}
