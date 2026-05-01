/**
 * UsersPage.jsx — Gerenciamento de usuarios do sistema CMML.
 * Acessivel apenas para administradores via Business 360 (porta 3001).
 *
 * Funcionalidades:
 *  - Listar usuarios (GET /api/admin/users)
 *  - Criar usuario (POST /api/admin/users)
 *  - Editar usuario: nome, role, ativo/inativo (PUT /api/admin/users/:id)
 *  - Desativar usuario (DELETE /api/admin/users/:id — soft delete)
 */
import { useState, useEffect, useCallback } from 'react'
import { apiFetch, getUsername } from '../api.js'

const ROLE_LABELS = { admin: 'Administrador', commercial: 'Comercial' }

export default function UsersPage() {
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [showForm, setShowForm] = useState(false)
  const [editingUser, setEditingUser] = useState(null)
  const [saving, setSaving] = useState(false)
  const currentUsername = getUsername()

  // Form state
  const [form, setForm] = useState({ username: '', password: '', full_name: '', role: 'commercial' })
  const [formError, setFormError] = useState(null)

  const fetchUsers = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)
      const res = await apiFetch('/api/admin/users')
      if (!res.ok) throw new Error('Erro ao buscar usuarios')
      setUsers(await res.json())
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchUsers() }, [fetchUsers])

  function openCreate() {
    setEditingUser(null)
    setForm({ username: '', password: '', full_name: '', role: 'commercial' })
    setFormError(null)
    setShowForm(true)
  }

  function openEdit(user) {
    setEditingUser(user)
    setForm({ username: user.username, password: '', full_name: user.full_name || '', role: user.role })
    setFormError(null)
    setShowForm(true)
  }

  function closeForm() {
    setShowForm(false)
    setEditingUser(null)
    setFormError(null)
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setSaving(true)
    setFormError(null)
    try {
      let res
      if (editingUser) {
        const body = { full_name: form.full_name, role: form.role }
        if (form.password) body.password = form.password
        res = await apiFetch(`/api/admin/users/${editingUser.user_id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        })
      } else {
        if (!form.username || !form.password) {
          setFormError('Username e senha sao obrigatorios')
          setSaving(false)
          return
        }
        res = await apiFetch('/api/admin/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username: form.username, password: form.password, full_name: form.full_name, role: form.role }),
        })
      }
      if (!res.ok) {
        const data = await res.json()
        throw new Error(data.detail || 'Erro ao salvar usuario')
      }
      closeForm()
      fetchUsers()
    } catch (err) {
      setFormError(err.message)
    } finally {
      setSaving(false)
    }
  }

  async function handleToggleActive(user) {
    try {
      const res = await apiFetch(`/api/admin/users/${user.user_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_active: !user.is_active }),
      })
      if (!res.ok) throw new Error('Erro ao atualizar status')
      fetchUsers()
    } catch (err) {
      alert(err.message)
    }
  }

  if (loading) return (
    <div className="state-center">
      <div className="spinner" />
      <span>Carregando usuarios...</span>
    </div>
  )

  if (error) return (
    <div className="state-center">
      <div className="error-box"><span>⚠</span><span>{error}</span></div>
    </div>
  )

  return (
    <main className="main">
      <section>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h2 className="section-title" style={{ margin: 0 }}>Gerenciar Usuarios</h2>
          <button
            className="refresh-btn"
            onClick={openCreate}
            style={{ background: 'var(--green, #22c55e)', color: '#fff', borderColor: 'transparent' }}
          >
            + Novo Usuario
          </button>
        </div>

        <div className="chart-card" style={{ padding: '1.5rem' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #e2e8f0', textAlign: 'left' }}>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Usuario</th>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Nome</th>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Perfil</th>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Status</th>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Criado em</th>
                <th style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600 }}>Acoes</th>
              </tr>
            </thead>
            <tbody>
              {users.map(u => (
                <tr key={u.user_id} style={{ borderBottom: '1px solid #f1f5f9' }}>
                  <td style={{ padding: '10px 12px', fontWeight: 500 }}>
                    {u.username}
                    {u.username === currentUsername && (
                      <span style={{ marginLeft: 6, fontSize: 11, color: '#64748b', background: '#f1f5f9', borderRadius: 4, padding: '2px 6px' }}>voce</span>
                    )}
                  </td>
                  <td style={{ padding: '10px 12px', color: '#475569' }}>{u.full_name || '—'}</td>
                  <td style={{ padding: '10px 12px' }}>
                    <span style={{
                      fontSize: 12, fontWeight: 600, borderRadius: 12, padding: '3px 10px',
                      background: u.role === 'admin' ? '#dbeafe' : '#f0fdf4',
                      color: u.role === 'admin' ? '#1d4ed8' : '#15803d',
                    }}>
                      {ROLE_LABELS[u.role] || u.role}
                    </span>
                  </td>
                  <td style={{ padding: '10px 12px' }}>
                    <span style={{
                      fontSize: 12, fontWeight: 600, borderRadius: 12, padding: '3px 10px',
                      background: u.is_active ? '#f0fdf4' : '#fef2f2',
                      color: u.is_active ? '#15803d' : '#dc2626',
                    }}>
                      {u.is_active ? 'Ativo' : 'Inativo'}
                    </span>
                  </td>
                  <td style={{ padding: '10px 12px', color: '#64748b', fontSize: 13 }}>
                    {new Date(u.created_at).toLocaleDateString('pt-BR')}
                  </td>
                  <td style={{ padding: '10px 12px' }}>
                    <div style={{ display: 'flex', gap: 8 }}>
                      <button
                        onClick={() => openEdit(u)}
                        style={{ fontSize: 12, padding: '4px 10px', borderRadius: 4, border: '1px solid #cbd5e1', background: '#fff', cursor: 'pointer' }}
                      >
                        Editar
                      </button>
                      {u.username !== currentUsername && (
                        <button
                          onClick={() => handleToggleActive(u)}
                          style={{
                            fontSize: 12, padding: '4px 10px', borderRadius: 4, border: 'none', cursor: 'pointer',
                            background: u.is_active ? '#fee2e2' : '#dcfce7',
                            color: u.is_active ? '#dc2626' : '#16a34a',
                          }}
                        >
                          {u.is_active ? 'Desativar' : 'Ativar'}
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr><td colSpan={6} style={{ padding: 24, textAlign: 'center', color: '#94a3b8' }}>Nenhum usuario cadastrado.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {showForm && (
        <div style={{
          position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
        }}>
          <div style={{
            background: '#fff', borderRadius: 12, padding: '2rem',
            width: '100%', maxWidth: 440, boxShadow: '0 20px 60px rgba(0,0,0,0.2)',
          }}>
            <h2 style={{ marginBottom: 20, fontSize: 18 }}>
              {editingUser ? `Editar: ${editingUser.username}` : 'Novo Usuario'}
            </h2>

            {formError && (
              <div style={{ background: '#fee2e2', color: '#dc2626', borderRadius: 6, padding: '10px 14px', marginBottom: 16, fontSize: 14 }}>
                {formError}
              </div>
            )}

            <form onSubmit={handleSubmit}>
              {!editingUser && (
                <div style={{ marginBottom: 14 }}>
                  <label style={{ display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#374151' }}>
                    Username *
                  </label>
                  <input
                    type="text"
                    value={form.username}
                    onChange={e => setForm(f => ({ ...f, username: e.target.value }))}
                    required
                    style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 14, boxSizing: 'border-box' }}
                  />
                </div>
              )}

              <div style={{ marginBottom: 14 }}>
                <label style={{ display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#374151' }}>
                  {editingUser ? 'Nova Senha (deixe em branco para manter)' : 'Senha *'}
                </label>
                <input
                  type="password"
                  value={form.password}
                  onChange={e => setForm(f => ({ ...f, password: e.target.value }))}
                  required={!editingUser}
                  style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 14, boxSizing: 'border-box' }}
                />
              </div>

              <div style={{ marginBottom: 14 }}>
                <label style={{ display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#374151' }}>
                  Nome Completo
                </label>
                <input
                  type="text"
                  value={form.full_name}
                  onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))}
                  style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 14, boxSizing: 'border-box' }}
                />
              </div>

              <div style={{ marginBottom: 20 }}>
                <label style={{ display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#374151' }}>
                  Perfil *
                </label>
                <select
                  value={form.role}
                  onChange={e => setForm(f => ({ ...f, role: e.target.value }))}
                  required
                  style={{ width: '100%', padding: '8px 12px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 14, boxSizing: 'border-box', background: '#fff' }}
                >
                  <option value="commercial">Comercial — acesso ao Business 360</option>
                  <option value="admin">Administrador — acesso total</option>
                </select>
              </div>

              <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
                <button
                  type="button"
                  onClick={closeForm}
                  style={{ padding: '8px 20px', borderRadius: 6, border: '1px solid #d1d5db', background: '#fff', cursor: 'pointer', fontSize: 14 }}
                >
                  Cancelar
                </button>
                <button
                  type="submit"
                  disabled={saving}
                  style={{ padding: '8px 20px', borderRadius: 6, border: 'none', background: '#3b82f6', color: '#fff', cursor: 'pointer', fontSize: 14, fontWeight: 600 }}
                >
                  {saving ? 'Salvando...' : editingUser ? 'Salvar' : 'Criar'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </main>
  )
}
