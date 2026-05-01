/**
 * UsersPage.jsx — Administração > Usuários
 * Dashboard ML (porta 3000) — área administrativa exclusiva para admins.
 *
 * Funcionalidades:
 *  - Listar usuários com busca/filtro (username ou nome)
 *  - Criar usuário (com validação e confirmação de senha)
 *  - Editar usuário (nome, role, status ativo)
 *  - Desativar/Ativar usuário (soft delete via is_active)
 *  - Alterar senha (fluxo dedicado e seguro, modal separado)
 */
import { useState, useEffect, useCallback } from 'react'
import { apiFetch, getUsername } from '../api.js'

const ROLE_LABELS = { admin: 'Administrador', commercial: 'Comercial' }

// ── Estilos inline reutilizáveis ──────────────────────────────────────────────
const inputStyle = {
  width: '100%', padding: '8px 12px', borderRadius: 6,
  border: '1px solid #d1d5db', fontSize: 14, boxSizing: 'border-box',
  outline: 'none',
}
const labelStyle = {
  display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#374151',
}
const fieldStyle = { marginBottom: 16 }

// ── Componente principal ──────────────────────────────────────────────────────
export default function UsersPage() {
  const [users, setUsers]         = useState([])
  const [loading, setLoading]     = useState(true)
  const [error, setError]         = useState(null)
  const [search, setSearch]       = useState('')

  // Modais
  const [modal, setModal]         = useState(null)  // null | 'create' | 'edit' | 'password'
  const [selectedUser, setSelectedUser] = useState(null)

  const currentUsername = getUsername()

  // Formulário criar/editar
  const [form, setForm]           = useState({ username: '', password: '', confirmPassword: '', full_name: '', role: 'commercial' })
  const [formError, setFormError] = useState(null)
  const [saving, setSaving]       = useState(false)

  // Formulário alterar senha
  const [pwForm, setPwForm]       = useState({ password: '', confirmPassword: '' })
  const [pwError, setPwError]     = useState(null)
  const [pwSaving, setPwSaving]   = useState(false)

  // ── Fetch ───────────────────────────────────────────────────────────────────
  const fetchUsers = useCallback(async () => {
    try {
      setLoading(true); setError(null)
      const res = await apiFetch('/api/admin/users')
      if (!res.ok) throw new Error('Erro ao buscar usuários')
      setUsers(await res.json())
    } catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { fetchUsers() }, [fetchUsers])

  // ── Filtro ──────────────────────────────────────────────────────────────────
  const filtered = users.filter(u => {
    const q = search.toLowerCase()
    return !q || u.username.toLowerCase().includes(q) || (u.full_name || '').toLowerCase().includes(q)
  })

  // ── Abrir modais ────────────────────────────────────────────────────────────
  function openCreate() {
    setForm({ username: '', password: '', confirmPassword: '', full_name: '', role: 'commercial' })
    setFormError(null); setModal('create')
  }
  function openEdit(u) {
    setSelectedUser(u)
    setForm({ username: u.username, password: '', confirmPassword: '', full_name: u.full_name || '', role: u.role })
    setFormError(null); setModal('edit')
  }
  function openPassword(u) {
    setSelectedUser(u)
    setPwForm({ password: '', confirmPassword: '' })
    setPwError(null); setModal('password')
  }
  function closeModal() { setModal(null); setSelectedUser(null) }

  // ── Validação de senha ──────────────────────────────────────────────────────
  function validatePassword(pw, confirm) {
    if (pw.length < 8) return 'A senha deve ter no mínimo 8 caracteres'
    if (pw !== confirm) return 'As senhas não coincidem'
    return null
  }

  // ── Submit criar ────────────────────────────────────────────────────────────
  async function handleCreate(e) {
    e.preventDefault(); setSaving(true); setFormError(null)
    if (!form.username.trim()) { setFormError('Username é obrigatório'); setSaving(false); return }
    const pwErr = validatePassword(form.password, form.confirmPassword)
    if (pwErr) { setFormError(pwErr); setSaving(false); return }
    try {
      const res = await apiFetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: form.username.trim(), password: form.password, full_name: form.full_name.trim() || null, role: form.role }),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Erro ao criar usuário') }
      closeModal(); fetchUsers()
    } catch (err) { setFormError(err.message) }
    finally { setSaving(false) }
  }

  // ── Submit editar ────────────────────────────────────────────────────────────
  async function handleEdit(e) {
    e.preventDefault(); setSaving(true); setFormError(null)
    try {
      const body = { full_name: form.full_name.trim() || null, role: form.role }
      const res = await apiFetch(`/api/admin/users/${selectedUser.user_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Erro ao atualizar') }
      closeModal(); fetchUsers()
    } catch (err) { setFormError(err.message) }
    finally { setSaving(false) }
  }

  // ── Submit alterar senha ─────────────────────────────────────────────────────
  async function handlePassword(e) {
    e.preventDefault(); setPwSaving(true); setPwError(null)
    const err = validatePassword(pwForm.password, pwForm.confirmPassword)
    if (err) { setPwError(err); setPwSaving(false); return }
    try {
      const res = await apiFetch(`/api/admin/users/${selectedUser.user_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: pwForm.password }),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Erro ao alterar senha') }
      closeModal(); fetchUsers()
    } catch (err) { setPwError(err.message) }
    finally { setPwSaving(false) }
  }

  // ── Toggle ativo/inativo ─────────────────────────────────────────────────────
  async function handleToggle(u) {
    try {
      const res = await apiFetch(`/api/admin/users/${u.user_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_active: !u.is_active }),
      })
      if (!res.ok) throw new Error('Erro ao atualizar status')
      fetchUsers()
    } catch (err) { alert(err.message) }
  }

  // ── Render ───────────────────────────────────────────────────────────────────
  if (loading) return (
    <div className="state-container"><div className="spinner" /><span>Carregando usuários...</span></div>
  )
  if (error) return (
    <div className="state-container">
      <div className="error-box"><span>⚠</span><span>{error}</span></div>
    </div>
  )

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div>
            <h1>CMML — Administração › Usuários</h1>
            <p>Gerenciamento de acesso ao sistema · {users.length} usuário{users.length !== 1 ? 's' : ''} cadastrado{users.length !== 1 ? 's' : ''}</p>
          </div>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <button className="refresh-btn" onClick={() => window.location.href = '/'}>← Dashboard ML</button>
            <button
              className="refresh-btn"
              onClick={openCreate}
              style={{ background: '#3b82f6', color: '#fff', borderColor: '#3b82f6' }}
            >
              + Novo Usuário
            </button>
          </div>
        </div>
      </header>

      <main className="main">
        <section>
          <h2 className="section-title">
            Usuários do Sistema
            {search && <span style={{ fontSize: 13, fontWeight: 400, color: '#64748b', marginLeft: 8 }}>— filtrando por "{search}"</span>}
          </h2>
          <div className="chart-card" style={{ padding: '1.5rem' }}>

            {/* Busca */}
            <div style={{ marginBottom: 16 }}>
              <input
                type="text"
                placeholder="Buscar por usuário ou nome..."
                value={search}
                onChange={e => setSearch(e.target.value)}
                style={{ ...inputStyle, maxWidth: 340 }}
              />
            </div>

            {/* Tabela */}
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid #e2e8f0' }}>
                    {['Usuário', 'Nome', 'Perfil', 'Status', 'Criado em', 'Ações'].map(h => (
                      <th key={h} style={{ padding: '8px 12px', color: '#64748b', fontWeight: 600, textAlign: 'left', fontSize: 13 }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(u => (
                    <tr key={u.user_id} style={{ borderBottom: '1px solid #f1f5f9' }}>
                      <td style={{ padding: '10px 12px', fontWeight: 500 }}>
                        {u.username}
                        {u.username === currentUsername && (
                          <span style={{ marginLeft: 6, fontSize: 11, color: '#64748b', background: '#f1f5f9', borderRadius: 4, padding: '2px 6px' }}>você</span>
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
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          <button onClick={() => openEdit(u)} style={{ fontSize: 12, padding: '4px 10px', borderRadius: 4, border: '1px solid #cbd5e1', background: '#fff', cursor: 'pointer' }}>Editar</button>
                          <button onClick={() => openPassword(u)} style={{ fontSize: 12, padding: '4px 10px', borderRadius: 4, border: '1px solid #cbd5e1', background: '#fff', cursor: 'pointer' }}>Senha</button>
                          {u.username !== currentUsername && (
                            <button
                              onClick={() => handleToggle(u)}
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
                  {filtered.length === 0 && (
                    <tr><td colSpan={6} style={{ padding: 32, textAlign: 'center', color: '#94a3b8' }}>
                      {search ? 'Nenhum usuário encontrado para esta busca.' : 'Nenhum usuário cadastrado.'}
                    </td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </main>

      {/* ── Modal overlay ────────────────────────────────────────────────────── */}
      {modal && (
        <div
          style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}
          onClick={e => { if (e.target === e.currentTarget) closeModal() }}
        >
          <div style={{ background: '#fff', borderRadius: 12, padding: '2rem', width: '100%', maxWidth: 460, boxShadow: '0 20px 60px rgba(0,0,0,0.2)' }}>

            {/* Modal — Criar */}
            {modal === 'create' && (
              <>
                <h2 style={{ marginBottom: 20, fontSize: 18 }}>Novo Usuário</h2>
                {formError && <div style={{ background: '#fee2e2', color: '#dc2626', borderRadius: 6, padding: '10px 14px', marginBottom: 16, fontSize: 14 }}>{formError}</div>}
                <form onSubmit={handleCreate}>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Username *</label>
                    <input style={inputStyle} type="text" value={form.username} required
                      onChange={e => setForm(f => ({ ...f, username: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Senha * <span style={{ fontWeight: 400, color: '#64748b' }}>(mínimo 8 caracteres)</span></label>
                    <input style={inputStyle} type="password" value={form.password} required
                      onChange={e => setForm(f => ({ ...f, password: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Confirmar Senha *</label>
                    <input style={inputStyle} type="password" value={form.confirmPassword} required
                      onChange={e => setForm(f => ({ ...f, confirmPassword: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Nome Completo</label>
                    <input style={inputStyle} type="text" value={form.full_name}
                      onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Perfil *</label>
                    <select style={inputStyle} value={form.role} required onChange={e => setForm(f => ({ ...f, role: e.target.value }))}>
                      <option value="commercial">Comercial — acesso ao Business 360°</option>
                      <option value="admin">Administrador — acesso total</option>
                    </select>
                  </div>
                  <ModalFooter onCancel={closeModal} saving={saving} label="Criar Usuário" />
                </form>
              </>
            )}

            {/* Modal — Editar */}
            {modal === 'edit' && (
              <>
                <h2 style={{ marginBottom: 4, fontSize: 18 }}>Editar Usuário</h2>
                <p style={{ marginBottom: 20, color: '#64748b', fontSize: 14 }}>@{selectedUser.username}</p>
                {formError && <div style={{ background: '#fee2e2', color: '#dc2626', borderRadius: 6, padding: '10px 14px', marginBottom: 16, fontSize: 14 }}>{formError}</div>}
                <form onSubmit={handleEdit}>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Nome Completo</label>
                    <input style={inputStyle} type="text" value={form.full_name}
                      onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Perfil *</label>
                    <select style={inputStyle} value={form.role} required onChange={e => setForm(f => ({ ...f, role: e.target.value }))}>
                      <option value="commercial">Comercial — acesso ao Business 360°</option>
                      <option value="admin">Administrador — acesso total</option>
                    </select>
                  </div>
                  <ModalFooter onCancel={closeModal} saving={saving} label="Salvar" />
                </form>
              </>
            )}

            {/* Modal — Alterar Senha */}
            {modal === 'password' && (
              <>
                <h2 style={{ marginBottom: 4, fontSize: 18 }}>Alterar Senha</h2>
                <p style={{ marginBottom: 20, color: '#64748b', fontSize: 14 }}>@{selectedUser.username}</p>
                {pwError && <div style={{ background: '#fee2e2', color: '#dc2626', borderRadius: 6, padding: '10px 14px', marginBottom: 16, fontSize: 14 }}>{pwError}</div>}
                <form onSubmit={handlePassword}>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Nova Senha * <span style={{ fontWeight: 400, color: '#64748b' }}>(mínimo 8 caracteres)</span></label>
                    <input style={inputStyle} type="password" value={pwForm.password} required
                      onChange={e => setPwForm(f => ({ ...f, password: e.target.value }))} />
                  </div>
                  <div style={fieldStyle}>
                    <label style={labelStyle}>Confirmar Nova Senha *</label>
                    <input style={inputStyle} type="password" value={pwForm.confirmPassword} required
                      onChange={e => setPwForm(f => ({ ...f, confirmPassword: e.target.value }))} />
                  </div>
                  <ModalFooter onCancel={closeModal} saving={pwSaving} label="Alterar Senha" />
                </form>
              </>
            )}

          </div>
        </div>
      )}
    </div>
  )
}

// ── Rodapé do modal (botões Cancelar / Submit) ────────────────────────────────
function ModalFooter({ onCancel, saving, label }) {
  return (
    <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end', marginTop: 8 }}>
      <button type="button" onClick={onCancel}
        style={{ padding: '8px 20px', borderRadius: 6, border: '1px solid #d1d5db', background: '#fff', cursor: 'pointer', fontSize: 14 }}>
        Cancelar
      </button>
      <button type="submit" disabled={saving}
        style={{ padding: '8px 20px', borderRadius: 6, border: 'none', background: saving ? '#93c5fd' : '#3b82f6', color: '#fff', cursor: saving ? 'default' : 'pointer', fontSize: 14, fontWeight: 600 }}>
        {saving ? 'Salvando...' : label}
      </button>
    </div>
  )
}
