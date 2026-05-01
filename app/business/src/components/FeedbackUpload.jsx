import { useState, useRef } from 'react'
import { apiFetch } from '../api.js'

export default function FeedbackUpload({ onClose, onSuccess }) {
  const [file, setFile] = useState(null)
  const [dragging, setDragging] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [result, setResult] = useState(null)
  const inputRef = useRef()

  function handleDrop(e) {
    e.preventDefault()
    setDragging(false)
    const f = e.dataTransfer.files[0]
    if (f && f.name.endsWith('.xlsx')) setFile(f)
  }

  function handleFileChange(e) {
    const f = e.target.files[0]
    if (f) setFile(f)
  }

  async function handleUpload() {
    if (!file) return
    setUploading(true)
    try {
      const formData = new FormData()
      formData.append('file', file)
      const res = await apiFetch('/api/recommendations/feedback/import', {
        method: 'POST',
        body: formData,
      })
      const data = await res.json()
      setResult(data)
      if (data.success > 0 && onSuccess) onSuccess()
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setUploading(false)
    }
  }

  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
    }} onClick={onClose}>
      <div style={{
        background: 'var(--bg, #fff)', borderRadius: 12, padding: 24,
        width: 480, maxWidth: '90vw', boxShadow: '0 8px 32px rgba(0,0,0,0.2)',
      }} onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 16 }}>Importar Feedback (Excel)</h3>
          <button onClick={onClose} style={{ background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: 'var(--muted)' }}>x</button>
        </div>

        {!result ? (
          <>
            <div
              onDragOver={e => { e.preventDefault(); setDragging(true) }}
              onDragLeave={() => setDragging(false)}
              onDrop={handleDrop}
              onClick={() => inputRef.current?.click()}
              style={{
                border: `2px dashed ${dragging ? 'var(--purple)' : 'var(--border)'}`,
                borderRadius: 8, padding: 32, textAlign: 'center', cursor: 'pointer',
                background: dragging ? 'rgba(139,92,246,0.05)' : 'transparent',
                transition: 'all 0.2s',
              }}
            >
              <input ref={inputRef} type="file" accept=".xlsx" onChange={handleFileChange} style={{ display: 'none' }} />
              {file ? (
                <div>
                  <div style={{ fontWeight: 600, fontSize: 14 }}>{file.name}</div>
                  <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 4 }}>{(file.size / 1024).toFixed(1)} KB</div>
                </div>
              ) : (
                <div style={{ color: 'var(--muted)', fontSize: 13 }}>
                  Arraste o arquivo .xlsx aqui ou clique para selecionar
                </div>
              )}
            </div>

            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} style={{
                padding: '8px 16px', borderRadius: 6, border: '1px solid var(--border)',
                background: 'var(--surface)', cursor: 'pointer', fontSize: 13,
              }}>Cancelar</button>
              <button onClick={handleUpload} disabled={!file || uploading} style={{
                padding: '8px 16px', borderRadius: 6, border: 'none',
                background: !file || uploading ? 'var(--muted)' : 'var(--purple)',
                color: '#fff', cursor: !file || uploading ? 'not-allowed' : 'pointer', fontSize: 13, fontWeight: 600,
              }}>{uploading ? 'Importando...' : 'Importar'}</button>
            </div>
          </>
        ) : (
          <div>
            {result.error ? (
              <div style={{ color: '#dc2626', padding: 12, background: '#fef2f2', borderRadius: 8, fontSize: 13 }}>
                Erro: {result.error}
              </div>
            ) : (
              <div style={{ fontSize: 13 }}>
                <div style={{ padding: 12, background: '#f0fdf4', borderRadius: 8, marginBottom: 12 }}>
                  <strong>{result.success}</strong> de <strong>{result.total}</strong> registros importados com sucesso.
                  {result.errors_count > 0 && <span style={{ color: '#dc2626' }}> ({result.errors_count} erros)</span>}
                </div>
                {result.errors && result.errors.length > 0 && (
                  <div style={{ maxHeight: 150, overflowY: 'auto', fontSize: 12, color: '#dc2626' }}>
                    {result.errors.map((e, i) => (
                      <div key={i}>Linha {e.line}: {e.error}</div>
                    ))}
                  </div>
                )}
              </div>
            )}
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} style={{
                padding: '8px 16px', borderRadius: 6, border: 'none',
                background: 'var(--purple)', color: '#fff', cursor: 'pointer', fontSize: 13, fontWeight: 600,
              }}>Fechar</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
