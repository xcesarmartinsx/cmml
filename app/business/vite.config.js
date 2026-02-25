import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3001,
    host: '0.0.0.0',
    proxy: {
      // Redireciona /api/* para o serviço FastAPI em tempo de desenvolvimento
      '/api': {
        target: 'http://api:8001',
        changeOrigin: true,
      },
    },
  },
})
