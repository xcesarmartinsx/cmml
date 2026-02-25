# ── Stage 1: build do React ────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Copia apenas package.json primeiro para aproveitar cache de camadas Docker.
# Se as dependências não mudaram, o `npm install` não roda novamente.
COPY package.json ./
RUN npm install

# Copia o restante dos fontes e executa o build de produção.
COPY . .
RUN npm run build

# ── Stage 2: servir com Nginx ─────────────────────────────────────────────
FROM nginx:alpine

# Copia os arquivos estáticos gerados pelo Vite
COPY --from=builder /app/dist /usr/share/nginx/html

# Copia a configuração customizada do Nginx (inclui proxy para /api/)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3001

CMD ["nginx", "-g", "daemon off;"]
