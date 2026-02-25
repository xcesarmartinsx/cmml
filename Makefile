# =============================================================
# Makefile — Projeto CMML
# [Proposto] — comandos operacionais principais
# =============================================================

.PHONY: help up down logs restart psql etl-sales etl-customers etl-products etl-stores etl reco baseline test clean status

# Carrega variáveis do .env para uso no make
include .env
export

# --- Ajuda ---
help:
	@echo ""
	@echo "  CMML — Pipeline de Dados e Recomendação"
	@echo ""
	@echo "  Containers:"
	@echo "    make up           Subir todos os containers"
	@echo "    make down         Parar containers (sem remover volumes)"
	@echo "    make restart      Reiniciar containers"
	@echo "    make logs         Ver logs em tempo real"
	@echo "    make status       Status dos containers"
	@echo ""
	@echo "  Banco de dados:"
	@echo "    make psql         Abrir psql interativo no PostgreSQL"
	@echo "    make ddl          Criar schemas e tabelas no PostgreSQL"
	@echo ""
	@echo "  ETL:"
	@echo "    make etl          Rodar pipeline ETL completo"
	@echo "    make etl-sales    Carregar fato de compras"
	@echo "    make etl-customers Carregar clientes"
	@echo "    make etl-products Carregar produtos"
	@echo "    make etl-stores   Carregar lojas"
	@echo ""
	@echo "  ML:"
	@echo "    make baseline     Gerar recomendações (Modelo 0)"
	@echo "    make reco         Gerar recomendações completas (todos os modelos)"
	@echo ""
	@echo "  Testes:"
	@echo "    make test         Rodar testes automatizados"
	@echo ""
	@echo "  Limpeza:"
	@echo "    make clean        Remover logs antigos (>30 dias)"
	@echo ""

# --- Containers ---
up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

status:
	docker compose ps

# --- PostgreSQL ---
psql:
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB)

ddl:
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/00_schemas.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/01_staging.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/02_curated.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/03_reco.sql

# --- ETL ---
etl-stores:
	python etl/load_stores.py

etl-products:
	python etl/load_products.py

etl-customers:
	python etl/load_customers.py

etl-sales:
	python etl/load_sales.py

etl: etl-stores etl-products etl-customers etl-sales

# --- ML ---
baseline:
	python ml/baseline.py

reco:
	python ml/candidate_generation.py
	python ml/ranking.py
	python ml/apply_rules.py

# --- Testes ---
test:
	python -m pytest tests/ -v

# --- Limpeza ---
clean:
	@mkdir -p logs
	find logs/ -name "*.log" -mtime +30 -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Limpeza concluída."
