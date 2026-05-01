# =============================================================
# Makefile — Projeto CMML
# [Proposto] — comandos operacionais principais
# =============================================================

.PHONY: help up down logs restart psql etl-sales etl-customers etl-products etl-stores etl reco baseline test clean status lifecycle-refresh seed-users feedback validate-offers validate-whatsapp validate-whatsapp-dry

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
	@echo "  Feedback:"
	@echo "    make feedback     Executar cross-reference ofertas vs vendas"
	@echo ""
	@echo "  ML:"
	@echo "    make baseline     Gerar recomendações (Modelo 0)"
	@echo "    make reco         Gerar recomendações completas (todos os modelos)"
	@echo "    make validate-offers  Validar qualidade das ofertas geradas"
	@echo ""
	@echo "  WhatsApp:"
	@echo "    make validate-whatsapp      Validar numeros via Evolution API"
	@echo "    make validate-whatsapp-dry   Dry-run (mostra pendentes sem validar)"
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
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/03_dw_marts.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/04_auth.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/05_feedback.sql
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/ddl/06_whatsapp_cache.sql

lifecycle-refresh:
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -c "REFRESH MATERIALIZED VIEW CONCURRENTLY reco.product_lifecycle;"

# --- ETL ---
etl-stores:
	python etl/load_stores.py

etl-products:
	python etl/load_products.py

etl-customers:
	python etl/load_customers.py

etl-sales:
	python etl/load_sales.py

etl: etl-stores etl-products etl-customers etl-sales feedback

# --- Feedback ---
feedback:
	python ml/feedback_loop.py

# --- ML ---
baseline:
	python ml/baseline.py

reco:
	python3 ml/baseline.py
	python3 ml/modelo_a_ranker.py --force-retrain
	python3 ml/modelo_b_colaborativo.py
	python3 ml/generate_offers.py
	python3 ml/validate_offers.py

validate-offers:
	python3 ml/validate_offers.py

# --- WhatsApp ---
validate-whatsapp: ## Validar numeros de celular via Evolution API
	python3 scripts/validate_whatsapp.py

validate-whatsapp-dry: ## Mostra numeros pendentes sem chamar a API
	python3 scripts/validate_whatsapp.py --dry-run

# --- Seed ---
seed-users: ## Inserir usuario inicial no banco (SEED_PASSWORD obrigatorio)
	SEED_PASSWORD=$(SEED_PASSWORD) python3 scripts/seed_users.py

# --- Testes ---
test:
	python -m pytest tests/ -v

# --- Limpeza ---
clean:
	@mkdir -p logs
	find logs/ -name "*.log" -mtime +30 -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Limpeza concluída."
