# 06 — Execução Operacional

## Resumo

Rotina operacional do projeto: como rodar o ETL, gerar recomendações, fazer checagens de saúde, backup e diferenças entre ambientes dev/prod.

---

## Rotina Diária (MVP Proposto)

```
02:00  ETL — extract + load (stg.*)
02:30  Transformação — curated (cur.*)
03:00  Candidate Generation (reco.candidates)
03:30  Ranking + Regras de Elegibilidade (reco.sugestoes)
04:00  Validação + alertas
```

### Agendamento via cron (proposto)

```bash
# Editar crontab:
crontab -e

# Adicionar:
0 2 * * * cd /home/gameserver/projects/cmml && python etl/load_sales.py >> logs/etl.log 2>&1
30 2 * * * cd /home/gameserver/projects/cmml && python etl/load_customers.py >> logs/etl.log 2>&1
0 3 * * * cd /home/gameserver/projects/cmml && python ml/candidate_generation.py >> logs/ml.log 2>&1
30 3 * * * cd /home/gameserver/projects/cmml && python ml/ranking.py >> logs/ml.log 2>&1
```

---

## Comandos de Operação

### Containers

```bash
# Subir ambiente:
docker compose up -d

# Parar:
docker compose down

# Status:
docker compose ps

# Logs em tempo real:
docker compose logs -f reco-postgres
docker compose logs -f sqlserver_gp
```

### ETL Manual

```bash
cd /home/gameserver/projects/cmml

# Carga completa (na ordem correta):
python etl/load_stores.py
python etl/load_products.py
python etl/load_customers.py
python etl/load_sales.py

# Ver watermarks atuais:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT * FROM etl.load_control ORDER BY updated_at DESC;"
```

### Verificações de Saúde

```bash
# Contar registros por tabela staging:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "
SELECT 'stg.sales'     AS tabela, COUNT(*) FROM stg.sales
UNION ALL
SELECT 'stg.customers',           COUNT(*) FROM stg.customers
UNION ALL
SELECT 'stg.products',            COUNT(*) FROM stg.products
UNION ALL
SELECT 'stg.stores',              COUNT(*) FROM stg.stores;
"

# Verificar data máxima carregada:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT MAX(sale_date) AS ultima_venda FROM stg.sales;"

# Contar recomendações geradas:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT COUNT(DISTINCT customer_id) AS clientes_com_reco FROM reco.sugestoes;"
```

---

## Makefile (Proposto)

Não existe no repositório. Template proposto para `Makefile`:

```makefile
# Makefile [Proposto]
.PHONY: up down logs etl reco test clean psql

# Containers
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ETL
etl:
	python etl/load_stores.py
	python etl/load_products.py
	python etl/load_customers.py
	python etl/load_sales.py

# Recomendações
reco:
	python ml/candidate_generation.py
	python ml/ranking.py
	python ml/apply_rules.py

# Testes
test:
	python -m pytest tests/ -v

# Acesso ao banco
psql:
	psql -h $$PG_HOST -U $$PG_USER -d $$PG_DB

# Limpeza de logs
clean:
	find logs/ -name "*.log" -mtime +30 -delete
```

Uso:
```bash
make up        # subir containers
make etl       # rodar pipeline ETL
make reco      # gerar recomendações
make test      # rodar testes
make psql      # abrir psql interativo
```

---

## Ambientes Dev vs Prod

| Aspecto | Desenvolvimento | Produção (Proposto) |
|---|---|---|
| Banco PostgreSQL | Container local (Docker) | Instância gerenciada (ex.: RDS, Cloud SQL) |
| Banco SQL Server | Container local + backup .BAK | Servidor ERP real ou réplica |
| Credenciais | `.env` local | Secrets manager (Vault, AWS Secrets Manager) |
| Agendamento | Manual ou cron local | Airflow, Prefect ou cron em servidor dedicado |
| Logs | Arquivos locais em `logs/` | Centralizados (CloudWatch, Datadog, ELK) |
| Monitoramento | Manual | Alertas automáticos (PagerDuty, Grafana) |
| Backup PostgreSQL | Manual | Automatizado com retenção de 30 dias |
| Deploy | `git pull` manual | CI/CD (GitHub Actions, GitLab CI) |
| Escala | 1 máquina | Múltiplas instâncias + load balancer |

---

## Backup e Restore

### Backup do PostgreSQL (proposto)

```bash
# Backup completo do banco cmml:
pg_dump -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  --format=custom \
  --file="/backup/cmml_$(date +%Y%m%d_%H%M%S).dump"

# Agendar via cron (diário às 01h):
# 0 1 * * * pg_dump -h $PG_HOST -U $PG_USER -d $PG_DB --format=custom --file=/backup/cmml_$(date +\%Y\%m\%d).dump
```

### Restore do PostgreSQL

```bash
# Restaurar dump:
pg_restore -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  --clean --if-exists \
  /backup/cmml_20260101_020000.dump
```

### Backup apenas das recomendações

```bash
# Exportar tabela de sugestões:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "\COPY reco.sugestoes TO '/backup/sugestoes_$(date +%Y%m%d).csv' CSV HEADER"
```

---

## Reprocessamento de Emergência

```bash
# 1. Zerar watermark de um dataset:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "UPDATE etl.load_control SET last_ts = NULL, last_id = NULL
      WHERE dataset_name = 'stg.sales';"

# 2. Rodar ETL:
python etl/load_sales.py

# 3. Verificar:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT * FROM etl.load_control WHERE dataset_name = 'stg.sales';"
```

---

## Monitoramento Mínimo (Proposto)

```bash
# Script de healthcheck (proposto: scripts/healthcheck.sh):
#!/bin/bash
set -e

# Verificar PostgreSQL
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" > /dev/null 2>&1 \
  && echo "PostgreSQL: OK" || echo "PostgreSQL: FALHOU"

# Verificar última carga (não deve ser mais antiga que 2 dias)
LAST=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -t -c \
  "SELECT last_ts FROM etl.load_control WHERE dataset_name='stg.sales'")
echo "Último ETL: $LAST"

# Verificar containers
docker compose ps | grep -v "Up" && echo "AVISO: container parado" || echo "Containers: OK"
```

---

## Próximos Passos

1. Criar o `Makefile` com os comandos acima.
2. Definir e implementar o cron/scheduler para execução automática diária.
3. Criar o script `scripts/healthcheck.sh`.
4. Configurar alertas por e-mail em caso de falha do ETL.
5. Documentar SLA de atualização das recomendações com o time de negócio.
