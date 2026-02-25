# 04 — ETL Pipeline

## Resumo

Descreve o pipeline Extract → Transform → Load implementado em `etl/common.py`, a estratégia de carga incremental, as validações propostas e o fluxo de reprocessamento.

---

## Módulo Implementado: `etl/common.py`

O módulo central do ETL contém toda a infraestrutura. Scripts individuais (ex.: `load_sales.py`) devem importar e usar suas funções.

### Funções disponíveis

```python
from etl.common import (
    get_pg_conn,           # conexão PostgreSQL
    get_mssql_conn,        # conexão SQL Server via ODBC
    ensure_etl_control,    # cria tabela de controle se não existir
    get_watermark,         # lê último timestamp/id carregado
    set_watermark,         # salva novo watermark após carga
    mssql_fetch_iter,      # extrai dados em chunks (iterador de dicts)
    pg_copy_upsert_stg,    # COPY + UPSERT na tabela staging
)
```

---

## Fluxo de Execução

```
┌─────────────────────────────────────────────────────────────┐
│  1. EXTRACT                                                  │
│                                                             │
│  a) Conectar ao SQL Server (get_mssql_conn)                 │
│  b) Ler watermark atual (get_watermark)                     │
│  c) Montar query com filtro incremental (WHERE DATA > :ts)  │
│  d) Extrair em chunks de FETCH_CHUNK linhas                 │
│     (mssql_fetch_iter — iterador lazy)                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  2. TRANSFORM (mínimo — raw-first approach)                  │
│                                                             │
│  a) Renomear colunas (alias definido na query)              │
│  b) Adicionar campo source_system                           │
│  c) Tipagem básica (datetime, decimal)                      │
│  NOTA: transformações pesadas ficam na camada cur.*         │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  3. LOAD                                                     │
│                                                             │
│  a) Conectar ao PostgreSQL (get_pg_conn)                    │
│  b) Criar tabela temp com mesmo layout da staging           │
│  c) COPY (bulk) para a tabela temp                          │
│  d) INSERT ... ON CONFLICT DO UPDATE (UPSERT) na stg.*      │
│  e) Atualizar watermark (set_watermark)                     │
│  f) commit                                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Estratégia Incremental

### Como funciona o watermark

A tabela `etl.load_control` registra por dataset:

| Campo | Uso |
|---|---|
| `dataset_name` | Nome do dataset (ex.: `'stg.sales'`) |
| `last_ts` | Último `sale_date` (ou `updated_at`) carregado com sucesso |
| `last_id` | Último `NUMDOCUMENTO` (ou ID numérico) carregado |
| `updated_at` | Quando o watermark foi atualizado |

### Query incremental (proposta para `stg.sales`)

```sql
-- Na primeira carga (watermark = NULL): carga full
SELECT ... FROM dbo.MOVIMENTO_DIA
WHERE TIPO = 1
  AND ENTIDADEID_CLIENTE IS NOT NULL
  AND PRODUTOID IS NOT NULL
  AND DATA IS NOT NULL;

-- Nas cargas subsequentes:
SELECT ... FROM dbo.MOVIMENTO_DIA
WHERE TIPO = 1
  AND ENTIDADEID_CLIENTE IS NOT NULL
  AND PRODUTOID IS NOT NULL
  AND DATA IS NOT NULL
  AND DATA > :last_ts;   -- watermark da carga anterior
```

### Considerações

- O campo `DATA` no ERP pode ter granularidade de **data** (sem hora) — usar `last_ts` como DATE.
- **Risco de late-arriving data**: pedidos com data retroativa não serão capturados na carga incremental. Considerar janela de segurança de 2–3 dias (ex.: `DATA > :last_ts - INTERVAL '3 days'`).
- O UPSERT garante idempotência: reprocessar o mesmo intervalo não cria duplicatas.

---

## Exemplo de Script ETL (Proposto: `etl/load_sales.py`)

```python
"""
etl/load_sales.py
=================
Extrai o fato de compras de MOVIMENTO_DIA e carrega em stg.sales.
"""
import logging
from etl.common import (
    get_pg_conn, get_mssql_conn,
    ensure_etl_control, get_watermark, set_watermark,
    mssql_fetch_iter, pg_copy_upsert_stg,
)

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("etl.sales")

DATASET = "stg.sales"
COLUMNS = [
    "order_id_src", "customer_id_src", "product_id_src",
    "store_id_src", "source_system",
    "sale_date", "quantity", "total_value",
]

def build_query(last_ts):
    base = """
    SELECT
        NUMDOCUMENTO        AS order_id_src,
        ENTIDADEID_CLIENTE  AS customer_id_src,
        PRODUTOID           AS product_id_src,
        DATA                AS sale_date,
        QUANTIDADE          AS quantity,
        VALORTOTAL          AS total_value,
        ENTIDADEID_LOJA     AS store_id_src,
        'sqlserver_gp'      AS source_system
    FROM dbo.MOVIMENTO_DIA
    WHERE TIPO = 1
      AND ENTIDADEID_CLIENTE IS NOT NULL
      AND PRODUTOID IS NOT NULL
      AND DATA IS NOT NULL
    """
    if last_ts:
        # Janela de segurança de 3 dias para late-arriving data
        base += f" AND DATA > DATEADD(day, -3, '{last_ts}')"
    return base

def main():
    pg = get_pg_conn()
    ms = get_mssql_conn()

    ensure_etl_control(pg)
    last_ts, _ = get_watermark(pg, DATASET)
    LOG.info(f"Watermark atual: {last_ts}")

    sql = build_query(last_ts)
    rows = mssql_fetch_iter(ms, sql)

    n = pg_copy_upsert_stg(pg, "stg.sales", COLUMNS, rows)
    LOG.info(f"Carregadas {n} linhas em stg.sales")

    # Pega o MAX(sale_date) carregado para atualizar watermark
    with pg.cursor() as cur:
        cur.execute("SELECT MAX(sale_date) FROM stg.sales")
        new_ts = cur.fetchone()[0]

    set_watermark(pg, DATASET, new_ts, None)
    LOG.info(f"Novo watermark: {new_ts}")

    pg.close()
    ms.close()

if __name__ == "__main__":
    main()
```

---

## Ordem de Execução dos Scripts ETL

```bash
# 1. Entidades de dimensão primeiro (sem dependências)
python etl/load_stores.py       # lojas
python etl/load_products.py     # produtos
python etl/load_customers.py    # clientes

# 2. Fato de compras (depende das dimensões)
python etl/load_sales.py        # fato de compras

# 3. Transformação para camada curada [Proposto — via SQL ou dbt]
psql ... -f sql/transform/01_cur_customers.sql
psql ... -f sql/transform/02_cur_products.sql
psql ... -f sql/transform/03_cur_order_items.sql
```

---

## Validações Propostas

### Após extração (antes do LOAD)

| Validação | Descrição | Ação em falha |
|---|---|---|
| Contagem de linhas > 0 | Dataset não retornou vazio inesperadamente | Alertar + não atualizar watermark |
| Sem `customer_id_src` NULL | Filtro já na query; verificar no staging | Remover + logar |
| `sale_date` dentro de intervalo esperado | Datas < 2000 ou futuras indicam problemas | Logar + quarentena |
| `quantity` > 0 | Quantidade não pode ser zero ou negativa | Remover + logar |
| `total_value` >= 0 | Valor negativo pode indicar devolução | Logar + revisar `TIPO` |

### Após LOAD (pós-carga)

```sql
-- Contagem por data (deve ser consistente com o histórico)
SELECT sale_date, COUNT(*) as n
FROM stg.sales
WHERE sale_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY sale_date
ORDER BY sale_date;

-- Verificar clientes sem histórico de loja
SELECT COUNT(*) as sem_loja
FROM stg.sales
WHERE store_id_src IS NULL;

-- Verificar duplicatas (não deve haver com UPSERT correto)
SELECT order_id_src, product_id_src, source_system, COUNT(*)
FROM stg.sales
GROUP BY order_id_src, product_id_src, source_system
HAVING COUNT(*) > 1;
```

---

## Logs e Reprocessamento

### Padrão de log (proposto)

```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler(),
    ]
)
```

### Reprocessar um intervalo

```bash
# Zerar o watermark de um dataset para forçar recarga total:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "UPDATE etl.load_control SET last_ts = NULL, last_id = NULL WHERE dataset_name = 'stg.sales';"

# Rodar novamente:
python etl/load_sales.py
```

### Reprocessar período específico

```bash
# Definir watermark para data específica:
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
  -c "UPDATE etl.load_control SET last_ts = '2025-01-01' WHERE dataset_name = 'stg.sales';"

python etl/load_sales.py
```

---

## Próximos Passos

1. Implementar `etl/load_sales.py` com o template acima.
2. Implementar `etl/load_customers.py` e `etl/load_products.py`.
3. Criar os DDLs das tabelas staging (`sql/ddl/01_staging.sql`).
4. Definir scheduler (cron ou Airflow) para execução diária automatizada (ver [`docs/06_execucao_operacional.md`](06_execucao_operacional.md)).
5. Implementar alertas em caso de falha (e-mail, Slack, ou log em tabela).
