# 08 — Guia de Contribuição

## Resumo

Padrões de desenvolvimento, fluxo de trabalho Git, convenções de código e processo de revisão.

---

## Configuração do Ambiente de Desenvolvimento

```bash
# Clonar o repositório:
git clone https://github.com/xcesarmartinsx/cmml.git
cd cmml

# Configurar ambiente:
cp .env.example .env
# Editar .env com credenciais de desenvolvimento

# Instalar dependências:
pip install -r requirements.txt
```

---

## Fluxo de Trabalho Git

```
main (produção)
  └── develop (integração)
        └── feature/nome-da-feature
        └── fix/nome-do-bug
        └── docs/nome-da-documentacao
```

### Criar uma feature

```bash
# Partir sempre de develop atualizado:
git checkout develop
git pull origin develop

# Criar branch:
git checkout -b feature/load-sales-etl

# Trabalhar... commit... push...
git add etl/load_sales.py
git commit -m "feat(etl): implement incremental sales load from MOVIMENTO_DIA"
git push origin feature/load-sales-etl

# Abrir Pull Request para develop
```

### Padrão de commits (Conventional Commits)

```
<tipo>(<escopo>): <descrição curta em inglês>

Tipos:
  feat     - nova funcionalidade
  fix      - correção de bug
  docs     - documentação
  refactor - refatoração sem mudança de comportamento
  test     - testes
  chore    - manutenção (deps, config)
  perf     - melhoria de performance

Exemplos:
  feat(etl): add incremental load for sales fact table
  fix(common): handle NULL values in pg_copy_upsert_stg
  docs(arch): add candidate generation architecture diagram
  perf(ml): vectorize similarity computation with pgvector
```

---

## Estrutura de um Script ETL

```python
"""
etl/load_ENTIDADE.py
====================
Breve descrição do que este script extrai e carrega.
"""
import logging
from etl.common import (
    get_pg_conn, get_mssql_conn,
    ensure_etl_control, get_watermark, set_watermark,
    mssql_fetch_iter, pg_copy_upsert_stg,
)

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("etl.ENTIDADE")

DATASET = "stg.ENTIDADE"
COLUMNS = ["col1", "col2", ...]

SQL = """
SELECT ... FROM dbo.TABELA WHERE ...
"""

def main():
    pg = get_pg_conn()
    ms = get_mssql_conn()
    ensure_etl_control(pg)
    last_ts, _ = get_watermark(pg, DATASET)
    rows = mssql_fetch_iter(ms, SQL)
    n = pg_copy_upsert_stg(pg, DATASET, COLUMNS, rows)
    LOG.info(f"Carregadas {n} linhas")
    set_watermark(pg, DATASET, last_ts, None)  # atualizar com valor real
    pg.close(); ms.close()

if __name__ == "__main__":
    main()
```

---

## Checklist de Pull Request

Antes de abrir um PR, verifique:

- [ ] O código lê variáveis do `.env` (sem hardcode de credenciais)
- [ ] Nenhum segredo, senha ou token no código ou nos commits
- [ ] A tabela de destino tem PK definida (obrigatório para UPSERT)
- [ ] Logging implementado (início, contagem de linhas, erros)
- [ ] Testado localmente com `docker compose up -d`
- [ ] Documentação atualizada se necessário (dicionário de dados, README)
- [ ] Testes unitários adicionados (se aplicável)

---

## Próximos Passos

1. Configurar branch protection em `main` e `develop` no GitHub.
2. Configurar GitHub Actions para rodar testes automaticamente em PRs.
3. Definir SLA de revisão de PRs (ex.: 2 dias úteis).
