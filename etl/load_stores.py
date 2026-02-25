"""
etl/load_stores.py
==================
Extrai o cadastro de lojas/filiais do ERP GP e carrega em stg.stores.

POR QUE ESTE DATASET É NECESSÁRIO PARA O ML:
  • stg.sales referencia store_id_src — lojas são dimensão obrigatória
  • Recomendações por loja: "top produtos desta filial" (baseline)
  • Filtros geográficos: recomendar produtos disponíveis na loja do cliente
  • Análise de sortimento: produto disponível em quais lojas?

ESTRATÉGIA DE CARGA: UPSERT controlado (pg_copy_upsert)
  • Atributos atualizáveis: name, city, state, active
  • Coluna protegida: first_seen_at (data de entrada da loja no pipeline)
  • ON CONFLICT (store_id_src, source_system) DO UPDATE SET ...

NOMES DE COLUNAS ERP (confirme antes de executar):
  Execute no SQL Server para verificar os nomes reais:
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'ENTIDADE' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION;

  ATENÇÃO: No ERP GP, lojas e clientes podem ser armazenados na mesma tabela
  ENTIDADE, diferenciados por um campo como TIPOENTIDADE ou TIPOCLIENTE.
  Confirme o filtro correto para extrair somente lojas (não clientes).

MODOS DE EXECUÇÃO:
  python etl/load_stores.py              # incremental (usa watermark)
  python etl/load_stores.py --full-load  # recarga completa
  python etl/load_stores.py --dry-run    # conta sem carregar
"""

# argparse: leitura dos argumentos CLI (--full-load, --dry-run).
import argparse
# logging: módulo de log — logger criado com setup_logging abaixo.
import logging
# os: acesso à variável de ambiente FETCH_CHUNK.
import os
# sys: sys.path para importação do etl.common e sys.exit para sinalizar falhas.
import sys
# datetime, timezone: geração do novo watermark UTC.
from datetime import datetime, timezone
# Path: resolução do caminho absoluto da raiz do projeto.
from pathlib import Path
# Optional: anotação de tipo para parâmetros que podem ser None.
from typing import Optional

# Resolve a raiz do projeto (pasta pai de etl/) para importação correta.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Insere a raiz no sys.path somente se ainda não estiver presente.
if str(_PROJECT_ROOT) not in sys.path:
    # Prioridade máxima na resolução de módulos.
    sys.path.insert(0, str(_PROJECT_ROOT))

# Importa as funções de infraestrutura ETL do módulo central.
from etl.common import (
    # close_batch: encerra o registro de batch com status e métricas.
    close_batch,
    # ensure_etl_control: garante existência das tabelas de controle ETL.
    ensure_etl_control,
    # get_mssql_conn: abre conexão com o SQL Server ERP.
    get_mssql_conn,
    # get_pg_conn: abre conexão com o PostgreSQL destino.
    get_pg_conn,
    # get_watermark: lê o watermark anterior para carga incremental.
    get_watermark,
    # mssql_fetch_iter: extrai dados do SQL Server em chunks (iterador lazy).
    mssql_fetch_iter,
    # open_batch: registra início do batch em etl.load_batches.
    open_batch,
    # pg_copy_upsert: carga bulk com UPSERT controlado (dimensões).
    pg_copy_upsert,
    # set_watermark: persiste o novo watermark após carga bem-sucedida.
    set_watermark,
    # setup_logging: configura logger com formatter estruturado.
    setup_logging,
)

# Logger específico para este script.
LOG = setup_logging("etl.stores")

# Nome lógico do dataset — chave em etl.load_control e etl.load_batches.
DATASET    = "stg.stores"
# Tamanho do chunk de extração.
CHUNK_SIZE = int(os.getenv("FETCH_CHUNK", "5000"))

# Colunas que serão carregadas em stg.stores.
# batch_id e extracted_at são injetados pelo common.py.
COLUMNS = [
    # ID original da loja no ERP — parte da chave primária composta.
    "store_id_src",
    # Sistema de origem — parte da chave primária composta.
    "source_system",
    # Nome da loja/filial.
    "name",
    # Cidade da loja — base de recomendações regionais.
    "city",
    # Estado (UF) da loja.
    "state",
    # Status ativo/inativo — lojas inativas devem ser excluídas das recomendações.
    "active",
    # ID do batch de carga (injetado pelo common.py).
    "batch_id",
    # Timestamp de extração da fonte (injetado pelo common.py).
    "extracted_at",
]

# Colunas que nunca devem ser sobrescritas no UPSERT.
PROTECTED_COLS = ["first_seen_at"]

# ---------------------------------------------------------------------------
# ATENÇÃO — CONFIRME OS NOMES REAIS DAS COLUNAS E O FILTRO CORRETO:
#
# No ERP GP, lojas podem estar em:
#   - dbo.ENTIDADE com filtro TIPOENTIDADE = X (ou TIPOCLIENTE = X)
#   - dbo.LOJA (tabela separada — verifique se existe)
#
# Execute para listar tabelas disponíveis:
#   SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
#   WHERE TABLE_SCHEMA = 'dbo' ORDER BY TABLE_NAME;
#
# Execute para verificar colunas de ENTIDADE:
#   SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
#   WHERE TABLE_NAME = 'ENTIDADE' AND TABLE_SCHEMA = 'dbo'
#   ORDER BY ORDINAL_POSITION;
#
# Substitua ENTIDADEID, NOME, CIDADE, UF, ATIVO, DATAALTERACAO
# e o filtro de tipo de entidade pelos valores reais.
# ---------------------------------------------------------------------------

# Query de carga completa de lojas — sem filtro de data.
# ATENÇÃO: o valor de TIPOENTIDADE para lojas deve ser confirmado no ERP.
SQL_FULL = """
SELECT
    CAST(E.ENTIDADEID       AS BIGINT)         AS store_id_src,
    'sqlserver_gp'                             AS source_system,
    CAST(E.DESCRICAO        AS VARCHAR(500))   AS name,
    CAST(E.CIDADE           AS VARCHAR(200))   AS city,
    CAST(E.UF               AS CHAR(2))        AS state,
    CASE WHEN E.ATIVO = 'S' THEN 1 ELSE 0 END AS active
FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '2'
"""

# Query incremental — filtra por DATA_ALTERACAO (coluna confirmada em ENTIDADES).
SQL_INCREMENTAL_TEMPLATE = """
SELECT
    CAST(E.ENTIDADEID       AS BIGINT)         AS store_id_src,
    'sqlserver_gp'                             AS source_system,
    CAST(E.DESCRICAO        AS VARCHAR(500))   AS name,
    CAST(E.CIDADE           AS VARCHAR(200))   AS city,
    CAST(E.UF               AS CHAR(2))        AS state,
    CASE WHEN E.ATIVO = 'S' THEN 1 ELSE 0 END AS active
FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '2'
  AND E.DATA_ALTERACAO >= '{from_ts}'
"""

# Query de contagem para --dry-run — sem subquery com ORDER BY.
SQL_COUNT_TEMPLATE = """
SELECT COUNT(*) FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '2'
  {date_filter}
"""


def build_sql(last_ts: Optional[datetime], full_load: bool) -> tuple[str, str, bool]:
    """
    Monta a query de extração e a query de contagem (dry-run) baseadas no watermark.
    Retorna: (sql_extract, sql_count, is_incremental)
    """
    # Se full_load ou sem watermark, usa carga completa.
    if full_load or last_ts is None:
        # Retorna as queries de full load e flag incremental=False.
        return SQL_FULL, SQL_COUNT_TEMPLATE.format(date_filter=""), False
    # Formata o watermark como string para interpolação no SQL.
    from_ts = last_ts.strftime("%Y-%m-%d %H:%M:%S")
    # Monta a query de extração incremental.
    sql_extract = SQL_INCREMENTAL_TEMPLATE.format(from_ts=from_ts)
    # Monta a query de contagem com o mesmo filtro de data.
    sql_count = SQL_COUNT_TEMPLATE.format(
        date_filter=f"AND E.DATAALTERACAO >= '{from_ts}'"
    )
    # Retorna as queries e a flag incremental=True.
    return sql_extract, sql_count, True


def validate_post_load(pg, batch_id: int) -> bool:
    """
    Executa validações de qualidade após a carga em stg.stores.

    Verificações:
    - Total inserido neste batch
    - Lojas sem nome
    - Lojas sem cidade (impacta recomendações regionais)
    - Proporção de lojas ativas
    - Total acumulado em stg.stores
    """
    # Flag de resultado.
    ok = True
    # Abre cursor para as queries de validação.
    with pg.cursor() as cur:

        # ── Validação 1: linhas processadas neste batch ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.stores WHERE batch_id = %s",
            (batch_id,),
        )
        # Lê a contagem do batch.
        n = cur.fetchone()[0]
        # Loga o total do batch.
        LOG.info(f"[batch={batch_id}] Validação ▸ linhas processadas neste batch: {n:,}")

        # ── Validação 2: lojas sem nome ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.stores WHERE batch_id = %s AND (name IS NULL OR name = '')",
            (batch_id,),
        )
        # Conta lojas sem nome neste batch.
        sem_nome = cur.fetchone()[0]
        # Emite aviso — lojas sem nome prejudicam a exibição de recomendações por loja.
        if sem_nome > 0:
            LOG.warning(f"[batch={batch_id}] AVISO ▸ {sem_nome:,} lojas sem nome")

        # ── Validação 3: lojas sem cidade ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.stores WHERE batch_id = %s AND city IS NULL",
            (batch_id,),
        )
        # Conta lojas sem cidade.
        sem_cidade = cur.fetchone()[0]
        # Emite aviso — sem cidade, recomendações regionais ficam comprometidas.
        if sem_cidade > 0:
            LOG.warning(f"[batch={batch_id}] AVISO ▸ {sem_cidade:,} lojas sem cidade")

        # ── Validação 4: proporção de lojas ativas ──
        cur.execute(
            "SELECT COUNT(*), SUM(CASE WHEN active THEN 1 ELSE 0 END) FROM stg.stores"
        )
        # Lê total e ativos.
        total, ativos = cur.fetchone()
        # Previne divisão por zero.
        pct = round(100 * ativos / total, 1) if total else 0
        # Loga a proporção de lojas ativas vs inativas.
        LOG.info(
            f"[batch={batch_id}] Validação ▸ stg.stores total: {total:,} | "
            f"ativas: {ativos:,} ({pct}%) | inativas: {total - ativos:,}"
        )

    # Retorna o resultado das validações.
    return ok


def main(full_load: bool = False, dry_run: bool = False) -> None:
    """
    Ponto de entrada do pipeline de carga de lojas.

    Fluxo idêntico aos outros scripts ETL:
    1. Abre conexões → 2. Controle ETL → 3. Watermark →
    4. SQL → 5. [dry-run] → 6. Batch → 7. Extrai+Carrega →
    8. Valida → 9. Watermark → 10. Fecha batch
    """
    # Separador de início do run.
    LOG.info("=" * 70)
    # Parâmetros do run.
    LOG.info(f"ETL STORES | full_load={full_load} | dry_run={dry_run}")
    # Fecha o cabeçalho.
    LOG.info("=" * 70)

    # Abre conexão com o PostgreSQL destino.
    pg = get_pg_conn()
    # Abre conexão com o SQL Server ERP.
    ms = get_mssql_conn()

    # Variáveis para o bloco except/finally.
    batch_id  = None
    extracted = 0
    upserted  = 0
    error_msg = None

    try:
        # ── Passo 1: garantir tabelas de controle ──
        ensure_etl_control(pg)

        # ── Passo 2: ler watermark ──
        last_ts, _ = get_watermark(pg, DATASET)
        # Loga o watermark atual.
        LOG.info(f"Watermark anterior: {last_ts}")

        # ── Passo 3: montar SQL ──
        sql_extract, sql_count, is_incremental = build_sql(last_ts, full_load)
        # Loga o modo de carga.
        LOG.info(f"Modo: {'incremental' if is_incremental else 'full'} | watermark: {last_ts}")

        # ── Passo 4: dry-run ──
        if dry_run:
            # Loga o SQL de extração.
            LOG.info("[DRY RUN] SQL de extração:\n" + sql_extract)
            # Executa a contagem sem subquery problemática.
            with ms.cursor() as cur:
                cur.execute(sql_count)
                # Loga a contagem.
                LOG.info(f"[DRY RUN] Registros que seriam extraídos: {cur.fetchone()[0]:,}")
            # Encerra sem carga.
            return

        # ── Passo 5: abrir batch ──
        batch_id = open_batch(pg, DATASET, last_ts, overlap_days=0)

        # ── Passo 6: extrair e carregar ──
        # Iterador lazy de extração do SQL Server.
        rows_iter = mssql_fetch_iter(ms, sql_extract, arraysize=CHUNK_SIZE)
        # Carrega via UPSERT — atualiza atributos sem sobrescrever first_seen_at.
        extracted, upserted, _ = pg_copy_upsert(
            pg, DATASET, COLUMNS, rows_iter,
            batch_id,
            protected_cols=PROTECTED_COLS,
            chunk_size=CHUNK_SIZE,
        )

        # ── Passo 7: validações ──
        validate_post_load(pg, batch_id)

        # ── Passo 8: avançar watermark ──
        # Usa timestamp atual — ENTIDADE pode não ter coluna de alteração confiável.
        new_wm = datetime.now(tz=timezone.utc)
        # Persiste o watermark.
        set_watermark(pg, DATASET, new_wm, None)

        # ── Passo 9: fechar batch ──
        close_batch(pg, batch_id, "success", new_wm, extracted, upserted, 0)

    except Exception as exc:
        # Registra o erro.
        error_msg = str(exc)
        # Loga com stacktrace completo.
        LOG.error(f"ERRO FATAL no ETL stores: {exc}", exc_info=True)
        # Tenta rollback.
        try:
            pg.rollback()
        except Exception:
            pass
        # Fecha o batch como 'failed' se foi aberto.
        if batch_id is not None:
            try:
                close_batch(pg, batch_id, "failed", None, extracted, upserted, 0, error_msg)
            except Exception:
                pass
        # Sinaliza falha ao orquestrador.
        sys.exit(1)

    finally:
        # Fecha conexões independentemente do resultado.
        for conn in (pg, ms):
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    # Configura o parser de argumentos CLI.
    parser = argparse.ArgumentParser(
        description="ETL: dbo.ENTIDADE tipo loja (ERP GP) → stg.stores (PostgreSQL)"
    )
    # Argumento para forçar recarga completa.
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignora watermark e recarrega TODAS as lojas do ERP",
    )
    # Argumento para modo simulação.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra SQL e contagem de registros sem efetuar a carga",
    )
    # Faz o parse.
    args = parser.parse_args()
    # Chama o main com os argumentos.
    main(full_load=args.full_load, dry_run=args.dry_run)
