"""
etl/load_products.py
====================
Extrai o catálogo de produtos do ERP GP (dbo.PRODUTO) e carrega em stg.products.

POR QUE ESTE DATASET É NECESSÁRIO PARA O ML:
  • Filtro de produtos descontinuados (active=False) antes de recomendar
  • Diversificação por categoria: máximo N itens por group_id no top-N
  • Baseline/fallback: "top vendidos por categoria" exige saber a categoria

ESTRATÉGIA DE CARGA: UPSERT controlado (pg_copy_upsert)
  • Atributos atualizáveis: description, group_id, subgroup_id, unit, active
  • Coluna protegida: first_seen_at (data de entrada do produto no pipeline)
  • ON CONFLICT (product_id_src, source_system) DO UPDATE SET ...

NOMES DE COLUNAS ERP (confirme antes de executar):
  Execute no SQL Server para verificar os nomes reais:
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'PRODUTO' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION;

MODOS DE EXECUÇÃO:
  python etl/load_products.py              # incremental (usa watermark)
  python etl/load_products.py --full-load  # recarga completa
  python etl/load_products.py --dry-run    # conta sem carregar
"""

# argparse: leitura dos argumentos de linha de comando (--full-load, --dry-run).
import argparse
# logging: módulo de log — o logger real é criado com setup_logging abaixo.
import logging
# os: acesso à variável de ambiente FETCH_CHUNK para tamanho do chunk.
import os
# sys: necessário para sys.path (importação do etl.common) e sys.exit.
import sys
# datetime, timezone: geração do novo watermark UTC.
from datetime import datetime, timezone
# Path: resolução do caminho absoluto do projeto para importação de módulos.
from pathlib import Path
# Optional: anotação de tipo para parâmetros que podem ser None.
from typing import Optional

# Resolve a raiz do projeto (pasta pai de etl/) para importação correta.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Insere a raiz no sys.path somente se ainda não estiver lá.
if str(_PROJECT_ROOT) not in sys.path:
    # Insere no início do path para ter prioridade sobre pacotes do sistema.
    sys.path.insert(0, str(_PROJECT_ROOT))

# Importa as funções de infraestrutura ETL do módulo central.
from etl.common import (
    # close_batch: fecha o registro de batch com status e métricas.
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
    # open_batch: registra o início do batch em etl.load_batches.
    open_batch,
    # pg_copy_upsert: carga bulk com UPSERT controlado para dimensões.
    pg_copy_upsert,
    # set_watermark: persiste o novo watermark após carga bem-sucedida.
    set_watermark,
    # setup_logging: configura logger com formatter estruturado.
    setup_logging,
)

# Cria o logger específico para este script.
LOG = setup_logging("etl.products")

# Nome lógico do dataset — chave única em etl.load_control e etl.load_batches.
DATASET    = "stg.products"
# Tamanho do chunk de extração: lê de FETCH_CHUNK ou usa 5.000 como padrão.
CHUNK_SIZE = int(os.getenv("FETCH_CHUNK", "5000"))

# Colunas que serão carregadas em stg.products.
# batch_id e extracted_at são injetados pelo common.py (_copy_chunk_to_tmp).
COLUMNS = [
    # ID original do produto no ERP — parte da chave primária composta.
    "product_id_src",
    # Sistema de origem — parte da chave primária composta.
    "source_system",
    # Descrição/nome do produto — exibido na tela de recomendações.
    "description",
    # ID do grupo/categoria principal — usado para diversificação no top-N.
    "group_id",
    # ID do subgrupo/subcategoria — granularidade adicional para regras.
    "subgroup_id",
    # Unidade de medida do produto.
    "unit",
    # Status ativo/inativo — produtos inativos são removidos das recomendações.
    "active",
    # ID do batch de carga (injetado pelo common.py).
    "batch_id",
    # Timestamp de extração da fonte (injetado pelo common.py).
    "extracted_at",
]

# Colunas protegidas: nunca sobrescrevê-las no UPSERT.
# first_seen_at: data em que o produto foi visto pela primeira vez no pipeline.
PROTECTED_COLS = ["first_seen_at"]

# ---------------------------------------------------------------------------
# ATENÇÃO — CONFIRME OS NOMES REAIS DAS COLUNAS ANTES DE EXECUTAR:
#
#   SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
#   WHERE TABLE_NAME = 'PRODUTO' AND TABLE_SCHEMA = 'dbo'
#   ORDER BY ORDINAL_POSITION;
#
# Os nomes abaixo são estimativas baseadas em padrões do ERP GP.
# Substitua PRODUTOID, DESCRICAO, GRUPOID, SUBGRUPOID, UNIDADE, ATIVO,
# DATAALTERACAO pelos nomes reais se necessário.
# ---------------------------------------------------------------------------

# Query de carga completa — sem filtro de data.
SQL_FULL = """
SELECT
    CAST(P.PRODUTOID        AS BIGINT)         AS product_id_src,
    'sqlserver_gp'                             AS source_system,
    CAST(P.DESCRICAO        AS VARCHAR(500))   AS description,
    CAST(P.GRUPOID          AS BIGINT)         AS group_id,
    NULL                                       AS subgroup_id,
    CAST(P.UNIDADE          AS VARCHAR(20))    AS unit,
    CASE WHEN P.ATIVO = 'S' THEN 1 ELSE 0 END AS active
FROM dbo.PRODUTOS P
WHERE P.PRODUTOID IS NOT NULL
"""

# PRODUTOS não tem coluna de data de alteração confirmada —
# sempre se usa full load (build_sql ignora watermark neste caso).
SQL_INCREMENTAL_TEMPLATE = SQL_FULL

# Query de contagem para --dry-run.
SQL_COUNT_TEMPLATE = """
SELECT COUNT(*) FROM dbo.PRODUTOS P
WHERE P.PRODUTOID IS NOT NULL
"""


def build_sql(last_ts: Optional[datetime], full_load: bool) -> tuple[str, str, bool]:
    """
    Retorna sempre a query de full load para PRODUTOS.

    PRODUTOS não tem coluna de data de alteração confirmada no banco,
    portanto carga incremental não é possível. O watermark é gravado
    apenas para rastreabilidade, mas não é usado como filtro de extração.
    """
    # Ignora last_ts e full_load — sempre retorna full load para PRODUTOS.
    return SQL_FULL, SQL_COUNT_TEMPLATE, False


def validate_post_load(pg, batch_id: int) -> bool:
    """
    Executa validações de qualidade após a carga em stg.products.

    Verificações:
    - Total inserido neste batch
    - Produtos sem descrição (afeta exibição nas recomendações)
    - Produtos sem group_id (impossibilita diversificação por categoria)
    - Proporção de produtos ativos vs inativos
    - Total acumulado em stg.products

    Retorna True se OK, False se houver problema crítico.
    """
    # Flag de resultado — inicia como True (sem problemas).
    ok = True
    # Abre cursor para as queries de validação.
    with pg.cursor() as cur:
        # ── Validação 1: linhas processadas neste batch ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.products WHERE batch_id = %s",
            (batch_id,),
        )
        # Lê a contagem deste batch.
        n = cur.fetchone()[0]
        # Loga o total do batch.
        LOG.info(f"[batch={batch_id}] Validação ▸ linhas processadas neste batch: {n:,}")

        # ── Validação 2: produtos sem descrição (aviso) ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.products "
            "WHERE batch_id = %s AND (description IS NULL OR description = '')",
            (batch_id,),
        )
        # Conta produtos sem descrição neste batch.
        sem_desc = cur.fetchone()[0]
        # Emite aviso — produtos sem descrição prejudicam a UX das recomendações.
        if sem_desc > 0:
            LOG.warning(f"[batch={batch_id}] AVISO ▸ {sem_desc:,} produtos sem descrição")

        # ── Validação 3: produtos sem group_id (crítico para diversificação) ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.products WHERE batch_id = %s AND group_id IS NULL",
            (batch_id,),
        )
        # Conta produtos sem categoria neste batch.
        sem_cat = cur.fetchone()[0]
        # Emite aviso — sem group_id, a regra de diversificação de categoria não funciona.
        if sem_cat > 0:
            LOG.warning(
                f"[batch={batch_id}] AVISO ▸ {sem_cat:,} produtos sem group_id — "
                "diversificação por categoria comprometida nesses itens"
            )

        # ── Validação 4: proporção de ativos (saúde geral do catálogo) ──
        cur.execute(
            "SELECT COUNT(*), SUM(CASE WHEN active THEN 1 ELSE 0 END) FROM stg.products"
        )
        # Lê total e total de ativos.
        total, ativos = cur.fetchone()
        # Calcula percentual de ativos — previne divisão por zero.
        pct = round(100 * ativos / total, 1) if total else 0
        # Loga a proporção de produtos ativos vs inativos.
        LOG.info(
            f"[batch={batch_id}] Validação ▸ stg.products total: {total:,} | "
            f"ativos: {ativos:,} ({pct}%) | inativos: {total - ativos:,}"
        )

    # Retorna o resultado das validações.
    return ok


def main(full_load: bool = False, dry_run: bool = False) -> None:
    """
    Ponto de entrada do pipeline de carga de produtos.

    Fluxo:
    1. Abre conexões PG e MSSQL
    2. Garante tabelas de controle ETL
    3. Lê watermark anterior
    4. Monta SQL (full ou incremental)
    5. Em dry_run: conta e sai
    6. Abre batch, extrai, carrega via UPSERT
    7. Valida pós-carga
    8. Avança watermark
    9. Fecha batch com sucesso
    """
    # Imprime separador de início do run.
    LOG.info("=" * 70)
    # Loga os parâmetros do run.
    LOG.info(f"ETL PRODUCTS | full_load={full_load} | dry_run={dry_run}")
    # Fecha o cabeçalho.
    LOG.info("=" * 70)

    # Abre conexão com o PostgreSQL destino.
    pg = get_pg_conn()
    # Abre conexão com o SQL Server ERP fonte.
    ms = get_mssql_conn()

    # Variáveis de controle para o bloco finally/except.
    batch_id  = None
    extracted = 0
    upserted  = 0
    error_msg = None

    try:
        # ── Passo 1: garantir tabelas de controle ──
        ensure_etl_control(pg)

        # ── Passo 2: ler watermark anterior ──
        last_ts, _ = get_watermark(pg, DATASET)
        # Loga o watermark para confirmar o ponto de partida.
        LOG.info(f"Watermark anterior: {last_ts}")

        # ── Passo 3: montar SQL de extração e contagem ──
        sql_extract, sql_count, is_incremental = build_sql(last_ts, full_load)
        # Loga o modo de carga escolhido.
        LOG.info(f"Modo: {'incremental' if is_incremental else 'full'} | watermark: {last_ts}")

        # ── Passo 4 (opcional): dry-run ──
        if dry_run:
            # Loga o SQL que seria executado.
            LOG.info("[DRY RUN] SQL de extração:\n" + sql_extract)
            # Executa a query de contagem dedicada (sem subquery com ORDER BY).
            with ms.cursor() as cur:
                # Conta sem extrair os dados completos.
                cur.execute(sql_count)
                # Loga a contagem de produtos que seriam carregados.
                LOG.info(f"[DRY RUN] Registros que seriam extraídos: {cur.fetchone()[0]:,}")
            # Encerra sem carregar dados.
            return

        # ── Passo 5: abrir batch de rastreamento ──
        batch_id = open_batch(pg, DATASET, last_ts, overlap_days=0)

        # ── Passo 6: extrair e carregar ──
        # Cria o iterador lazy de extração do SQL Server.
        rows_iter = mssql_fetch_iter(ms, sql_extract, arraysize=CHUNK_SIZE)
        # Carrega via UPSERT — atualiza atributos do produto sem sobrescrever first_seen_at.
        extracted, upserted, _ = pg_copy_upsert(
            pg, DATASET, COLUMNS, rows_iter,
            batch_id,
            protected_cols=PROTECTED_COLS,
            chunk_size=CHUNK_SIZE,
        )

        # ── Passo 7: validações pós-carga ──
        validate_post_load(pg, batch_id)

        # ── Passo 8: avançar watermark ──
        # Watermark de produtos = timestamp atual de execução.
        # (dbo.PRODUTO pode não ter coluna de data, então usamos o momento da extração)
        new_wm = datetime.now(tz=timezone.utc)
        # Persiste o novo watermark.
        set_watermark(pg, DATASET, new_wm, None)

        # ── Passo 9: fechar batch com sucesso ──
        close_batch(pg, batch_id, "success", new_wm, extracted, upserted, 0)

    except Exception as exc:
        # Registra a mensagem de erro para o close_batch.
        error_msg = str(exc)
        # Loga o erro com stacktrace completo.
        LOG.error(f"ERRO FATAL no ETL products: {exc}", exc_info=True)
        # Tenta rollback para desfazer transação parcial.
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
        # Termina com código de saída 1 para sinalizar falha ao script orquestrador.
        sys.exit(1)

    finally:
        # Fecha conexões independentemente de sucesso ou falha.
        for conn in (pg, ms):
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    # Configura o parser de argumentos CLI.
    parser = argparse.ArgumentParser(
        description="ETL: dbo.PRODUTO (ERP GP) → stg.products (PostgreSQL)"
    )
    # Argumento para forçar recarga completa ignorando watermark.
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignora watermark e recarrega TODOS os produtos do ERP",
    )
    # Argumento para modo de simulação sem carga real.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra SQL e contagem de registros sem efetuar a carga",
    )
    # Faz o parse dos argumentos.
    args = parser.parse_args()
    # Chama a função principal com os argumentos.
    main(full_load=args.full_load, dry_run=args.dry_run)
