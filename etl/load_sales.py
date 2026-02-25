"""
etl/load_sales.py
=================
Extrai o fato de compras (dbo.MOVIMENTO_DIA, TIPO=1) do ERP GP
e carrega em stg.sales com as seguintes garantias:

GARANTIAS DE LINHAGEM TEMPORAL
  • Watermark incremental: extrai somente dados novos (sale_date > last_ts - overlap)
  • Janela de overlap (padrão: 3 dias): captura late-arriving data sem duplicatas
    (ON CONFLICT DO NOTHING protege o destino de duplicação)
  • Watermark só avança em caso de SUCCESS — falha = reprocessamento seguro

GARANTIAS DE NÃO-OVERWRITE
  • INSERT ON CONFLICT DO NOTHING: registros existentes são INTOCÁVEIS
  • A chave de negócio (order_id_src, product_id_src, source_system)
    determina unicidade — duplicatas da fonte são silenciosamente ignoradas

GARANTIAS DE RASTREABILIDADE
  • Cada linha em stg.sales tem batch_id → FK para etl.load_batches
  • etl.load_batches registra: quando rodou, de onde extraiu, métricas e resultado

NOMES DE COLUNAS ERP (confirme antes de executar):
  Execute no SQL Server para verificar:
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'MOVIMENTO_DIA' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION;

MODOS DE EXECUÇÃO:
  python etl/load_sales.py                     # incremental (usa watermark)
  python etl/load_sales.py --full-load         # recarga completa
  python etl/load_sales.py --overlap-days 7    # overlap de 7 dias
  python etl/load_sales.py --dry-run           # conta sem carregar
"""

# argparse: leitura dos argumentos CLI (--full-load, --overlap-days, --dry-run).
import argparse
# logging: módulo de log — logger criado com setup_logging abaixo.
import logging
# os: acesso à variável de ambiente FETCH_CHUNK.
import os
# sys: sys.path para importação do etl.common e sys.exit para sinalizar falhas.
import sys
# date, datetime, timedelta, timezone: manipulação de datas para watermark e overlap.
from datetime import date, datetime, timedelta, timezone
# Path: resolução do caminho absoluto da raiz do projeto.
from pathlib import Path
# Optional: anotação de tipo para parâmetros que podem ser None.
from typing import Optional

# Resolve a raiz do projeto (pasta pai de etl/) para importar etl.common.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Insere a raiz no sys.path somente se ainda não estiver presente.
if str(_PROJECT_ROOT) not in sys.path:
    # Inserir no início garante prioridade na resolução de módulos.
    sys.path.insert(0, str(_PROJECT_ROOT))

# Importa todas as funções de infraestrutura do módulo central ETL.
from etl.common import (
    # close_batch: encerra o registro de batch com status e métricas.
    close_batch,
    # ensure_etl_control: cria tabelas de controle ETL se não existirem.
    ensure_etl_control,
    # get_mssql_conn: abre conexão com o SQL Server ERP via ODBC.
    get_mssql_conn,
    # get_pg_conn: abre conexão com o PostgreSQL destino.
    get_pg_conn,
    # get_watermark: lê o watermark anterior para carga incremental.
    get_watermark,
    # mssql_fetch_iter: extrai dados em chunks do SQL Server (iterador lazy).
    mssql_fetch_iter,
    # open_batch: registra início do batch em etl.load_batches.
    open_batch,
    # pg_copy_append: carga bulk INSERT ON CONFLICT DO NOTHING (tabelas de fato).
    pg_copy_append,
    # set_watermark: persiste o novo watermark após carga bem-sucedida.
    set_watermark,
    # setup_logging: configura logger com formatter estruturado.
    setup_logging,
)

# ---------------------------------------------------------------------------
# Configurações do dataset
# ---------------------------------------------------------------------------

# Logger específico para este script — separado do logger raiz 'etl'.
LOG = setup_logging("etl.sales")

# Nome lógico do dataset — chave em etl.load_control e etl.load_batches.
DATASET = "stg.sales"
# Overlap padrão: 3 dias para capturar late-arriving data no ERP.
OVERLAP_DAYS = 3
# Tamanho do chunk de extração — lê de FETCH_CHUNK ou usa 5.000 como padrão.
CHUNK_SIZE = int(os.getenv("FETCH_CHUNK", "5000"))

# Lista de colunas que serão carregadas em stg.sales.
# batch_id e extracted_at são injetados pelo common.py.
COLUMNS = [
    # Número do documento/pedido no ERP — parte da chave primária composta.
    "order_id_src",
    # ID do cliente que realizou a compra.
    "customer_id_src",
    # ID do produto comprado — parte da chave primária composta.
    "product_id_src",
    # ID da loja onde ocorreu a venda.
    "store_id_src",
    # Sistema de origem — parte da chave primária composta.
    "source_system",
    # Data da venda — base do watermark incremental e das features de recência.
    "sale_date",
    # Quantidade vendida — feature de intensidade de preferência.
    "quantity",
    # Valor total da linha — pode ser usado como proxy de importância da compra.
    "total_value",
    # ID do batch de carga (injetado pelo common.py).
    "batch_id",
    # Timestamp de extração da fonte (injetado pelo common.py).
    "extracted_at",
]


# ---------------------------------------------------------------------------
# Montagem da query de extração
# ---------------------------------------------------------------------------

def build_extract_sql(from_date: Optional[date], overlap_days: int) -> str:
    """
    Monta a query de extração do ERP com filtro temporal incremental e janela de overlap.

    Lógica do overlap:
    ──────────────────
    Se from_date = 2025-12-10 e overlap_days = 3:
      → extrai WHERE DATA >= 2025-12-07
      → vendas de 07-09/12 já existem em stg.sales → ON CONFLICT DO NOTHING (skip)
      → vendas de 10/12 em diante são novas → INSERT

    Isso garante que late-arriving data (pedidos com datas retroativas no ERP)
    sejam capturados sem criar duplicatas no destino.

    Por que não ORDER BY?
    ─────────────────────
    ORDER BY na query de extração não é necessário para carga com UPSERT:
    - A unicidade é garantida pela PK composta da tabela destino
    - ORDER BY em subquery MSSQL exige TOP/OFFSET — quebraria o dry-run
    - Remover ORDER BY melhora ligeiramente a performance no SQL Server
    """
    # Filtro de data para carga incremental com overlap de segurança.
    date_filter = ""
    # Se há watermark, aplica o filtro de data com overlap.
    if from_date is not None:
        # Subtrai os dias de overlap para capturar late-arriving data.
        safe_from = from_date - timedelta(days=overlap_days)
        # Formata a data de corte para interpolação no SQL.
        date_filter = f"  AND  PV.DATA >= '{safe_from.isoformat()}'\n"
        # Loga o filtro aplicado.
        LOG.info(
            f"Filtro temporal: PV.DATA >= {safe_from} "
            f"(watermark={from_date}, overlap={overlap_days}d)"
        )
    else:
        LOG.info("Carga completa: sem filtro de data (primeiro run ou --full-load)")
    # Monta e retorna o JOIN entre PREVENDA (cabeçalho) e PREVENDAITEM (itens).
    # MOVIMENTO_DIA é tabela de cabeçalho de nota fiscal — não tem PRODUTOID.
    # PREVENDA + PREVENDAITEM é o modelo correto para obter customer+product+date.
    # STATUS='3' = venda finalizada (confirmado: 152.496 registros, valor dominante).
    return f"""
    SELECT
        CAST(PV.PREVENDAID          AS VARCHAR(50))    AS order_id_src,
        CAST(PV.ENTIDADEID_CLIENTE  AS BIGINT)         AS customer_id_src,
        CAST(PI.PRODUTOID           AS BIGINT)         AS product_id_src,
        CAST(PV.ENTIDADEID_LOJA     AS BIGINT)         AS store_id_src,
        'sqlserver_gp'                                 AS source_system,
        CAST(PV.DATA                AS DATE)           AS sale_date,
        CAST(PI.QUANTIDADE          AS DECIMAL(18,4))  AS quantity,
        CAST(PI.PRECOUNIT * PI.QUANTIDADE AS DECIMAL(18,2)) AS total_value
    FROM dbo.PREVENDA     PV
    JOIN dbo.PREVENDAITEM PI
        ON  PI.PREVENDAID      = PV.PREVENDAID
        AND PI.ENTIDADEID_LOJA = PV.ENTIDADEID_LOJA
    WHERE PV.STATUS             = '3'
      AND PV.ENTIDADEID_CLIENTE IS NOT NULL
      AND PI.PRODUTOID          IS NOT NULL
      AND PV.DATA               IS NOT NULL
      AND PI.QUANTIDADE         > 0
      AND PI.PRECOUNIT          >= 0
    {date_filter}
    """


def build_count_sql(from_date: Optional[date], overlap_days: int) -> str:
    """
    Monta uma query de COUNT para o --dry-run, sem envolver subqueries com ORDER BY.
    Conta apenas com os mesmos filtros da query de extração principal.
    """
    # Filtro de data — mesmo cálculo de overlap da query de extração.
    date_filter = ""
    # Se há watermark, aplica o filtro de data com overlap.
    if from_date is not None:
        # Calcula a data de corte com overlap.
        safe_from = (from_date - timedelta(days=overlap_days)).isoformat()
        # Monta o filtro de data para o COUNT.
        date_filter = f"  AND  PV.DATA >= '{safe_from}'\n"
    # Retorna a query de contagem usando o mesmo JOIN/filtros da extração principal.
    return f"""
    SELECT COUNT(*)
    FROM dbo.PREVENDA     PV
    JOIN dbo.PREVENDAITEM PI
        ON  PI.PREVENDAID      = PV.PREVENDAID
        AND PI.ENTIDADEID_LOJA = PV.ENTIDADEID_LOJA
    WHERE PV.STATUS             = '3'
      AND PV.ENTIDADEID_CLIENTE IS NOT NULL
      AND PI.PRODUTOID          IS NOT NULL
      AND PV.DATA               IS NOT NULL
      AND PI.QUANTIDADE         > 0
      AND PI.PRECOUNIT          >= 0
    {date_filter}
    """


# ---------------------------------------------------------------------------
# Validações pós-carga
# ---------------------------------------------------------------------------

def validate_post_load(pg, batch_id: int) -> bool:
    """
    Executa validações de qualidade após a carga em stg.sales.

    Verificações:
    1. Linhas inseridas neste batch
    2. Intervalo de datas inserido
    3. Datas suspeitas (< 2000 ou futuras)
    4. Campos obrigatórios inválidos
    5. Total acumulado em stg.sales

    Retorna True se OK, False se encontrou problema crítico.
    """
    # Flag de resultado.
    ok = True
    # Abre cursor para as queries de validação.
    with pg.cursor() as cur:

        # ── Validação 1: linhas inseridas neste batch ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.sales WHERE batch_id = %s",
            (batch_id,),
        )
        # Conta as linhas efetivamente inseridas neste batch.
        n_batch = cur.fetchone()[0]
        # Loga o total do batch.
        LOG.info(f"[batch={batch_id}] Validação ▸ linhas inseridas neste batch: {n_batch:,}")

        # ── Validação 2: intervalo de datas do que foi inserido ──
        cur.execute(
            "SELECT MIN(sale_date), MAX(sale_date) FROM stg.sales WHERE batch_id = %s",
            (batch_id,),
        )
        # Lê o intervalo mínimo e máximo das datas inseridas.
        min_d, max_d = cur.fetchone()
        # Loga o intervalo de datas — confirma que o watermark está funcionando.
        LOG.info(f"[batch={batch_id}] Validação ▸ intervalo de datas: [{min_d} → {max_d}]")

        # ── Validação 3a: datas muito antigas (possível erro de conversão no ERP) ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.sales WHERE batch_id = %s AND sale_date < '2000-01-01'",
            (batch_id,),
        )
        # Conta linhas com datas suspeitas (anteriores a 2000).
        old_count = cur.fetchone()[0]
        # Emite aviso se houver datas muito antigas — pode indicar erro de tipo DATE no ERP.
        if old_count > 0:
            LOG.warning(
                f"[batch={batch_id}] AVISO ▸ {old_count:,} linhas com sale_date < 2000-01-01 "
                "— verificar conversão de tipo DATE no ERP"
            )

        # ── Validação 3b: datas no futuro (impossível em fato de vendas) ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.sales WHERE batch_id = %s AND sale_date > CURRENT_DATE",
            (batch_id,),
        )
        # Conta linhas com datas futuras.
        future_count = cur.fetchone()[0]
        # Datas futuras em vendas indicam erro na fonte — marcar como problema crítico.
        if future_count > 0:
            LOG.warning(
                f"[batch={batch_id}] AVISO ▸ {future_count:,} linhas com sale_date > hoje "
                "— verificar dados de origem"
            )
            # Marca como não-OK pois datas futuras nunca devem ocorrer em fatos de venda.
            ok = False

        # ── Validação 4: campos obrigatórios inválidos ──
        cur.execute(
            """
            SELECT COUNT(*) FROM stg.sales
            WHERE batch_id = %s
              AND (customer_id_src IS NULL OR product_id_src IS NULL OR quantity <= 0)
            """,
            (batch_id,),
        )
        # Conta linhas com dados obrigatórios ausentes ou inválidos.
        bad_rows = cur.fetchone()[0]
        # Emite aviso — linhas com dados inválidos não devem existir (filtros na extração).
        if bad_rows > 0:
            LOG.warning(
                f"[batch={batch_id}] AVISO ▸ {bad_rows:,} linhas com dados obrigatórios inválidos"
            )
            # Dados inválidos chegando ao staging indicam falha nos filtros da query.
            ok = False

        # ── Validação 5: total acumulado em stg.sales (saúde geral) ──
        cur.execute("SELECT COUNT(*), MAX(sale_date) FROM stg.sales")
        # Lê total de linhas e data mais recente no staging.
        total_stg, max_stg = cur.fetchone()
        # Loga o resumo geral da tabela de staging.
        LOG.info(
            f"[batch={batch_id}] Validação ▸ stg.sales total: {total_stg:,} linhas | "
            f"data mais recente: {max_stg}"
        )

    # Retorna o resultado geral das validações.
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(full_load: bool = False, overlap_days: int = OVERLAP_DAYS, dry_run: bool = False) -> None:
    """
    Ponto de entrada do pipeline de carga de vendas.

    Fluxo:
    1. Abre conexões PG e MSSQL
    2. Garante tabelas de controle ETL
    3. Lê watermark anterior (MAX sale_date da carga anterior)
    4. Monta SQL com filtro de overlap
    5. Em dry_run: conta e sai
    6. Abre batch, extrai e carrega via INSERT DO NOTHING
    7. Descobre novo watermark (MAX sale_date inserido neste batch)
    8. Valida pós-carga
    9. Avança watermark (somente se inseriu dados além do overlap)
    10. Fecha batch com sucesso
    """
    # Imprime separador de início do run.
    LOG.info("=" * 70)
    # Loga os parâmetros do run para rastreabilidade.
    LOG.info(f"ETL SALES | full_load={full_load} | overlap_days={overlap_days} | dry_run={dry_run}")
    # Fecha o cabeçalho.
    LOG.info("=" * 70)

    # Abre conexão com o PostgreSQL destino.
    pg = get_pg_conn()
    # Abre conexão com o SQL Server ERP fonte.
    ms = get_mssql_conn()

    # Variáveis de controle para o bloco except/finally.
    batch_id    = None
    extracted   = 0
    inserted    = 0
    skipped     = 0
    error_msg   = None

    try:
        # ── Passo 1: garantir tabelas de controle ──
        # Cria etl.load_control e etl.load_batches se não existirem.
        ensure_etl_control(pg)

        # ── Passo 2: ler watermark anterior ──
        # last_ts é o MAX(sale_date) da última carga bem-sucedida.
        last_ts, _ = get_watermark(pg, DATASET)
        # from_date: converte para date ou None para o filtro da query.
        from_date: Optional[date] = None
        # Se não é full_load e há watermark, extrai somente desde o watermark.
        if not full_load and last_ts is not None:
            # Converte para date — last_ts pode ser datetime ou date dependendo do banco.
            from_date = last_ts.date() if isinstance(last_ts, datetime) else last_ts
            # Loga o watermark que será usado como ponto de partida.
            LOG.info(f"Watermark anterior: {from_date}")
        else:
            # Sem watermark ou full_load: extrai tudo.
            LOG.info("Watermark ignorado → carga completa")

        # ── Passo 3: montar SQL de extração ──
        # Monta a query principal com filtros de overlap.
        sql_extract = build_extract_sql(from_date, overlap_days)

        # ── Passo 4 (opcional): dry-run ──
        if dry_run:
            # Loga o SQL de extração que seria executado.
            LOG.info("[DRY RUN] SQL de extração:")
            LOG.info(sql_extract)
            # Monta a query de contagem (sem subquery com ORDER BY — safe para MSSQL).
            sql_count = build_count_sql(from_date, overlap_days)
            # Executa a contagem no SQL Server.
            with ms.cursor() as cur:
                cur.execute(sql_count)
                # Lê e loga a contagem.
                count = cur.fetchone()[0]
            # Loga o resultado do dry-run.
            LOG.info(f"[DRY RUN] Registros que seriam extraídos: {count:,}")
            LOG.info("[DRY RUN] Nenhum dado foi carregado.")
            # Encerra sem carga.
            return

        # ── Passo 5: abrir batch ──
        # Converte from_date para datetime UTC para gravar em watermark_from.
        watermark_from_dt = (
            datetime.combine(from_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            if from_date else None
        )
        # Registra o início do batch com o watermark e overlap usados.
        batch_id = open_batch(pg, DATASET, watermark_from_dt, overlap_days)

        # ── Passo 6: extrair + carregar (streaming, sem overwrite) ──
        # Cria iterador lazy que lê do SQL Server em chunks de CHUNK_SIZE linhas.
        rows_iter = mssql_fetch_iter(ms, sql_extract, arraysize=CHUNK_SIZE)
        # Carrega via INSERT DO NOTHING — registros existentes são preservados intactos.
        extracted, inserted, skipped = pg_copy_append(
            pg, DATASET, COLUMNS, rows_iter, batch_id, chunk_size=CHUNK_SIZE
        )

        # ── Passo 7: descobrir novo watermark ──
        # Novo watermark = MAX(sale_date) das linhas INSERIDAS neste batch.
        with pg.cursor() as cur:
            # Consulta o MAX da data de venda apenas nas linhas deste batch.
            cur.execute(
                "SELECT MAX(sale_date) FROM stg.sales WHERE batch_id = %s",
                (batch_id,),
            )
            # Lê a data máxima inserida.
            row = cur.fetchone()
        # new_wm pode ser None se nenhuma linha foi inserida (tudo era overlap).
        new_wm = row[0] if row else None

        # ── Passo 8: validação pós-carga ──
        # Executa verificações de qualidade e emite avisos.
        valid = validate_post_load(pg, batch_id)
        # Se houve alertas graves, avisa o operador para revisar antes de prosseguir.
        if not valid:
            LOG.warning("Validações com alertas — revise os logs antes de prosseguir.")

        # ── Passo 9: avançar watermark (somente se há dados novos além do overlap) ──
        # Lógica:
        # - new_wm = MAX(sale_date) das linhas inseridas neste batch
        # - Se todas as linhas foram do overlap e foram skippadas, inserted=0 e new_wm=None
        # - Se houve inserções com date > last_ts, new_wm > from_date → avança
        should_advance = (
            new_wm is not None
            and (from_date is None or new_wm > from_date)
        )
        # Se há dados novos além do overlap, atualiza o watermark.
        if should_advance:
            # Persiste o novo watermark em etl.load_control.
            set_watermark(pg, DATASET, new_wm, None)
            # Loga o avanço do watermark.
            LOG.info(f"Watermark avançado: {from_date} → {new_wm}")
        else:
            # Sem dados novos: mantém o watermark atual.
            LOG.info(
                f"Watermark mantido em {from_date} "
                f"(inserted={inserted:,} — nenhum dado além do overlap)"
            )

        # ── Passo 10: fechar batch com sucesso ──
        # Atualiza etl.load_batches com status 'success', métricas e watermark.
        close_batch(
            pg, batch_id, "success", new_wm,
            extracted, inserted, skipped,
        )

    except Exception as exc:
        # Captura qualquer erro inesperado e registra.
        error_msg = str(exc)
        # Loga o erro com stacktrace para diagnóstico.
        LOG.error(f"ERRO FATAL no ETL sales: {exc}", exc_info=True)
        # Tenta rollback para desfazer qualquer transação parcial não-commitada.
        try:
            pg.rollback()
        except Exception:
            # Ignora erros de rollback — conexão pode estar comprometida.
            pass
        # Se o batch foi aberto, fecha-o como 'failed' para rastreabilidade.
        if batch_id is not None:
            try:
                close_batch(
                    pg, batch_id, "failed", None,
                    extracted, inserted, skipped, error_msg,
                )
            except Exception as inner:
                # Loga falha ao fechar batch — não deixa passar silenciosamente.
                LOG.error(f"Falha ao registrar erro no batch: {inner}")
        # Encerra com código 1 para sinalizar falha ao orquestrador.
        sys.exit(1)

    finally:
        # Fecha conexões de banco independentemente do resultado.
        for conn in (pg, ms):
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Entrypoint CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Configura o parser com descrição e exemplos de uso.
    parser = argparse.ArgumentParser(
        description="ETL: dbo.MOVIMENTO_DIA (ERP GP) → stg.sales (PostgreSQL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python etl/load_sales.py                     # incremental (usa watermark)
  python etl/load_sales.py --full-load         # recarga completa
  python etl/load_sales.py --overlap-days 7    # overlap de 7 dias
  python etl/load_sales.py --dry-run           # conta registros sem carregar
        """,
    )
    # Argumento para forçar carga completa ignorando watermark.
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignora watermark e extrai todos os registros da fonte",
    )
    # Argumento para configurar o overlap de dias para late-arriving data.
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=OVERLAP_DAYS,
        help=f"Dias de overlap para late-arriving data (padrão: {OVERLAP_DAYS})",
    )
    # Argumento para modo simulação sem carga real.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra SQL e contagem de registros sem efetuar carga",
    )
    # Faz o parse dos argumentos.
    args = parser.parse_args()
    # Chama a função principal com os argumentos parseados.
    main(full_load=args.full_load, overlap_days=args.overlap_days, dry_run=args.dry_run)
