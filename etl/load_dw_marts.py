#!/usr/bin/env python3
"""
etl/load_dw_marts.py
====================

ETL que popula as tabelas mart do schema DW a partir das camadas
stg.* e cur.* já curadas.

TABELAS ATUALIZADAS
  dw.mart_revenue_daily    – agregação diária de faturamento (incremental)
  dw.mart_product_ranking  – ranking acumulado de produtos (full-refresh)
  dw.mart_customer_summary – sumário lifetime de clientes (full-refresh)
  dw.mart_state_summary    – faturamento por estado (full-refresh)

MECANISMOS DE CONTROLE
  • etl.load_batches  – registra cada execução com status running/success/failed
  • etl.load_control  – armazena watermark (última data processada) por dataset
  • dw.mart_refresh_log – log complementar específico dos marts (append-only)

  Para mart_revenue_daily:
    - Usa watermark para processar apenas os DIAS NOVOS desde a última carga.
    - Em caso de falha, o watermark NÃO avança → próxima execução reprocessa.

  Para os demais marts (full-refresh):
    - TRUNCATE + INSERT em transação única → ou tudo ou nada.
    - Watermark guarda timestamp do último refresh bem-sucedido.

IDEMPOTÊNCIA
  • Executar múltiplas vezes o script é seguro:
      - mart_revenue_daily: ON CONFLICT DO UPDATE (upsert por date)
      - Outros marts: TRUNCATE + INSERT em transação atômica
  • Se --full-load: ignora watermark e reprocessa todo o histórico

USO
  python etl/load_dw_marts.py                # incremental (usa watermark)
  python etl/load_dw_marts.py --full-load    # recarga completa
  python etl/load_dw_marts.py --dry-run      # apenas conta, não grava
  python etl/load_dw_marts.py --mart revenue # carrega apenas 1 mart
"""

# ── Biblioteca padrão ──────────────────────────────────────────────────────────
import argparse          # parse de argumentos da linha de comando
import logging           # logging estruturado
import os               # variáveis de ambiente e paths
import sys              # manipulação de sys.path para importar common.py
import traceback        # captura de stacktrace em caso de exceção
from datetime import date, datetime, timedelta   # manipulação de datas
from typing import Optional, Tuple               # type hints

# ── Psycopg2 (PostgreSQL) ──────────────────────────────────────────────────────
import psycopg2                    # driver PostgreSQL principal
import psycopg2.extras             # RealDictCursor para acesso por nome de coluna

# ── Importa utilitários comuns do ETL ─────────────────────────────────────────
# Insere o diretório deste próprio arquivo no PATH para encontrar common.py
# mesmo que o script seja executado a partir de qualquer diretório.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (       # noqa: E402  (import após sys.path.insert é esperado)
    setup_logging,         # configura handlers de log (stdout + opcional arquivo)
    get_pg_conn,           # abre conexão PostgreSQL a partir do .env
    ensure_etl_control,    # cria tabelas etl.load_control e etl.load_batches se ausentes
    open_batch,            # registra início de execução em etl.load_batches
    close_batch,           # finaliza execução com métricas e status
    get_watermark,         # lê último timestamp/id processado para um dataset
    set_watermark,         # grava novo watermark APENAS em caso de sucesso
)

# ── Logger do módulo ──────────────────────────────────────────────────────────
# Usa o nome do arquivo (sem .py) como identificador no log.
LOG = logging.getLogger("load_dw_marts")

# ── Constantes ────────────────────────────────────────────────────────────────
# Dataset names usados como chave em etl.load_control (devem ser únicos e estáveis).
DS_REVENUE   = "dw.mart_revenue_daily"    # watermark: última DATE processada
DS_PRODUCTS  = "dw.mart_product_ranking"  # watermark: timestamp do último refresh
DS_CUSTOMERS = "dw.mart_customer_summary" # watermark: timestamp do último refresh
DS_STATES    = "dw.mart_state_summary"    # watermark: timestamp do último refresh

# Data inicial padrão para full-load (antes de qualquer dado no ERP).
FULL_LOAD_START = date(2000, 1, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────────────────────

def _open_refresh_log(pg: psycopg2.extensions.connection,
                      mart_name: str,
                      watermark_from: Optional[date],
                      watermark_to: Optional[date]) -> int:
    """
    Registra o início de uma execução no log complementar dw.mart_refresh_log.
    Retorna o refresh_id gerado.

    Este log é ADICIONAL ao etl.load_batches — mantém histórico específico
    dos marts para facilitar auditoria no dashboard sem misturar com logs das
    cargas stg/cur.
    """
    # Insere linha com status 'running' e captura o ID gerado pela sequência.
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.mart_refresh_log
                (mart_name, status, watermark_from, watermark_to)
            VALUES (%s, 'running', %s, %s)
            RETURNING refresh_id
            """,
            # Parâmetros: nome do mart e janela de datas processada.
            (mart_name, watermark_from, watermark_to),
        )
        # Lê o refresh_id gerado pelo BIGSERIAL.
        refresh_id: int = cur.fetchone()[0]
    # Comita imediatamente para o log ficar visível mesmo em caso de falha posterior.
    pg.commit()
    return refresh_id


def _close_refresh_log(pg: psycopg2.extensions.connection,
                       refresh_id: int,
                       status: str,
                       rows_processed: int,
                       error: Optional[str] = None) -> None:
    """
    Atualiza o registro do refresh_log com status final e métricas.
    Chamado tanto em sucesso quanto em falha.
    """
    # Atualiza o registro existente com os resultados da execução.
    with pg.cursor() as cur:
        cur.execute(
            """
            UPDATE dw.mart_refresh_log SET
                status         = %s,
                rows_processed = %s,
                error_message  = %s,
                finished_at    = now()
            WHERE refresh_id = %s
            """,
            # Parâmetros na ordem dos %s do SQL.
            (status, rows_processed, error, refresh_id),
        )
    # Comita o encerramento do log.
    pg.commit()


def _get_source_date_range(pg: psycopg2.extensions.connection) -> Tuple[date, date]:
    """
    Retorna (min_date, max_date) de cur.order_items para saber o intervalo
    total de dados disponíveis na fonte.

    Usado para:
    - Definir watermark_to no full-load
    - Verificar se há dados novos no incremental
    """
    with pg.cursor() as cur:
        # Busca data mínima e máxima dos dados curados de uma vez só.
        cur.execute("SELECT MIN(sale_date), MAX(sale_date) FROM cur.order_items")
        row = cur.fetchone()
    # row[0] = data mais antiga, row[1] = data mais recente.
    return row[0], row[1]


# ─────────────────────────────────────────────────────────────────────────────
# 1. mart_revenue_daily — Carga incremental
# ─────────────────────────────────────────────────────────────────────────────

def load_mart_revenue_daily(pg: psycopg2.extensions.connection,
                            full_load: bool,
                            dry_run: bool) -> None:
    """
    Popula dw.mart_revenue_daily com agrega­ções diárias de venda.

    ESTRATÉGIA
      Incremental: processa apenas os dias posteriores ao último watermark.
      Full-load:   reprocessa todo o histórico desde FULL_LOAD_START.

    IDEMPOTÊNCIA
      Usa INSERT … ON CONFLICT DO UPDATE — executar para o mesmo dia
      sobrescreve os valores anteriores (seguro repetir sem duplicar).

    WATERMARK
      Armazenado como TIMESTAMPTZ em etl.load_control com last_ts = último
      DATE processado + 00:00:00 UTC.  Avança SOMENTE em caso de sucesso.
    """
    LOG.info("=== mart_revenue_daily: início ===")

    # ── 1. Determina intervalo de datas a processar ────────────────────────
    # Lê o intervalo total de dados disponíveis na camada curada.
    min_src, max_src = _get_source_date_range(pg)
    if max_src is None:
        # Não há dados em cur.order_items — nada a fazer.
        LOG.warning("cur.order_items está vazio — skipping mart_revenue_daily")
        return

    if full_load:
        # Full-load: ignora watermark e processa desde a data configurada.
        watermark_from = FULL_LOAD_START
        LOG.info(f"Modo full-load: processando a partir de {FULL_LOAD_START}")
    else:
        # Incremental: lê último timestamp gravado no etl.load_control.
        last_ts, _ = get_watermark(pg, DS_REVENUE)
        if last_ts is None:
            # Primeiro run (nunca foi carregado): começa do início dos dados.
            watermark_from = min_src
            LOG.info("Primeiro run detectado — processando histórico completo")
        else:
            # Avança 1 dia além do último processado para não re-processar.
            watermark_from = last_ts.date() + timedelta(days=1)
            LOG.info(f"Incremental: processando a partir de {watermark_from}")

    # Define o limite superior do intervalo como o max disponível na fonte.
    watermark_to = max_src

    # Verifica se há dados novos a processar.
    if watermark_from > watermark_to:
        LOG.info(f"Nenhum dado novo (watermark_from={watermark_from} > max_src={watermark_to})")
        return

    LOG.info(f"Intervalo: {watermark_from} → {watermark_to}")

    # ── 2. Conta linhas que seriam processadas (útil para --dry-run) ──────
    count_sql = """
        SELECT COUNT(DISTINCT sale_date) AS dias,
               COUNT(*) AS itens
        FROM cur.order_items
        WHERE sale_date BETWEEN %s AND %s
    """
    with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Executa contagem sem modificar dados (seguro em dry-run).
        cur.execute(count_sql, (watermark_from, watermark_to))
        cnt = cur.fetchone()

    LOG.info(f"A processar: {cnt['dias']} dias | {cnt['itens']:,} linhas de item")

    # Em modo dry-run encerra aqui sem gravar nada.
    if dry_run:
        LOG.info("DRY-RUN: nenhum dado gravado.")
        return

    # ── 3. Registra batch em etl.load_batches ─────────────────────────────
    # open_batch registra o início da execução para auditoria de linhagem.
    batch_id = open_batch(pg, DS_REVENUE, watermark_from, overlap_days=0)

    # Registra também no log específico de marts.
    refresh_id = _open_refresh_log(pg, DS_REVENUE, watermark_from, watermark_to)

    rows_upserted = 0  # contador de linhas inseridas/atualizadas

    try:
        # ── 4. Query de agregação diária ──────────────────────────────────
        # Agrega todos os campos necessários por dia de venda em uma única query.
        # Campos calendário (year, quarter, etc.) são derivados de sale_date
        # diretamente no SQL para evitar processamento Python desnecessário.
        agg_sql = """
            SELECT
                -- Data do dia — chave primária do mart
                oi.sale_date                              AS date,

                -- Campos calendário derivados da data
                EXTRACT(year    FROM oi.sale_date)::SMALLINT  AS year,
                EXTRACT(quarter FROM oi.sale_date)::SMALLINT  AS quarter,
                EXTRACT(month   FROM oi.sale_date)::SMALLINT  AS month,
                EXTRACT(week    FROM oi.sale_date)::SMALLINT  AS week_of_year,
                EXTRACT(dow     FROM oi.sale_date)::SMALLINT  AS day_of_week,

                -- Métricas de venda do dia
                COALESCE(SUM(oi.total_value), 0)::NUMERIC(16,2)    AS total_revenue,
                COUNT(DISTINCT oi.order_id_src)                     AS total_orders,
                COUNT(*)                                            AS total_items,
                COALESCE(SUM(oi.quantity), 0)::NUMERIC(14,3)       AS total_qty,
                COUNT(DISTINCT oi.customer_id)                      AS unique_customers,
                COUNT(DISTINCT oi.product_id)                       AS unique_products

            FROM cur.order_items oi
            WHERE oi.sale_date BETWEEN %s AND %s
            GROUP BY oi.sale_date
            ORDER BY oi.sale_date
        """

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Executa a agregação no banco — mais eficiente que processar em Python.
            cur.execute(agg_sql, (watermark_from, watermark_to))
            # Busca todos os resultados em memória (número de dias é pequeno).
            rows = cur.fetchall()

        LOG.info(f"Agregados {len(rows)} dias de venda")

        # ── 5. UPSERT no mart ─────────────────────────────────────────────
        # INSERT … ON CONFLICT DO UPDATE garante idempotência:
        # se o dia já existe (ex.: reprocessamento), atualiza os valores.
        upsert_sql = """
            INSERT INTO dw.mart_revenue_daily
                (date, year, quarter, month, week_of_year, day_of_week,
                 total_revenue, total_orders, total_items, total_qty,
                 unique_customers, unique_products, refreshed_at)
            VALUES
                (%(date)s, %(year)s, %(quarter)s, %(month)s,
                 %(week_of_year)s, %(day_of_week)s,
                 %(total_revenue)s, %(total_orders)s, %(total_items)s,
                 %(total_qty)s, %(unique_customers)s, %(unique_products)s,
                 now())
            ON CONFLICT (date) DO UPDATE SET
                -- Atualiza todas as métricas mas preserva a data original
                year             = EXCLUDED.year,
                quarter          = EXCLUDED.quarter,
                month            = EXCLUDED.month,
                week_of_year     = EXCLUDED.week_of_year,
                day_of_week      = EXCLUDED.day_of_week,
                total_revenue    = EXCLUDED.total_revenue,
                total_orders     = EXCLUDED.total_orders,
                total_items      = EXCLUDED.total_items,
                total_qty        = EXCLUDED.total_qty,
                unique_customers = EXCLUDED.unique_customers,
                unique_products  = EXCLUDED.unique_products,
                refreshed_at     = now()
        """

        with pg.cursor() as cur:
            # executemany processa cada linha do resultado com o SQL de upsert.
            # psycopg2 usa mogrify interno para segurança contra SQL injection.
            cur.executemany(upsert_sql, [dict(r) for r in rows])
            # Registra a contagem de linhas afetadas pelo executemany.
            rows_upserted = len(rows)

        # Comita a transação — único ponto de commit (atomicidade).
        pg.commit()
        LOG.info(f"Upsert realizado: {rows_upserted} dias gravados/atualizados")

        # ── 6. Fecha batch e atualiza watermark ───────────────────────────
        # Fecha o batch como 'success' com o novo watermark.
        close_batch(
            pg, batch_id,
            status        = "success",
            watermark_to  = watermark_to,
            rows_extracted = cnt['itens'],
            rows_inserted  = rows_upserted,
            rows_skipped   = 0,
        )

        # IMPORTANTE: watermark só avança APÓS confirmação de sucesso.
        # Gravar antes causaria perda de dados em caso de falha.
        set_watermark(pg, DS_REVENUE, last_ts=watermark_to, last_id=None)

        # Fecha o log complementar de marts.
        _close_refresh_log(pg, refresh_id, "success", rows_upserted)

        LOG.info(f"=== mart_revenue_daily: concluído ({rows_upserted} dias) ===")

    except Exception as exc:
        # Em caso de qualquer erro, faz rollback da transação parcial.
        pg.rollback()
        # Captura o traceback completo para diagnóstico.
        err_msg = traceback.format_exc()
        LOG.error(f"FALHA em mart_revenue_daily: {exc}")
        # Fecha o batch como 'failed' — watermark NÃO avança.
        close_batch(pg, batch_id, "failed",
                    watermark_to=None, rows_extracted=0,
                    rows_inserted=0, rows_skipped=0, error=err_msg)
        # Atualiza log de marts com o erro.
        _close_refresh_log(pg, refresh_id, "failed", 0, error=str(exc))
        # Re-lança a exceção para interromper o pipeline no nível acima.
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 2. mart_product_ranking — Full-refresh
# ─────────────────────────────────────────────────────────────────────────────

def load_mart_product_ranking(pg: psycopg2.extensions.connection,
                               dry_run: bool) -> None:
    """
    Recalcula o ranking acumulado de produtos e substitui a tabela inteira.

    ESTRATÉGIA FULL-REFRESH
      1. BEGIN transaction
      2. TRUNCATE mart_product_ranking (apaga tudo)
      3. INSERT … SELECT (recalcula a partir de cur.order_items + cur.products)
      4. COMMIT   → ambas operações confirmadas atomicamente
         ROLLBACK → em erro, tabela volta ao estado anterior (sem dados perdidos)

    Inclui RANK() window functions para calcular posição ordinal dos produtos
    por faturamento e por quantidade.
    """
    LOG.info("=== mart_product_ranking: início (full-refresh) ===")

    # Abre batch de auditoria — mesmo padrão das cargas stg/cur.
    batch_id   = open_batch(pg, DS_PRODUCTS, watermark_from=None, overlap_days=0)
    refresh_id = _open_refresh_log(pg, DS_PRODUCTS, None, None)

    rows_inserted = 0  # será preenchido após o INSERT

    try:
        with pg.cursor() as cur:
            # ── TRUNCATE + INSERT em transação única ──────────────────────
            # TRUNCATE remove todas as linhas da tabela de forma eficiente.
            # Está dentro da mesma transação que o INSERT abaixo.
            cur.execute("TRUNCATE TABLE dw.mart_product_ranking")

            # Conta antes de inserir (para dry-run e métricas).
            cur.execute("SELECT COUNT(DISTINCT product_id) FROM cur.order_items")
            total_products = cur.fetchone()[0]

            if dry_run:
                # Em dry-run, faz rollback do TRUNCATE e retorna sem gravar.
                pg.rollback()
                LOG.info(f"DRY-RUN: {total_products} produtos seriam processados")
                return

            # ── INSERT com RANK() window functions ────────────────────────
            # CTE 'agg' calcula métricas brutas por produto.
            # CTE 'ranked' adiciona as posições ordinais usando RANK() OVER (...).
            # INSERT final seleciona da CTE 'ranked'.
            insert_sql = """
                INSERT INTO dw.mart_product_ranking
                    (product_id, description, unit, active,
                     total_revenue, total_qty, order_count, unique_customers,
                     first_sale_date, last_sale_date,
                     revenue_rank, qty_rank, refreshed_at)
                WITH agg AS (
                    -- Agrega métricas por produto a partir de todos os itens curados
                    SELECT
                        oi.product_id,
                        MAX(p.description)                   AS description,
                        MAX(p.unit)                          AS unit,
                        BOOL_AND(p.active)                   AS active,

                        -- Métricas de venda acumuladas (todo o histórico)
                        COALESCE(SUM(oi.total_value),0)::NUMERIC(16,2) AS total_revenue,
                        COALESCE(SUM(oi.quantity),0)::NUMERIC(14,3)    AS total_qty,
                        COUNT(DISTINCT oi.order_id_src)                 AS order_count,
                        COUNT(DISTINCT oi.customer_id)                  AS unique_customers,

                        -- Janela temporal de vendas do produto
                        MIN(oi.sale_date) AS first_sale_date,
                        MAX(oi.sale_date) AS last_sale_date

                    FROM cur.order_items oi
                    -- LEFT JOIN preserva produtos sem vendas (mas order_items só tem produtos com venda)
                    LEFT JOIN cur.products p
                           ON p.product_id = oi.product_id
                          AND p.source_system = oi.source_system
                    GROUP BY oi.product_id
                ),
                ranked AS (
                    -- Adiciona posição ordinal por faturamento e por quantidade
                    SELECT
                        *,
                        -- RANK() atribui a mesma posição em caso de empate
                        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
                        RANK() OVER (ORDER BY total_qty     DESC) AS qty_rank
                    FROM agg
                )
                SELECT
                    product_id, description, unit, active,
                    total_revenue, total_qty, order_count, unique_customers,
                    first_sale_date, last_sale_date,
                    revenue_rank, qty_rank,
                    now() AS refreshed_at
                FROM ranked
            """

            # Executa o INSERT e verifica quantas linhas foram inseridas.
            cur.execute(insert_sql)
            rows_inserted = cur.rowcount  # psycopg2 preenche rowcount após INSERT

        # Comita TRUNCATE + INSERT atomicamente.
        pg.commit()
        LOG.info(f"Ranking gravado: {rows_inserted} produtos")

        # Fecha batch e watermark com o timestamp atual do refresh.
        close_batch(pg, batch_id, "success",
                    watermark_to    = datetime.utcnow(),
                    rows_extracted  = rows_inserted,
                    rows_inserted   = rows_inserted,
                    rows_skipped    = 0)
        set_watermark(pg, DS_PRODUCTS, last_ts=datetime.utcnow(), last_id=None)
        _close_refresh_log(pg, refresh_id, "success", rows_inserted)
        LOG.info("=== mart_product_ranking: concluído ===")

    except Exception as exc:
        # Rollback desfaz TRUNCATE + INSERT: tabela volta ao estado pré-execução.
        pg.rollback()
        err_msg = traceback.format_exc()
        LOG.error(f"FALHA em mart_product_ranking: {exc}")
        close_batch(pg, batch_id, "failed",
                    watermark_to=None, rows_extracted=0,
                    rows_inserted=0, rows_skipped=0, error=err_msg)
        _close_refresh_log(pg, refresh_id, "failed", 0, error=str(exc))
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 3. mart_customer_summary — Full-refresh
# ─────────────────────────────────────────────────────────────────────────────

def load_mart_customer_summary(pg: psycopg2.extensions.connection,
                                dry_run: bool) -> None:
    """
    Recalcula o sumário lifetime de cada cliente e substitui a tabela inteira.

    Enriquece os dados de cur.order_items com cidade/estado de stg.customers
    via LEFT JOIN — mantém clientes mesmo se stg.customers não os tiver
    (proteção contra inconsistência entre camadas).
    """
    LOG.info("=== mart_customer_summary: início (full-refresh) ===")

    batch_id   = open_batch(pg, DS_CUSTOMERS, watermark_from=None, overlap_days=0)
    refresh_id = _open_refresh_log(pg, DS_CUSTOMERS, None, None)
    rows_inserted = 0

    try:
        with pg.cursor() as cur:
            # Conta clientes únicos para dry-run e métricas.
            cur.execute("SELECT COUNT(DISTINCT customer_id) FROM cur.order_items")
            total_customers = cur.fetchone()[0]

            if dry_run:
                pg.rollback()
                LOG.info(f"DRY-RUN: {total_customers} clientes seriam processados")
                return

            # Remove todas as linhas antes de recarregar.
            cur.execute("TRUNCATE TABLE dw.mart_customer_summary")

            insert_sql = """
                INSERT INTO dw.mart_customer_summary
                    (customer_id, city, state,
                     first_purchase, last_purchase,
                     total_orders, total_revenue, total_qty,
                     unique_products, avg_ticket, active_span_days,
                     refreshed_at)
                SELECT
                    oi.customer_id,

                    -- Dados geográficos do cliente via stg.customers (LEFT JOIN → NULL se ausente)
                    MAX(sc.city)   AS city,
                    MAX(sc.state)  AS state,

                    -- Datas da primeira e última compra
                    MIN(oi.sale_date) AS first_purchase,
                    MAX(oi.sale_date) AS last_purchase,

                    -- Métricas de frequência e valor
                    COUNT(DISTINCT oi.order_id_src)                AS total_orders,
                    COALESCE(SUM(oi.total_value),0)::NUMERIC(16,2) AS total_revenue,
                    COALESCE(SUM(oi.quantity),0)::NUMERIC(14,3)    AS total_qty,
                    COUNT(DISTINCT oi.product_id)                  AS unique_products,

                    -- Ticket médio: evita divisão por zero com NULLIF
                    CASE
                        WHEN COUNT(DISTINCT oi.order_id_src) > 0
                        THEN (SUM(oi.total_value) / COUNT(DISTINCT oi.order_id_src))::NUMERIC(12,2)
                    END AS avg_ticket,

                    -- Amplitude de compra em dias (NULL se só 1 pedido)
                    CASE
                        WHEN MIN(oi.sale_date) <> MAX(oi.sale_date)
                        THEN (MAX(oi.sale_date) - MIN(oi.sale_date))
                    END AS active_span_days,

                    now() AS refreshed_at

                FROM cur.order_items oi
                LEFT JOIN stg.customers sc
                       ON sc.customer_id_src = oi.customer_id
                      AND sc.source_system   = oi.source_system
                GROUP BY oi.customer_id
            """

            cur.execute(insert_sql)
            rows_inserted = cur.rowcount

        pg.commit()
        LOG.info(f"Clientes gravados: {rows_inserted}")

        close_batch(pg, batch_id, "success",
                    watermark_to   = datetime.utcnow(),
                    rows_extracted = rows_inserted,
                    rows_inserted  = rows_inserted,
                    rows_skipped   = 0)
        set_watermark(pg, DS_CUSTOMERS, last_ts=datetime.utcnow(), last_id=None)
        _close_refresh_log(pg, refresh_id, "success", rows_inserted)
        LOG.info("=== mart_customer_summary: concluído ===")

    except Exception as exc:
        pg.rollback()
        err_msg = traceback.format_exc()
        LOG.error(f"FALHA em mart_customer_summary: {exc}")
        close_batch(pg, batch_id, "failed",
                    watermark_to=None, rows_extracted=0,
                    rows_inserted=0, rows_skipped=0, error=err_msg)
        _close_refresh_log(pg, refresh_id, "failed", 0, error=str(exc))
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 4. mart_state_summary — Full-refresh
# ─────────────────────────────────────────────────────────────────────────────

def load_mart_state_summary(pg: psycopg2.extensions.connection,
                             dry_run: bool) -> None:
    """
    Agrega faturamento e clientes por estado (UF) e substitui a tabela.

    Utiliza mart_customer_summary (já populado) como fonte de dados
    geográficos para evitar re-join com stg.customers.
    DEPENDÊNCIA: deve ser chamado APÓS load_mart_customer_summary.
    """
    LOG.info("=== mart_state_summary: início (full-refresh) ===")

    batch_id   = open_batch(pg, DS_STATES, watermark_from=None, overlap_days=0)
    refresh_id = _open_refresh_log(pg, DS_STATES, None, None)
    rows_inserted = 0

    try:
        with pg.cursor() as cur:
            if dry_run:
                # Conta estados únicos disponíveis na fonte.
                cur.execute("""
                    SELECT COUNT(DISTINCT state) FROM dw.mart_customer_summary
                    WHERE state IS NOT NULL
                """)
                n = cur.fetchone()[0]
                LOG.info(f"DRY-RUN: {n} estados seriam processados")
                pg.rollback()
                return

            cur.execute("TRUNCATE TABLE dw.mart_state_summary")

            insert_sql = """
                INSERT INTO dw.mart_state_summary
                    (state, total_customers, total_orders, total_revenue, avg_ticket, refreshed_at)
                SELECT
                    -- Normaliza NULL para 'N/D' para não perder clientes sem estado cadastrado
                    COALESCE(cs.state, 'N/D')                          AS state,
                    COUNT(DISTINCT cs.customer_id)                      AS total_customers,
                    SUM(cs.total_orders)                                AS total_orders,
                    SUM(cs.total_revenue)::NUMERIC(16,2)                AS total_revenue,

                    -- Ticket médio ponderado pelo número de pedidos do estado
                    CASE
                        WHEN SUM(cs.total_orders) > 0
                        THEN (SUM(cs.total_revenue) / SUM(cs.total_orders))::NUMERIC(12,2)
                    END AS avg_ticket,

                    now() AS refreshed_at

                FROM dw.mart_customer_summary cs
                GROUP BY COALESCE(cs.state, 'N/D')
                ORDER BY SUM(cs.total_revenue) DESC
            """

            cur.execute(insert_sql)
            rows_inserted = cur.rowcount

        pg.commit()
        LOG.info(f"Estados gravados: {rows_inserted}")

        close_batch(pg, batch_id, "success",
                    watermark_to   = datetime.utcnow(),
                    rows_extracted = rows_inserted,
                    rows_inserted  = rows_inserted,
                    rows_skipped   = 0)
        set_watermark(pg, DS_STATES, last_ts=datetime.utcnow(), last_id=None)
        _close_refresh_log(pg, refresh_id, "success", rows_inserted)
        LOG.info("=== mart_state_summary: concluído ===")

    except Exception as exc:
        pg.rollback()
        err_msg = traceback.format_exc()
        LOG.error(f"FALHA em mart_state_summary: {exc}")
        close_batch(pg, batch_id, "failed",
                    watermark_to=None, rows_extracted=0,
                    rows_inserted=0, rows_skipped=0, error=err_msg)
        _close_refresh_log(pg, refresh_id, "failed", 0, error=str(exc))
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(full_load: bool = False,
                 dry_run: bool = False,
                 mart: Optional[str] = None) -> None:
    """
    Orquestra a execução de todos (ou um) mart(s) em sequência.

    Ordem de execução importa:
      1. mart_revenue_daily    → independente
      2. mart_product_ranking  → independente
      3. mart_customer_summary → independente (enriquece com stg.customers)
      4. mart_state_summary    → DEPENDE de mart_customer_summary

    Parâmetros
    ──────────
    full_load : se True, ignora watermarks e reprocessa tudo
    dry_run   : se True, apenas conta e loga — não grava nada
    mart      : None = todos | 'revenue' | 'products' | 'customers' | 'states'
    """
    LOG.info(
        f"Pipeline DW Marts | full_load={full_load} | dry_run={dry_run} | mart={mart or 'todos'}"
    )

    # Abre conexão PostgreSQL uma única vez para todo o pipeline.
    pg = get_pg_conn()

    try:
        # Garante que as tabelas de controle ETL existam antes de abrir batches.
        ensure_etl_control(pg)

        # Executa apenas o mart solicitado (ou todos se mart=None).
        if mart in (None, "revenue"):
            load_mart_revenue_daily(pg, full_load, dry_run)

        if mart in (None, "products"):
            load_mart_product_ranking(pg, dry_run)

        if mart in (None, "customers"):
            load_mart_customer_summary(pg, dry_run)

        if mart in (None, "states"):
            # Depende de mart_customer_summary — deve sempre rodar após customers.
            load_mart_state_summary(pg, dry_run)

    finally:
        # Fecha a conexão independentemente de sucesso ou falha.
        pg.close()
        LOG.info("Conexão PostgreSQL encerrada.")


# ─────────────────────────────────────────────────────────────────────────────
# Entrada CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    """Monta o parser de argumentos da linha de comando."""
    p = argparse.ArgumentParser(
        prog="load_dw_marts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Flag de recarga completa: ignora watermark e reprocessa todo o histórico.
    p.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Ignora watermark e reprocessa todo o histórico (sobrescreve dados existentes).",
    )
    # Flag de dry-run: simula sem gravar.
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Apenas conta os dados a processar — não grava nada no banco.",
    )
    # Opção para executar apenas um mart específico.
    p.add_argument(
        "--mart",
        choices=["revenue", "products", "customers", "states"],
        default=None,
        help=(
            "Executa apenas o mart especificado.\n"
            "  revenue   → mart_revenue_daily\n"
            "  products  → mart_product_ranking\n"
            "  customers → mart_customer_summary\n"
            "  states    → mart_state_summary (requer customers já populado)"
        ),
    )
    # Nível de log configurável pelo usuário.
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de detalhe do log (padrão: INFO).",
    )
    return p


if __name__ == "__main__":
    # Faz o parse dos argumentos da linha de comando.
    args = _build_parser().parse_args()

    # Configura o sistema de logging antes de qualquer operação.
    # setup_logging configura handlers de stdout com formatação padrão do projeto.
    setup_logging("load_dw_marts", level=args.log_level)

    # Executa o pipeline com os parâmetros recebidos.
    run_pipeline(
        full_load = args.full_load,
        dry_run   = args.dry_run,
        mart      = args.mart,
    )
