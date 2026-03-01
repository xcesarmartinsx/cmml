"""
routers/business.py
===================

Endpoints da API para o Dashboard Visão 360° da empresa.

Todos os endpoints consultam as tabelas mart do schema DW:
  dw.mart_revenue_daily    → séries temporais e KPIs
  dw.mart_product_ranking  → ranking de produtos
  dw.mart_customer_summary → métricas de clientes
  dw.mart_state_summary    → distribuição geográfica

FILTROS DISPONÍVEIS
  year_from / year_to  → filtro de período (por ano)
  granularity          → monthly | quarterly | yearly (para séries temporais)
  limit                → número máximo de itens retornados em listagens

MONTAGEM
  Importado e registrado em main.py via:
      app.include_router(business_router, prefix="")
"""

# ── Biblioteca padrao ──────────────────────────────────────────────────────────
import os
from typing import Optional

# ── FastAPI ────────────────────────────────────────────────────────────────────
from fastapi import APIRouter, HTTPException, Query

# ── Psycopg2 ──────────────────────────────────────────────────────────────────
import psycopg2.extras

# ── Pool compartilhado ────────────────────────────────────────────────────────
from deps import get_db, release_db

# ── Router ─────────────────────────────────────────────────────────────────────
# Prefixo vazio — o prefixo /api/business é definido ao incluir em main.py.
router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# Utilitário de conexão
# ─────────────────────────────────────────────────────────────────────────────

def _get_db() -> psycopg2.extensions.connection:
    """
    Abre uma nova conexão PostgreSQL a partir das variáveis de ambiente.
    Cada request abre e fecha sua própria conexão (stateless).
    Variáveis: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD.
    """
    return psycopg2.connect(
        host     = os.getenv("PG_HOST", "postgres"),  # host do container postgres
        port     = int(os.getenv("PG_PORT", "5432")), # porta padrão PostgreSQL
        dbname   = os.getenv("PG_DB",   "reco"),      # database do projeto
        user     = os.getenv("PG_USER", "reco"),      # usuário de leitura
        password = os.environ["PG_PASSWORD"],           # sem fallback — falha se ausente
    )


def _rows(conn: psycopg2.extensions.connection, sql: str, params=()) -> list:
    """
    Executa uma query e retorna todos os resultados como lista de dicts.
    Usa RealDictCursor para acesso por nome de coluna (não por índice).
    Fecha o cursor após a leitura — a conexão é fechada pelo chamador.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Executa o SQL com parâmetros escapados (seguro contra SQL injection).
        cur.execute(sql, params)
        # Converte para lista de dicts Python — RealDictRow não é JSON-serializável.
        return [dict(r) for r in cur.fetchall()]


def _scalar(conn: psycopg2.extensions.connection, sql: str, params=()) -> any:
    """
    Executa uma query e retorna o primeiro campo da primeira linha.
    Usado para queries escalares (COUNT, SUM, MAX, etc.).
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    # Retorna None se a query não retornou linhas.
    return row[0] if row else None


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/meta
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/meta")
def get_meta():
    """
    Retorna metadados sobre os dados disponíveis:
      - Intervalo de anos na base (year_min, year_max)
      - Total de registros de cada mart
      - Última atualização dos marts

    Usado pelo frontend para configurar os controles de filtro de período.
    """
    conn = _get_db()
    try:
        # Busca o intervalo de anos disponíveis no mart de receita diária.
        years = _rows(conn, """
            SELECT MIN(year) AS year_min,
                   MAX(year) AS year_max,
                   -- Conta dias com dados para validar que o mart está populado
                   COUNT(*)  AS days_loaded
            FROM dw.mart_revenue_daily
        """)

        # Conta linhas em cada mart para feedback de saúde no dashboard.
        counts = {
            "revenue_days"   : _scalar(conn, "SELECT COUNT(*) FROM dw.mart_revenue_daily"),
            "products"       : _scalar(conn, "SELECT COUNT(*) FROM dw.mart_product_ranking"),
            "customers"      : _scalar(conn, "SELECT COUNT(*) FROM dw.mart_customer_summary"),
            "states"         : _scalar(conn, "SELECT COUNT(*) FROM dw.mart_state_summary"),
        }

        # Última execução bem-sucedida do ETL nos marts.
        last_refresh = _scalar(conn, """
            SELECT MAX(finished_at)
            FROM dw.mart_refresh_log
            WHERE status = 'success'
        """)

        return {
            # Intervalo de anos com dados
            "year_min"     : years[0]["year_min"]    if years else None,
            "year_max"     : years[0]["year_max"]    if years else None,
            "days_loaded"  : years[0]["days_loaded"] if years else 0,
            # Contagem por mart (útil para verificar saúde)
            "counts"       : counts,
            # ISO string da última atualização
            "last_refresh" : last_refresh.isoformat() if last_refresh else None,
        }
    finally:
        # Garante fechamento da conexão mesmo em caso de exceção.
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/kpis
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/kpis")
def get_kpis(
    year_from: Optional[int] = Query(None, description="Ano inicial do filtro (inclusive)"),
    year_to:   Optional[int] = Query(None, description="Ano final do filtro (inclusive)"),
):
    """
    Retorna KPIs consolidados para o período selecionado:
      - Faturamento total
      - Total de pedidos
      - Ticket médio
      - Dias com venda
      - Clientes únicos (aproximado pelo mart)
      - Crescimento YoY (variação % vs mesmo período do ano anterior)
    """
    conn = _get_db()
    try:
        # ── KPIs do período selecionado ───────────────────────────────────
        # Filtro dinâmico por ano: adiciona cláusula WHERE apenas se informado.
        where_parts = []
        params_current = []

        if year_from:
            where_parts.append("year >= %s")
            params_current.append(year_from)
        if year_to:
            where_parts.append("year <= %s")
            params_current.append(year_to)

        # Monta a cláusula WHERE ou string vazia se sem filtro.
        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # Agrega KPIs do período filtrado em uma única query.
        kpi_sql = f"""
            SELECT
                -- Faturamento total do período
                COALESCE(SUM(total_revenue), 0)::NUMERIC(16,2)   AS total_revenue,
                -- Total de pedidos no período
                COALESCE(SUM(total_orders), 0)                    AS total_orders,
                -- Número de dias com pelo menos 1 venda
                COUNT(*)                                          AS days_with_sales,
                -- Ticket médio = faturamento / pedidos (evita divisão por zero)
                CASE
                    WHEN SUM(total_orders) > 0
                    THEN (SUM(total_revenue) / SUM(total_orders))::NUMERIC(12,2)
                END                                               AS avg_ticket,
                -- Clientes únicos no período (soma diária — super-set do real)
                -- Para exato, usaríamos mart_customer_summary + filtro de data
                COALESCE(SUM(unique_customers), 0)                AS customer_days
            FROM dw.mart_revenue_daily
            {where_clause}
        """
        kpis = _rows(conn, kpi_sql, params_current)
        result = kpis[0] if kpis else {}

        # ── Cálculo do crescimento YoY ────────────────────────────────────
        # Compara o faturamento total do período atual com o período anterior
        # do mesmo tamanho (ex.: 2024 vs 2023 para filtro year=2024).
        yoy_growth = None
        if year_from and year_to:
            span = year_to - year_from        # tamanho do intervalo em anos
            prev_from = year_from - span - 1  # início do período anterior
            prev_to   = year_to   - span - 1  # fim do período anterior

            prev_sql = """
                SELECT COALESCE(SUM(total_revenue), 0)::NUMERIC(16,2) AS rev
                FROM dw.mart_revenue_daily
                WHERE year BETWEEN %s AND %s
            """
            prev = _rows(conn, prev_sql, (prev_from, prev_to))
            prev_rev = float(prev[0]["rev"]) if prev else 0

            curr_rev = float(result.get("total_revenue", 0))

            # Variação percentual: evita divisão por zero se período anterior = 0.
            if prev_rev and prev_rev > 0:
                yoy_growth = round(((curr_rev - prev_rev) / prev_rev) * 100, 2)

        # Adiciona crescimento YoY ao resultado.
        result["yoy_growth"] = yoy_growth

        # Converte Decimal para float (JSON não serializa Decimal nativamente).
        for k, v in result.items():
            if hasattr(v, "__float__"):
                result[k] = float(v)

        return result
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/revenue
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/revenue")
def get_revenue(
    granularity: str = Query(
        "monthly",
        regex="^(daily|monthly|quarterly|yearly)$",
        description="Granularidade: daily | monthly | quarterly | yearly",
    ),
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna série temporal de faturamento agregado pela granularidade escolhida.

    Cada item da série contém:
      - label   : string descritiva do período (ex.: '2024-03', 'Q2 2024', '2024')
      - period  : chave de ordenação ISO-compatível
      - revenue : faturamento total do período
      - orders  : pedidos do período
      - avg_ticket : ticket médio

    Usado pelo gráfico de linha principal do dashboard.
    """
    conn = _get_db()
    try:
        # Cláusula WHERE dinâmica para filtro de período.
        where_parts = []
        params = []
        if year_from:
            where_parts.append("year >= %s")
            params.append(year_from)
        if year_to:
            where_parts.append("year <= %s")
            params.append(year_to)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        if granularity == "daily":
            # Série diária: uma linha por dia de venda.
            sql = f"""
                SELECT
                    date::TEXT                                        AS period,
                    to_char(date, 'DD/MM/YYYY')                      AS label,
                    total_revenue::FLOAT                             AS revenue,
                    total_orders                                     AS orders,
                    -- Ticket médio diário
                    CASE WHEN total_orders > 0
                         THEN (total_revenue / total_orders)::NUMERIC(10,2)::FLOAT
                    END                                              AS avg_ticket
                FROM dw.mart_revenue_daily
                {where}
                ORDER BY date
            """

        elif granularity == "monthly":
            # Série mensal: agrega todos os dias de cada mês.
            sql = f"""
                SELECT
                    (year::TEXT || '-' || LPAD(month::TEXT, 2, '0'))  AS period,
                    to_char(make_date(year::INT, month::INT, 1), 'Mon/YYYY') AS label,
                    SUM(total_revenue)::FLOAT                          AS revenue,
                    SUM(total_orders)                                  AS orders,
                    CASE WHEN SUM(total_orders) > 0
                         THEN (SUM(total_revenue) / SUM(total_orders))::NUMERIC(10,2)::FLOAT
                    END                                                AS avg_ticket
                FROM dw.mart_revenue_daily
                {where}
                GROUP BY year, month
                ORDER BY year, month
            """

        elif granularity == "quarterly":
            # Série trimestral: agrega por ano + trimestre.
            sql = f"""
                SELECT
                    (year::TEXT || '-Q' || quarter::TEXT)             AS period,
                    ('Q' || quarter::TEXT || ' ' || year::TEXT)      AS label,
                    SUM(total_revenue)::FLOAT                          AS revenue,
                    SUM(total_orders)                                  AS orders,
                    CASE WHEN SUM(total_orders) > 0
                         THEN (SUM(total_revenue) / SUM(total_orders))::NUMERIC(10,2)::FLOAT
                    END                                                AS avg_ticket
                FROM dw.mart_revenue_daily
                {where}
                GROUP BY year, quarter
                ORDER BY year, quarter
            """

        else:  # yearly
            # Série anual: uma linha por ano.
            sql = f"""
                SELECT
                    year::TEXT                                        AS period,
                    year::TEXT                                        AS label,
                    SUM(total_revenue)::FLOAT                          AS revenue,
                    SUM(total_orders)                                  AS orders,
                    CASE WHEN SUM(total_orders) > 0
                         THEN (SUM(total_revenue) / SUM(total_orders))::NUMERIC(10,2)::FLOAT
                    END                                                AS avg_ticket
                FROM dw.mart_revenue_daily
                {where}
                GROUP BY year
                ORDER BY year
            """

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/products
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/products")
def get_products(
    limit: int = Query(20, ge=5, le=100, description="Número de produtos no ranking"),
    sort:  str = Query(
        "revenue",
        regex="^(revenue|qty|orders|customers)$",
        description="Campo de ordenação: revenue | qty | orders | customers",
    ),
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna o ranking dos produtos mais vendidos/lucrativos.

    Se year_from/year_to forem informados, calcula o ranking para o
    período específico consultando cur.order_items diretamente.
    Caso contrário, usa o mart pré-calculado (mais rápido).
    """
    conn = _get_db()
    try:
        # Mapeamento de nome de campo da query para coluna SQL.
        sort_col_map = {
            "revenue"   : "total_revenue",
            "qty"       : "total_qty",
            "orders"    : "order_count",
            "customers" : "unique_customers",
        }
        order_col = sort_col_map.get(sort, "total_revenue")

        if year_from or year_to:
            # ── Ranking filtrado por período ──────────────────────────────
            # Consulta cur.order_items com filtro de data e faz JOIN com cur.products
            # para obter descrição e unidade do produto.
            where_parts = []
            params: list = []
            if year_from:
                where_parts.append("EXTRACT(year FROM oi.sale_date) >= %s")
                params.append(year_from)
            if year_to:
                where_parts.append("EXTRACT(year FROM oi.sale_date) <= %s")
                params.append(year_to)

            where = "WHERE " + " AND ".join(where_parts)
            params.append(limit)  # parâmetro do LIMIT no final

            sql = f"""
                SELECT
                    oi.product_id,
                    MAX(p.description)                              AS description,
                    MAX(p.unit)                                     AS unit,
                    BOOL_AND(p.active)                              AS active,
                    SUM(oi.total_value)::FLOAT                     AS total_revenue,
                    SUM(oi.quantity)::FLOAT                        AS total_qty,
                    COUNT(DISTINCT oi.order_id_src)                 AS order_count,
                    COUNT(DISTINCT oi.customer_id)                  AS unique_customers,
                    MIN(oi.sale_date)::TEXT                        AS first_sale_date,
                    MAX(oi.sale_date)::TEXT                        AS last_sale_date,
                    -- Posição ordinal calculada dinamicamente para o período filtrado
                    RANK() OVER (ORDER BY SUM(oi.total_value) DESC) AS revenue_rank,
                    RANK() OVER (ORDER BY SUM(oi.quantity) DESC)    AS qty_rank
                FROM cur.order_items oi
                LEFT JOIN cur.products p
                       ON p.product_id   = oi.product_id
                      AND p.source_system = oi.source_system
                {where}
                GROUP BY oi.product_id
                ORDER BY {order_col} DESC
                LIMIT %s
            """

        else:
            # ── Ranking pré-calculado (mart) ──────────────────────────────
            # Usa o mart para resposta mais rápida quando não há filtro de período.
            sql = f"""
                SELECT
                    product_id,
                    description,
                    unit,
                    active,
                    total_revenue::FLOAT,
                    total_qty::FLOAT,
                    order_count,
                    unique_customers,
                    first_sale_date::TEXT,
                    last_sale_date::TEXT,
                    revenue_rank,
                    qty_rank
                FROM dw.mart_product_ranking
                ORDER BY {order_col} DESC
                LIMIT %s
            """
            params = [limit]

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/seasonality
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/seasonality")
def get_seasonality(
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna a sazonalidade mensal: faturamento médio por mês (Jan-Dez).

    Calcula a MÉDIA do faturamento mensal ao longo dos anos do período,
    não a soma — isso elimina o efeito de ter mais ou menos anos no filtro.

    Exemplo: se Jan 2020 teve R$100k e Jan 2021 teve R$120k,
             o retorno para Janeiro = R$110k (média).
    """
    conn = _get_db()
    try:
        where_parts = []
        params = []
        if year_from:
            where_parts.append("year >= %s")
            params.append(year_from)
        if year_to:
            where_parts.append("year <= %s")
            params.append(year_to)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        sql = f"""
            SELECT
                month,
                -- Nome do mês para exibição no eixo X
                to_char(make_date(2000, month::INT, 1), 'Mon') AS month_name,
                -- Média do faturamento mensal ao longo dos anos filtrados
                AVG(monthly_revenue)::FLOAT                    AS avg_revenue,
                -- Soma total do mês para referência
                SUM(monthly_revenue)::FLOAT                    AS total_revenue,
                -- Número de anos com dados neste mês
                COUNT(*)                                       AS years_count
            FROM (
                -- Sub-query: agrega por mês+ano antes de calcular a média
                SELECT
                    year,
                    month,
                    SUM(total_revenue) AS monthly_revenue
                FROM dw.mart_revenue_daily
                {where}
                GROUP BY year, month
            ) monthly_agg
            GROUP BY month
            ORDER BY month
        """

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/yoy
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/yoy")
def get_yoy(
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna comparativo ano a ano (Year-over-Year):
      - Faturamento total por ano
      - Variação percentual vs ano anterior
      - Total de pedidos por ano
      - Ticket médio anual
    """
    conn = _get_db()
    try:
        where_parts = []
        params = []
        if year_from:
            where_parts.append("year >= %s")
            params.append(year_from)
        if year_to:
            where_parts.append("year <= %s")
            params.append(year_to)
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        sql = f"""
            SELECT
                year,
                SUM(total_revenue)::FLOAT   AS revenue,
                SUM(total_orders)           AS orders,
                -- Ticket médio anual
                CASE WHEN SUM(total_orders) > 0
                     THEN (SUM(total_revenue) / SUM(total_orders))::NUMERIC(10,2)::FLOAT
                END                         AS avg_ticket,
                -- Variação YoY via LAG (compara com o ano imediatamente anterior)
                -- LAG só olha para o ano anterior na janela ordenada, não o período inteiro
                CASE
                    WHEN LAG(SUM(total_revenue)) OVER (ORDER BY year) IS NOT NULL
                     AND LAG(SUM(total_revenue)) OVER (ORDER BY year) > 0
                    THEN ROUND(
                        ((SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (ORDER BY year))
                         / LAG(SUM(total_revenue)) OVER (ORDER BY year)) * 100,
                    2)::FLOAT
                END                         AS yoy_pct
            FROM dw.mart_revenue_daily
            {where}
            GROUP BY year
            ORDER BY year
        """

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/geography
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/geography")
def get_geography():
    """
    Retorna faturamento, pedidos e clientes por estado (UF).
    Usa dw.mart_state_summary (full-refresh, sem filtro de período disponível).

    Inclui share percentual de receita para cada estado.
    """
    conn = _get_db()
    try:
        sql = """
            SELECT
                state,
                total_customers,
                total_orders,
                total_revenue::FLOAT,
                avg_ticket::FLOAT,
                -- Share percentual da receita deste estado no total
                ROUND(
                    (total_revenue / NULLIF(SUM(total_revenue) OVER (), 0)) * 100,
                2)::FLOAT AS revenue_share_pct
            FROM dw.mart_state_summary
            ORDER BY total_revenue DESC
        """
        return _rows(conn, sql)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/top-customers
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/top-customers")
def get_top_customers(
    limit: int = Query(15, ge=5, le=50),
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna os maiores clientes por faturamento, incluindo nome e telefone.

    Os dados de nome e telefone são obtidos via LEFT JOIN com stg.customers.
    O telefone prioriza o campo 'mobile' (celular) e cai no 'phone' se ausente.
    Ambos os campos podem estar NULL caso o ETL de clientes não os tenha extraído.
    """
    conn = _get_db()
    try:
        if year_from or year_to:
            # ── Período filtrado: agrega direto de cur.order_items ─────────
            where_parts = []
            params: list = []
            if year_from:
                # Filtra vendas cujo ano é >= year_from.
                where_parts.append("EXTRACT(year FROM oi.sale_date) >= %s")
                params.append(year_from)
            if year_to:
                # Filtra vendas cujo ano é <= year_to.
                where_parts.append("EXTRACT(year FROM oi.sale_date) <= %s")
                params.append(year_to)
            where = "WHERE " + " AND ".join(where_parts)
            # O LIMIT é o último parâmetro da query.
            params.append(limit)

            sql = f"""
                SELECT
                    -- Nome do cliente vindo de stg.customers; NULL se não encontrado.
                    MAX(sc.name)                                        AS customer_name,
                    -- Telefone: prefere celular (mobile), cai no fixo (phone) se ausente.
                    -- Retorna NULL se nenhum dos dois estiver preenchido no ETL.
                    MAX(COALESCE(sc.mobile, sc.phone))                  AS phone,
                    -- Métricas de venda no período selecionado
                    SUM(oi.total_value)::FLOAT                          AS total_revenue,
                    COUNT(DISTINCT oi.order_id_src)                     AS total_orders,
                    (SUM(oi.total_value) /
                     NULLIF(COUNT(DISTINCT oi.order_id_src), 0)
                    )::NUMERIC(10,2)::FLOAT                             AS avg_ticket,
                    MIN(oi.sale_date)::TEXT                             AS first_purchase,
                    MAX(oi.sale_date)::TEXT                             AS last_purchase
                FROM cur.order_items oi
                -- LEFT JOIN preserva clientes sem cadastro em stg.customers
                LEFT JOIN stg.customers sc
                       ON sc.customer_id_src = oi.customer_id
                      AND sc.source_system   = oi.source_system
                {where}
                GROUP BY oi.customer_id
                ORDER BY total_revenue DESC
                LIMIT %s
            """
        else:
            # ── Período completo: mart + JOIN para nome/telefone ───────────
            # dw.mart_customer_summary tem o customer_id mas não o nome nem o telefone.
            # Por isso fazemos JOIN com stg.customers para enriquecer os dados.
            sql = """
                SELECT
                    -- Nome do cliente de stg.customers; NULL se não cadastrado.
                    sc.name                                             AS customer_name,
                    -- Telefone com fallback celular → fixo.
                    COALESCE(sc.mobile, sc.phone)                      AS phone,
                    -- Métricas lifetime do mart pré-calculado
                    cs.total_revenue::FLOAT,
                    cs.total_orders,
                    cs.avg_ticket::FLOAT,
                    cs.first_purchase::TEXT,
                    cs.last_purchase::TEXT
                FROM dw.mart_customer_summary cs
                -- LEFT JOIN: mantém clientes do mart mesmo sem registro em stg.customers
                LEFT JOIN stg.customers sc
                       ON sc.customer_id_src::TEXT = cs.customer_id
                      -- source_system não está em mart_customer_summary;
                      -- o filtro abaixo garante que pegamos o registro do sistema correto.
                      AND sc.source_system = 'sqlserver_gp'
                ORDER BY cs.total_revenue DESC
                LIMIT %s
            """
            params = [limit]

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/customer-share
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/customer-share")
def get_customer_share(
    top: int = Query(9, ge=3, le=20, description="Clientes individuais exibidos; o restante vira 'Outros'"),
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
):
    """
    Retorna a participação percentual de cada cliente no faturamento total.

    Estrutura de retorno:
      - Os `top` maiores clientes individualmente, em ordem decrescente de receita.
      - Um item 'Outros' agregando todos os demais clientes.
      - Cada item traz: customer_name, revenue, share_pct, is_others.

    Fonte: cur.order_items + stg.customers (período filtrado) ou
           dw.mart_customer_summary + stg.customers (período completo).
    """
    conn = _get_db()
    try:
        if year_from or year_to:
            # ── Período filtrado: agrega diretamente de cur.order_items ──────
            where_parts = []
            params: list = []
            if year_from:
                # Inclui apenas vendas a partir de year_from.
                where_parts.append("EXTRACT(year FROM oi.sale_date) >= %s")
                params.append(year_from)
            if year_to:
                # Inclui apenas vendas até year_to.
                where_parts.append("EXTRACT(year FROM oi.sale_date) <= %s")
                params.append(year_to)
            where = "WHERE " + " AND ".join(where_parts)
            # 'top' é usado duas vezes: no WHERE do primeiro SELECT e no WHERE do Outros.
            params.extend([top, top])

            sql = f"""
                WITH base AS (
                    -- Agrega receita por cliente e calcula posição no ranking.
                    -- ROW_NUMBER é calculado após o GROUP BY (comportamento padrão PostgreSQL).
                    SELECT
                        COALESCE(MAX(sc.name), 'ID ' || oi.customer_id::TEXT) AS customer_name,
                        SUM(oi.total_value)                                    AS revenue,
                        ROW_NUMBER() OVER (ORDER BY SUM(oi.total_value) DESC)  AS rn
                    FROM cur.order_items oi
                    -- LEFT JOIN preserva clientes sem cadastro em stg.customers
                    LEFT JOIN stg.customers sc
                           ON sc.customer_id_src = oi.customer_id
                          AND sc.source_system   = oi.source_system
                    {where}
                    GROUP BY oi.customer_id
                ),
                -- Calcula o faturamento total do período para o cálculo de share.
                total AS (SELECT SUM(revenue) AS total_rev FROM base)

                -- Top N clientes individualmente
                SELECT
                    customer_name,
                    revenue::FLOAT                                             AS revenue,
                    ROUND((revenue / total.total_rev) * 100, 2)::FLOAT        AS share_pct,
                    false                                                      AS is_others
                FROM base, total
                WHERE rn <= %s

                UNION ALL

                -- Fatia 'Outros': soma de todos os clientes além do top N
                SELECT
                    'Outros'::TEXT                                             AS customer_name,
                    COALESCE(SUM(b.revenue), 0)::FLOAT                        AS revenue,
                    COALESCE(
                        ROUND((SUM(b.revenue) / MAX(total.total_rev)) * 100, 2)::FLOAT,
                        0
                    )                                                          AS share_pct,
                    true                                                       AS is_others
                FROM base b, total
                WHERE b.rn > %s
                -- Suprime a linha 'Outros' quando todos os clientes cabem no top N
                HAVING SUM(b.revenue) > 0

                ORDER BY is_others, revenue DESC
            """

        else:
            # ── Período completo: usa mart pré-calculado + JOIN para nome ─────
            params = [top, top]

            sql = """
                WITH base AS (
                    -- Lê o mart de clientes e enriquece com nome via stg.customers.
                    SELECT
                        COALESCE(sc.name, 'ID ' || cs.customer_id)       AS customer_name,
                        cs.total_revenue                                   AS revenue,
                        ROW_NUMBER() OVER (ORDER BY cs.total_revenue DESC) AS rn
                    FROM dw.mart_customer_summary cs
                    LEFT JOIN stg.customers sc
                           ON sc.customer_id_src::TEXT = cs.customer_id
                          AND sc.source_system = 'sqlserver_gp'
                ),
                total AS (SELECT SUM(revenue) AS total_rev FROM base)

                SELECT
                    customer_name,
                    revenue::FLOAT,
                    ROUND((revenue / total.total_rev) * 100, 2)::FLOAT AS share_pct,
                    false AS is_others
                FROM base, total
                WHERE rn <= %s

                UNION ALL

                SELECT
                    'Outros'::TEXT,
                    COALESCE(SUM(b.revenue), 0)::FLOAT,
                    COALESCE(
                        ROUND((SUM(b.revenue) / MAX(total.total_rev)) * 100, 2)::FLOAT,
                        0
                    ),
                    true
                FROM base b, total
                WHERE b.rn > %s
                HAVING SUM(b.revenue) > 0

                ORDER BY is_others, revenue DESC
            """

        return _rows(conn, sql, params)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/etl-status
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/etl-status")
def get_etl_status():
    """
    Retorna o histórico recente de execuções do ETL de marts.
    Útil para monitorar quando os dados foram atualizados pela última vez.
    """
    conn = _get_db()
    try:
        sql = """
            SELECT
                refresh_id,
                mart_name,
                status,
                rows_processed,
                watermark_from::TEXT,
                watermark_to::TEXT,
                error_message,
                started_at,
                finished_at,
                -- Duração formatada
                CASE
                    WHEN finished_at IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (finished_at - started_at))::INT
                END AS duration_seconds
            FROM dw.mart_refresh_log
            ORDER BY started_at DESC
            LIMIT 40
        """
        rows = _rows(conn, sql)
        # Converte timestamps para ISO string (não são JSON-serializáveis nativamente).
        for r in rows:
            if r.get("started_at"):
                r["started_at"]  = r["started_at"].isoformat()
            if r.get("finished_at"):
                r["finished_at"] = r["finished_at"].isoformat()
        return rows
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/business/customers/{customer_id}/purchase-history
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/business/customers/{customer_id}/purchase-history")
def get_purchase_history(customer_id: int):
    """
    Retorna o histórico completo de compras de um cliente, ordenado por data
    decrescente (compra mais recente primeiro).

    Retorna 404 se o customer_id não existir em cur.customers.
    """
    conn = _get_db()
    try:
        # Verifica existência e obtém nome do cliente (stg.customers, chave customer_id_src)
        customer_row = _rows(
            conn,
            """
            SELECT customer_id_src AS customer_id, name
            FROM stg.customers
            WHERE customer_id_src = %s
              AND source_system = 'sqlserver_gp'
            LIMIT 1
            """,
            (customer_id,),
        )
        if not customer_row:
            raise HTTPException(status_code=404, detail=f"Cliente {customer_id} não encontrado.")

        customer_name = customer_row[0]["name"]

        # Histórico completo de compras ordenado por data desc
        # cur.order_items não tem unit_price — calculado como total_value / quantity
        purchases = _rows(
            conn,
            """
            SELECT
                oi.sale_date::TEXT                                  AS sale_date,
                oi.order_id_src                                     AS order_id,
                p.description                                       AS product_name,
                oi.quantity::FLOAT                                  AS quantity,
                CASE
                    WHEN oi.quantity > 0
                    THEN ROUND((oi.total_value / oi.quantity)::numeric, 2)::FLOAT
                    ELSE NULL
                END                                                 AS unit_price,
                oi.total_value::FLOAT                               AS total_value
            FROM cur.order_items oi
            JOIN cur.products p
              ON p.product_id    = oi.product_id
             AND p.source_system = oi.source_system
            WHERE oi.customer_id = %s
            ORDER BY oi.sale_date DESC, oi.order_id_src
            """,
            (customer_id,),
        )

        return {
            "customer_id"     : customer_id,
            "customer_name"   : customer_name,
            "total_purchases" : len(purchases),
            "purchases"       : purchases,
        }
    finally:
        conn.close()
