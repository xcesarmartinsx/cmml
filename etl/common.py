"""
etl/common.py
=============
Infraestrutura compartilhada do pipeline ETL. Todos os scripts de carga importam
daqui. Centraliza:

1. LINHAGEM COMPLETA
   - Cada batch de carga gera um registro em etl.load_batches com métricas.
   - Cada linha no staging carrega batch_id rastreável (FK para etl.load_batches).
   - Auditável: "quem carregou este registro, quando, de onde e com qual resultado".

2. IMUTABILIDADE DE FATOS (stg.sales)
   - pg_copy_append → INSERT ON CONFLICT DO NOTHING
   - Registros existentes JAMAIS são sobrescritos.
   - Reprocessar o mesmo intervalo é seguro (idempotente).

3. LINHAGEM TEMPORAL CORRETA (watermark + overlap)
   - Watermark armazena o MAX(sale_date) carregado com sucesso.
   - Janela de overlap (padrão: 3 dias) captura late-arriving data.
   - Watermark só avança em caso de SUCCESS.

4. STREAMING REAL (baixo uso de memória)
   - Extração via ODBC em blocos de FETCH_CHUNK linhas (default 5.000).
   - COPY em chunks para tabela temporária — não bufferiza tudo em RAM.
   - Commit por chunk — progresso gravado mesmo em cargas longas.

5. UPSERT CONTROLADO (dimensões: clientes, produtos, lojas)
   - pg_copy_upsert → ON CONFLICT DO UPDATE apenas em colunas não-protegidas.
   - Colunas protegidas (ex.: first_seen_at) nunca são sobrescritas.
"""

# io: fornece StringIO para montar o buffer TSV em memória sem I/O de disco.
import io
# logging: módulo padrão da stdlib — configurado com formatter estruturado abaixo.
import logging
# os: acesso a variáveis de ambiente via os.getenv — evita credenciais hardcoded.
import os
# socket: obter o hostname da máquina para gravar em etl.load_batches.host_name.
import socket
# datetime, timezone: manipulação de timestamps com fuso UTC explícito.
from datetime import datetime, timezone
# Any, Dict, Iterable, Iterator, List, Optional, Tuple: anotações de tipo para clareza.
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

# psycopg2: driver nativo PostgreSQL para Python — mais rápido que SQLAlchemy para bulk ops.
import psycopg2
# psycopg2.extensions: expõe o tipo `connection` usado nas anotações de parâmetros.
import psycopg2.extensions
# pyodbc: driver ODBC para SQL Server — único modo confiável de conectar no Linux.
import pyodbc
# load_dotenv: lê o arquivo .env na raiz do projeto e injeta os valores em os.environ.
from dotenv import load_dotenv

# Carrega variáveis do .env imediatamente quando o módulo é importado.
# Assim qualquer os.getenv() abaixo já encontra as variáveis preenchidas.
load_dotenv()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(
    name: str = "etl",
    level: str = "INFO",
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Configura e retorna um logger com formatter estruturado.

    Idempotente: se o logger já tiver handlers (ex.: chamado duas vezes),
    retorna o existente sem duplicar handlers.

    Parâmetros
    ──────────
    name     : nome do logger (ex.: 'etl', 'etl.sales')
    level    : nível mínimo de log ('DEBUG', 'INFO', 'WARNING', 'ERROR')
    log_file : caminho para arquivo de log adicional (além do console).
               Se None, loga somente no console.
               Pode ser passado via variável de ambiente ETL_LOG_FILE.
    """
    # Obtém ou cria o logger com o nome fornecido.
    log = logging.getLogger(name)
    # Se o logger já foi configurado (tem handlers), retorna sem alterar.
    if log.handlers:
        return log
    # Define o nível mínimo — mensagens abaixo desse nível são silenciadas.
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Define o formato: timestamp | nome | nível | mensagem.
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Cria handler de console (stderr) — sempre ativo.
    console_handler = logging.StreamHandler()
    # Aplica o formatter ao handler de console.
    console_handler.setFormatter(fmt)
    # Registra o handler de console no logger.
    log.addHandler(console_handler)
    # Desativa propagação para o logger pai — evita dupla emissão quando tanto
    # 'etl' (common.py) quanto 'etl.sales' (script filho) têm seus próprios handlers.
    log.propagate = False
    # Se um caminho de arquivo foi fornecido, adiciona handler de arquivo também.
    effective_log_file = log_file or os.getenv("ETL_LOG_FILE")
    # Só cria o handler de arquivo se o caminho foi especificado.
    if effective_log_file:
        # Cria o diretório do arquivo de log se não existir (evita FileNotFoundError).
        os.makedirs(os.path.dirname(os.path.abspath(effective_log_file)), exist_ok=True)
        # Cria handler de arquivo em modo append — não sobrescreve logs anteriores.
        file_handler = logging.FileHandler(effective_log_file, encoding="utf-8")
        # Aplica o mesmo formatter ao handler de arquivo.
        file_handler.setFormatter(fmt)
        # Registra o handler de arquivo no logger.
        log.addHandler(file_handler)
    # Retorna o logger configurado.
    return log


# Logger raiz do módulo etl — usado internamente por common.py.
# Scripts filhos devem criar seu próprio logger com setup_logging("etl.nome_do_script").
LOG = setup_logging("etl")


# ---------------------------------------------------------------------------
# Conexões
# ---------------------------------------------------------------------------

def get_pg_conn() -> psycopg2.extensions.connection:
    """
    Abre e retorna uma conexão com o PostgreSQL a partir das variáveis do .env.

    autocommit=False — controle explícito de transação. Cada script faz commit
    ou rollback explicitamente, garantindo atomicidade por chunk.

    Variáveis esperadas no .env:
      PG_HOST         (padrão: 127.0.0.1)
      PG_PORT         (padrão: 5432)
      PG_DB           (obrigatório — ex.: reco)
      PG_APP_USER     (preferido — ex.: cmml_app) ou PG_USER (fallback legado)
      PG_APP_PASSWORD (preferido) ou PG_PASSWORD (fallback legado)
    """
    # Lê o host do PostgreSQL — padrão localhost para ambiente local/Docker.
    host = os.getenv("PG_HOST", "127.0.0.1")
    # Lê a porta — padrão 5432 (porta padrão do PostgreSQL).
    port = int(os.getenv("PG_PORT", "5432"))
    # Lê o nome do banco de dados — obrigatório (deve ser 'reco' conforme docker-compose).
    db   = os.getenv("PG_DB")
    # Prefere PG_APP_USER (menor privilégio), fallback para PG_USER (legado).
    user = os.getenv("PG_APP_USER") or os.getenv("PG_USER")
    # Prefere PG_APP_PASSWORD, fallback para PG_PASSWORD (legado).
    pwd  = os.getenv("PG_APP_PASSWORD") or os.getenv("PG_PASSWORD")
    # Verifica quais variáveis obrigatórias estão ausentes no .env.
    missing = [k for k, v in {"PG_DB": db, "PG_USER": user, "PG_PASSWORD": pwd}.items() if not v]
    # Falha imediatamente com mensagem clara se alguma variável obrigatória não está definida.
    if missing:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(missing)}")
    # Abre a conexão com os parâmetros lidos do .env.
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)
    # Desativa autocommit — exige commit/rollback explícito para controle transacional.
    conn.autocommit = False
    # Retorna a conexão pronta para uso.
    return conn


def get_mssql_conn() -> pyodbc.Connection:
    """
    Abre e retorna uma conexão com o SQL Server via ODBC 18.

    Pré-requisito: ODBC Driver 18 for SQL Server instalado no sistema.
      Ubuntu/Debian: curl https://packages.microsoft.com/... | apt install msodbcsql18

    Variáveis esperadas no .env:
      MSSQL_HOST         (padrão: 127.0.0.1)
      MSSQL_PORT         (padrão: 1433)
      MSSQL_DB           (obrigatório — ex.: GP_CASADASREDES)
      MSSQL_ETL_USER     (preferido — ex.: cmml_etl) ou MSSQL_USER (fallback legado)
      MSSQL_ETL_PASSWORD (preferido) ou MSSQL_PASSWORD (fallback legado)
    """
    # Lê o host do SQL Server — padrão localhost para ambiente Docker local.
    host = os.getenv("MSSQL_HOST", "127.0.0.1")
    # Lê a porta — padrão 1433 (porta padrão do SQL Server).
    port = os.getenv("MSSQL_PORT", "1433")
    # Lê o nome do banco de dados do ERP.
    db   = os.getenv("MSSQL_DB")
    # Prefere MSSQL_ETL_USER (menor privilégio), fallback para MSSQL_USER (legado/sa).
    user = os.getenv("MSSQL_ETL_USER") or os.getenv("MSSQL_USER")
    # Prefere MSSQL_ETL_PASSWORD, fallback para MSSQL_PASSWORD (legado).
    pwd  = os.getenv("MSSQL_ETL_PASSWORD") or os.getenv("MSSQL_PASSWORD")
    # Verifica variáveis obrigatórias ausentes.
    missing = [k for k, v in {"MSSQL_DB": db, "MSSQL_USER": user, "MSSQL_PASSWORD": pwd}.items() if not v]
    # Falha com mensagem clara se alguma variável obrigatória não está definida.
    if missing:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(missing)}")
    # Monta a string de conexão ODBC com todos os parâmetros necessários.
    dsn = (
        # Driver ODBC: versão 18 é obrigatória para SQL Server 2019/2022.
        "DRIVER={ODBC Driver 18 for SQL Server};"
        # Endereço e porta do servidor SQL Server.
        f"SERVER={host},{port};"
        # Nome do banco de dados ERP a conectar.
        f"DATABASE={db};"
        # Credenciais de autenticação SQL (não Windows).
        f"UID={user};PWD={pwd};"
        # Aceita certificado TLS auto-assinado do container SQL Server.
        "TrustServerCertificate=yes;"
        # Timeout de conexão em segundos — falha rápida se o host não responder.
        "Connection Timeout=30;"
    )
    # Abre a conexão com o SQL Server via ODBC.
    conn = pyodbc.connect(dsn)
    # timeout=0 desativa timeout de query — necessário para extrações longas (> 30s).
    conn.timeout = 0
    # Retorna a conexão pronta para uso.
    return conn


# ---------------------------------------------------------------------------
# Bootstrap das tabelas de controle ETL
# ---------------------------------------------------------------------------

# SQL idempotente que cria schema etl + tabelas de controle.
# Executado automaticamente por ensure_etl_control() no início de cada script.
# DDL formal também existe em sql/ddl/01_staging.sql — mantidos em sincronia.
_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.load_control (
    dataset_name   TEXT        PRIMARY KEY,
    last_ts        TIMESTAMPTZ NULL,
    last_id        BIGINT      NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl.load_batches (
    batch_id        BIGSERIAL    PRIMARY KEY,
    dataset_name    TEXT         NOT NULL,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT         NOT NULL DEFAULT 'running',
    watermark_from  TIMESTAMPTZ,
    watermark_to    TIMESTAMPTZ,
    overlap_days    INT          NOT NULL DEFAULT 0,
    rows_extracted  BIGINT       NOT NULL DEFAULT 0,
    rows_inserted   BIGINT       NOT NULL DEFAULT 0,
    rows_skipped    BIGINT       NOT NULL DEFAULT 0,
    error_message   TEXT,
    host_name       TEXT
);

CREATE INDEX IF NOT EXISTS idx_load_batches_dataset_started
    ON etl.load_batches (dataset_name, started_at DESC);
"""


def ensure_etl_control(pg: psycopg2.extensions.connection) -> None:
    """
    Cria o schema etl e as tabelas de controle se ainda não existirem.
    Idempotente — seguro chamar múltiplas vezes na mesma sessão.
    """
    # Abre cursor para executar o DDL de bootstrap.
    with pg.cursor() as cur:
        # Executa todo o bloco DDL de criação das tabelas de controle.
        cur.execute(_BOOTSTRAP_SQL)
    # Comita o DDL — CREATE TABLE IF NOT EXISTS é transacional no PostgreSQL.
    pg.commit()
    # Loga confirmação em nível DEBUG (não polui o log de INFO durante cargas normais).
    LOG.debug("Tabelas de controle ETL verificadas/criadas.")


# ---------------------------------------------------------------------------
# Batch — unidade rastreável de execução
# ---------------------------------------------------------------------------

def open_batch(
    pg: psycopg2.extensions.connection,
    dataset_name: str,
    watermark_from: Optional[datetime],
    overlap_days: int = 0,
) -> int:
    """
    Registra o início de uma execução em etl.load_batches com status 'running'.
    Retorna o batch_id gerado — deve ser usado em todas as linhas carregadas
    para manter rastreabilidade de linhagem.

    Parâmetros
    ──────────
    pg             : conexão PostgreSQL
    dataset_name   : nome do dataset (ex.: 'stg.sales')
    watermark_from : timestamp do início da janela extraída (None = full load)
    overlap_days   : quantos dias de overlap foram usados na extração
    """
    # Abre cursor para inserir o registro de início do batch.
    with pg.cursor() as cur:
        # Insere o batch com status 'running' e captura o batch_id gerado pela sequência.
        cur.execute(
            """
            INSERT INTO etl.load_batches
                (dataset_name, watermark_from, overlap_days, host_name)
            VALUES (%s, %s, %s, %s)
            RETURNING batch_id
            """,
            # Parâmetros: dataset, janela inicial, overlap e hostname da máquina executora.
            (dataset_name, watermark_from, overlap_days, socket.gethostname()),
        )
        # Lê o batch_id gerado automaticamente pela sequência BIGSERIAL.
        batch_id: int = cur.fetchone()[0]
    # Comita imediatamente — o batch fica visível mesmo se o script falhar depois.
    pg.commit()
    # Loga o início do batch com metadados para auditoria.
    LOG.info(f"[batch={batch_id}] ABERTO | dataset={dataset_name} | from={watermark_from}")
    # Retorna o batch_id para ser usado como referência em todas as linhas carregadas.
    return batch_id


def close_batch(
    pg: psycopg2.extensions.connection,
    batch_id: int,
    status: str,
    watermark_to: Optional[Any],
    rows_extracted: int,
    rows_inserted: int,
    rows_skipped: int,
    error: Optional[str] = None,
) -> None:
    """
    Finaliza o batch com status, métricas e novo watermark.

    Parâmetros
    ──────────
    status         : 'success' | 'failed'
    watermark_to   : novo watermark (MAX date/id carregado) — None em caso de falha
    rows_extracted : total lido da fonte
    rows_inserted  : total efetivamente gravado no destino
    rows_skipped   : total ignorado (ON CONFLICT DO NOTHING)
    error          : mensagem de erro com stacktrace (None em sucesso)
    """
    # Abre cursor para atualizar o registro do batch com os resultados finais.
    with pg.cursor() as cur:
        # Atualiza todos os campos de encerramento do batch em uma única operação.
        cur.execute(
            """
            UPDATE etl.load_batches SET
                finished_at    = now(),
                status         = %s,
                watermark_to   = %s,
                rows_extracted = %s,
                rows_inserted  = %s,
                rows_skipped   = %s,
                error_message  = %s
            WHERE batch_id = %s
            """,
            # Parâmetros na mesma ordem dos %s do SQL acima.
            (status, watermark_to, rows_extracted, rows_inserted,
             rows_skipped, error, batch_id),
        )
    # Comita o encerramento do batch.
    pg.commit()
    # Loga o resumo final do batch — linha mais importante para monitoramento.
    LOG.info(
        f"[batch={batch_id}] {status.upper()} | "
        f"extracted={rows_extracted:,} | inserted={rows_inserted:,} | "
        f"skipped={rows_skipped:,} | new_watermark={watermark_to}"
    )


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def get_watermark(
    pg: psycopg2.extensions.connection,
    dataset_name: str,
) -> Tuple[Optional[datetime], Optional[int]]:
    """
    Lê o watermark mais recente do dataset em etl.load_control.
    Retorna (None, None) se o dataset ainda não foi carregado (primeiro run).

    Retorno: (last_ts, last_id)
      last_ts : último timestamp carregado (usado em carga incremental por data)
      last_id : último ID carregado (alternativa para fontes sem coluna de data)
    """
    # Abre cursor de leitura.
    with pg.cursor() as cur:
        # Consulta o watermark do dataset específico.
        cur.execute(
            "SELECT last_ts, last_id FROM etl.load_control WHERE dataset_name = %s",
            # Parâmetro: nome do dataset a consultar.
            (dataset_name,),
        )
        # Lê a linha retornada — pode ser None se o dataset nunca foi carregado.
        row = cur.fetchone()
    # Retorna (last_ts, last_id) se encontrou, ou (None, None) para primeiro run.
    return (row[0], row[1]) if row else (None, None)


def set_watermark(
    pg: psycopg2.extensions.connection,
    dataset_name: str,
    last_ts: Optional[Any],
    last_id: Optional[int],
) -> None:
    """
    Grava ou atualiza o watermark em etl.load_control.

    IMPORTANTE: chamar SOMENTE após confirmação de sucesso da carga.
    Avançar o watermark em caso de falha causaria perda de dados na próxima execução.
    """
    # Abre cursor para gravar/atualizar o watermark.
    with pg.cursor() as cur:
        # UPSERT: insere se não existe, atualiza se já existe.
        cur.execute(
            """
            INSERT INTO etl.load_control (dataset_name, last_ts, last_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (dataset_name) DO UPDATE
                SET last_ts    = EXCLUDED.last_ts,
                    last_id    = EXCLUDED.last_id,
                    updated_at = now()
            """,
            # Parâmetros: nome do dataset, novo timestamp e novo ID.
            (dataset_name, last_ts, last_id),
        )
    # Comita o watermark atualizado.
    pg.commit()
    # Loga a atualização do watermark — essencial para rastrear progresso incremental.
    LOG.info(f"Watermark atualizado: dataset={dataset_name} | last_ts={last_ts}")


# ---------------------------------------------------------------------------
# Extração do SQL Server — streaming real (iterador por chunk)
# ---------------------------------------------------------------------------

def mssql_fetch_iter(
    conn: pyodbc.Connection,
    sql: str,
    params: tuple = (),
    arraysize: int = 5_000,
) -> Iterator[Dict[str, Any]]:
    """
    Executa query no SQL Server e retorna um ITERADOR lazy de dicts.

    Por que iterador e não lista?
    ─────────────────────────────
    Evita carregar milhões de linhas em memória de uma vez.
    O processamento ocorre chunk a chunk: extrai 5.000 → processa → extrai mais 5.000.
    Ideal para tabelas grandes como MOVIMENTO_DIA (pode ter milhões de registros).

    Parâmetros
    ──────────
    conn      : conexão SQL Server aberta por get_mssql_conn()
    sql       : query de extração (sem ORDER BY — não é necessário para UPSERT)
    params    : parâmetros da query (para queries parametrizadas)
    arraysize : linhas por fetch — ajuste conforme RAM disponível
    """
    # Abre cursor no SQL Server para executar a query de extração.
    cur = conn.cursor()
    # Define quantas linhas o driver busca por roundtrip de rede (afeta performance).
    cur.arraysize = arraysize
    # Loga início da extração em DEBUG para não poluir o log de INFO.
    LOG.debug(f"MSSQL → executando query (chunk={arraysize})")
    # Executa a query com os parâmetros fornecidos.
    cur.execute(sql, params)
    # Extrai os nomes das colunas da descrição do cursor para mapear por nome.
    cols = [d[0] for d in cur.description]
    # Contador de chunks para log de progresso.
    chunk_n = 0
    # Contador acumulado de linhas para log final.
    total = 0
    # Loop: continua até fetchmany retornar lista vazia (fim dos dados).
    while True:
        # Busca próximo bloco de linhas da fonte.
        rows = cur.fetchmany(arraysize)
        # Se não há mais linhas, encerra o loop.
        if not rows:
            break
        # Incrementa contador de chunks.
        chunk_n += 1
        # Acumula o total de linhas extraídas.
        total += len(rows)
        # Loga progresso do chunk em DEBUG.
        LOG.debug(f"MSSQL ← chunk {chunk_n}: {len(rows)} linhas (acumulado: {total:,})")
        # Itera sobre as linhas do chunk e produz um dict por linha (lazy).
        for r in rows:
            # zip(cols, r) mapeia nome da coluna → valor; dict() cria o dict da linha.
            yield dict(zip(cols, r))
    # Fecha o cursor após esgotar todos os dados.
    cur.close()
    # Loga o total final da extração — confirma que tudo foi lido.
    LOG.info(f"MSSQL → extração concluída: {total:,} linhas em {chunk_n} chunks")


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _pg_val(v: Any) -> str:
    """
    Converte um valor Python para string TSV segura para psycopg2.copy_from.

    Regras:
      None     → '\\N'  (representa NULL no formato COPY do PostgreSQL)
      bool     → 't' ou 'f'  (representação booleana do COPY)
      datetime → isoformat (ex.: '2025-01-15T10:30:00+00:00')
      outros   → str(v) com escape de chars especiais do TSV
    """
    # None deve virar o marcador de NULL do COPY — qualquer outro valor quebraria o parser.
    if v is None:
        return r"\N"
    # Booleanos devem usar 't'/'f' (não True/False) para o parser do COPY entender.
    if isinstance(v, bool):
        return "t" if v else "f"
    # datetime deve ser serializado em isoformat para que o PostgreSQL parse corretamente.
    if isinstance(v, datetime):
        return v.isoformat()
    # Converte qualquer outro tipo para string para concatenar no TSV.
    s = str(v)
    # Escapa backslash primeiro (deve ser o primeiro escape — senão escapa os próprios escapes).
    s = s.replace("\\", "\\\\")
    # Substitui tab por espaço — tab é o delimitador do TSV, não pode aparecer no valor.
    s = s.replace("\t", " ")
    # Substitui newline por espaço — quebraria a linha do COPY.
    s = s.replace("\n", " ")
    # Remove carriage return — pode vir de dados Windows e corromperia o TSV.
    s = s.replace("\r", "")
    # Retorna o valor sanitizado pronto para o TSV.
    return s


def _get_pk_cols(pg: psycopg2.extensions.connection, table: str) -> List[str]:
    """
    Descobre as colunas que compõem a PRIMARY KEY de uma tabela.
    Consulta pg_index (catálogo interno do PostgreSQL) — funciona para qualquer tabela.

    Lança RuntimeError se a tabela não tiver PK definida (necessária para UPSERT).
    """
    # Abre cursor para consultar o catálogo do PostgreSQL.
    with pg.cursor() as cur:
        # Consulta pg_index para encontrar o índice primário da tabela.
        cur.execute(
            """
            SELECT a.attname
            FROM   pg_index     i
            JOIN   pg_attribute a
                   ON a.attrelid = i.indrelid
                  AND a.attnum   = ANY(i.indkey)
            WHERE  i.indrelid  = %s::regclass
              AND  i.indisprimary
            ORDER  BY a.attnum
            """,
            # Cast para regclass resolve schema + nome da tabela automaticamente.
            (table,),
        )
        # Coleta os nomes das colunas da PK.
        pk = [r[0] for r in cur.fetchall()]
    # Se a tabela não tem PK, UPSERT não é possível — falha com mensagem clara.
    if not pk:
        raise RuntimeError(
            f"Tabela '{table}' não tem PRIMARY KEY. "
            "Defina uma PK antes de usar pg_copy_append / pg_copy_upsert."
        )
    # Retorna lista com os nomes das colunas da PK.
    return pk


def _copy_chunk_to_tmp(
    cur: psycopg2.extensions.cursor,
    tmp_table: str,
    cols: List[str],
    chunk: List[Dict[str, Any]],
    batch_id: int,
    extracted_at: str,
) -> None:
    """
    Serializa um chunk de linhas em TSV e envia via COPY para a tabela temporária.

    COPY é significativamente mais rápido que INSERT linha a linha:
    - Não tem parse overhead por linha
    - Usa o protocolo binário interno do PostgreSQL
    - Benchmarks: ~10x mais rápido que INSERT para volumes > 1.000 linhas

    batch_id e extracted_at são injetados em cada linha para rastreabilidade.
    """
    # Cria buffer em memória para montar o TSV — não grava em disco.
    buf = io.StringIO()
    # Itera sobre cada linha do chunk para serializar no buffer TSV.
    for row in chunk:
        # Injeta batch_id se ainda não estiver na linha (não sobrescreve se existir).
        row.setdefault("batch_id", batch_id)
        # Injeta extracted_at se ainda não estiver na linha.
        row.setdefault("extracted_at", extracted_at)
        # Serializa a linha: valores separados por tab, linha encerrada com \n.
        # _pg_val já escapa backslashes, tabs e newlines — não usamos csv.writer aqui
        # porque csv com QUOTE_NONE+escapechar="\\" re-escapa o \N (sentinel de NULL)
        # para \\N, que o PostgreSQL interpreta como string literal e não como NULL.
        buf.write("\t".join(_pg_val(row.get(c)) for c in cols) + "\n")
    # Retorna o cursor do buffer para o início antes de ler.
    buf.seek(0)
    # Envia o buffer TSV para a tabela temporária via COPY — operação bulk mais rápida.
    cur.copy_from(buf, tmp_table, sep="\t", null=r"\N", columns=cols)


# ---------------------------------------------------------------------------
# CARGA — Fato (INSERT ONLY, sem overwrite)
# ---------------------------------------------------------------------------

def pg_copy_append(
    pg: psycopg2.extensions.connection,
    stg_table: str,
    columns: List[str],
    rows: Iterable[Dict[str, Any]],
    batch_id: int,
    chunk_size: int = 5_000,
) -> Tuple[int, int, int]:
    """
    Carga segura para tabelas de FATO (stg.sales).

    Estratégia: COPY temp table → INSERT ON CONFLICT DO NOTHING
    ─────────────────────────────────────────────────────────────
    ✓ Registros existentes no destino NUNCA são alterados
    ✓ Idempotente: reprocessar o mesmo intervalo não cria duplicatas
    ✓ Rastreável: batch_id e extracted_at gravados em cada linha
    ✓ Streaming: commit por chunk, baixo uso de memória

    Retorna
    ───────
    (rows_extracted, rows_inserted, rows_skipped)
    rows_skipped = rows_extracted - rows_inserted (já existiam na tabela)
    """
    # Garante que a lista de colunas seja uma lista (não um iterável genérico).
    cols = list(columns)
    # Descobre as colunas da PK da tabela destino para usar no ON CONFLICT.
    pk_cols = _get_pk_cols(pg, stg_table)
    # Monta nome único da tabela temporária baseado no nome da tabela destino.
    # Substitui '.' e '-' por '_' para que o nome seja um identificador SQL válido.
    tmp = f"_etl_tmp_{stg_table.replace('.', '_').replace('-', '_')}"
    # Captura o timestamp da extração uma única vez para toda a sessão (consistência).
    extracted_at = datetime.now(tz=timezone.utc).isoformat()
    # Monta a lista de colunas para o INSERT e o SELECT da temp table.
    insert_cols  = ", ".join(cols)
    # Select da temp table usa as mesmas colunas do INSERT.
    select_cols  = ", ".join(cols)
    # Acumulador do total de linhas lidas da fonte.
    total_extracted = 0
    # Acumulador do total de linhas efetivamente inseridas no destino.
    total_inserted  = 0
    # Buffer do chunk atual — acumula linhas até atingir chunk_size.
    chunk: List[Dict] = []

    # Cria a tabela temporária com o mesmo layout da tabela destino.
    # LIKE ... INCLUDING DEFAULTS copia tipos, NOT NULL e defaults (mas não FK, não PK).
    # ON COMMIT PRESERVE ROWS: a tabela temporária persiste entre commits no mesmo lote.
    with pg.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE IF NOT EXISTS {tmp} "
            f"(LIKE {stg_table} INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS"
        )
    # Comita a criação da tabela temporária.
    pg.commit()

    def _flush() -> int:
        """Envia o chunk atual: COPY temp → INSERT DO NOTHING → retorna n inseridos."""
        # Se o chunk está vazio, nada a fazer.
        if not chunk:
            return 0
        # Abre cursor para a operação de COPY + INSERT.
        with pg.cursor() as cur:
            # Copia o chunk para a tabela temporária via COPY (bulk).
            _copy_chunk_to_tmp(cur, tmp, cols, chunk, batch_id, extracted_at)
            # INSERT de todos os registros da temp table para o destino.
            # ON CONFLICT DO NOTHING: linhas que já existem são silenciosamente ignoradas.
            # WITH ins AS (...) RETURNING 1 permite contar exatamente quantas foram inseridas.
            cur.execute(
                f"""
                WITH ins AS (
                    INSERT INTO {stg_table} ({insert_cols})
                    SELECT {select_cols} FROM {tmp}
                    ON CONFLICT DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*) FROM ins
                """
            )
            # Lê o contador de linhas efetivamente inseridas.
            n_inserted: int = cur.fetchone()[0]
            # Limpa a tabela temporária para o próximo chunk.
            cur.execute(f"TRUNCATE TABLE {tmp}")
        # Comita o chunk inserido — progresso salvo mesmo se o script for interrompido.
        pg.commit()
        # Retorna o número de linhas inseridas neste flush.
        return n_inserted

    # Itera sobre todas as linhas vindas do iterador da fonte.
    for row in rows:
        # Contabiliza cada linha lida da fonte.
        total_extracted += 1
        # Adiciona a linha ao buffer do chunk atual.
        chunk.append(row)
        # Quando o chunk atingir o tamanho máximo, envia para o PostgreSQL.
        if len(chunk) >= chunk_size:
            # Acumula o total de linhas inseridas.
            total_inserted += _flush()
            # Loga o progresso do flush em DEBUG.
            LOG.debug(
                f"[batch={batch_id}] Flush chunk | "
                f"extracted={total_extracted:,} | inserted={total_inserted:,}"
            )
            # Reinicia o buffer do chunk.
            chunk = []

    # Flush final: envia o último chunk (pode ser menor que chunk_size).
    total_inserted += _flush()
    # Calcula linhas puladas: lidas mas não inseridas (ON CONFLICT DO NOTHING).
    total_skipped = total_extracted - total_inserted
    # Retorna a tupla de métricas para close_batch.
    return total_extracted, total_inserted, total_skipped


# ---------------------------------------------------------------------------
# CARGA — Dimensão (UPSERT controlado, preserva colunas protegidas)
# ---------------------------------------------------------------------------

def pg_copy_upsert(
    pg: psycopg2.extensions.connection,
    stg_table: str,
    columns: List[str],
    rows: Iterable[Dict[str, Any]],
    batch_id: int,
    protected_cols: Optional[List[str]] = None,
    chunk_size: int = 5_000,
) -> Tuple[int, int, int]:
    """
    Carga para tabelas de DIMENSÃO (stg.customers, stg.products, stg.stores).

    Estratégia: COPY temp table → INSERT ON CONFLICT DO UPDATE
    ────────────────────────────────────────────────────────────
    ✓ Atualiza atributos não-chave quando o registro já existe
    ✓ Colunas em protected_cols NUNCA são sobrescritas
    ✓ Colunas da PK são automaticamente protegidas
    ✓ Rastreável via batch_id

    protected_cols: colunas que NÃO devem ser atualizadas em re-cargas
                   (ex.: first_seen_at — a data de entrada do cliente no pipeline)

    Retorna
    ───────
    (rows_extracted, rows_upserted, 0)
    rows_skipped=0 porque toda linha resulta em INSERT ou UPDATE.
    """
    # Garante que a lista de colunas seja uma lista modificável.
    cols = list(columns)
    # Descobre as colunas da PK — usadas tanto na cláusula ON CONFLICT quanto na proteção.
    pk_cols = _get_pk_cols(pg, stg_table)
    # Constrói o conjunto de colunas protegidas: PK + colunas explicitamente protegidas.
    protected = set(pk_cols) | set(protected_cols or [])
    # Filtra as colunas que serão atualizadas no UPDATE (remove protegidas e PK).
    update_cols = [c for c in cols if c not in protected]
    # Se não há colunas atualizáveis (todas protegidas), delega para append (DO NOTHING).
    if not update_cols:
        LOG.info(f"pg_copy_upsert: sem colunas atualizáveis em '{stg_table}' → usando DO NOTHING")
        # Delega para pg_copy_append que usa ON CONFLICT DO NOTHING.
        return pg_copy_append(pg, stg_table, columns, rows, batch_id, chunk_size)
    # Monta a lista de colunas do ON CONFLICT (colunas da PK separadas por vírgula).
    conflict_cols = ", ".join(pk_cols)
    # Monta a cláusula SET do UPDATE: col = EXCLUDED.col para cada coluna atualizável.
    set_clause    = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    # Monta a lista de colunas do INSERT.
    insert_cols   = ", ".join(cols)
    # Monta a lista de colunas do SELECT da temp table.
    select_cols   = ", ".join(cols)
    # Captura timestamp de extração para consistência entre os chunks do mesmo batch.
    extracted_at  = datetime.now(tz=timezone.utc).isoformat()
    # Monta nome único da tabela temporária.
    tmp = f"_etl_tmp_{stg_table.replace('.', '_').replace('-', '_')}"
    # Acumulador de linhas lidas da fonte.
    total_extracted = 0
    # Acumulador de linhas upserted (inseridas + atualizadas).
    total_upserted  = 0
    # Buffer do chunk atual.
    chunk: List[Dict] = []

    # Cria tabela temporária com o mesmo layout da tabela destino.
    with pg.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE IF NOT EXISTS {tmp} "
            f"(LIKE {stg_table} INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS"
        )
    # Comita a criação da tabela temporária.
    pg.commit()

    def _flush() -> int:
        """Envia o chunk: COPY temp → UPSERT destino → retorna n linhas processadas."""
        # Se o chunk está vazio, não há nada a enviar.
        if not chunk:
            return 0
        # Abre cursor para COPY + UPSERT.
        with pg.cursor() as cur:
            # Envia o chunk para a tabela temporária via COPY.
            _copy_chunk_to_tmp(cur, tmp, cols, chunk, batch_id, extracted_at)
            # UPSERT: insere se não existe, atualiza as colunas não-protegidas se existe.
            cur.execute(
                f"""
                WITH ups AS (
                    INSERT INTO {stg_table} ({insert_cols})
                    SELECT {select_cols} FROM {tmp}
                    ON CONFLICT ({conflict_cols})
                    DO UPDATE SET {set_clause}
                    RETURNING 1
                )
                SELECT COUNT(*) FROM ups
                """
            )
            # Lê o número de linhas processadas (inseridas + atualizadas).
            n: int = cur.fetchone()[0]
            # Limpa a tabela temporária para o próximo chunk.
            cur.execute(f"TRUNCATE TABLE {tmp}")
        # Comita o chunk — progresso persistido.
        pg.commit()
        # Retorna o número de linhas processadas neste flush.
        return n

    # Itera sobre todas as linhas do iterador da fonte.
    for row in rows:
        # Contabiliza a linha lida.
        total_extracted += 1
        # Adiciona ao buffer do chunk.
        chunk.append(row)
        # Ao atingir o tamanho máximo do chunk, envia para o PostgreSQL.
        if len(chunk) >= chunk_size:
            # Acumula o total de upserts.
            total_upserted += _flush()
            # Loga progresso em DEBUG.
            LOG.debug(
                f"[batch={batch_id}] Flush chunk | "
                f"extracted={total_extracted:,} | upserted={total_upserted:,}"
            )
            # Reinicia o buffer.
            chunk = []

    # Flush final do último chunk.
    total_upserted += _flush()
    # rows_skipped=0 porque todo UPSERT resulta em INSERT ou UPDATE (sem rejeições silenciosas).
    return total_extracted, total_upserted, 0
