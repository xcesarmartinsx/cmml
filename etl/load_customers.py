"""
etl/load_customers.py
=====================
Extrai o cadastro de clientes do ERP GP (dbo.ENTIDADE) e carrega em stg.customers.

POR QUE ESTE DATASET É NECESSÁRIO PARA O ML:
  • Cold-start (cliente novo sem histórico): fallback por cidade/estado/perfil
  • Segmentação regional: recomendações localizadas por cidade/UF
  • Filtro LGPD: ter o registro do cliente permite aplicar opt-out e direito de apagamento

ESTRATÉGIA DE CARGA: UPSERT controlado (pg_copy_upsert)
  • Atributos atualizáveis: name, city, state, document_type, active
  • Coluna protegida: first_seen_at (nunca sobrescrever — registra data de entrada no pipeline)
  • ON CONFLICT (customer_id_src, source_system) DO UPDATE SET ...

LGPD — ATENÇÃO:
  • CPF/CNPJ nunca armazenado em plain text
  • Armazenado apenas o hash SHA-256 para deduplicação (campo hash_document)
  • CPF/CNPJ nunca deve ser usado como feature de ML — somente customer_id interno

NOMES DE COLUNAS ERP (confirme antes de executar):
  Execute no SQL Server para verificar os nomes reais:
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'ENTIDADE' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION;

MODOS DE EXECUÇÃO:
  python etl/load_customers.py              # incremental (usa watermark)
  python etl/load_customers.py --full-load  # recarga completa (ignora watermark)
  python etl/load_customers.py --dry-run    # conta registros sem carregar
"""

# argparse: leitura dos argumentos de linha de comando (--full-load, --dry-run).
import argparse
# hashlib: geração do hash SHA-256 do CPF/CNPJ para conformidade LGPD.
import hashlib
# logging: módulo de log — o logger real é criado com setup_logging abaixo.
import logging
# os: acesso à variável de ambiente FETCH_CHUNK para tamanho do chunk.
import os
# sys: necessário para sys.path (importação do módulo etl.common) e sys.exit.
import sys
# datetime, timezone: geração do novo watermark (timestamp UTC atual).
from datetime import datetime, timezone
# Path: resolução do caminho absoluto do projeto para importação de módulos.
from pathlib import Path
# Any, Dict, Iterator, Optional: anotações de tipo para as funções deste módulo.
from typing import Any, Dict, Iterator, Optional

# Resolve a raiz do projeto (pasta pai de etl/) e adiciona ao sys.path.
# Necessário para que `from etl.common import ...` funcione quando o script
# é executado diretamente (python etl/load_customers.py).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Verifica se a raiz já está no path antes de inserir — evita duplicar.
if str(_PROJECT_ROOT) not in sys.path:
    # Insere no início do path para ter prioridade sobre outros módulos.
    sys.path.insert(0, str(_PROJECT_ROOT))

# Importa todas as funções do módulo de infraestrutura ETL.
from etl.common import (
    # close_batch: fecha o registro do batch com status e métricas finais.
    close_batch,
    # ensure_etl_control: cria schema etl e tabelas de controle se não existirem.
    ensure_etl_control,
    # get_mssql_conn: abre conexão com o SQL Server ERP via ODBC.
    get_mssql_conn,
    # get_pg_conn: abre conexão com o PostgreSQL destino.
    get_pg_conn,
    # get_watermark: lê o último timestamp carregado para carga incremental.
    get_watermark,
    # mssql_fetch_iter: extrai dados do SQL Server em chunks (iterador lazy).
    mssql_fetch_iter,
    # open_batch: registra o início do batch em etl.load_batches.
    open_batch,
    # pg_copy_upsert: carga bulk com UPSERT controlado (dimensões).
    pg_copy_upsert,
    # set_watermark: atualiza o watermark após carga bem-sucedida.
    set_watermark,
    # setup_logging: configura logger com formatter estruturado.
    setup_logging,
)

# Cria o logger específico para este script — separado do logger raiz 'etl'.
LOG = setup_logging("etl.customers")

# Nome lógico do dataset — usado como chave no watermark e no registro de batch.
DATASET    = "stg.customers"
# Tamanho do chunk de extração: lê da env var ou usa 5.000 como padrão.
CHUNK_SIZE = int(os.getenv("FETCH_CHUNK", "5000"))

# Lista de colunas que serão carregadas em stg.customers.
# batch_id e extracted_at são injetados automaticamente por _copy_chunk_to_tmp.
# first_seen_at e loaded_at são preenchidos pelas DEFAULTs da tabela destino.
COLUMNS = [
    # ID original do cliente no ERP — parte da chave primária composta.
    "customer_id_src",
    # Identificador do sistema de origem — parte da chave primária composta.
    "source_system",
    # Nome do cliente — atualizado no UPSERT se mudar no ERP.
    "name",
    # Cidade do cliente — base de segmentação regional.
    "city",
    # Estado (UF) do cliente.
    "state",
    # Tipo de documento derivado: 'PF' (CPF ≤ 11 dígitos) ou 'PJ' (> 11 dígitos).
    "document_type",
    # Hash SHA-256 do CPF/CNPJ limpo — nunca o documento em plain text (LGPD).
    "hash_document",
    # Cliente ativo ou inativo no ERP.
    "active",
    # Telefone fixo principal (E.FONE1 do ERP GP).
    "phone",
    # Telefone SMS/celular — preferencial para WhatsApp (E.FONESMS do ERP GP).
    "mobile",
    # ID do batch de carga — rastreabilidade de linhagem (injetado pelo common.py).
    "batch_id",
    # Timestamp da extração — quando os dados foram lidos da fonte (injetado).
    "extracted_at",
]

# Colunas que nunca devem ser sobrescritas no UPSERT.
# first_seen_at: data em que o cliente foi visto pela primeira vez no pipeline.
PROTECTED_COLS = ["first_seen_at"]

# ---------------------------------------------------------------------------
# ATENÇÃO — CONFIRME OS NOMES REAIS DAS COLUNAS ANTES DE EXECUTAR:
#
#   SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
#   WHERE TABLE_NAME = 'ENTIDADE' AND TABLE_SCHEMA = 'dbo'
#   ORDER BY ORDINAL_POSITION;
#
# As colunas abaixo são estimativas baseadas no padrão do ERP GP.
# Substitua ENTIDADEID, NOME, CIDADE, UF, CPFCNPJ, ATIVO, TIPOCLIENTE,
# DATAALTERACAO pelos nomes reais se necessário.
# ---------------------------------------------------------------------------

# Query de carga completa (full load) — sem filtro de data.
# Usada na primeira execução ou quando --full-load é passado.
SQL_FULL = """
SELECT
    CAST(E.ENTIDADEID       AS BIGINT)       AS customer_id_src,
    'sqlserver_gp'                           AS source_system,
    CAST(E.DESCRICAO        AS VARCHAR(500)) AS name,
    CAST(E.CIDADE           AS VARCHAR(200)) AS city,
    CAST(E.UF               AS CHAR(2))      AS state,
    CASE
        WHEN LEN(REPLACE(REPLACE(E.CNPJ_CPF,'.',''),'-','')) <= 11 THEN 'PF'
        WHEN LEN(REPLACE(REPLACE(E.CNPJ_CPF,'.',''),'-','')) > 11  THEN 'PJ'
        ELSE NULL
    END                                      AS document_type,
    E.CNPJ_CPF                               AS raw_document,
    CASE WHEN E.ATIVO = 'S' THEN 1 ELSE 0 END AS active,
    CAST(NULLIF(LTRIM(RTRIM(E.FONE1)),   '') AS VARCHAR(30)) AS phone,
    CAST(NULLIF(LTRIM(RTRIM(E.FONESMS)), '') AS VARCHAR(30)) AS mobile
FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '1'
"""

# Query incremental — filtra por DATA_ALTERACAO (coluna confirmada em ENTIDADES).
SQL_INCREMENTAL_TEMPLATE = """
SELECT
    CAST(E.ENTIDADEID       AS BIGINT)       AS customer_id_src,
    'sqlserver_gp'                           AS source_system,
    CAST(E.DESCRICAO        AS VARCHAR(500)) AS name,
    CAST(E.CIDADE           AS VARCHAR(200)) AS city,
    CAST(E.UF               AS CHAR(2))      AS state,
    CASE
        WHEN LEN(REPLACE(REPLACE(E.CNPJ_CPF,'.',''),'-','')) <= 11 THEN 'PF'
        WHEN LEN(REPLACE(REPLACE(E.CNPJ_CPF,'.',''),'-','')) > 11  THEN 'PJ'
        ELSE NULL
    END                                      AS document_type,
    E.CNPJ_CPF                               AS raw_document,
    CASE WHEN E.ATIVO = 'S' THEN 1 ELSE 0 END AS active,
    CAST(NULLIF(LTRIM(RTRIM(E.FONE1)),   '') AS VARCHAR(30)) AS phone,
    CAST(NULLIF(LTRIM(RTRIM(E.FONESMS)), '') AS VARCHAR(30)) AS mobile
FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '1'
  AND E.DATA_ALTERACAO >= '{from_ts}'
"""

# Query de contagem para --dry-run — sem subquery com ORDER BY.
SQL_COUNT_TEMPLATE = """
SELECT COUNT(*) FROM dbo.ENTIDADES E
WHERE E.ENTIDADEID IS NOT NULL
  AND E.TIPO = '1'
  {date_filter}
"""


def _hash_document(raw: Optional[str]) -> Optional[str]:
    """
    Transforma CPF/CNPJ em hash SHA-256 hexadecimal.

    LGPD: o documento original nunca é armazenado.
    O hash serve apenas para deduplicação sem expor o dado pessoal.
    """
    # Se o valor for None ou string vazia, não há documento para hashear.
    if not raw:
        return None
    # Remove formatação do CPF/CNPJ antes de hashear — garante consistência do hash.
    clean = raw.replace(".", "").replace("-", "").replace("/", "").strip()
    # Se após limpeza não restou nada, retorna None.
    if not clean:
        return None
    # Gera o hash SHA-256 do documento limpo em formato hexadecimal.
    return hashlib.sha256(clean.encode()).hexdigest()


def _transform_customers(rows: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """
    Aplica transformações LGPD em cada linha antes da carga no PostgreSQL.

    Transformações aplicadas:
    - Remove o campo raw_document da linha (CPF/CNPJ — nunca chega ao destino)
    - Adiciona hash_document (SHA-256) no lugar do raw_document
    """
    # Itera sobre cada linha do iterador de entrada.
    for row in rows:
        # Remove raw_document da linha — garantia de que o CPF/CNPJ não irá para o destino.
        raw_doc = row.pop("raw_document", None)
        # Substitui pelo hash SHA-256 do documento limpo.
        row["hash_document"] = _hash_document(raw_doc)
        # Produz a linha transformada para o próximo estágio do pipeline.
        yield row


def build_sql(last_ts: Optional[datetime], full_load: bool) -> tuple[str, str, bool]:
    """
    Monta a query de extração e a query de contagem (dry-run) com base no watermark.

    Retorna: (sql_extract, sql_count, is_incremental)
    """
    # Se full_load solicitado ou sem watermark, usa a query completa sem filtro de data.
    if full_load or last_ts is None:
        # Retorna a query full, a contagem sem filtro de data, e flag incremental=False.
        return SQL_FULL, SQL_COUNT_TEMPLATE.format(date_filter=""), False
    # Formata o watermark como string YYYY-MM-DD HH:MM:SS para o filtro SQL.
    from_ts = last_ts.strftime("%Y-%m-%d %H:%M:%S")
    # Monta a query incremental com o watermark formatado.
    sql_extract = SQL_INCREMENTAL_TEMPLATE.format(from_ts=from_ts)
    # Monta a query de contagem com o mesmo filtro de data.
    sql_count = SQL_COUNT_TEMPLATE.format(
        date_filter=f"AND E.DATAALTERACAO >= '{from_ts}'"
    )
    # Retorna a query incremental, a contagem e flag incremental=True.
    return sql_extract, sql_count, True


def validate_post_load(pg, batch_id: int) -> bool:
    """
    Executa validações de qualidade de dados após a carga em stg.customers.

    Verificações:
    - Total de linhas inseridas neste batch
    - Clientes sem cidade (impacta recomendações regionais)
    - Ausência de raw_document na tabela destino (segurança LGPD)
    - Distribuição por tipo de documento (PF/PJ)
    - Total acumulado em stg.customers

    Retorna True se OK, False se encontrou problema crítico.
    """
    # Flag de resultado: começa como True e é setado False se houver problema crítico.
    ok = True
    # Abre cursor para executar as queries de validação.
    with pg.cursor() as cur:
        # ── Validação 1: linhas inseridas neste batch ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.customers WHERE batch_id = %s",
            (batch_id,),
        )
        # Lê a contagem de linhas deste batch específico.
        n = cur.fetchone()[0]
        # Loga o total inserido neste batch.
        LOG.info(f"[batch={batch_id}] Validação ▸ linhas processadas neste batch: {n:,}")

        # ── Validação 2: clientes sem cidade (aviso — não crítico) ──
        cur.execute(
            "SELECT COUNT(*) FROM stg.customers WHERE batch_id = %s AND city IS NULL",
            (batch_id,),
        )
        # Conta clientes sem cidade neste batch.
        sem_cidade = cur.fetchone()[0]
        # Emite aviso se houver clientes sem cidade — impacta cold-start regional.
        if sem_cidade > 0:
            LOG.warning(f"[batch={batch_id}] AVISO ▸ {sem_cidade:,} clientes sem cidade")

        # ── Validação 3: verificação de segurança LGPD ──
        # Garante que a coluna raw_document não existe em stg.customers.
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = 'stg' AND table_name = 'customers' "
            "AND column_name = 'raw_document'"
        )
        # Se a coluna existe, o CPF/CNPJ está sendo exposto — problema crítico LGPD.
        if cur.fetchone()[0] > 0:
            LOG.error(
                "RISCO DE SEGURANÇA LGPD: coluna 'raw_document' existe em stg.customers! "
                "CPF/CNPJ não pode ser armazenado em plain text. Remova imediatamente."
            )
            # Marca validação como falha crítica.
            ok = False

        # ── Validação 4: distribuição por tipo de documento ──
        cur.execute(
            "SELECT document_type, COUNT(*) FROM stg.customers GROUP BY document_type"
        )
        # Loga a contagem por tipo de documento (PF, PJ, NULL).
        for doc_type, count in cur.fetchall():
            LOG.info(f"[batch={batch_id}] Validação ▸ document_type={doc_type}: {count:,}")

        # ── Validação 5: total acumulado (saúde geral da tabela) ──
        cur.execute("SELECT COUNT(*) FROM stg.customers")
        # Lê o total de clientes no staging.
        total = cur.fetchone()[0]
        # Loga o total acumulado.
        LOG.info(f"[batch={batch_id}] Validação ▸ stg.customers total: {total:,}")

    # Retorna o resultado das validações.
    return ok


def main(full_load: bool = False, dry_run: bool = False) -> None:
    """
    Ponto de entrada do pipeline de carga de clientes.

    Fluxo:
    1. Abre conexões PG e MSSQL
    2. Garante tabelas de controle ETL
    3. Lê watermark anterior
    4. Monta SQL (full ou incremental)
    5. Em dry_run: apenas conta e sai
    6. Abre batch, extrai, transforma (LGPD), carrega via UPSERT
    7. Valida pós-carga
    8. Avança watermark
    9. Fecha batch com sucesso
    Em qualquer falha: rollback + fecha batch como 'failed' + sys.exit(1)
    """
    # Imprime separador e cabeçalho do run para facilitar leitura dos logs.
    LOG.info("=" * 70)
    # Loga os parâmetros do run atual.
    LOG.info(f"ETL CUSTOMERS | full_load={full_load} | dry_run={dry_run}")
    # Fecha o cabeçalho com separador.
    LOG.info("=" * 70)

    # Abre a conexão com o PostgreSQL destino.
    pg = get_pg_conn()
    # Abre a conexão com o SQL Server ERP fonte.
    ms = get_mssql_conn()

    # Variáveis de controle para o bloco except/finally.
    # batch_id inicia como None — se open_batch falhar, não tentará close_batch.
    batch_id  = None
    # extracted inicia como 0 para métricas mesmo em caso de falha precoce.
    extracted = 0
    # upserted inicia como 0 para métricas.
    upserted  = 0
    # error_msg armazenará a mensagem de erro se ocorrer exceção.
    error_msg = None

    try:
        # ── Passo 1: garantir tabelas de controle ETL ──
        # Cria etl.load_control e etl.load_batches se não existirem (idempotente).
        ensure_etl_control(pg)

        # ── Passo 2: ler watermark anterior ──
        # Retorna (last_ts, last_id) — para clientes usamos somente last_ts.
        last_ts, _ = get_watermark(pg, DATASET)
        # Loga o watermark atual para confirmar o ponto de partida incremental.
        LOG.info(f"Watermark anterior: {last_ts}")

        # ── Passo 3: montar SQL de extração e contagem ──
        # Escolhe entre full load e incremental com base nos parâmetros e watermark.
        sql_extract, sql_count, is_incremental = build_sql(last_ts, full_load)
        # Loga o modo de carga escolhido.
        LOG.info(f"Modo: {'incremental' if is_incremental else 'full'} | watermark: {last_ts}")

        # ── Passo 4 (opcional): dry-run — contar sem carregar ──
        if dry_run:
            # Loga a query que seria executada.
            LOG.info("[DRY RUN] SQL de extração:\n" + sql_extract)
            # Abre cursor no SQL Server para executar a query de contagem.
            with ms.cursor() as cur:
                # Executa a query de contagem dedicada (não subquery com ORDER BY).
                cur.execute(sql_count)
                # Lê a contagem retornada.
                LOG.info(f"[DRY RUN] Registros que seriam extraídos: {cur.fetchone()[0]:,}")
            # Encerra o script sem carregar dados.
            return

        # ── Passo 5: abrir batch de rastreamento ──
        # Registra o início da carga em etl.load_batches com status 'running'.
        batch_id = open_batch(pg, DATASET, last_ts, overlap_days=0)

        # ── Passo 6: pipeline de transformação — extrai → mascara LGPD → carrega ──
        # Iterador lazy: lê do SQL Server em chunks de CHUNK_SIZE linhas.
        raw_iter         = mssql_fetch_iter(ms, sql_extract, arraysize=CHUNK_SIZE)
        # Iterador de transformação: remove raw_document e adiciona hash_document.
        transformed_iter = _transform_customers(raw_iter)
        # Carrega via UPSERT: insere novos clientes, atualiza existentes.
        # first_seen_at nunca é atualizado (está em PROTECTED_COLS).
        extracted, upserted, _ = pg_copy_upsert(
            pg, DATASET, COLUMNS, transformed_iter,
            batch_id,
            protected_cols=PROTECTED_COLS,
            chunk_size=CHUNK_SIZE,
        )

        # ── Passo 7: validações pós-carga ──
        # Verifica qualidade dos dados inseridos e emite avisos se necessário.
        validate_post_load(pg, batch_id)

        # ── Passo 8: avançar watermark ──
        # Usa o timestamp atual como novo watermark — próxima carga incremental
        # buscará clientes alterados a partir deste momento.
        new_wm = datetime.now(tz=timezone.utc)
        # Persiste o novo watermark em etl.load_control.
        set_watermark(pg, DATASET, new_wm, None)

        # ── Passo 9: fechar batch com sucesso ──
        # Atualiza etl.load_batches com status 'success', métricas e novo watermark.
        close_batch(pg, batch_id, "success", new_wm, extracted, upserted, 0)

    except Exception as exc:
        # Captura qualquer exceção e registra a mensagem de erro.
        error_msg = str(exc)
        # Loga o erro com stacktrace completo para diagnóstico.
        LOG.error(f"ERRO FATAL no ETL customers: {exc}", exc_info=True)
        # Tenta fazer rollback para desfazer qualquer transação parcial.
        try:
            pg.rollback()
        except Exception:
            # Ignora erros de rollback — conexão pode estar em estado inválido.
            pass
        # Se o batch foi aberto, fecha-o como 'failed' com a mensagem de erro.
        if batch_id is not None:
            try:
                close_batch(pg, batch_id, "failed", None, extracted, upserted, 0, error_msg)
            except Exception:
                # Ignora falha ao fechar o batch — conexão pode estar comprometida.
                pass
        # Encerra o processo com código de saída 1 (indica falha para scripts externos).
        sys.exit(1)

    finally:
        # Fecha as conexões de banco de dados independentemente de sucesso ou falha.
        for conn in (pg, ms):
            try:
                conn.close()
            except Exception:
                # Ignora erros de fechamento de conexão — já fomos tão longe quanto podíamos.
                pass


if __name__ == "__main__":
    # Configura o parser de argumentos de linha de comando.
    parser = argparse.ArgumentParser(
        description="ETL: dbo.ENTIDADE (ERP GP) → stg.customers (PostgreSQL)"
    )
    # Argumento --full-load: ignora o watermark e recarrega todos os clientes.
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignora watermark e recarrega TODOS os clientes do ERP",
    )
    # Argumento --dry-run: conta registros sem efetuar carga.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra SQL e contagem de registros sem efetuar a carga",
    )
    # Faz o parse dos argumentos passados na linha de comando.
    args = parser.parse_args()
    # Chama a função principal com os argumentos parseados.
    main(full_load=args.full_load, dry_run=args.dry_run)
