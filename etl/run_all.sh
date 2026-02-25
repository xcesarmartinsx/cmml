#!/usr/bin/env bash
# =============================================================================
# etl/run_all.sh
# Executa o pipeline ETL completo na ordem correta, com logs e validações.
#
# USO:
#   ./etl/run_all.sh                    # incremental (padrão)
#   ./etl/run_all.sh --full-load        # recarga completa de todos os datasets
#   ./etl/run_all.sh --dry-run          # simula sem carregar dados
#
# O script deve ser executado a partir da RAIZ do projeto:
#   cd /home/gameserver/projects/cmml && ./etl/run_all.sh
#
# VARIÁVEIS DE AMBIENTE:
#   ETL_LOG_FILE : caminho do arquivo de log (padrão: logs/etl_YYYYMMDD_HHMMSS.log)
#   PYTHON       : interpretador Python a usar (padrão: detectado automaticamente)
# =============================================================================

# -e: encerra o script imediatamente se qualquer comando retornar código != 0
set -e
# -u: encerra se usar variável não definida (evita erros silenciosos de typo)
set -u
# -o pipefail: falha de qualquer parte de um pipe encerra o script
set -o pipefail

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

# Resolve o diretório raiz do projeto: pasta pai de etl/ onde este script está.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Raiz do projeto = pasta pai de etl/
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
# Garante que o script é executado sempre a partir da raiz do projeto.
cd "$PROJECT_ROOT"

# Detecta o interpretador Python: preferência para python3, fallback para python.
PYTHON="${PYTHON:-$(command -v python3 2>/dev/null || command -v python 2>/dev/null || echo "")}"
# Se nenhum Python for encontrado, aborta com mensagem clara.
if [[ -z "$PYTHON" ]]; then
    echo "[ERRO] Python não encontrado. Instale Python 3.8+ ou defina a variável PYTHON."
    exit 1
fi

# Cria o diretório de logs se não existir.
mkdir -p "$PROJECT_ROOT/logs"
# Define o caminho do arquivo de log com timestamp para identificação única do run.
LOG_FILE="${ETL_LOG_FILE:-$PROJECT_ROOT/logs/etl_$(date '+%Y%m%d_%H%M%S').log}"
# Exporta ETL_LOG_FILE para que os scripts Python também escrevam no mesmo arquivo.
export ETL_LOG_FILE="$LOG_FILE"

# Timestamp de início do pipeline (em segundos desde epoch).
START_TIME=$(date +%s)
# Data e hora de início legível para o cabeçalho do log.
START_DATETIME=$(date '+%Y-%m-%d %H:%M:%S')

# Lê o argumento passado ao script (--full-load, --dry-run, ou vazio).
EXTRA_ARGS="${*:-}"

# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

# log_msg: escreve mensagem no console e no arquivo de log simultaneamente.
log_msg() {
    # Formata a mensagem com timestamp e prefixo [ETL].
    local msg="$(date '+%Y-%m-%d %H:%M:%S') | run_all.sh           | INFO     | $*"
    # Exibe no console (stdout).
    echo "$msg"
    # Anexa ao arquivo de log.
    echo "$msg" >> "$LOG_FILE"
}

# log_err: escreve mensagem de erro no stderr e no arquivo de log.
log_err() {
    # Formata a mensagem de erro.
    local msg="$(date '+%Y-%m-%d %H:%M:%S') | run_all.sh           | ERROR    | $*"
    # Exibe no stderr.
    echo "$msg" >&2
    # Anexa ao arquivo de log.
    echo "$msg" >> "$LOG_FILE"
}

# run_etl: executa um script ETL Python e registra início/fim com tempo decorrido.
run_etl() {
    # Nome do script a executar (ex.: etl/load_stores.py).
    local script="$1"
    # Argumentos adicionais (--full-load, --dry-run, ou vazio).
    local args="${2:-}"
    # Registra o início da execução do script.
    log_msg "▶ INICIANDO: $script $args"
    # Marca o tempo de início em segundos.
    local t0=$(date +%s)
    # Executa o script Python com os argumentos e redireciona stderr para o log também.
    # 2>&1 captura tanto stdout quanto stderr no tee.
    "$PYTHON" "$script" $args 2>&1 | tee -a "$LOG_FILE"
    # Captura o código de saída do script Python (não do tee).
    local exit_code="${PIPESTATUS[0]}"
    # Calcula o tempo decorrido em segundos.
    local elapsed=$(( $(date +%s) - t0 ))
    # Se o script retornou código != 0, registra o erro e falha.
    if [[ "$exit_code" -ne 0 ]]; then
        log_err "✗ FALHOU: $script (código=$exit_code, tempo=${elapsed}s)"
        # Propaga o erro para encerrar o pipeline (set -e).
        return "$exit_code"
    fi
    # Registra a conclusão bem-sucedida com o tempo decorrido.
    log_msg "✓ CONCLUÍDO: $script (tempo=${elapsed}s)"
}

# ---------------------------------------------------------------------------
# Verificações pré-execução
# ---------------------------------------------------------------------------

# Verifica se o arquivo .env existe antes de tentar carregar.
if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
    # Aborta se .env não encontrado — sem credenciais o ETL falha de qualquer forma.
    log_err ".env não encontrado em $PROJECT_ROOT — crie-o a partir de .env.example"
    exit 1
fi

# Carrega as variáveis do .env no ambiente do shell atual.
# set -a exporta automaticamente todas as variáveis definidas abaixo.
set -a
# shellcheck source=../.env
# Fonte o .env para injetar PG_HOST, PG_USER, etc. no ambiente.
. "$PROJECT_ROOT/.env"
# Desativa o auto-export após o source do .env.
set +a

# Verifica se as variáveis obrigatórias do PostgreSQL estão definidas.
for var in PG_HOST PG_PORT PG_DB PG_USER PG_PASSWORD; do
    # Usa ${!var} para referenciar a variável pelo nome.
    if [[ -z "${!var:-}" ]]; then
        log_err "Variável obrigatória $var não está definida no .env"
        exit 1
    fi
done

# Verifica se as variáveis obrigatórias do SQL Server estão definidas.
for var in MSSQL_HOST MSSQL_PORT MSSQL_DB MSSQL_USER MSSQL_PASSWORD; do
    if [[ -z "${!var:-}" ]]; then
        log_err "Variável obrigatória $var não está definida no .env"
        exit 1
    fi
done

# Verifica se o virtual environment existe e o ativa se presente.
if [[ -f "$PROJECT_ROOT/../.venv/bin/activate" ]]; then
    # Ativa o venv do projeto — garante uso das dependências corretas.
    # shellcheck disable=SC1091
    . "$PROJECT_ROOT/../.venv/bin/activate"
    log_msg "Virtual environment ativado: $(which python3)"
fi

# ---------------------------------------------------------------------------
# Cabeçalho do run
# ---------------------------------------------------------------------------

log_msg "=============================================================="
log_msg "PIPELINE ETL — INÍCIO"
log_msg "Data/hora  : $START_DATETIME"
log_msg "Log        : $LOG_FILE"
log_msg "Python     : $PYTHON"
log_msg "Banco PG   : $PG_USER@$PG_HOST:$PG_PORT/$PG_DB"
log_msg "SQL Server : $MSSQL_USER@$MSSQL_HOST:$MSSQL_PORT/$MSSQL_DB"
log_msg "Argumentos : ${EXTRA_ARGS:-[nenhum — modo incremental]}"
log_msg "=============================================================="

# ---------------------------------------------------------------------------
# (C) EXECUÇÃO DOS LOADs NA ORDEM CORRETA
# ---------------------------------------------------------------------------
# Ordem importa: dimensões antes dos fatos para consistência lógica.
# (stg.* não tem FK para outras tabelas stg.*, mas é boa prática carregar
# dimensões primeiro para facilitar validações cruzadas nos logs.)
#
# Ordem:
#   1. stg.stores    — lojas (sem dependências)
#   2. stg.products  — produtos (sem dependências)
#   3. stg.customers — clientes (sem dependências)
#   4. stg.sales     — fato de compras (referencia as três acima)
# ---------------------------------------------------------------------------

# Executa a carga de lojas — deve rodar antes de stg.sales.
run_etl "etl/load_stores.py"    "$EXTRA_ARGS"
# Executa a carga de produtos — deve rodar antes de stg.sales.
run_etl "etl/load_products.py"  "$EXTRA_ARGS"
# Executa a carga de clientes — deve rodar antes de stg.sales.
run_etl "etl/load_customers.py" "$EXTRA_ARGS"
# Executa a carga de vendas — depende logicamente das três tabelas acima.
run_etl "etl/load_sales.py"     "$EXTRA_ARGS"

# ---------------------------------------------------------------------------
# Validações pós-carga (SELECT COUNT por tabela)
# ---------------------------------------------------------------------------

log_msg "=============================================================="
log_msg "VALIDAÇÕES PÓS-CARGA"
log_msg "=============================================================="

# Executa as queries de validação no PostgreSQL e exibe/salva o resultado.
PGPASSWORD="$PG_PASSWORD" psql \
    -h "$PG_HOST" \
    -p "$PG_PORT" \
    -U "$PG_USER" \
    -d "$PG_DB" \
    --no-password \
    -c "
-- Resumo das cargas por tabela de staging
SELECT 'stg.stores'    AS tabela, COUNT(*) AS total_linhas, MAX(loaded_at) AS ultima_carga
  FROM stg.stores
UNION ALL
SELECT 'stg.products'  AS tabela, COUNT(*), MAX(loaded_at) FROM stg.products
UNION ALL
SELECT 'stg.customers' AS tabela, COUNT(*), MAX(loaded_at) FROM stg.customers
UNION ALL
SELECT 'stg.sales'     AS tabela, COUNT(*), MAX(loaded_at) FROM stg.sales
ORDER BY tabela;
" 2>&1 | tee -a "$LOG_FILE"

# Exibe o resumo dos últimos batches executados neste run.
PGPASSWORD="$PG_PASSWORD" psql \
    -h "$PG_HOST" \
    -p "$PG_PORT" \
    -U "$PG_USER" \
    -d "$PG_DB" \
    --no-password \
    -c "
-- Últimos batches: status, métricas e duração
SELECT
    dataset_name,
    status,
    rows_extracted,
    rows_inserted,
    rows_skipped,
    EXTRACT(EPOCH FROM (finished_at - started_at))::INT AS duration_s,
    started_at::timestamp(0) AS started_at
FROM etl.load_batches
WHERE started_at >= NOW() - INTERVAL '2 hours'
ORDER BY started_at DESC;
" 2>&1 | tee -a "$LOG_FILE"

# ---------------------------------------------------------------------------
# Rodapé do run
# ---------------------------------------------------------------------------

# Calcula o tempo total do pipeline completo.
ELAPSED=$(( $(date +%s) - START_TIME ))
log_msg "=============================================================="
log_msg "PIPELINE ETL — CONCLUÍDO COM SUCESSO"
log_msg "Tempo total: ${ELAPSED}s"
log_msg "Log salvo : $LOG_FILE"
log_msg "=============================================================="
