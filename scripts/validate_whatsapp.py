#!/usr/bin/env python3
"""
scripts/validate_whatsapp.py
============================
Valida numeros de celular via Evolution API (WhatsApp) e armazena resultados
em reco.whatsapp_cache.

Fluxo:
  1. Consulta stg.customers para numeros classificados como "mobile" pela heuristica
  2. Filtra os que NAO estao em reco.whatsapp_cache ou cujo cache expirou (TTL)
  3. Envia em lotes para POST /chat/whatsappNumbers/{instance}
  4. Grava resultados em reco.whatsapp_cache (UPSERT)

Uso:
  python3 scripts/validate_whatsapp.py              # valida todos os pendentes
  python3 scripts/validate_whatsapp.py --limit 100  # valida ate 100 numeros
  python3 scripts/validate_whatsapp.py --dry-run    # apenas mostra o que faria
"""
import argparse
import logging
import os
import sys
import time

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("validate_whatsapp")


def normalize_phone(raw: str) -> str | None:
    """
    Extrai digitos e adiciona DDI 55 se necessario.
    '(88) 99961-2137' -> '5588999612137'
    '88999612137'     -> '5588999612137'
    '5588999612137'   -> '5588999612137'
    Retorna None se o numero nao parece celular valido.
    """
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 11 and digits[2] == "9":
        return f"55{digits}"
    if len(digits) == 13 and digits.startswith("55") and digits[4] == "9":
        return digits
    return None


def get_pending_numbers(conn, ttl_days: int, limit: int | None):
    """
    Retorna lista de (raw_phone, normalized_phone) para numeros que:
    - Sao classificados como 'mobile' pela heuristica (11 digitos, 3o = 9)
    - NAO estao no cache, ou o cache expirou (> ttl_days)
    """
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT DISTINCT
            COALESCE(sc.mobile, sc.phone) AS raw_phone,
            '55' || REGEXP_REPLACE(COALESCE(sc.mobile, sc.phone), '[^0-9]', '', 'g') AS normalized
        FROM stg.customers sc
        WHERE LENGTH(REGEXP_REPLACE(COALESCE(sc.mobile, sc.phone), '[^0-9]', '', 'g')) = 11
          AND SUBSTRING(REGEXP_REPLACE(COALESCE(sc.mobile, sc.phone), '[^0-9]', '', 'g'), 3, 1) = '9'
          AND NOT EXISTS (
              SELECT 1 FROM reco.whatsapp_cache wc
              WHERE wc.phone_number = '55' || REGEXP_REPLACE(COALESCE(sc.mobile, sc.phone), '[^0-9]', '', 'g')
                AND wc.validated_at > now() - make_interval(days => {int(ttl_days)})
          )
        {limit_clause}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return [(r["raw_phone"], r["normalized"]) for r in cur.fetchall()]


def validate_batch(numbers: list[str], api_url: str, api_key: str, instance: str) -> dict:
    """
    Chama POST {api_url}/chat/whatsappNumbers/{instance}
    Retorna dict {number: (exists: bool, jid: str|None)}
    """
    resp = requests.post(
        f"{api_url}/chat/whatsappNumbers/{instance}",
        json={"numbers": numbers},
        headers={"apikey": api_key, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    results = {}
    for item in resp.json():
        num = item.get("number", "")
        exists = item.get("exists", False)
        jid = item.get("jid")
        results[num] = (exists, jid)
    return results


def save_results(conn, results: dict):
    """UPSERT em reco.whatsapp_cache."""
    if not results:
        return
    with conn.cursor() as cur:
        for number, (exists, jid) in results.items():
            cur.execute("""
                INSERT INTO reco.whatsapp_cache (phone_number, has_whatsapp, jid, validated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (phone_number) DO UPDATE SET
                    has_whatsapp = EXCLUDED.has_whatsapp,
                    jid = EXCLUDED.jid,
                    validated_at = now()
            """, (number, exists, jid))


def main():
    parser = argparse.ArgumentParser(description="Validate WhatsApp numbers via Evolution API")
    parser.add_argument("--limit", type=int, default=None, help="Max numbers to validate")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without calling API")
    args = parser.parse_args()

    api_url = os.getenv("EVOLUTION_API_URL", "http://localhost:8080")
    api_key = os.getenv("EVOLUTION_API_KEY")
    instance = os.getenv("EVOLUTION_INSTANCE", "cmml")
    ttl_days = int(os.getenv("EVOLUTION_CACHE_TTL_DAYS", "30"))
    batch_size = int(os.getenv("EVOLUTION_BATCH_SIZE", "50"))
    batch_delay = float(os.getenv("EVOLUTION_BATCH_DELAY", "3"))

    if not api_key and not args.dry_run:
        logger.error("EVOLUTION_API_KEY nao definida. Configure no .env")
        sys.exit(1)

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "reco"),
        user=os.getenv("PG_USER", "reco"),
        password=os.environ["PG_PASSWORD"],
    )

    pending = get_pending_numbers(conn, ttl_days, args.limit)
    logger.info(f"{len(pending)} numeros pendentes de validacao")

    if not pending:
        logger.info("Nenhum numero para validar. Cache esta atualizado.")
        conn.close()
        return

    if args.dry_run:
        for raw, norm in pending[:30]:
            print(f"  {raw:20s} -> {norm}")
        if len(pending) > 30:
            print(f"  ... e mais {len(pending) - 30} numeros")
        conn.close()
        return

    # Testa conectividade com Evolution API
    try:
        resp = requests.get(api_url, timeout=10)
        logger.info(f"Evolution API acessivel em {api_url}")
    except requests.ConnectionError:
        logger.error(f"Evolution API inacessivel em {api_url}. Verifique se o container esta rodando.")
        conn.close()
        sys.exit(1)

    total_checked = 0
    total_whatsapp = 0

    for i in range(0, len(pending), batch_size):
        batch_items = pending[i : i + batch_size]
        batch_numbers = [norm for _, norm in batch_items]

        try:
            results = validate_batch(batch_numbers, api_url, api_key, instance)
            save_results(conn, results)
            conn.commit()

            batch_wpp = sum(1 for exists, _ in results.values() if exists)
            total_checked += len(batch_numbers)
            total_whatsapp += batch_wpp

            batch_num = i // batch_size + 1
            total_batches = (len(pending) + batch_size - 1) // batch_size
            logger.info(
                f"Lote {batch_num}/{total_batches}: "
                f"{len(batch_numbers)} verificados, {batch_wpp} com WhatsApp"
            )
        except requests.HTTPError as e:
            logger.error(f"Erro na API (lote {i // batch_size + 1}): {e}")
            logger.error("Interrompendo para evitar bloqueio da conta.")
            break
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            break

        # Rate limiting entre lotes
        if i + batch_size < len(pending):
            time.sleep(batch_delay)

    pct = total_whatsapp * 100 // max(total_checked, 1)
    logger.info(f"Concluido: {total_checked} verificados, {total_whatsapp} com WhatsApp ({pct}%)")
    conn.close()


if __name__ == "__main__":
    main()
