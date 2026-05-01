#!/usr/bin/env python3
"""
Product Deduplication Script
=============================
Identifica e agrupa produtos duplicados/similares no catálogo.

REGRA FUNDAMENTAL: marcas diferentes = produtos diferentes.
LENCOL CASAL QUEEN TEKA ≠ LENCOL CASAL QUEEN KARSTEN

Saída:
  - CSV com grupos de duplicatas para validação
  - Tabela reco.product_canonical com mapeamento product_id → canonical_id

Uso:
  python scripts/product_dedup.py                  # relatório apenas
  python scripts/product_dedup.py --apply           # aplica na base
  python scripts/product_dedup.py --csv-only        # só gera CSV
"""

import argparse
import csv
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
from rapidfuzz import fuzz

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.common import get_pg_conn, setup_logging

LOG = logging.getLogger("scripts.product_dedup")

# ── Marcas conhecidas (ordem importa: mais longas primeiro para match correto) ──
KNOWN_BRANDS = [
    "KACYUMARA", "ALTENBURG", "ATLANTICA", "ARTESANALLE", "AETESANALLE",
    "BUETTNER", "CORTTEX", "NIAZITEX", "SANTISTA", "RAFIMEX",
    "DOHLER", "KARSTEN", "LEPPER", "SULTAN", "ARTEX",
    "TEKA", "BUD", "LIZ", "WAN",
]

# Sufixos a remover na normalização
REMOVE_SUFFIXES = re.compile(
    r'\s+(REVISADO|REVIZADO|REVISAO|REV)\s*$', re.IGNORECASE
)
# Códigos numéricos soltos no final (ex: "GOIABA 5394", "BEGE 4181")
TRAILING_CODE = re.compile(r'\s+\d{3,5}\s*$')

# Padrões de tamanho que diferenciam produtos (NÃO são duplicatas)
SIZE_PATTERNS = re.compile(
    r'\b(PP|GG|EG|XG|XXG|KING|QUEEN|SOLTEIRO|SOLTEIRÃO|SOLTEIRAO|'
    r'BANHAO|BANHÃO|BANHO|ROSTO|PISO|LAVABO|'
    r'CASAL|BERCO|BERÇO)\b'
)

# Abreviações a expandir
ABBREVIATIONS = {
    r'\bCS\b': 'CASAL',
    r'\bST\b': 'SOLTEIRO',
    r'\bCJ\b': 'CONJUNTO',
    r'\bJG\b': 'JOGO',
    r'\bTLH\b': 'TOALHA',
    r'\bPC\b': 'PECA',
    r'\bC/\b': 'COM ',
    r'\bP/\b': 'PARA ',
    r'\bS/\b': 'SEM ',
}


def extract_brand(description: str) -> str:
    """Extrai a marca do nome do produto. Retorna '' se não identificada."""
    desc_upper = description.upper()
    for brand in KNOWN_BRANDS:
        if brand in desc_upper:
            # Normalizar typos conhecidos
            if brand == "AETESANALLE":
                return "ARTESANALLE"
            return brand
    return ""


def normalize_description(desc: str) -> str:
    """Normaliza descrição para comparação de dedup."""
    if not desc:
        return ""
    s = desc.strip().upper()
    # Colapsar espaços múltiplos
    s = re.sub(r'\s+', ' ', s)
    # Remover sufixos irrelevantes
    s = REMOVE_SUFFIXES.sub('', s)
    # Remover códigos numéricos soltos no final
    s = TRAILING_CODE.sub('', s)
    # Trim final
    s = s.strip()
    return s


def normalize_expanded(desc: str) -> str:
    """Normaliza com expansão de abreviações (para fuzzy matching)."""
    s = normalize_description(desc)
    for pattern, expansion in ABBREVIATIONS.items():
        s = re.sub(pattern, expansion, s)
    return s


def load_products(pg) -> list:
    """Carrega todos os produtos ativos com contagem de vendas."""
    cur = pg.cursor()
    cur.execute("""
        SELECT cp.product_id, cp.description,
               COALESCE(sales.cnt, 0) AS sale_count
        FROM cur.products cp
        LEFT JOIN (
            SELECT product_id, COUNT(*) AS cnt
            FROM cur.order_items
            GROUP BY product_id
        ) sales ON sales.product_id = cp.product_id
        WHERE cp.active = TRUE
          AND cp.source_system = 'sqlserver_gp'
        ORDER BY cp.description
    """)
    products = []
    for row in cur.fetchall():
        products.append({
            "product_id": row[0],
            "description": row[1] or "",
            "sale_count": row[2],
        })
    LOG.info(f"Loaded {len(products):,} active products")
    return products


def find_exact_duplicates(products: list) -> dict:
    """
    Nível 1: Duplicatas exatas após normalização.
    Agrupa por (brand, normalized_description).
    Retorna {group_key: [product_dicts]}.
    """
    groups = defaultdict(list)
    for p in products:
        norm = normalize_description(p["description"])
        brand = extract_brand(p["description"])
        key = (brand, norm)
        groups[key].append(p)

    # Filtrar apenas grupos com >1 membro
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    total_dupes = sum(len(v) for v in duplicates.values())
    LOG.info(
        f"Exact duplicates: {len(duplicates):,} groups, "
        f"{total_dupes:,} products total"
    )
    return duplicates


def find_fuzzy_duplicates(products: list, threshold: float = 90.0) -> list:
    """
    Nível 2: Fuzzy matching dentro da mesma marca.
    Retorna lista de (product_a, product_b, similarity_score).
    """
    # Agrupar por marca
    by_brand = defaultdict(list)
    for p in products:
        brand = extract_brand(p["description"])
        by_brand[brand].append(p)

    fuzzy_pairs = []
    for brand, brand_products in by_brand.items():
        if len(brand_products) < 2:
            continue
        # Pré-normalizar
        normalized = [
            (p, normalize_expanded(p["description"]))
            for p in brand_products
        ]
        # Comparar todos os pares (dentro da marca)
        for i in range(len(normalized)):
            for j in range(i + 1, len(normalized)):
                p_a, norm_a = normalized[i]
                p_b, norm_b = normalized[j]
                # Skip se já são exatas (tratado no nível 1)
                if norm_a == norm_b:
                    continue
                # Skip se diferem apenas por tamanho/tipo (P vs PP, BANHO vs BANHAO)
                sizes_a = set(SIZE_PATTERNS.findall(norm_a))
                sizes_b = set(SIZE_PATTERNS.findall(norm_b))
                if sizes_a != sizes_b:
                    continue
                score = fuzz.ratio(norm_a, norm_b)
                if score >= threshold:
                    fuzzy_pairs.append((p_a, p_b, score))

    LOG.info(f"Fuzzy pairs (>={threshold}%): {len(fuzzy_pairs):,}")
    return fuzzy_pairs


def elect_canonical(group: list) -> int:
    """Elege o canonical_id: produto com mais vendas no grupo."""
    return max(group, key=lambda p: p["sale_count"])["product_id"]


def generate_csv_report(
    exact_groups: dict,
    fuzzy_pairs: list,
    output_dir: str = "docs",
) -> str:
    """Gera CSV com relatório de duplicatas para validação."""
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "product_dedup_report.csv")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "group_id", "match_type", "canonical_id", "product_id",
            "description", "brand", "sale_count", "similarity",
        ])

        group_id = 0

        # Exatas
        for (brand, norm_desc), members in sorted(exact_groups.items()):
            group_id += 1
            canonical = elect_canonical(members)
            for p in sorted(members, key=lambda x: -x["sale_count"]):
                w.writerow([
                    group_id, "exact", canonical, p["product_id"],
                    p["description"], brand, p["sale_count"], 100.0,
                ])

        # Fuzzy
        for p_a, p_b, score in sorted(fuzzy_pairs, key=lambda x: -x[2]):
            group_id += 1
            canonical = p_a["product_id"] if p_a["sale_count"] >= p_b["sale_count"] else p_b["product_id"]
            brand = extract_brand(p_a["description"])
            w.writerow([
                group_id, "fuzzy", canonical, p_a["product_id"],
                p_a["description"], brand, p_a["sale_count"], score,
            ])
            w.writerow([
                group_id, "fuzzy", canonical, p_b["product_id"],
                p_b["description"], brand, p_b["sale_count"], score,
            ])

    LOG.info(f"Report written to {csv_path}")
    return csv_path


def apply_to_database(pg, exact_groups: dict, fuzzy_pairs: list) -> int:
    """Cria tabela reco.product_canonical e insere mapeamentos."""
    cur = pg.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reco.product_canonical (
            product_id   BIGINT PRIMARY KEY,
            canonical_id BIGINT NOT NULL,
            match_type   TEXT NOT NULL,
            similarity   NUMERIC DEFAULT 100.0,
            group_label  TEXT
        )
    """)
    cur.execute("TRUNCATE reco.product_canonical")

    rows = []

    # Exatas
    for (brand, norm_desc), members in exact_groups.items():
        canonical = elect_canonical(members)
        label = f"{brand}|{norm_desc}" if brand else norm_desc
        for p in members:
            rows.append((
                p["product_id"], canonical, "exact", 100.0, label,
            ))

    # Fuzzy
    for p_a, p_b, score in fuzzy_pairs:
        canonical = p_a["product_id"] if p_a["sale_count"] >= p_b["sale_count"] else p_b["product_id"]
        label = f"fuzzy:{p_a['description'][:40]}~{p_b['description'][:40]}"
        # Apenas adicionar se não já mapeado (exatas têm prioridade)
        existing_ids = {r[0] for r in rows}
        if p_a["product_id"] not in existing_ids:
            rows.append((p_a["product_id"], canonical, "fuzzy", score, label))
        if p_b["product_id"] not in existing_ids:
            rows.append((p_b["product_id"], canonical, "fuzzy", score, label))

    # Adicionar identidade para todos os produtos NÃO duplicados
    cur.execute("""
        SELECT product_id FROM cur.products
        WHERE active = TRUE AND source_system = 'sqlserver_gp'
    """)
    all_ids = {r[0] for r in cur.fetchall()}
    mapped_ids = {r[0] for r in rows}
    for pid in all_ids - mapped_ids:
        rows.append((pid, pid, "unique", 100.0, None))

    cur.executemany(
        """INSERT INTO reco.product_canonical
           (product_id, canonical_id, match_type, similarity, group_label)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (product_id) DO UPDATE SET
               canonical_id = EXCLUDED.canonical_id,
               match_type = EXCLUDED.match_type,
               similarity = EXCLUDED.similarity,
               group_label = EXCLUDED.group_label
        """,
        rows,
    )
    pg.commit()
    n_deduped = sum(1 for r in rows if r[0] != r[1])
    LOG.info(
        f"product_canonical: {len(rows):,} rows inserted "
        f"({n_deduped:,} mapped to a different canonical_id)"
    )
    return n_deduped


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Product deduplication")
    parser.add_argument("--apply", action="store_true",
                        help="Apply to database (create reco.product_canonical)")
    parser.add_argument("--csv-only", action="store_true",
                        help="Only generate CSV report, no DB changes")
    parser.add_argument("--fuzzy-threshold", type=float, default=90.0,
                        help="Fuzzy matching threshold (default: 90.0)")
    args = parser.parse_args()

    pg = get_pg_conn()

    try:
        products = load_products(pg)

        LOG.info("=" * 60)
        LOG.info("STEP 1: Exact duplicates (after normalization)")
        LOG.info("=" * 60)
        exact_groups = find_exact_duplicates(products)

        LOG.info("=" * 60)
        LOG.info("STEP 2: Fuzzy duplicates (within same brand)")
        LOG.info("=" * 60)
        fuzzy_pairs = find_fuzzy_duplicates(
            products, threshold=args.fuzzy_threshold
        )

        # Generate CSV report
        csv_path = generate_csv_report(exact_groups, fuzzy_pairs)
        print(f"\nCSV report: {csv_path}")

        # Summary
        exact_count = sum(len(v) for v in exact_groups.values())
        print(f"\n{'='*50}")
        print(f"SUMMARY")
        print(f"{'='*50}")
        print(f"Exact duplicate groups:  {len(exact_groups):,}")
        print(f"Exact duplicate products: {exact_count:,}")
        print(f"Fuzzy pairs found:       {len(fuzzy_pairs):,}")
        print(f"CSV report:              {csv_path}")

        if args.apply and not args.csv_only:
            LOG.info("=" * 60)
            LOG.info("STEP 3: Applying to database")
            LOG.info("=" * 60)
            n = apply_to_database(pg, exact_groups, fuzzy_pairs)
            print(f"Database updated:        {n:,} products remapped")
        elif not args.csv_only:
            print("\nUse --apply to write to database.")

    finally:
        pg.close()


if __name__ == "__main__":
    main()
