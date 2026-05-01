#!/usr/bin/env python3
"""Seed inicial de usuarios no banco reco.users."""
import os
import sys

from passlib.context import CryptContext
import psycopg2

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def main():
    username = os.getenv("SEED_USERNAME", "cesarmartins")
    password = os.getenv("SEED_PASSWORD")
    full_name = os.getenv("SEED_FULLNAME", "Cesar Martins")

    if not password:
        print("ERRO: Defina SEED_PASSWORD=<senha> na variavel de ambiente")
        sys.exit(1)

    password_hash = pwd_context.hash(password)

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", 5432),
        dbname=os.getenv("PG_DB", "reco"),
        user=os.getenv("PG_USER", "reco"),
        password=os.getenv("PG_PASSWORD", "reco"),
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO reco.users (username, password_hash, full_name)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (username) DO NOTHING""",
                (username, password_hash, full_name),
            )
            print(f"Usuario '{username}' inserido com sucesso (ou ja existia).")

    conn.close()


if __name__ == "__main__":
    main()
