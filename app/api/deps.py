"""
app/api/deps.py
===============
Dependencias compartilhadas da API FastAPI.

- get_current_user(): dependencia de autenticacao JWT.
- get_db(): dependencia que fornece conexao do pool PostgreSQL.
"""

import os
import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

# ── Connection Pool ──────────────────────────────────────────────────────────
# Inicializado em init_pool() chamado pelo lifespan do app.
_pool: ThreadedConnectionPool | None = None


def init_pool() -> None:
    """Cria o pool de conexoes PostgreSQL. Chamar uma vez no startup do app."""
    global _pool
    _pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=int(os.getenv("PG_POOL_MAX", "10")),
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "reco"),
        user=os.getenv("PG_USER", "reco"),
        password=os.environ["PG_PASSWORD"],
    )
    logger.info("PostgreSQL connection pool initialized (max=%s)", os.getenv("PG_POOL_MAX", "10"))


def close_pool() -> None:
    """Fecha todas as conexoes do pool. Chamar no shutdown do app."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("PostgreSQL connection pool closed")


def get_db():
    """
    FastAPI dependency que fornece uma conexao do pool.

    Uso:
        conn = get_db()  # ou via Depends(get_db) em endpoints
    A conexao e devolvida ao pool no finally do endpoint.

    Para uso como generator dependency (yield), use get_db_dep().
    """
    if _pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database pool not initialized",
        )
    return _pool.getconn()


def release_db(conn) -> None:
    """Devolve uma conexao ao pool."""
    if _pool is not None and conn is not None:
        _pool.putconn(conn)


# ── JWT Auth ─────────────────────────────────────────────────────────────────
_bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> str:
    """
    Valida o token JWT e retorna o subject (username).

    Retorna 401 com mensagem generica se o token for invalido ou expirado.
    Nunca revela detalhes sobre o motivo da falha.
    """
    secret_key = os.getenv("JWT_SECRET_KEY")
    if not secret_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured",
        )

    try:
        payload = jwt.decode(
            credentials.credentials,
            secret_key,
            algorithms=["HS256"],
        )
        subject: str = payload.get("sub")
        if subject is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return subject
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
