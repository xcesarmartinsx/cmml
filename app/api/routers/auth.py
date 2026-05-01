"""
app/api/routers/auth.py
=======================
Router de autenticacao JWT para a API CMML.

Endpoints:
  POST /api/auth/token -- recebe {username, password}, retorna {access_token, token_type}
  GET  /api/auth/me    -- retorna {username, role} do usuario autenticado
"""

import os
from datetime import datetime, timedelta, timezone

import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException, Request, status
from jose import jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from deps import get_current_user_info, get_db, release_db, limiter

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _get_secret_key() -> str:
    key = os.getenv("JWT_SECRET_KEY")
    if not key:
        raise RuntimeError(
            "JWT_SECRET_KEY nao definida. "
            "Gere com: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
        )
    return key


def _get_expire_minutes() -> int:
    return int(os.getenv("JWT_EXPIRE_MINUTES", "480"))


class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MeResponse(BaseModel):
    username: str
    role: str


def _authenticate_user(username: str, password: str) -> dict | None:
    """
    Verifica credenciais contra a tabela reco.users.
    Retorna {"username": str, "role": str} se valido, None caso contrario.
    """
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT username, password_hash, role FROM reco.users "
                "WHERE username = %s AND is_active = TRUE",
                (username,),
            )
            row = cur.fetchone()
    finally:
        release_db(conn)

    if row is None:
        return None
    if not pwd_context.verify(password, row["password_hash"]):
        return None
    return {"username": row["username"], "role": row["role"]}


def create_access_token(subject: str, role: str) -> str:
    """Cria um JWT assinado com expiracao configuravel."""
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=_get_expire_minutes())
    payload = {
        "sub": subject,
        "role": role,
        "exp": expire,
    }
    return jwt.encode(payload, _get_secret_key(), algorithm="HS256")


_login_rate = os.getenv("RATE_LIMIT_LOGIN", "5/minute")


@router.post("/api/auth/token", response_model=TokenResponse)
@limiter.limit(_login_rate)
def login(request: Request, body: TokenRequest):
    """
    Autentica o usuario e retorna um token JWT com role incluida.
    Rate limit: 5 tentativas/minuto por IP.
    """
    user = _authenticate_user(body.username, body.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(subject=user["username"], role=user["role"])
    return TokenResponse(access_token=token)


@router.get("/api/auth/me", response_model=MeResponse)
@limiter.limit("60/minute")
def get_me(request: Request, user_info: dict = Depends(get_current_user_info)):
    """Retorna username e role do usuario autenticado."""
    return MeResponse(username=user_info["username"], role=user_info["role"])
