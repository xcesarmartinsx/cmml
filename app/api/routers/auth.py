"""
app/api/routers/auth.py
=======================
Router de autenticação JWT para a API CMML.

Endpoint:
  POST /api/auth/token — recebe {username, password}, retorna {access_token, token_type}

Credenciais de admin configuradas via variáveis de ambiente:
  CMML_ADMIN_USER     — username do administrador
  CMML_ADMIN_PASSWORD — senha do administrador (hash bcrypt gerado na primeira execução)
  JWT_SECRET_KEY      — chave secreta para assinatura dos tokens (obrigatória)
  JWT_EXPIRE_MINUTES  — tempo de expiração do token em minutos (default: 480 = 8h)
"""

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, status
from jose import jwt
from passlib.context import CryptContext
from pydantic import BaseModel

router = APIRouter()

# ── Hashing de senha ──────────────────────────────────────────────────────────
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── Configuração via variáveis de ambiente ────────────────────────────────────

def _get_secret_key() -> str:
    key = os.getenv("JWT_SECRET_KEY")
    if not key:
        raise RuntimeError(
            "JWT_SECRET_KEY não definida. "
            "Gere com: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
        )
    return key


def _get_admin_user() -> str:
    user = os.getenv("CMML_ADMIN_USER")
    if not user:
        raise RuntimeError("CMML_ADMIN_USER não definida no ambiente.")
    return user


def _get_admin_password_hash() -> str:
    raw = os.getenv("CMML_ADMIN_PASSWORD")
    if not raw:
        raise RuntimeError("CMML_ADMIN_PASSWORD não definida no ambiente.")
    return pwd_context.hash(raw)


def _get_expire_minutes() -> int:
    return int(os.getenv("JWT_EXPIRE_MINUTES", "480"))


# ── Modelos ───────────────────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _authenticate_user(username: str, password: str) -> bool:
    """Verifica se as credenciais correspondem ao admin configurado."""
    expected_user = _get_admin_user()
    if username != expected_user:
        return False
    expected_hash = _get_admin_password_hash()
    return pwd_context.verify(password, expected_hash)


def create_access_token(subject: str) -> str:
    """Cria um JWT assinado com expiração configurável."""
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=_get_expire_minutes())
    payload = {
        "sub": subject,
        "exp": expire,
    }
    return jwt.encode(payload, _get_secret_key(), algorithm="HS256")


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/api/auth/token", response_model=TokenResponse)
def login(body: TokenRequest):
    """
    Autentica o usuário e retorna um token JWT.

    Retorna 401 com mensagem genérica se credenciais inválidas
    (não revela se o usuário existe ou se a senha está errada).
    """
    if not _authenticate_user(body.username, body.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(subject=body.username)
    return TokenResponse(access_token=token)
