"""
app/api/routers/users.py
========================
CRUD de usuarios (reco.users) para administracao da API.

Endpoints (todos protegidos por JWT admin):
  POST   /api/admin/users            -- criar usuario
  GET    /api/admin/users            -- listar usuarios
  PUT    /api/admin/users/{user_id}  -- atualizar usuario
  DELETE /api/admin/users/{user_id}  -- desativar usuario (soft delete)
"""

from datetime import datetime
from typing import Optional

import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException, Request, status
from passlib.context import CryptContext
from pydantic import BaseModel

from deps import require_admin, get_db, release_db, limiter, _default_rate

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# -- Modelos --

class UserCreate(BaseModel):
    username: str
    password: str
    full_name: Optional[str] = None
    role: str = "commercial"


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    password: Optional[str] = None
    is_active: Optional[bool] = None
    role: Optional[str] = None


class UserResponse(BaseModel):
    user_id: int
    username: str
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: str


# -- Endpoints --

@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit(_default_rate)
def create_user(
    request: Request,
    body: UserCreate,
    _admin: dict = Depends(require_admin),
):
    """Criar novo usuario. Apenas admins."""
    if body.role not in ("admin", "commercial"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="role deve ser 'admin' ou 'commercial'",
        )
    password_hash = pwd_context.hash(body.password)
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """INSERT INTO reco.users (username, password_hash, full_name, role)
                   VALUES (%s, %s, %s, %s)
                   RETURNING user_id, username, full_name, role, is_active, created_at""",
                (body.username, password_hash, body.full_name, body.role),
            )
            conn.commit()
            row = cur.fetchone()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Username '{body.username}' already exists",
        )
    finally:
        release_db(conn)

    row["created_at"] = row["created_at"].isoformat()
    return UserResponse(**row)


@router.get("/users", response_model=list[UserResponse])
@limiter.limit(_default_rate)
def list_users(
    request: Request,
    _admin: dict = Depends(require_admin),
):
    """Listar todos os usuarios. Apenas admins."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT user_id, username, full_name, role, is_active, created_at "
                "FROM reco.users ORDER BY user_id"
            )
            rows = cur.fetchall()
    finally:
        release_db(conn)

    for row in rows:
        row["created_at"] = row["created_at"].isoformat()
    return [UserResponse(**row) for row in rows]


@router.put("/users/{user_id}", response_model=UserResponse)
@limiter.limit(_default_rate)
def update_user(
    request: Request,
    user_id: int,
    body: UserUpdate,
    _admin: dict = Depends(require_admin),
):
    """Atualizar usuario. Apenas admins."""
    sets = []
    params = []

    if body.full_name is not None:
        sets.append("full_name = %s")
        params.append(body.full_name)
    if body.password is not None:
        sets.append("password_hash = %s")
        params.append(pwd_context.hash(body.password))
    if body.is_active is not None:
        sets.append("is_active = %s")
        params.append(body.is_active)
    if body.role is not None:
        if body.role not in ("admin", "commercial"):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="role deve ser 'admin' ou 'commercial'",
            )
        sets.append("role = %s")
        params.append(body.role)

    if not sets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )

    sets.append("updated_at = now()")
    params.append(user_id)

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"UPDATE reco.users SET {', '.join(sets)} WHERE user_id = %s "
                "RETURNING user_id, username, full_name, role, is_active, created_at",
                params,
            )
            conn.commit()
            row = cur.fetchone()
    finally:
        release_db(conn)

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    row["created_at"] = row["created_at"].isoformat()
    return UserResponse(**row)


@router.delete("/users/{user_id}", response_model=UserResponse)
@limiter.limit(_default_rate)
def delete_user(
    request: Request,
    user_id: int,
    _admin: dict = Depends(require_admin),
):
    """Soft delete: desativa usuario. Apenas admins."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "UPDATE reco.users SET is_active = FALSE, updated_at = now() WHERE user_id = %s "
                "RETURNING user_id, username, full_name, role, is_active, created_at",
                (user_id,),
            )
            conn.commit()
            row = cur.fetchone()
    finally:
        release_db(conn)

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    row["created_at"] = row["created_at"].isoformat()
    return UserResponse(**row)
