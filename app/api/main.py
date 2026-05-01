from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded
import psycopg2.extras
import os
from typing import Optional

# Importa o router do dashboard de negocios (Visao 360)
from routers.business import router as business_router
# Importa o router de recomendacoes/ofertas
from routers.recommendations import router as recommendations_router
# Importa o router de autenticacao JWT
from routers.auth import router as auth_router
# Importa o router de administracao de usuarios
from routers.users import router as users_router
# Importa dependencias compartilhadas
from deps import get_current_user, require_admin, init_pool, close_pool, get_db, release_db, limiter, _default_rate


# ── Lifespan (startup/shutdown) ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    yield
    close_pool()


app = FastAPI(title="CMML Dashboard API", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Try again later."},
    )


# CORS: origens permitidas via variavel de ambiente (comma-separated).
_default_origins = "http://localhost:3000,http://localhost:3001"
_cors_origins = [
    o.strip()
    for o in os.getenv("CORS_ORIGINS", _default_origins).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

# Registra o router de autenticacao (publico)
app.include_router(auth_router)
# Registra as rotas de negocio (/api/business/*) — protegidas por JWT
app.include_router(
    business_router,
    dependencies=[Depends(get_current_user)],
)
# Registra as rotas de recomendacoes (/api/recommendations/*) — protegidas por JWT
app.include_router(
    recommendations_router,
    dependencies=[Depends(get_current_user)],
)
# Registra as rotas de administracao de usuarios (/api/admin/*) — protegidas por JWT (admin only)
app.include_router(
    users_router,
    prefix="/api/admin",
    tags=["admin"],
)


def row_to_dict(row: dict) -> dict:
    r = dict(row)
    if r.get("evaluated_at"):
        r["evaluated_at"] = r["evaluated_at"].isoformat()
    for col in ["precision_at_k", "recall_at_k", "ndcg_at_k", "map_at_k"]:
        if r.get(col) is not None:
            r[col] = float(r[col])
    return r


@app.get("/api/evaluation-runs")
@limiter.limit(_default_rate)
def get_evaluation_runs(
    request: Request,
    strategy: Optional[str] = Query(None),
    k: Optional[int] = Query(None),
    _user: str = Depends(get_current_user),
):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT run_id, strategy, k, precision_at_k, recall_at_k,
                       ndcg_at_k, map_at_k, n_customers, notes, evaluated_at
                FROM reco.evaluation_runs
                WHERE 1=1
            """
            params = []
            if strategy:
                query += " AND strategy = %s"
                params.append(strategy)
            if k:
                query += " AND k = %s"
                params.append(k)
            query += " ORDER BY evaluated_at ASC, strategy, k"
            cur.execute(query, params)
            return [row_to_dict(r) for r in cur.fetchall()]
    finally:
        release_db(conn)


@app.get("/api/models/latest")
@limiter.limit(_default_rate)
def get_models_latest(request: Request, _user: str = Depends(get_current_user)):
    """Latest evaluation run per strategy (all K values grouped)."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (strategy, k)
                    run_id, strategy, k, precision_at_k, recall_at_k,
                    ndcg_at_k, map_at_k, n_customers, notes, evaluated_at
                FROM reco.evaluation_runs
                ORDER BY strategy, k, evaluated_at DESC
            """)
            return [row_to_dict(r) for r in cur.fetchall()]
    finally:
        release_db(conn)


@app.get("/api/strategies")
@limiter.limit(_default_rate)
def get_strategies(request: Request, _user: str = Depends(get_current_user)):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT strategy FROM reco.evaluation_runs ORDER BY strategy"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        release_db(conn)


@app.get("/api/k-values")
@limiter.limit(_default_rate)
def get_k_values(request: Request, _user: str = Depends(get_current_user)):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT k FROM reco.evaluation_runs ORDER BY k"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        release_db(conn)
