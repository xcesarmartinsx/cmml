"""
tests/test_security_api.py
==========================
Testes de segurança para a API FastAPI do CMML.

Verifica:
- Endpoints protegidos retornam 401/403 sem token JWT
- Login com credenciais inválidas retorna 401 genérico
- Headers de segurança HTTP estão presentes em todas as respostas
- CORS não permite origens arbitrárias
- Endpoints públicos (somente /api/auth/token) acessíveis sem JWT

Todos os testes usam TestClient do FastAPI — sem dependência de banco real.
O banco é mockado via monkeypatch para isolar a lógica de autenticação.
"""

import os
import sys
import pytest

# Garante que o diretório da API está no path para import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app", "api"))


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    """Configura variáveis de ambiente mínimas para o app subir."""
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-tests-only-32chars!!")
    monkeypatch.setenv("CMML_ADMIN_USER", "testadmin")
    monkeypatch.setenv("CMML_ADMIN_PASSWORD", "testpassword123")
    monkeypatch.setenv("PG_PASSWORD", "fake-pg-password")
    monkeypatch.setenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:3001")


@pytest.fixture
def client(monkeypatch):
    """TestClient com pool de banco mockado."""
    # Mock do pool para não precisar de PostgreSQL real
    monkeypatch.setattr("deps._pool", None)

    def _fake_init_pool():
        pass  # não cria pool real

    def _fake_close_pool():
        pass

    monkeypatch.setattr("deps.init_pool", _fake_init_pool)
    monkeypatch.setattr("deps.close_pool", _fake_close_pool)

    from fastapi.testclient import TestClient
    import main as app_module
    with TestClient(app_module.app) as c:
        yield c


@pytest.fixture
def valid_token(client):
    """Obtém um token JWT válido via login."""
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "testpassword123"},
    )
    assert resp.status_code == 200, f"Login falhou: {resp.text}"
    return resp.json()["access_token"]


# ── Testes de autenticação ────────────────────────────────────────────────────

PROTECTED_ENDPOINTS = [
    ("GET", "/api/evaluation-runs"),
    ("GET", "/api/models/latest"),
    ("GET", "/api/strategies"),
    ("GET", "/api/k-values"),
    ("GET", "/api/business/customers"),
    ("GET", "/api/recommendations/offers"),
]


@pytest.mark.parametrize("method,path", PROTECTED_ENDPOINTS)
def test_endpoint_sem_token_retorna_403(client, method, path):
    """Todo endpoint protegido deve retornar 403 sem Authorization header."""
    resp = getattr(client, method.lower())(path)
    assert resp.status_code in (401, 403), (
        f"{method} {path} deveria ser protegido, retornou {resp.status_code}"
    )


@pytest.mark.parametrize("method,path", PROTECTED_ENDPOINTS)
def test_endpoint_com_token_invalido_retorna_403(client, method, path):
    """Token inválido deve ser rejeitado com 401/403."""
    resp = getattr(client, method.lower())(
        path, headers={"Authorization": "Bearer token-invalido"}
    )
    assert resp.status_code in (401, 403), (
        f"{method} {path} aceitou token inválido, retornou {resp.status_code}"
    )


# ── Testes de login ───────────────────────────────────────────────────────────

def test_login_credenciais_validas(client):
    """Login com credenciais corretas retorna token JWT."""
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "testpassword123"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_login_senha_errada_retorna_401(client):
    """Login com senha errada deve retornar 401 genérico."""
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "senha-errada"},
    )
    assert resp.status_code == 401
    # Mensagem deve ser genérica — não revelar se usuário existe
    assert resp.json()["detail"] == "Invalid credentials"


def test_login_usuario_inexistente_retorna_401(client):
    """Login com usuário inexistente deve retornar 401 genérico (sem enumerar usuários)."""
    resp = client.post(
        "/api/auth/token",
        json={"username": "nao-existe", "password": "qualquer"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"


def test_login_resposta_nao_revela_existencia_usuario(client):
    """Resposta de usuário inválido e senha inválida devem ser idênticas."""
    resp_bad_user = client.post(
        "/api/auth/token",
        json={"username": "nao-existe", "password": "qualquer"},
    )
    resp_bad_pass = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "errada"},
    )
    assert resp_bad_user.status_code == resp_bad_pass.status_code == 401
    assert resp_bad_user.json()["detail"] == resp_bad_pass.json()["detail"]


# ── Testes de headers de segurança ───────────────────────────────────────────

SECURITY_HEADERS = [
    "X-Content-Type-Options",
    "X-Frame-Options",
    "X-XSS-Protection",
    "Referrer-Policy",
]


@pytest.mark.parametrize("header", SECURITY_HEADERS)
def test_security_header_presente_em_resposta_publica(client, header):
    """Headers de segurança devem estar presentes mesmo em endpoints públicos."""
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "testpassword123"},
    )
    assert header in resp.headers, f"Header '{header}' ausente na resposta"


def test_x_content_type_options_valor(client):
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "testpassword123"},
    )
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"


def test_x_frame_options_valor(client):
    resp = client.post(
        "/api/auth/token",
        json={"username": "testadmin", "password": "testpassword123"},
    )
    assert resp.headers.get("X-Frame-Options") == "DENY"


# ── Testes de CORS ────────────────────────────────────────────────────────────

def test_cors_origem_permitida(client):
    """Origem configurada deve ser aceita pelo CORS."""
    resp = client.options(
        "/api/auth/token",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert resp.status_code in (200, 204)
    assert "access-control-allow-origin" in resp.headers


def test_cors_origem_nao_permitida_nao_refletida(client):
    """Origem não configurada NÃO deve aparecer no header Allow-Origin."""
    resp = client.options(
        "/api/auth/token",
        headers={
            "Origin": "http://atacante.com",
            "Access-Control-Request-Method": "POST",
        },
    )
    allow_origin = resp.headers.get("access-control-allow-origin", "")
    assert allow_origin != "http://atacante.com", (
        "CORS está refletindo origem não autorizada!"
    )
