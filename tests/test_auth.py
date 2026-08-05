import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check(async_client: AsyncClient):
    response = await async_client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_auth_me_unauthorized(async_client: AsyncClient):
    response = await async_client.get("/api/v1/auth/me")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_auth_me_authorized(async_client: AsyncClient, admin_token_headers: dict):
    response = await async_client.get("/api/v1/auth/me", headers=admin_token_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "test_admin_id"
    assert data["role"] == "admin"
    assert data["status"] == "authenticated"
