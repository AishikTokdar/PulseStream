import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_analytics_unauthorized(async_client: AsyncClient):
    response = await async_client.get("/api/v1/analytics?tenant_id=tenant_test")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_analytics_forbidden_for_ingest_role(async_client: AsyncClient, ingest_token_headers: dict):
    # Ingest role is NOT allowed to view analytics (only analyst & admin)
    response = await async_client.get(
        "/api/v1/analytics?tenant_id=tenant_test", headers=ingest_token_headers
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_analytics_allowed_for_analyst_role(async_client: AsyncClient, analyst_token_headers: dict):
    # Analyst role IS allowed to view analytics
    response = await async_client.get(
        "/api/v1/analytics?tenant_id=tenant_test", headers=analyst_token_headers
    )
    # 200 OK (returns empty summary if no DB data present in test runner)
    assert response.status_code == 200
    data = response.json()
    assert data["tenant_id"] == "tenant_test"
    assert "total_events" in data
    assert "hourly_aggregates" in data
