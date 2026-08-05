import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ingest_event_unauthorized(async_client: AsyncClient):
    event_data = {
        "tenant_id": "tenant_test",
        "event_type": "page_view",
        "payload": {"url": "/home"},
    }
    response = await async_client.post("/api/v1/events", json=event_data)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_ingest_event_forbidden_role(async_client: AsyncClient, analyst_token_headers: dict):
    # Analyst role is not allowed to ingest events (only ingest & admin)
    event_data = {
        "tenant_id": "tenant_test",
        "event_type": "page_view",
        "payload": {"url": "/home"},
    }
    response = await async_client.post("/api/v1/events", json=event_data, headers=analyst_token_headers)
    assert response.status_code == 403
