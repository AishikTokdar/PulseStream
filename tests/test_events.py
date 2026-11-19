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


@pytest.mark.asyncio
async def test_ingest_event_authorized(async_client: AsyncClient, ingest_token_headers: dict):
    event_data = {
        "tenant_id": "tenant_test",
        "event_type": "page_view",
        "idempotency_key": "unique_key_123",
        "payload": {"url": "/home", "user_agent": "Mozilla/5.0"},
    }
    response = await async_client.post("/api/v1/events", json=event_data, headers=ingest_token_headers)
    assert response.status_code == 201
    res_json = response.json()
    assert res_json["event_type"] == "page_view"
    assert res_json["tenant_id"] == "tenant_test"


@pytest.mark.asyncio
async def test_ingest_batch_authorized(async_client: AsyncClient, ingest_token_headers: dict):
    batch_data = [
        {
            "tenant_id": "tenant_test",
            "event_type": "page_view",
            "idempotency_key": f"batch_key_{i}",
            "payload": {"url": f"/page/{i}"},
        }
        for i in range(5)
    ]
    response = await async_client.post("/api/v1/events/batch", json=batch_data, headers=ingest_token_headers)
    assert response.status_code == 201
    res_json = response.json()
    assert res_json["ingested_count"] == 5
    assert res_json["skipped_count"] == 0
    assert len(res_json["events"]) == 5

