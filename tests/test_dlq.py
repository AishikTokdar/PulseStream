import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_dlq_events_unauthorized(async_client: AsyncClient):
    response = await async_client.get("/api/v1/dlq?tenant_id=tenant_test")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_dlq_replay_unauthorized(async_client: AsyncClient):
    fake_id = "00000000-0000-0000-0000-000000000001"
    response = await async_client.post(f"/api/v1/dlq/{fake_id}/replay?tenant_id=tenant_test")
    assert response.status_code == 401
