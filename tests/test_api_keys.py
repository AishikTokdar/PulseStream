import pytest
from httpx import AsyncClient

from app.services.api_key_service import ApiKeyService


def test_api_key_hash_consistency():
    key = "ps_live_1234567890abcdef1234567890abcdef"
    hash1 = ApiKeyService.hash_key(key)
    hash2 = ApiKeyService.hash_key(key)
    assert hash1 == hash2
    assert len(hash1) == 64


@pytest.mark.asyncio
async def test_create_api_key_unauthorized(async_client: AsyncClient):
    payload = {
        "name": "Test Ingestion Key",
        "scopes": ["events:write"],
    }
    response = await async_client.post("/api/v1/api-keys?tenant_id=tenant_test", json=payload)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_api_keys_requires_auth(async_client: AsyncClient):
    response = await async_client.get("/api/v1/api-keys?tenant_id=tenant_test")
    assert response.status_code == 401
