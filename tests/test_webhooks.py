import pytest
from httpx import AsyncClient

from app.services.webhook_service import WebhookService


def test_webhook_hmac_signature():
    secret = "my_secret_key_12345"
    payload = b'{"event":"test","status":"ok"}'
    sig1 = WebhookService.compute_signature(secret, payload)
    sig2 = WebhookService.compute_signature(secret, payload)
    assert sig1 == sig2
    assert len(sig1) == 64

    # Different secret yields different signature
    diff_sig = WebhookService.compute_signature("other_secret", payload)
    assert sig1 != diff_sig


@pytest.mark.asyncio
async def test_create_webhook_unauthorized(async_client: AsyncClient):
    payload = {
        "target_url": "https://example.com/webhook",
        "event_types": ["user_signup"],
    }
    response = await async_client.post("/api/v1/webhooks?tenant_id=tenant_test", json=payload)
    assert response.status_code == 401
