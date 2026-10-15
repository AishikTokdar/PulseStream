import pytest
from httpx import AsyncClient

from app.services.schema_service import SchemaRegistryService


def test_schema_payload_validation_logic():
    schema = {
        "required": ["user_id", "plan"],
        "properties": {
            "user_id": {"type": "string"},
            "plan": {"type": "string", "minLength": 3},
            "age": {"type": "integer", "minimum": 18},
        },
    }

    # Valid payload
    valid_payload = {"user_id": "usr_100", "plan": "pro", "age": 25}
    is_valid, errors = SchemaRegistryService.validate_payload(valid_payload, schema)
    assert is_valid is True
    assert len(errors) == 0

    # Missing required field
    missing_payload = {"plan": "pro"}
    is_valid, errors = SchemaRegistryService.validate_payload(missing_payload, schema)
    assert is_valid is False
    assert any("Missing required field" in err for err in errors)

    # Invalid type & constraint violation
    invalid_payload = {"user_id": "usr_100", "plan": "p", "age": 16}
    is_valid, errors = SchemaRegistryService.validate_payload(invalid_payload, schema)
    assert is_valid is False
    assert len(errors) == 2


@pytest.mark.asyncio
async def test_register_schema_unauthorized(async_client: AsyncClient):
    schema_payload = {
        "event_type": "checkout",
        "json_schema": {"type": "object"},
        "is_strict": True,
    }
    response = await async_client.post("/api/v1/schemas?tenant_id=tenant_test", json=schema_payload)
    assert response.status_code == 401
