from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional, Tuple
import uuid
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.schema import EventSchema


class EventSchemaCreateSchema(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=100)
    json_schema: Dict[str, Any] = Field(..., description="JSON Schema definition")
    is_strict: bool = Field(default=True, description="If true, reject invalid payloads with HTTP 422")
    description: Optional[str] = Field(None, max_length=255)


class EventSchemaResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    tenant_id: str
    event_type: str
    version: int
    json_schema: Dict[str, Any]
    is_strict: bool
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class SchemaRegistryService:
    def __init__(self, db: AsyncSession, redis_client: Optional[Redis] = None):
        self.db = db
        self.redis = redis_client

    async def register_schema(
        self, tenant_id: str, data: EventSchemaCreateSchema
    ) -> EventSchema:
        # Check current latest version
        stmt = (
            select(EventSchema)
            .where(
                EventSchema.tenant_id == tenant_id,
                EventSchema.event_type == data.event_type,
            )
            .order_by(EventSchema.version.desc())
        )
        existing = (await self.db.execute(stmt)).scalars().first()
        next_version = (existing.version + 1) if existing else 1

        schema_record = EventSchema(
            tenant_id=tenant_id,
            event_type=data.event_type,
            version=next_version,
            json_schema=data.json_schema,
            is_strict=data.is_strict,
            description=data.description,
        )
        self.db.add(schema_record)
        await self.db.commit()
        await self.db.refresh(schema_record)

        # Invalidate/update Redis cache
        if self.redis:
            cache_key = f"schema:{tenant_id}:{data.event_type}"
            try:
                dump = EventSchemaResponseSchema.model_validate(schema_record).model_dump(mode="json")
                await self.redis.set(cache_key, json.dumps(dump), ex=600)
            except Exception as e:
                print(f"Redis schema cache write error: {e}")

        return schema_record

    async def get_latest_schema(
        self, tenant_id: str, event_type: str
    ) -> Optional[EventSchemaResponseSchema]:
        cache_key = f"schema:{tenant_id}:{event_type}"
        if self.redis:
            try:
                cached = await self.redis.get(cache_key)
                if cached:
                    return EventSchemaResponseSchema(**json.loads(cached))
            except Exception as e:
                print(f"Redis schema cache read error: {e}")

        stmt = (
            select(EventSchema)
            .where(
                EventSchema.tenant_id == tenant_id,
                EventSchema.event_type == event_type,
            )
            .order_by(EventSchema.version.desc())
        )
        result = await self.db.execute(stmt)
        record = result.scalars().first()

        if not record:
            return None

        schema_resp = EventSchemaResponseSchema.model_validate(record)
        if self.redis:
            try:
                await self.redis.set(
                    cache_key,
                    json.dumps(schema_resp.model_dump(mode="json")),
                    ex=600,
                )
            except Exception:
                pass

        return schema_resp

    async def list_schemas(self, tenant_id: str) -> List[EventSchema]:
        stmt = (
            select(EventSchema)
            .where(EventSchema.tenant_id == tenant_id)
            .order_by(EventSchema.event_type.asc(), EventSchema.version.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    def validate_payload(
        payload: Dict[str, Any], json_schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Validates payload against JSON schema properties and required fields."""
        errors: List[str] = []
        required_fields = json_schema.get("required", [])
        for field in required_fields:
            if field not in payload:
                errors.append(f"Missing required field: '{field}'")

        properties = json_schema.get("properties", {})
        type_mapping = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        for prop_name, prop_rules in properties.items():
            if prop_name not in payload:
                continue

            value = payload[prop_name]
            expected_type = prop_rules.get("type")

            if expected_type in type_mapping:
                expected_cls = type_mapping[expected_type]
                # boolean is subclass of int in python; distinguish them
                if expected_type == "integer" and isinstance(value, bool):
                    errors.append(f"Field '{prop_name}' expected integer, got boolean")
                elif expected_type == "number" and isinstance(value, bool):
                    errors.append(f"Field '{prop_name}' expected number, got boolean")
                elif not isinstance(value, expected_cls):
                    errors.append(f"Field '{prop_name}' expected type '{expected_type}', got '{type(value).__name__}'")

            # Min/max constraints
            if isinstance(value, (int, float)):
                if "minimum" in prop_rules and value < prop_rules["minimum"]:
                    errors.append(f"Field '{prop_name}' value {value} is less than minimum {prop_rules['minimum']}")
                if "maximum" in prop_rules and value > prop_rules["maximum"]:
                    errors.append(f"Field '{prop_name}' value {value} exceeds maximum {prop_rules['maximum']}")

            if isinstance(value, str):
                if "minLength" in prop_rules and len(value) < prop_rules["minLength"]:
                    errors.append(f"Field '{prop_name}' length is below minLength {prop_rules['minLength']}")
                if "maxLength" in prop_rules and len(value) > prop_rules["maxLength"]:
                    errors.append(f"Field '{prop_name}' length exceeds maxLength {prop_rules['maxLength']}")

        return len(errors) == 0, errors
