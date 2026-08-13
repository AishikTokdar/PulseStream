from datetime import datetime, timezone
import hashlib
import json
import secrets
from typing import Any, List, Optional
import uuid
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.api_key import ApiKey


class ApiKeyCreateSchema(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    scopes: List[str] = Field(default_factory=lambda: ["events:write"])
    expires_at: Optional[datetime] = None


class ApiKeyResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    tenant_id: str
    name: str
    key_prefix: str
    scopes: List[str]
    is_active: bool
    expires_at: Optional[datetime] = None
    created_at: datetime


class ApiKeyCreatedResponseSchema(ApiKeyResponseSchema):
    raw_key: str = Field(..., description="Full secret API key. Only shown once upon creation.")


class ApiKeyService:
    def __init__(self, db: AsyncSession, redis_client: Optional[Redis] = None):
        self.db = db
        self.redis = redis_client

    @staticmethod
    def hash_key(raw_key: str) -> str:
        return hashlib.sha256(raw_key.strip().encode("utf-8")).hexdigest()

    async def create_api_key(
        self, tenant_id: str, data: ApiKeyCreateSchema
    ) -> tuple[ApiKey, str]:
        random_part = secrets.token_hex(24)
        raw_key = f"ps_live_{random_part}"
        key_prefix = raw_key[:16]
        hashed = self.hash_key(raw_key)

        api_key = ApiKey(
            tenant_id=tenant_id,
            name=data.name,
            key_prefix=key_prefix,
            hashed_key=hashed,
            scopes=data.scopes,
            is_active=True,
            expires_at=data.expires_at,
        )
        self.db.add(api_key)
        await self.db.commit()
        await self.db.refresh(api_key)

        return api_key, raw_key

    async def validate_api_key(self, raw_key: str) -> Optional[ApiKeyResponseSchema]:
        if not raw_key.startswith("ps_live_"):
            return None

        hashed = self.hash_key(raw_key)
        cache_key = f"api_key:{hashed}"

        if self.redis:
            try:
                cached = await self.redis.get(cache_key)
                if cached:
                    data = json.loads(cached)
                    return ApiKeyResponseSchema(**data)
            except Exception as e:
                print(f"Redis API key cache read error: {e}")

        stmt = select(ApiKey).where(
            ApiKey.hashed_key == hashed,
            ApiKey.is_active.is_(True),
        )
        result = await self.db.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            return None

        if api_key.expires_at and api_key.expires_at < datetime.now(timezone.utc):
            return None

        key_schema = ApiKeyResponseSchema.model_validate(api_key)

        if self.redis:
            try:
                await self.redis.set(
                    cache_key,
                    json.dumps(key_schema.model_dump(mode="json")),
                    ex=300,
                )
            except Exception as e:
                print(f"Redis API key cache write error: {e}")

        return key_schema

    async def list_api_keys(self, tenant_id: str) -> List[ApiKey]:
        stmt = (
            select(ApiKey)
            .where(ApiKey.tenant_id == tenant_id)
            .order_by(ApiKey.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def revoke_api_key(self, tenant_id: str, key_id: uuid.UUID) -> bool:
        stmt = select(ApiKey).where(
            ApiKey.id == key_id,
            ApiKey.tenant_id == tenant_id,
        )
        result = await self.db.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            return False

        api_key.is_active = False
        await self.db.commit()

        if self.redis:
            try:
                await self.redis.delete(f"api_key:{api_key.hashed_key}")
            except Exception:
                pass

        return True
