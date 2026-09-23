from datetime import datetime
import hashlib
import hmac
import json
import secrets
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.webhook import WebhookDeliveryLog, WebhookSubscription


class WebhookCreateSchema(BaseModel):
    target_url: str = Field(..., min_length=10, max_length=500)
    event_types: List[str] = Field(default_factory=lambda: ["*"], description="Subscribed event types or '*' for all")
    description: Optional[str] = Field(None, max_length=255)


class WebhookResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    tenant_id: str
    target_url: str
    event_types: List[str]
    is_active: bool
    description: Optional[str] = None
    created_at: datetime
    secret_key: str = Field(..., description="HMAC secret used for signature verification")


class WebhookLogResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    subscription_id: uuid.UUID
    tenant_id: str
    event_id: str
    event_type: str
    status_code: Optional[int] = None
    delivery_status: str
    duration_ms: float
    attempt: int
    error_message: Optional[str] = None
    created_at: datetime


class WebhookService:
    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def generate_secret() -> str:
        return secrets.token_hex(32)

    @staticmethod
    def compute_signature(secret_key: str, payload_bytes: bytes) -> str:
        return hmac.new(
            secret_key.encode("utf-8"), payload_bytes, hashlib.sha256
        ).hexdigest()

    async def create_subscription(
        self, tenant_id: str, data: WebhookCreateSchema
    ) -> WebhookSubscription:
        secret = self.generate_secret()
        sub = WebhookSubscription(
            tenant_id=tenant_id,
            target_url=data.target_url,
            secret_key=secret,
            event_types=data.event_types,
            is_active=True,
            description=data.description,
        )
        self.db.add(sub)
        await self.db.commit()
        await self.db.refresh(sub)
        return sub

    async def list_subscriptions(self, tenant_id: str) -> List[WebhookSubscription]:
        stmt = (
            select(WebhookSubscription)
            .where(WebhookSubscription.tenant_id == tenant_id)
            .order_by(WebhookSubscription.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def delete_subscription(self, tenant_id: str, sub_id: uuid.UUID) -> bool:
        stmt = select(WebhookSubscription).where(
            WebhookSubscription.id == sub_id,
            WebhookSubscription.tenant_id == tenant_id,
        )
        result = await self.db.execute(stmt)
        sub = result.scalar_one_or_none()
        if not sub:
            return False

        await self.db.delete(sub)
        await self.db.commit()
        return True

    async def get_active_subscriptions(
        self, tenant_id: str, event_type: str
    ) -> List[WebhookSubscription]:
        stmt = select(WebhookSubscription).where(
            WebhookSubscription.tenant_id == tenant_id,
            WebhookSubscription.is_active.is_(True),
        )
        result = await self.db.execute(stmt)
        all_subs = result.scalars().all()

        matching: List[WebhookSubscription] = []
        for s in all_subs:
            if "*" in s.event_types or event_type in s.event_types:
                matching.append(s)
        return matching

    async def get_delivery_logs(
        self, tenant_id: str, sub_id: uuid.UUID, limit: int = 50
    ) -> List[WebhookDeliveryLog]:
        stmt = (
            select(WebhookDeliveryLog)
            .where(
                WebhookDeliveryLog.tenant_id == tenant_id,
                WebhookDeliveryLog.subscription_id == sub_id,
            )
            .order_by(WebhookDeliveryLog.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
