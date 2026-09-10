from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid
from fastapi import HTTPException, status
from pydantic import BaseModel, ConfigDict
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.dlq import DeadLetterEvent
from app.db.models.event import Event


class DeadLetterResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    tenant_id: str
    event_type: str
    payload: Dict[str, Any]
    failure_reason: str
    error_details: Dict[str, Any]
    retry_count: int
    status: str
    created_at: datetime
    replayed_at: Optional[datetime] = None


class DeadLetterBatchReplaySchema(BaseModel):
    event_ids: List[uuid.UUID]


class DeadLetterBatchReplayResponseSchema(BaseModel):
    replayed_count: int
    failed_count: int
    replayed_ids: List[uuid.UUID]
    failed_details: List[Dict[str, Any]]


class DeadLetterService:
    def __init__(self, db: AsyncSession, redis_client: Optional[Redis] = None):
        self.db = db
        self.redis = redis_client

    async def quarantine_event(
        self,
        tenant_id: str,
        event_type: str,
        payload: Dict[str, Any],
        failure_reason: str,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> DeadLetterEvent:
        dlq_event = DeadLetterEvent(
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
            failure_reason=failure_reason,
            error_details=error_details or {},
            status="quarantined",
            retry_count=0,
        )
        self.db.add(dlq_event)
        await self.db.commit()
        await self.db.refresh(dlq_event)
        return dlq_event

    async def list_quarantined(
        self,
        tenant_id: str,
        status: Optional[str] = "quarantined",
        event_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[DeadLetterEvent]:
        stmt = select(DeadLetterEvent).where(DeadLetterEvent.tenant_id == tenant_id)
        if status:
            stmt = stmt.where(DeadLetterEvent.status == status)
        if event_type:
            stmt = stmt.where(DeadLetterEvent.event_type == event_type)

        stmt = stmt.order_by(DeadLetterEvent.created_at.desc()).limit(limit).offset(offset)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, tenant_id: str, event_id: uuid.UUID) -> Optional[DeadLetterEvent]:
        stmt = select(DeadLetterEvent).where(
            DeadLetterEvent.id == event_id,
            DeadLetterEvent.tenant_id == tenant_id,
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def discard_event(self, tenant_id: str, event_id: uuid.UUID) -> bool:
        event = await self.get_by_id(tenant_id, event_id)
        if not event:
            return False
        event.status = "discarded"
        await self.db.commit()
        return True

    async def replay_event(self, tenant_id: str, event_id: uuid.UUID) -> Event:
        # Avoid circular import by importing EventService inside method
        from app.services.event_service import EventCreateSchema, EventService

        dlq_event = await self.get_by_id(tenant_id, event_id)
        if not dlq_event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dead letter event {event_id} not found for tenant '{tenant_id}'",
            )

        event_service = EventService(self.db, self.redis)
        event_in = EventCreateSchema(
            tenant_id=dlq_event.tenant_id,
            event_type=dlq_event.event_type,
            payload=dlq_event.payload,
        )

        replayed_event = await event_service.ingest_event(event_in)

        dlq_event.status = "replayed"
        dlq_event.replayed_at = datetime.now(timezone.utc)
        dlq_event.retry_count += 1
        await self.db.commit()

        return replayed_event

    async def replay_batch(
        self, tenant_id: str, event_ids: List[uuid.UUID]
    ) -> DeadLetterBatchReplayResponseSchema:
        replayed_ids: List[uuid.UUID] = []
        failed_details: List[Dict[str, Any]] = []

        for e_id in event_ids:
            try:
                await self.replay_event(tenant_id, e_id)
                replayed_ids.append(e_id)
            except Exception as e:
                failed_details.append({"event_id": str(e_id), "error": str(e)})

        return DeadLetterBatchReplayResponseSchema(
            replayed_count=len(replayed_ids),
            failed_count=len(failed_details),
            replayed_ids=replayed_ids,
            failed_details=failed_details,
        )
