import json
from datetime import datetime, timezone
from typing import Any, List, Optional
import uuid
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import String, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.event import Event


class EventCreateSchema(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=100)
    event_type: str = Field(..., min_length=1, max_length=100)
    idempotency_key: Optional[str] = Field(None, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[datetime] = None


class EventResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    tenant_id: str
    event_type: str
    idempotency_key: Optional[str] = None
    payload: dict[str, Any]
    timestamp: datetime
    created_at: datetime


class EventBatchResponseSchema(BaseModel):
    ingested_count: int
    skipped_count: int
    events: List[EventResponseSchema]


class EventService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def ingest_event(self, event_in: EventCreateSchema) -> Event:
        # Check idempotency if key provided
        if event_in.idempotency_key:
            query = select(Event).where(
                Event.tenant_id == event_in.tenant_id,
                Event.idempotency_key == event_in.idempotency_key,
            )
            existing = (await self.db.execute(query)).scalar_one_or_none()
            if existing:
                return existing

        payload_text = f"{event_in.event_type} {json.dumps(event_in.payload)}"
        event_ts = event_in.timestamp or datetime.now(timezone.utc)

        db_event = Event(
            tenant_id=event_in.tenant_id,
            event_type=event_in.event_type,
            idempotency_key=event_in.idempotency_key,
            payload=event_in.payload,
            timestamp=event_ts,
            search_vector=func.to_tsvector("english", payload_text),
        )
        self.db.add(db_event)
        await self.db.commit()
        await self.db.refresh(db_event)
        return db_event

    async def ingest_batch(self, events_in: List[EventCreateSchema]) -> EventBatchResponseSchema:
        ingested: List[Event] = []
        skipped_count = 0

        for event_in in events_in:
            if event_in.idempotency_key:
                query = select(Event).where(
                    Event.tenant_id == event_in.tenant_id,
                    Event.idempotency_key == event_in.idempotency_key,
                )
                existing = (await self.db.execute(query)).scalar_one_or_none()
                if existing:
                    skipped_count += 1
                    ingested.append(existing)
                    continue

            payload_text = f"{event_in.event_type} {json.dumps(event_in.payload)}"
            event_ts = event_in.timestamp or datetime.now(timezone.utc)
            db_event = Event(
                tenant_id=event_in.tenant_id,
                event_type=event_in.event_type,
                idempotency_key=event_in.idempotency_key,
                payload=event_in.payload,
                timestamp=event_ts,
                search_vector=func.to_tsvector("english", payload_text),
            )
            self.db.add(db_event)
            ingested.append(db_event)

        await self.db.commit()
        for e in ingested:
            await self.db.refresh(e)

        return EventBatchResponseSchema(
            ingested_count=len(ingested) - skipped_count,
            skipped_count=skipped_count,
            events=[EventResponseSchema.model_validate(e) for e in ingested],
        )

    async def search_events(
        self,
        tenant_id: str,
        query_text: Optional[str] = None,
        event_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Event]:
        stmt = select(Event).where(Event.tenant_id == tenant_id)

        if event_type:
            stmt = stmt.where(Event.event_type == event_type)

        if start_time:
            stmt = stmt.where(Event.timestamp >= start_time)

        if end_time:
            stmt = stmt.where(Event.timestamp <= end_time)

        if query_text:
            formatted_query = func.plainto_tsquery("english", query_text)
            stmt = stmt.where(
                (Event.search_vector.bool_op("@@")(formatted_query))
                | (func.to_tsvector("english", Event.payload.cast(String)).bool_op("@@")(formatted_query))
            )

        stmt = stmt.order_by(Event.timestamp.desc()).limit(limit).offset(offset)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
