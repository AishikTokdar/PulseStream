from datetime import datetime
from typing import Any, List, Optional
from fastapi import APIRouter, Depends, Query, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.core.redis import get_redis_client
from app.services.event_service import (
    EventBatchResponseSchema,
    EventCreateSchema,
    EventResponseSchema,
    EventService,
)
from app.services.rate_limiter import RateLimiter
from app.workers.tasks import dispatch_webhook_event_task, enrich_and_process_event_task
from app.websocket.manager import ws_manager

router = APIRouter(prefix="/events", tags=["Event Ingestion & Search"])

ingest_permission = RoleChecker(
    [UserRole.INGEST, UserRole.ADMIN], required_scopes=["events:write"]
)
query_permission = RoleChecker(
    [UserRole.INGEST, UserRole.ANALYST, UserRole.ADMIN], required_scopes=["events:read", "events:write"]
)



@router.post("", response_model=EventResponseSchema, status_code=status.HTTP_201_CREATED)
async def ingest_single_event(
    event_in: EventCreateSchema,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(ingest_permission),
) -> Any:
    # Rate Limiting per tenant
    rate_limiter = RateLimiter(redis_client)
    await rate_limiter.check_rate_limit(f"tenant:{event_in.tenant_id}")

    event_service = EventService(db, redis_client)
    event = await event_service.ingest_event(event_in)

    # Broadcast event via WebSockets
    await ws_manager.broadcast_to_tenant(
        event_in.tenant_id,
        {
            "type": "event_ingested",
            "event_id": str(event.id),
            "tenant_id": event.tenant_id,
            "event_type": event.event_type,
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
            "payload": event.payload,
        },
    )

    # Enqueue background enrichment and webhook Celery tasks
    try:
        enrich_and_process_event_task.delay(str(event.id))
        dispatch_webhook_event_task.delay(
            str(event.id), event.tenant_id, event.event_type, event.payload
        )
    except Exception as e:
        print(f"Celery task dispatch warning: {e}")

    return event


@router.post("/batch", response_model=EventBatchResponseSchema, status_code=status.HTTP_201_CREATED)
async def ingest_batch_events(
    events_in: List[EventCreateSchema],
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(ingest_permission),
) -> Any:
    if not events_in:
        return EventBatchResponseSchema(ingested_count=0, skipped_count=0, events=[])

    # Rate limiting on the first tenant in batch
    tenant_id = events_in[0].tenant_id
    rate_limiter = RateLimiter(redis_client)
    await rate_limiter.check_rate_limit(f"tenant:{tenant_id}")

    event_service = EventService(db, redis_client)
    batch_result = await event_service.ingest_batch(events_in)

    # Broadcast batch notification to WebSockets and trigger worker tasks
    if batch_result.events:
        await ws_manager.broadcast_to_tenant(
            tenant_id,
            {
                "type": "batch_ingested",
                "tenant_id": tenant_id,
                "count": batch_result.ingested_count,
            },
        )
        for ev in batch_result.events:
            try:
                enrich_and_process_event_task.delay(str(ev.id))
                dispatch_webhook_event_task.delay(
                    str(ev.id), ev.tenant_id, ev.event_type, ev.payload
                )
            except Exception:
                pass

    return batch_result


@router.get("/search", response_model=List[EventResponseSchema])
async def search_events(
    tenant_id: str = Query(..., min_length=1),
    q: Optional[str] = Query(None, description="Full-text search query"),
    event_type: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(query_permission),
) -> Any:
    event_service = EventService(db)
    events = await event_service.search_events(
        tenant_id=tenant_id,
        query_text=q,
        event_type=event_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )
    return events
