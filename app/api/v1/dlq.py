from typing import Any, List, Optional
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.core.redis import get_redis_client
from app.services.dlq_service import (
    DeadLetterBatchReplayResponseSchema,
    DeadLetterBatchReplaySchema,
    DeadLetterResponseSchema,
    DeadLetterService,
)

router = APIRouter(prefix="/dlq", tags=["Dead Letter Queue & Replay"])

dlq_permission = RoleChecker(
    [UserRole.ADMIN, UserRole.ANALYST],
    required_scopes=["dlq:read", "dlq:write", "admin"],
)
admin_permission = RoleChecker([UserRole.ADMIN], required_scopes=["admin"])


@router.get("", response_model=List[DeadLetterResponseSchema])
async def list_dead_letter_events(
    tenant_id: str = Query(..., min_length=1, max_length=100),
    status_filter: Optional[str] = Query("quarantined", alias="status"),
    event_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(dlq_permission),
) -> Any:
    service = DeadLetterService(db)
    events = await service.list_quarantined(
        tenant_id=tenant_id,
        status=status_filter,
        event_type=event_type,
        limit=limit,
        offset=offset,
    )
    return [DeadLetterResponseSchema.model_validate(e) for e in events]


@router.get("/{event_id}", response_model=DeadLetterResponseSchema)
async def get_dead_letter_event(
    event_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(dlq_permission),
) -> Any:
    service = DeadLetterService(db)
    event = await service.get_by_id(tenant_id=tenant_id, event_id=event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dead letter event '{event_id}' not found for tenant '{tenant_id}'",
        )
    return event


@router.post("/{event_id}/replay", status_code=status.HTTP_200_OK)
async def replay_dead_letter_event(
    event_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(dlq_permission),
) -> Any:
    service = DeadLetterService(db, redis_client)
    replayed = await service.replay_event(tenant_id=tenant_id, event_id=event_id)
    return {
        "status": "success",
        "message": f"Event {event_id} successfully replayed and ingested",
        "new_event_id": str(replayed.id),
    }


@router.post("/replay-batch", response_model=DeadLetterBatchReplayResponseSchema)
async def replay_dead_letter_batch(
    data: DeadLetterBatchReplaySchema,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(dlq_permission),
) -> Any:
    service = DeadLetterService(db, redis_client)
    result = await service.replay_batch(tenant_id=tenant_id, event_ids=data.event_ids)
    return result


@router.delete("/{event_id}", status_code=status.HTTP_200_OK)
async def discard_dead_letter_event(
    event_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(admin_permission),
) -> Any:
    service = DeadLetterService(db)
    discarded = await service.discard_event(tenant_id=tenant_id, event_id=event_id)
    if not discarded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dead letter event '{event_id}' not found for tenant '{tenant_id}'",
        )
    return {"status": "success", "message": f"Dead letter event {event_id} marked as discarded"}
