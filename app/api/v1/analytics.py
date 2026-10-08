from typing import Any, Optional
from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.core.redis import get_redis_client
from app.services.analytics_service import (
    AnalyticsBreakdownResponseSchema,
    AnalyticsService,
    AnalyticsSummaryResponseSchema,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])

analytics_permission = RoleChecker(
    [UserRole.ANALYST, UserRole.ADMIN],
    required_scopes=["analytics:read", "admin"],
)


@router.get("", response_model=AnalyticsSummaryResponseSchema)
async def get_hourly_analytics(
    tenant_id: str = Query(..., min_length=1, description="Tenant ID to query metrics for"),
    limit: int = Query(24, ge=1, le=168, description="Number of lookback hours"),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(analytics_permission),
) -> Any:
    """Returns pre-computed hourly aggregates (event count, avg payload size, top event types)

    populated by Celery Beat periodic jobs and cached in Redis.
    Accessible to 'analyst' and 'admin' roles only.
    """
    analytics_service = AnalyticsService(db, redis_client)
    return await analytics_service.get_hourly_analytics(tenant_id=tenant_id, limit=limit)


@router.get("/breakdown", response_model=AnalyticsBreakdownResponseSchema)
async def get_payload_breakdown(
    tenant_id: str = Query(..., min_length=1, description="Tenant ID to query"),
    field: str = Query(..., min_length=1, max_length=100, description="Payload JSON attribute key (e.g. country, plan)"),
    event_type: Optional[str] = Query(None, description="Optional event type filter"),
    hours: int = Query(24, ge=1, le=720, description="Lookback window in hours"),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(analytics_permission),
) -> Any:
    """Computes dynamic breakdown aggregations over arbitrary JSONB payload attributes."""
    analytics_service = AnalyticsService(db, redis_client)
    return await analytics_service.get_payload_breakdown(
        tenant_id=tenant_id,
        field_path=field,
        event_type=event_type,
        hours=hours,
    )
