import json
from datetime import datetime
from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.analytics import HourlyAnalytics


class HourlyAnalyticsResponseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    tenant_id: str
    hour: datetime
    event_count: int
    avg_payload_size: float
    top_event_types: dict[str, int]


class AnalyticsSummaryResponseSchema(BaseModel):
    tenant_id: str
    total_events: int
    overall_avg_payload_size: float
    hourly_aggregates: List[HourlyAnalyticsResponseSchema]
    cached: bool = False


class AnalyticsService:
    def __init__(self, db: AsyncSession, redis_client: Optional[Redis] = None):
        self.db = db
        self.redis = redis_client

    async def get_hourly_analytics(
        self, tenant_id: str, limit: int = 24
    ) -> AnalyticsSummaryResponseSchema:
        cache_key = f"analytics:hourly:{tenant_id}:{limit}"

        # Try fetching from Redis cache first
        if self.redis:
            try:
                cached_data = await self.redis.get(cache_key)
                if cached_data:
                    parsed = json.loads(cached_data)
                    parsed["cached"] = True
                    return AnalyticsSummaryResponseSchema(**parsed)
            except Exception as e:
                print(f"Redis cache read error: {e}")

        # Fetch from PostgreSQL HourlyAnalytics table
        stmt = (
            select(HourlyAnalytics)
            .where(HourlyAnalytics.tenant_id == tenant_id)
            .order_by(HourlyAnalytics.hour.desc())
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        hourly_records = list(result.scalars().all())

        if not hourly_records:
            summary = AnalyticsSummaryResponseSchema(
                tenant_id=tenant_id,
                total_events=0,
                overall_avg_payload_size=0.0,
                hourly_aggregates=[],
                cached=False,
            )
            return summary

        total_events = sum(r.event_count for r in hourly_records)
        weighted_size_sum = sum(r.event_count * r.avg_payload_size for r in hourly_records)
        overall_avg_size = round(weighted_size_sum / total_events, 2) if total_events > 0 else 0.0

        hourly_schemas = [
            HourlyAnalyticsResponseSchema(
                tenant_id=r.tenant_id,
                hour=r.hour,
                event_count=r.event_count,
                avg_payload_size=r.avg_payload_size,
                top_event_types=r.top_event_types,
            )
            for r in hourly_records
        ]

        summary = AnalyticsSummaryResponseSchema(
            tenant_id=tenant_id,
            total_events=total_events,
            overall_avg_payload_size=overall_avg_size,
            hourly_aggregates=hourly_schemas,
            cached=False,
        )

        # Cache in Redis with 300 seconds TTL
        if self.redis:
            try:
                # Convert datetimes to isoformat string for json serialization
                cache_dict = summary.model_dump(mode="json")
                await self.redis.set(cache_key, json.dumps(cache_dict), ex=300)
            except Exception as e:
                print(f"Redis cache set error: {e}")

        return summary
