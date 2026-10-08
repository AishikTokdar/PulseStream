from datetime import datetime, timedelta, timezone
import json
from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.analytics import HourlyAnalytics
from app.db.models.event import Event


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


class BreakdownItemSchema(BaseModel):
    key: str
    count: int
    percentage: float


class AnalyticsBreakdownResponseSchema(BaseModel):
    tenant_id: str
    field_path: str
    total_events: int
    breakdown: List[BreakdownItemSchema]
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
                cache_dict = summary.model_dump(mode="json")
                await self.redis.set(cache_key, json.dumps(cache_dict), ex=300)
            except Exception as e:
                print(f"Redis cache set error: {e}")

        return summary

    async def get_payload_breakdown(
        self,
        tenant_id: str,
        field_path: str,
        event_type: Optional[str] = None,
        hours: int = 24,
    ) -> AnalyticsBreakdownResponseSchema:
        clean_event_type = event_type or "all"
        cache_key = f"analytics:breakdown:{tenant_id}:{field_path}:{clean_event_type}:{hours}"

        if self.redis:
            try:
                cached = await self.redis.get(cache_key)
                if cached:
                    parsed = json.loads(cached)
                    parsed["cached"] = True
                    return AnalyticsBreakdownResponseSchema(**parsed)
            except Exception as e:
                print(f"Redis breakdown cache read warning: {e}")

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        # JSONB extraction query
        field_expr = Event.payload[field_path].astext.label("field_val")
        stmt = (
            select(field_expr, func.count(Event.id).label("val_count"))
            .where(
                Event.tenant_id == tenant_id,
                Event.timestamp >= cutoff,
                Event.payload.has_key(field_path),
            )
        )
        if event_type:
            stmt = stmt.where(Event.event_type == event_type)

        stmt = stmt.group_by(field_expr).order_by(func.count(Event.id).desc()).limit(20)

        result = await self.db.execute(stmt)
        rows = result.all()

        total_in_breakdown = sum(row[1] for row in rows)
        items = [
            BreakdownItemSchema(
                key=str(row[0]) if row[0] is not None else "null",
                count=row[1],
                percentage=round((row[1] / total_in_breakdown * 100), 2) if total_in_breakdown > 0 else 0.0,
            )
            for row in rows
        ]

        summary = AnalyticsBreakdownResponseSchema(
            tenant_id=tenant_id,
            field_path=field_path,
            total_events=total_in_breakdown,
            breakdown=items,
            cached=False,
        )

        if self.redis:
            try:
                dump = summary.model_dump(mode="json")
                await self.redis.set(cache_key, json.dumps(dump), ex=180)
            except Exception as e:
                print(f"Redis breakdown cache set warning: {e}")

        return summary
