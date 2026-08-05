import asyncio
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Dict
import redis
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.db.models.analytics import HourlyAnalytics
from app.db.models.event import Event
from app.workers.celery_app import celery_app

# Synchronous DB engine for Celery tasks
sync_engine = create_engine(settings.SYNC_DATABASE_URL, pool_pre_ping=True)
SyncSessionLocal = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False)


@celery_app.task(name="app.workers.tasks.compute_hourly_analytics_task")
def compute_hourly_analytics_task() -> Dict[str, Any]:
    """Celery Beat periodic job computing pre-aggregated hourly analytics metrics

    and caching results in Redis.
    """
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    lookback_start = current_hour - timedelta(hours=24)

    session = SyncSessionLocal()
    processed_tenants = 0
    try:
        # Get distinct tenants
        tenants_query = select(Event.tenant_id).distinct()
        tenants = session.execute(tenants_query).scalars().all()

        redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

        for tenant_id in tenants:
            # Query events for tenant in lookback window
            events_stmt = select(Event).where(
                Event.tenant_id == tenant_id,
                Event.timestamp >= lookback_start,
            )
            events = session.execute(events_stmt).scalars().all()
            if not events:
                continue

            # Group events by hour
            hourly_groups: Dict[datetime, list[Event]] = {}
            for event in events:
                event_hr = event.timestamp.replace(minute=0, second=0, microsecond=0)
                hourly_groups.setdefault(event_hr, []).append(event)

            hourly_summaries = []
            for hour_ts, ev_list in hourly_groups.items():
                event_count = len(ev_list)
                total_payload_bytes = sum(len(json.dumps(e.payload)) for e in ev_list)
                avg_size = round(total_payload_bytes / event_count, 2) if event_count > 0 else 0.0

                type_counts: Dict[str, int] = {}
                for e in ev_list:
                    type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1

                # Upsert into hourly_analytics
                stmt = (
                    pg_insert(HourlyAnalytics)
                    .values(
                        tenant_id=tenant_id,
                        hour=hour_ts,
                        event_count=event_count,
                        avg_payload_size=avg_size,
                        top_event_types=type_counts,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        constraint="uq_hourly_analytics_tenant_hour",
                        set_={
                            "event_count": event_count,
                            "avg_payload_size": avg_size,
                            "top_event_types": type_counts,
                            "updated_at": now,
                        },
                    )
                )
                session.execute(stmt)

                hourly_summaries.append(
                    {
                        "tenant_id": tenant_id,
                        "hour": hour_ts.isoformat(),
                        "event_count": event_count,
                        "avg_payload_size": avg_size,
                        "top_event_types": type_counts,
                    }
                )

            session.commit()

            # Cache the latest hourly analytics summary in Redis for fast API response
            hourly_summaries.sort(key=lambda x: str(x["hour"]), reverse=True)
            total_events = sum(s["event_count"] for s in hourly_summaries)
            weighted_size_sum = sum(s["event_count"] * s["avg_payload_size"] for s in hourly_summaries)
            overall_avg_size = round(weighted_size_sum / total_events, 2) if total_events > 0 else 0.0

            analytics_cache_data = {
                "tenant_id": tenant_id,
                "total_events": total_events,
                "overall_avg_payload_size": overall_avg_size,
                "hourly_aggregates": hourly_summaries,
            }

            cache_key = f"analytics:hourly:{tenant_id}:24"
            redis_client.set(cache_key, json.dumps(analytics_cache_data), ex=300)
            processed_tenants += 1

        redis_client.close()
        return {"status": "success", "processed_tenants": processed_tenants, "hour": current_hour.isoformat()}
    except Exception as e:
        session.rollback()
        print(f"Error computing hourly analytics: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        session.close()


@celery_app.task(name="app.workers.tasks.enrich_and_process_event_task")
def enrich_and_process_event_task(event_id: str) -> Dict[str, Any]:
    """Background task to asynchronously enrich event payloads."""
    session = SyncSessionLocal()
    try:
        stmt = select(Event).where(Event.id == event_id)
        event = session.execute(stmt).scalar_one_or_none()
        if not event:
            return {"status": "not_found", "event_id": event_id}

        # Simulating enrichment metadata addition
        payload = dict(event.payload)
        payload["_enriched"] = True
        payload["_processed_at"] = datetime.now(timezone.utc).isoformat()

        event.payload = payload
        session.commit()
        return {"status": "success", "event_id": event_id, "tenant_id": event.tenant_id}
    except Exception as e:
        session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        session.close()


@celery_app.task(name="app.workers.tasks.cleanup_old_events_task")
def cleanup_old_events_task(days_retention: int = 90) -> Dict[str, Any]:
    """Periodic task to prune events older than retention period."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_retention)
    session = SyncSessionLocal()
    try:
        result = session.execute(delete(Event).where(Event.created_at < cutoff_date))
        deleted_count = result.rowcount
        session.commit()
        return {"status": "success", "deleted_events_count": deleted_count}
    except Exception as e:
        session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        session.close()
