import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import time
from typing import Any, Dict, List
import httpx
import redis
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.db.models.analytics import HourlyAnalytics
from app.db.models.event import Event
from app.db.models.webhook import WebhookDeliveryLog, WebhookSubscription
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

        # PII Sanitization & Enrichment
        from app.services.pii_service import PIIRedactionService
        sanitized_payload, redacted_count = PIIRedactionService.redact_payload(dict(event.payload))
        sanitized_payload["_enriched"] = True
        sanitized_payload["_processed_at"] = datetime.now(timezone.utc).isoformat()
        if redacted_count > 0:
            sanitized_payload["_pii_sanitized"] = True
            sanitized_payload["_pii_fields_masked"] = redacted_count

        event.payload = sanitized_payload
        session.commit()
        return {
            "status": "success",
            "event_id": event_id,
            "tenant_id": event.tenant_id,
            "pii_fields_masked": redacted_count,
        }
    except Exception as e:
        session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        session.close()


@celery_app.task(name="app.workers.tasks.dispatch_webhook_event_task")
def dispatch_webhook_event_task(
    event_id: str, tenant_id: str, event_type: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    """Dispatches webhook payloads to subscribed external URLs with HMAC signatures."""
    session = SyncSessionLocal()
    try:
        stmt = select(WebhookSubscription).where(
            WebhookSubscription.tenant_id == tenant_id,
            WebhookSubscription.is_active.is_(True),
        )
        subs = session.execute(stmt).scalars().all()

        # Filter matching subscriptions
        matched_subs = [
            s for s in subs if "*" in s.event_types or event_type in s.event_types
        ]
        if not matched_subs:
            return {"status": "skipped", "reason": "no_matching_subscriptions"}

        dispatched_count = 0
        now_iso = datetime.now(timezone.utc).isoformat()
        body_dict = {
            "event_id": event_id,
            "tenant_id": tenant_id,
            "event_type": event_type,
            "payload": payload,
            "timestamp": now_iso,
        }
        body_bytes = json.dumps(body_dict, default=str).encode("utf-8")

        with httpx.Client(timeout=10.0) as client:
            for sub in matched_subs:
                signature = hmac.new(
                    sub.secret_key.encode("utf-8"), body_bytes, hashlib.sha256
                ).hexdigest()

                headers = {
                    "Content-Type": "application/json",
                    "X-PulseStream-Signature": f"sha256={signature}",
                    "X-PulseStream-Event": event_type,
                    "X-PulseStream-Delivery": event_id,
                }

                start_time = time.perf_counter()
                delivery_status = "failed"
                status_code = None
                error_msg = None

                try:
                    resp = client.post(sub.target_url, content=body_bytes, headers=headers)
                    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                    status_code = resp.status_code
                    if 200 <= resp.status_code < 300:
                        delivery_status = "delivered"
                    else:
                        error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
                except Exception as exc:
                    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                    error_msg = str(exc)[:200]

                log_entry = WebhookDeliveryLog(
                    subscription_id=sub.id,
                    tenant_id=tenant_id,
                    event_id=event_id,
                    event_type=event_type,
                    status_code=status_code,
                    delivery_status=delivery_status,
                    duration_ms=duration_ms,
                    attempt=1,
                    error_message=error_msg,
                )
                session.add(log_entry)
                dispatched_count += 1

        session.commit()
        return {"status": "success", "dispatched_webhooks": dispatched_count}
    except Exception as e:
        session.rollback()
        print(f"Error dispatching webhooks: {e}")
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
