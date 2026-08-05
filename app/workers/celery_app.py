from celery import Celery
from celery.schedules import crontab

from app.core.config import settings

celery_app = Celery(
    "pulsestream_workers",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    beat_schedule={
        "compute-hourly-analytics-periodic": {
            "task": "app.workers.tasks.compute_hourly_analytics_task",
            "schedule": 60.0,  # Every 60 seconds for demo/real-time updates
        },
        "cleanup-old-events-periodic": {
            "task": "app.workers.tasks.cleanup_old_events_task",
            "schedule": crontab(hour=3, minute=0),  # Daily at 3:00 AM UTC
        },
    },
)
