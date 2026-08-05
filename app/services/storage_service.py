import json
from datetime import datetime, timezone
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.minio import minio_client
from app.db.models.event import Event


class StorageService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def export_tenant_events_to_minio(
        self, tenant_id: str, limit: int = 1000
    ) -> dict[str, str]:
        stmt = (
            select(Event)
            .where(Event.tenant_id == tenant_id)
            .order_by(Event.timestamp.desc())
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        events = result.scalars().all()

        events_data = [
            {
                "id": str(e.id),
                "tenant_id": e.tenant_id,
                "event_type": e.event_type,
                "idempotency_key": e.idempotency_key,
                "payload": e.payload,
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ]

        json_bytes = json.dumps(events_data, indent=2).encode("utf-8")
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        object_name = f"exports/{tenant_id}/events_{timestamp_str}_{uuid.uuid4().hex[:8]}.json"

        file_url = minio_client.upload_file_bytes(
            object_name=object_name,
            data=json_bytes,
            content_type="application/json",
        )

        return {
            "tenant_id": tenant_id,
            "exported_count": str(len(events_data)),
            "object_name": object_name,
            "file_url": file_url,
        }
