import uuid
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class HourlyAnalytics(Base):
    __tablename__ = "hourly_analytics"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    avg_payload_size: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    top_event_types: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "hour", name="uq_hourly_analytics_tenant_hour"),
        Index("idx_hourly_analytics_tenant_hour", "tenant_id", "hour"),
    )
