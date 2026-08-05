from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis_client

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "service": "PulseStream"}


@router.get("/liveness")
async def liveness_check() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/readiness")
async def readiness_check(
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
) -> Any:
    status_details: dict[str, str] = {}
    is_ready = True

    # Check Database
    try:
        await db.execute(text("SELECT 1"))
        status_details["database"] = "connected"
    except Exception as e:
        status_details["database"] = f"unhealthy: {e}"
        is_ready = False

    # Check Redis
    try:
        ping = await redis_client.ping()
        if ping:
            status_details["redis"] = "connected"
        else:
            status_details["redis"] = "unresponsive"
            is_ready = False
    except Exception as e:
        status_details["redis"] = f"unhealthy: {e}"
        is_ready = False

    if not is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not_ready", "details": status_details},
        )

    return {"status": "ready", "details": status_details}
