from typing import Any
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.services.storage_service import StorageService

router = APIRouter(prefix="/export", tags=["Data Export"])

export_permission = RoleChecker([UserRole.ANALYST, UserRole.ADMIN])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def export_tenant_events(
    tenant_id: str = Query(..., min_length=1),
    limit: int = Query(1000, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(export_permission),
) -> Any:
    """Exports tenant events to MinIO Object Storage bucket as JSON archive."""
    storage_service = StorageService(db)
    result = await storage_service.export_tenant_events_to_minio(tenant_id=tenant_id, limit=limit)
    return {
        "message": "Export created successfully",
        "details": result,
    }
