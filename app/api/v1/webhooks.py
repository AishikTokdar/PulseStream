from typing import Any, List
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.services.webhook_service import (
    WebhookCreateSchema,
    WebhookLogResponseSchema,
    WebhookResponseSchema,
    WebhookService,
)

router = APIRouter(prefix="/webhooks", tags=["Webhook Subscriptions"])

manage_permission = RoleChecker(
    [UserRole.ADMIN, UserRole.ANALYST],
    required_scopes=["webhooks:write", "admin"],
)
read_permission = RoleChecker(
    [UserRole.ADMIN, UserRole.ANALYST],
    required_scopes=["webhooks:read", "webhooks:write", "admin"],
)
admin_permission = RoleChecker([UserRole.ADMIN], required_scopes=["admin"])


@router.post("", response_model=WebhookResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_webhook_subscription(
    data: WebhookCreateSchema,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(manage_permission),
) -> Any:
    service = WebhookService(db)
    sub = await service.create_subscription(tenant_id=tenant_id, data=data)
    return sub


@router.get("", response_model=List[WebhookResponseSchema])
async def list_webhook_subscriptions(
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(read_permission),
) -> Any:
    service = WebhookService(db)
    subs = await service.list_subscriptions(tenant_id=tenant_id)
    return [WebhookResponseSchema.model_validate(s) for s in subs]


@router.delete("/{subscription_id}", status_code=status.HTTP_200_OK)
async def delete_webhook_subscription(
    subscription_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(admin_permission),
) -> Any:
    service = WebhookService(db)
    deleted = await service.delete_subscription(tenant_id=tenant_id, sub_id=subscription_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Webhook subscription {subscription_id} not found for tenant '{tenant_id}'",
        )
    return {"status": "success", "message": f"Webhook subscription {subscription_id} deleted"}


@router.get("/{subscription_id}/logs", response_model=List[WebhookLogResponseSchema])
async def get_webhook_delivery_logs(
    subscription_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(read_permission),
) -> Any:
    service = WebhookService(db)
    logs = await service.get_delivery_logs(tenant_id=tenant_id, sub_id=subscription_id, limit=limit)
    return [WebhookLogResponseSchema.model_validate(l) for l in logs]
