from typing import Any, List
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.core.redis import get_redis_client
from app.services.api_key_service import (
    ApiKeyCreatedResponseSchema,
    ApiKeyCreateSchema,
    ApiKeyResponseSchema,
    ApiKeyService,
)

router = APIRouter(prefix="/api-keys", tags=["API Key Management"])

admin_permission = RoleChecker([UserRole.ADMIN])
manage_permission = RoleChecker([UserRole.ADMIN, UserRole.ANALYST])


@router.post("", response_model=ApiKeyCreatedResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    data: ApiKeyCreateSchema,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(manage_permission),
) -> Any:
    service = ApiKeyService(db, redis_client)
    api_key, raw_key = await service.create_api_key(tenant_id=tenant_id, data=data)
    response_data = ApiKeyCreatedResponseSchema(
        id=api_key.id,
        tenant_id=api_key.tenant_id,
        name=api_key.name,
        key_prefix=api_key.key_prefix,
        scopes=api_key.scopes,
        is_active=api_key.is_active,
        expires_at=api_key.expires_at,
        created_at=api_key.created_at,
        raw_key=raw_key,
    )
    return response_data


@router.get("", response_model=List[ApiKeyResponseSchema])
async def list_api_keys(
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(manage_permission),
) -> Any:
    service = ApiKeyService(db)
    keys = await service.list_api_keys(tenant_id=tenant_id)
    return [ApiKeyResponseSchema.model_validate(k) for k in keys]


@router.delete("/{key_id}", status_code=status.HTTP_200_OK)
async def revoke_api_key(
    key_id: uuid.UUID,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(admin_permission),
) -> Any:
    service = ApiKeyService(db, redis_client)
    revoked = await service.revoke_api_key(tenant_id=tenant_id, key_id=key_id)
    if not revoked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key {key_id} not found for tenant '{tenant_id}'",
        )
    return {"status": "success", "message": f"API key {key_id} revoked successfully"}
