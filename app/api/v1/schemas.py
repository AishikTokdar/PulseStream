from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.rbac import RoleChecker, TokenData, UserRole
from app.core.redis import get_redis_client
from app.services.schema_service import (
    EventSchemaCreateSchema,
    EventSchemaResponseSchema,
    SchemaRegistryService,
)

router = APIRouter(prefix="/schemas", tags=["Event Schema Registry"])

manage_permission = RoleChecker([UserRole.ADMIN, UserRole.ANALYST], required_scopes=["schemas:write", "admin"])
read_permission = RoleChecker(
    [UserRole.ADMIN, UserRole.ANALYST, UserRole.INGEST],
    required_scopes=["schemas:read", "schemas:write", "events:write", "admin"],
)


@router.post("", response_model=EventSchemaResponseSchema, status_code=status.HTTP_201_CREATED)
async def register_event_schema(
    data: EventSchemaCreateSchema,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(manage_permission),
) -> Any:
    service = SchemaRegistryService(db, redis_client)
    schema_record = await service.register_schema(tenant_id=tenant_id, data=data)
    return schema_record


@router.get("", response_model=List[EventSchemaResponseSchema])
async def list_event_schemas(
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    token_data: TokenData = Depends(read_permission),
) -> Any:
    service = SchemaRegistryService(db)
    schemas = await service.list_schemas(tenant_id=tenant_id)
    return [EventSchemaResponseSchema.model_validate(s) for s in schemas]


@router.get("/{event_type}", response_model=EventSchemaResponseSchema)
async def get_event_schema(
    event_type: str,
    tenant_id: str = Query(..., min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    token_data: TokenData = Depends(read_permission),
) -> Any:
    service = SchemaRegistryService(db, redis_client)
    schema = await service.get_latest_schema(tenant_id=tenant_id, event_type=event_type)
    if not schema:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No schema registered for tenant '{tenant_id}' and event type '{event_type}'",
        )
    return schema
