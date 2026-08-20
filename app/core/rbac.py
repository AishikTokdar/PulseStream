from enum import Enum
from typing import List, Optional
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis_client
from app.core.security import decode_token
from app.services.api_key_service import ApiKeyService

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class UserRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    INGEST = "ingest"


class TokenData:
    def __init__(
        self,
        user_id: str,
        role: str,
        token_type: str,
        tenant_id: Optional[str] = None,
        scopes: Optional[List[str]] = None,
    ):
        self.user_id = user_id
        self.role = role
        self.token_type = token_type
        self.tenant_id = tenant_id
        self.scopes = scopes or []

    def has_scope(self, required_scope: str) -> bool:
        return required_scope in self.scopes or "admin" in self.scopes or "*" in self.scopes


async def get_current_user_payload(
    token: Optional[str] = Depends(oauth2_scheme),
    raw_api_key: Optional[str] = Security(api_key_header),
    db: AsyncSession = Depends(get_db),
    redis_client: Optional[Redis] = Depends(get_redis_client),
) -> TokenData:
    # 1. Check if X-API-Key header is supplied
    if raw_api_key:
        api_key_service = ApiKeyService(db, redis_client)
        validated_key = await api_key_service.validate_api_key(raw_api_key)
        if not validated_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API Key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        # Determine effective role based on scopes
        role = UserRole.INGEST.value
        if "admin" in validated_key.scopes:
            role = UserRole.ADMIN.value
        elif "analytics:read" in validated_key.scopes or "export:create" in validated_key.scopes:
            role = UserRole.ANALYST.value

        return TokenData(
            user_id=f"api_key:{validated_key.id}",
            role=role,
            token_type="api_key",
            tenant_id=validated_key.tenant_id,
            scopes=validated_key.scopes,
        )

    # 2. Check Bearer JWT token
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    try:
        payload = decode_token(token)
        user_id: str = payload.get("sub", "")
        role: str = payload.get("role", "")
        token_type: str = payload.get("type", "")
        tenant_id: Optional[str] = payload.get("tenant_id")
        if not user_id or not role or token_type != "access":
            raise credentials_exception
        return TokenData(
            user_id=user_id,
            role=role,
            token_type=token_type,
            tenant_id=tenant_id,
            scopes=["admin"] if role == UserRole.ADMIN.value else [f"{role}:access"],
        )
    except Exception:
        raise credentials_exception


class RoleChecker:
    def __init__(
        self,
        allowed_roles: List[UserRole],
        required_scopes: Optional[List[str]] = None,
    ):
        self.allowed_roles = [r.value if isinstance(r, UserRole) else r for r in allowed_roles]
        self.required_scopes = required_scopes or []

    def __call__(self, token_data: TokenData = Depends(get_current_user_payload)) -> TokenData:
        # Admin bypass
        if token_data.role == UserRole.ADMIN.value or "admin" in token_data.scopes:
            return token_data

        # If authenticated via API Key, verify required scopes if specified
        if token_data.token_type == "api_key" and self.required_scopes:
            has_matching_scope = any(token_data.has_scope(s) for s in self.required_scopes)
            if not has_matching_scope:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"API Key lacks required scopes: {self.required_scopes}. Available scopes: {token_data.scopes}",
                )
            return token_data

        # Check role permission
        if token_data.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"User with role '{token_data.role}' does not have sufficient permissions. Required roles: {self.allowed_roles}",
            )
        return token_data
