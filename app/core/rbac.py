from enum import Enum
from typing import List
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import decode_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


class UserRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    INGEST = "ingest"


class TokenData:
    def __init__(self, user_id: str, role: str, token_type: str):
        self.user_id = user_id
        self.role = role
        self.token_type = token_type


async def get_current_user_payload(token: str = Depends(oauth2_scheme)) -> TokenData:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_token(token)
        user_id: str = payload.get("sub", "")
        role: str = payload.get("role", "")
        token_type: str = payload.get("type", "")
        if not user_id or not role or token_type != "access":
            raise credentials_exception
        return TokenData(user_id=user_id, role=role, token_type=token_type)
    except Exception:
        raise credentials_exception


class RoleChecker:
    def __init__(self, allowed_roles: List[UserRole]):
        self.allowed_roles = [r.value if isinstance(r, UserRole) else r for r in allowed_roles]

    def __call__(self, token_data: TokenData = Depends(get_current_user_payload)) -> TokenData:
        if token_data.role not in self.allowed_roles and token_data.role != UserRole.ADMIN.value:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"User with role '{token_data.role}' does not have sufficient permissions. Required roles: {self.allowed_roles}",
            )
        return token_data
