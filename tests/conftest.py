from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.database import get_db
from app.core.rbac import UserRole
from app.core.redis import get_redis_client
from app.core.security import create_access_token
from app.main import app
from app.workers.celery_app import celery_app
from app.workers.tasks import dispatch_webhook_event_task, enrich_and_process_event_task

celery_app.conf.task_always_eager = True
enrich_and_process_event_task.delay = MagicMock()
enrich_and_process_event_task.apply_async = MagicMock()
dispatch_webhook_event_task.delay = MagicMock()
dispatch_webhook_event_task.apply_async = MagicMock()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def override_deps():
    mock_db = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_result.scalars.return_value.first.return_value = None
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute.return_value = mock_result

    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.set.return_value = True

    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[0, 1, 1, True])
    mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipe.__aexit__ = AsyncMock(return_value=None)
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)

    async def _mock_get_db():
        yield mock_db

    async def _mock_get_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _mock_get_db
    app.dependency_overrides[get_redis_client] = _mock_get_redis
    yield
    app.dependency_overrides.clear()


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


@pytest.fixture
def admin_token_headers() -> dict[str, str]:
    token = create_access_token(subject="test_admin_id", role=UserRole.ADMIN.value)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def analyst_token_headers() -> dict[str, str]:
    token = create_access_token(subject="test_analyst_id", role=UserRole.ANALYST.value)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def ingest_token_headers() -> dict[str, str]:
    token = create_access_token(subject="test_ingest_id", role=UserRole.INGEST.value)
    return {"Authorization": f"Bearer {token}"}
