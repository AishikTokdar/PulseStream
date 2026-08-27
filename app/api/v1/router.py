from fastapi import APIRouter

from app.api.v1.analytics import router as analytics_router
from app.api.v1.api_keys import router as api_keys_router
from app.api.v1.auth import router as auth_router
from app.api.v1.events import router as events_router
from app.api.v1.export import router as export_router
from app.api.v1.health import router as health_router
from app.api.v1.schemas import router as schemas_router
from app.api.v1.websocket import router as ws_router

api_v1_router = APIRouter(prefix="/api/v1")

api_v1_router.include_router(auth_router)
api_v1_router.include_router(api_keys_router)
api_v1_router.include_router(schemas_router)
api_v1_router.include_router(events_router)
api_v1_router.include_router(analytics_router)
api_v1_router.include_router(export_router)
api_v1_router.include_router(ws_router)
api_v1_router.include_router(health_router)
