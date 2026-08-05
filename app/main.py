from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_v1_router
from app.core.config import settings
from app.core.redis import close_redis_client, get_redis_client
from app.observability.otel import setup_observability


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup actions
    print(f"Starting {settings.PROJECT_NAME} in environment: {settings.ENVIRONMENT}")
    try:
        await get_redis_client()
    except Exception as e:
        print(f"Redis startup connection warning: {e}")

    yield

    # Shutdown actions
    print("Shutting down PulseStream services...")
    await close_redis_client()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description="High-concurrency real-time event ingestion, processing, and analytics platform",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Set CORS middleware
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Wire OpenTelemetry & Prometheus
setup_observability(app)

# Include Routers
app.include_router(api_v1_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "project": settings.PROJECT_NAME,
        "docs": "/docs",
        "health": "/api/v1/health",
        "version": "0.1.0",
    }
