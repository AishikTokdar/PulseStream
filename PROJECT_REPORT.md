# PulseStream Platform - Technical Project Report

- **Document Version**: 1.0.0
- **Author**: Aishik Tokdar
- **Repository**: https://github.com/AishikTokdar/PulseStream
- **License**: MIT License
- **Status**: Production-Ready / Active Specification

---

## Executive Summary

PulseStream is an enterprise-grade, high-concurrency real-time event ingestion, processing, and analytics platform engineered by Aishik Tokdar. Built with modern Python 3.11+ backend technologies, the platform is designed to ingest high-frequency event data streams via REST and WebSockets, process records asynchronously using RabbitMQ-backed Celery workers, perform PostgreSQL full-text search over JSONB payloads, cache pre-computed analytics in Redis, export data archives to MinIO object storage, and expose complete telemetry metrics across OpenTelemetry, Prometheus, Grafana, Loki, and Tempo.

This project report serves as the authoritative architectural specification and maintenance manual for PulseStream. It is structured to accommodate future feature alterations, performance enhancements, and system extensions.

---

## 1. System Requirements and Specifications

### 1.1 Functional Requirements Matrix
- **Authentication and RBAC**: Secure user registration, OAuth2 Password Flow authentication, JWT access/refresh token generation, and role-based access control (`admin`, `analyst`, `ingest`).
- **High-Concurrency REST Ingestion**: Single (`POST /api/v1/events`) and batch (`POST /api/v1/events/batch`) event ingestion with payload validation, sliding-window rate limiting, and idempotency protection.
- **Real-Time Streaming**: Persistent full-duplex WebSockets (`/api/v1/ws/events`) broadcasting live ingested events to multi-tenant subscribers.
- **Database & Full-Text Search**: Asynchronous PostgreSQL storage using SQLAlchemy 2.0 ORM and GIN-indexed TSVector inverted search over event types and JSONB payloads.
- **Asynchronous Background Workers**: RabbitMQ task queue routing tasks to Celery workers for payload enrichment and retention pruning.
- **Periodic Metrics Aggregations**: Celery Beat scheduled jobs computing hourly aggregate metrics (`event_count`, `avg_payload_size`, `top_event_types`) stored in PostgreSQL and cached in Redis.
- **Sub-Millisecond Analytics Endpoint**: `/api/v1/analytics` serving pre-computed hourly metrics cached in Redis, restricted to `analyst` and `admin` roles.
- **MinIO Object Storage Exports**: `/api/v1/export` endpoint generating JSON event export archives stored in MinIO S3 buckets.
- **Observability Pipeline**: Automatic OpenTelemetry tracing (Tempo), Prometheus metric collection (`/metrics`), Loki log aggregation, and pre-provisioned Grafana dashboards.

### 1.2 Non-Functional Specifications
- **Python Compatibility**: Version independent (`requires-python = ">=3.11"`), supporting Python 3.11 up to the latest Python 3.x releases.
- **Concurrency & Throughput**: ASGI non-blocking event loop capability handling thousands of concurrent HTTP and WebSocket connections per second.
- **Fault Tolerance**: Isolated worker queues, rate-limit protection against denial-of-service, and idempotent event processing.
- **Maintainability**: Clean architecture separation, 100% type annotation coverage, automated formatting (Ruff, Black, isort), static type checking (Mypy), and single-command Docker deployment.

---

## 2. Architecture and Subsystem Specifications

### 2.1 API Gateway and Reverse Proxy Subsystem
- **Component**: Nginx (`nginx/nginx.conf`)
- **Function**: Entry point listening on port 8000. Proxies requests to FastAPI application instances, manages client header forwarding, and handles HTTP-to-WebSocket connection upgrades.

### 2.2 Application Engine Subsystem
- **Component**: FastAPI & Uvicorn (`app/main.py`, `app/api/v1/`)
- **Function**: Handles REST routing, schema validation, dependency injection, and WebSocket subscription lifecycles.

### 2.3 Security and Authorization Subsystem
- **Components**: `app/core/security.py`, `app/core/rbac.py`, `app/api/v1/auth.py`
- **Function**: Hashes passwords using bcrypt, issues signed HS256 JWT tokens, and enforces `RoleChecker` permission guards across protected routes.

### 2.4 Data Persistence and Search Subsystem
- **Components**: PostgreSQL 16, SQLAlchemy 2.0, Alembic (`app/db/models/`, `alembic/`)
- **Function**: Stores relational data (`users`, `events`, `hourly_analytics`). Uses GIN indexes for TSVector search queries and composite indexes for fast tenant/timestamp filtering.

### 2.5 In-Memory Cache and Rate Limiting Subsystem
- **Components**: Redis 7, `app/services/rate_limiter.py`, `app/core/redis.py`
- **Function**: Executes sliding-window rate limiting using Redis Sorted Sets (`ZADD`, `ZREMRANGEBYSCORE`) and caches pre-computed analytics summaries under key `analytics:hourly:{tenant_id}:24`.

### 2.6 Background Worker and Job Scheduling Subsystem
- **Components**: RabbitMQ, Celery, Celery Beat (`app/workers/celery_app.py`, `app/workers/tasks.py`)
- **Function**: RabbitMQ buffers background task messages. Celery workers execute payload enrichment. Celery Beat executes periodic 60-second cron jobs to pre-compute hourly analytics.

### 2.7 Object Storage Export Subsystem
- **Components**: MinIO, `app/core/minio.py`, `app/services/storage_service.py`
- **Function**: Generates JSON file archives from database queries and uploads object streams to MinIO bucket `pulsestream-exports`.

### 2.8 Observability and Telemetry Subsystem
- **Components**: OpenTelemetry SDK, Prometheus, Loki, Tempo, Grafana (`app/observability/otel.py`)
- **Function**: OpenTelemetry auto-instruments FastAPI, SQLAlchemy, Redis, and Celery. Spans ship to Tempo, metrics to Prometheus, logs to Loki, and all are visualized inside pre-configured Grafana dashboards.

---

## 3. Codebase Directory and Component Inventory

| Directory / File | Component Category | Description | Change Impact Level |
| :--- | :--- | :--- | :--- |
| `app/main.py` | Application Core | FastAPI app entry point, lifespan, middleware, OTEL setup | High |
| `app/core/config.py` | Configuration | Pydantic-settings configuration for environment variables | Medium |
| `app/core/database.py` | Database Core | Async SQLAlchemy 2.0 engine, sessionmaker, Base model | High |
| `app/core/redis.py` | Cache Core | Redis async connection manager | Medium |
| `app/core/minio.py` | Storage Core | MinIO S3 client wrapper and bucket initialization | Medium |
| `app/core/security.py` | Security | Passlib bcrypt password hashing, JWT encoding/decoding | High |
| `app/core/rbac.py` | Security | Role definitions (`UserRole`) and `RoleChecker` guards | High |
| `app/db/models/` | Models | SQLAlchemy ORM model definitions (`user`, `event`, `analytics`) | High |
| `app/services/` | Services | Core business logic (auth, event ingestion, rate limit, storage) | High |
| `app/workers/` | Workers | Celery application config, Celery Beat schedule, background tasks | Medium |
| `app/websocket/` | WebSockets | ConnectionManager tracking active socket connections | Medium |
| `app/observability/` | Telemetry | OpenTelemetry TracerProvider and Prometheus exporter setup | Medium |
| `app/api/v1/` | API Routers | Endpoint route definitions for auth, events, analytics, export | High |
| `alembic/` | Migrations | Alembic database migration scripts and env.py | High |
| `tests/` | Testing | Pytest test suite, conftest fixtures, mock dependencies | Low |
| `k6/` | Load Testing | k6 load test script for performance benchmarking | Low |
| `nginx/` | Reverse Proxy | Nginx configuration file supporting HTTP and WebSockets | Medium |
| `docker/` | Docker Infrastructure | Multi-stage Dockerfile and entrypoint script | High |
| `docker-compose.yml` | Container Orchestration | Docker Compose 12-service stack definition | High |

---

## 4. Feature Extension and Modification Guide (For Future Maintenance)

This guide provides step-by-step instructions for future engineers modifying, adding, or refactoring features in PulseStream.

### 4.1 Adding a New API Endpoint
1. Create a new route file in `app/api/v1/` or edit an existing router file.
2. Define the route function with appropriate Pydantic request/response schemas.
3. Inject the required RBAC guard dependency: `Depends(RoleChecker([UserRole.ADMIN]))`.
4. Register the new router inside `app/api/v1/router.py`.
5. Add corresponding integration tests in `tests/`.

### 4.2 Adding or Modifying a Database Model
1. Create or update the model class in `app/db/models/` inheriting from `Base`.
2. Ensure new models are exported in `app/db/models/__init__.py`.
3. Generate a new revision script by executing:
   ```bash
   docker compose exec app alembic revision --autogenerate -m "describe_changes"
   ```
4. Review the generated file in `alembic/versions/` and apply migrations:
   ```bash
   docker compose exec app alembic upgrade head
   ```

### 4.3 Adding a New Celery Background Task or Cron Job
1. Define the task function inside `app/workers/tasks.py` using `@celery_app.task`.
2. Use `SyncSessionLocal()` for synchronous database sessions inside Celery tasks.
3. To make it a periodic cron job, add a schedule entry inside `beat_schedule` in `app/workers/celery_app.py`:
   ```python
   "my-periodic-task": {
       "task": "app.workers.tasks.my_task_name",
       "schedule": 300.0,  # Run every 5 minutes
   }
   ```

### 4.4 Modifying RBAC Roles or Permission Policies
1. Add new role values to `UserRole` enum in `app/core/rbac.py`.
2. Update `RoleChecker` allowed role arrays in `app/api/v1/` route handlers.
3. Update Alembic migrations and database seed script (`scripts/seed_data.py`).

### 4.5 Modifying Rate Limiting or Caching TTL Rules
1. Adjust default rate limiting settings (`RATE_LIMIT_REQUESTS`, `RATE_LIMIT_WINDOW_SECONDS`) in `.env` or `app/core/config.py`.
2. To alter analytics cache TTL, update `ex=300` parameter in `app/services/analytics_service.py` and `app/workers/tasks.py`.

---

## 5. Verification and Quality Audit Results

- **Docker Compose Stack Build**: Verified clean startup of all 12 services (`app`, `postgres`, `redis`, `rabbitmq`, `minio`, `nginx`, `prometheus`, `grafana`, `loki`, `tempo`, `celery_worker`, `celery_beat`).
- **Database Migrations**: Verified smooth execution of initial migration `0001_initial_schema.py`.
- **Database Seed Script**: Verified successful execution of `scripts/seed_data.py`.
- **Pytest Integration Tests**: All unit and endpoint integration tests pass cleanly.
- **k6 Load Tests**: Script `k6/load_test.js` verified with high request throughput and latency thresholds.
- **Static Code Analysis**: Passed `ruff check .` and `mypy app` with 100% type annotation coverage.

---

## 6. Revision and Feature Modification Log

This table tracks version revisions, feature additions, schema updates, and performance improvements as the PulseStream project evolves over time.

| Version | Date | Author | Description of Changes / Extensions | Status |
| :--- | :--- | :--- | :--- | :--- |
| **1.0.0** | 2026-08-05 | Aishik Tokdar | Initial production release: REST/WebSocket event ingestion, PostgreSQL TSVector search, Redis rate limiting, Celery Beat hourly analytics, MinIO export pipeline, OpenTelemetry/Prometheus/Grafana observability stack, Docker Compose orchestration. | Production-Ready |
| **1.1.0** | Pending | Maintainer | *Reserved for future features (e.g., Kafka stream integration, ClickHouse OLAP backend, GraphQL gateway).* | Planned |

---

## Author and License

- **Author**: Aishik Tokdar
- **Repository**: https://github.com/AishikTokdar/PulseStream
- **License**: MIT License - see the [LICENSE](LICENSE) file for complete details.

Copyright (c) 2026 Aishik Tokdar. All rights reserved.
