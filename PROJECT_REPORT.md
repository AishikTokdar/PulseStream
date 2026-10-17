# PulseStream Platform - Technical Project Report

- **Document Version**: 1.1.0
- **Author**: Aishik Tokdar
- **Repository**: https://github.com/AishikTokdar/PulseStream
- **License**: MIT License
- **Status**: Production-Ready / Active Specification

---

## Executive Summary

PulseStream is an enterprise-grade, high-concurrency real-time event ingestion, processing, and analytics platform engineered by Aishik Tokdar. Built with modern Python 3.11+ backend technologies, the platform is designed to ingest high-frequency event data streams via REST and WebSockets, process records asynchronously using RabbitMQ-backed Celery workers, perform PostgreSQL full-text search over JSONB payloads, cache pre-computed analytics in Redis, export data archives to MinIO object storage, and expose complete telemetry metrics across OpenTelemetry, Prometheus, Grafana, Loki, and Tempo.

Version 1.1.0 introduces major platform extensions: programmatic API Key management with fine-grained scopes (`X-API-Key`), a dynamic tenant-scoped Event Schema Registry with strict validation, a Dead Letter Queue (DLQ) and event replay engine, an outbound Webhook dispatcher with HMAC-SHA256 signatures, and a PII payload sanitization pipeline.

This project report serves as the authoritative architectural specification and maintenance manual for PulseStream. It is structured to accommodate future feature alterations, performance enhancements, and system extensions.

---

## 1. System Requirements and Specifications

### 1.1 Functional Requirements Matrix
- **Authentication, Scopes & API Keys**: Dual authentication via OAuth2 Password Flow JWT tokens and hashed programmatic API Keys (`X-API-Key`) with granular scopes (`events:write`, `events:read`, `analytics:read`, `schemas:write`, `webhooks:write`).
- **High-Concurrency REST Ingestion**: Single (`POST /api/v1/events`) and batch (`POST /api/v1/events/batch`) event ingestion with payload validation, sliding-window rate limiting, and idempotency protection.
- **Dynamic Event Schema Registry**: Tenant-defined JSON schemas with strict (rejection via HTTP 422) and loose (payload flagging) enforcement modes.
- **Real-Time Streaming**: Persistent full-duplex WebSockets (`/api/v1/ws/events`) broadcasting live ingested events to multi-tenant subscribers.
- **Dead Letter Queue (DLQ) & Quarantine**: Storage and automated replay engine for failed, malformed, or quarantined ingestion payloads (`/api/v1/dlq`).
- **Outbound Webhooks with HMAC**: Asynchronous webhook dispatch to external HTTPS endpoints signed with `HMAC-SHA256` digest headers (`X-PulseStream-Signature`).
- **PII Sanitization & Compliance**: Automated redaction of credit cards, SSNs, phone numbers, and secrets in background worker pipelines.
- **Database & Full-Text Search**: Asynchronous PostgreSQL storage using SQLAlchemy 2.0 ORM and GIN-indexed TSVector inverted search over event types and JSONB payloads.
- **Periodic Metrics Aggregations**: Celery Beat scheduled jobs computing hourly aggregate metrics (`event_count`, `avg_payload_size`, `top_event_types`) stored in PostgreSQL and cached in Redis.
- **Dynamic Breakdown Analytics**: `/api/v1/analytics/breakdown` calculating real-time distributions over arbitrary JSONB payload attributes.
- **MinIO Object Storage Exports**: `/api/v1/export` endpoint generating JSON event export archives stored in MinIO S3 buckets.
- **Observability Pipeline**: Automatic OpenTelemetry tracing (Tempo), Prometheus metric collection (`/metrics`), Loki log aggregation, and pre-provisioned Grafana dashboards.

### 1.2 Non-Functional Specifications
- **Python Compatibility**: Version independent (`requires-python = ">=3.11"`), supporting Python 3.11 up to the latest Python 3.x releases.
- **Concurrency & Throughput**: ASGI non-blocking event loop capability handling thousands of concurrent HTTP and WebSocket connections per second.
- **Fault Tolerance**: Isolated worker queues, rate-limit protection against denial-of-service, dead-letter quarantine, and idempotent event processing.
- **Maintainability**: Clean architecture separation, 100% type annotation coverage, automated formatting (Ruff, Black, isort), static type checking (Mypy), and single-command Docker deployment.

---

## 2. Architecture and Subsystem Specifications

### 2.1 API Gateway and Reverse Proxy Subsystem
- **Component**: Nginx (`nginx/nginx.conf`)
- **Function**: Entry point listening on port 8000. Proxies requests to FastAPI application instances, manages client header forwarding, and handles HTTP-to-WebSocket connection upgrades.

### 2.2 Application Engine Subsystem
- **Component**: FastAPI & Uvicorn (`app/main.py`, `app/api/v1/`)
- **Function**: Handles REST routing, schema validation, dependency injection, and WebSocket subscription lifecycles.

### 2.3 Security, API Key and RBAC Subsystem
- **Components**: `app/core/security.py`, `app/core/rbac.py`, `app/services/api_key_service.py`, `app/api/v1/api_keys.py`
- **Function**: Issues signed HS256 JWT tokens, hashes programmatic API keys via SHA-256, verifies `X-API-Key` headers, and enforces `RoleChecker` and scope permissions.

### 2.4 Event Schema Registry Subsystem
- **Components**: `app/db/models/schema.py`, `app/services/schema_service.py`, `app/api/v1/schemas.py`
- **Function**: Stores versioned JSON schema definitions per tenant, caches active schemas in Redis, and validates incoming event payloads before database write.

### 2.5 Dead Letter Queue & Replay Subsystem
- **Components**: `app/db/models/dlq.py`, `app/services/dlq_service.py`, `app/api/v1/dlq.py`
- **Function**: Quarantines unprocessable or schema-rejected events, provides management APIs for inspection, and enables single/batch replay.

### 2.6 Outbound Webhook Subsystem
- **Components**: `app/db/models/webhook.py`, `app/services/webhook_service.py`, `app/api/v1/webhooks.py`, `app/workers/tasks.py`
- **Function**: Dispatches event payloads asynchronously to subscribed target URLs, signs requests using HMAC-SHA256, and records delivery logs and latency.

### 2.7 Data Persistence and Search Subsystem
- **Components**: PostgreSQL 16, SQLAlchemy 2.0, Alembic (`app/db/models/`, `alembic/`)
- **Function**: Stores relational data (`users`, `events`, `api_keys`, `event_schemas`, `dead_letter_events`, `webhooks`, `hourly_analytics`). Uses GIN indexes for TSVector search queries.

### 2.8 In-Memory Cache and Rate Limiting Subsystem
- **Components**: Redis 7, `app/services/rate_limiter.py`, `app/core/redis.py`
- **Function**: Executes sliding-window rate limiting using Redis Sorted Sets (`ZADD`, `ZREMRANGEBYSCORE`) and caches pre-computed analytics summaries and API keys.

### 2.9 Background Worker and Job Scheduling Subsystem
- **Components**: RabbitMQ, Celery, Celery Beat (`app/workers/celery_app.py`, `app/workers/tasks.py`)
- **Function**: RabbitMQ buffers background task messages. Celery workers execute payload enrichment, PII redaction, and webhook dispatching. Celery Beat executes periodic 60-second rollup jobs.

### 2.10 Object Storage Export Subsystem
- **Components**: MinIO, `app/core/minio.py`, `app/services/storage_service.py`
- **Function**: Generates JSON file archives from database queries and uploads object streams to MinIO bucket `pulsestream-exports`.

### 2.11 Observability and Telemetry Subsystem
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
| `app/core/rbac.py` | Security | Role definitions (`UserRole`), API key header guard, and `RoleChecker` | High |
| `app/db/models/api_key.py` | Models | ApiKey ORM model storing prefix, SHA-256 hash, and scopes | High |
| `app/db/models/schema.py` | Models | EventSchema ORM model storing versioned JSON schemas | High |
| `app/db/models/dlq.py` | Models | DeadLetterEvent ORM model for quarantined events | High |
| `app/db/models/webhook.py` | Models | WebhookSubscription and WebhookDeliveryLog ORM models | High |
| `app/db/models/event.py` | Models | Event model with TSVector and GIN indexing | High |
| `app/db/models/analytics.py` | Models | HourlyAnalytics aggregation model | High |
| `app/db/models/user.py` | Models | User authentication model | High |
| `app/services/api_key_service.py` | Services | API key generation, SHA-256 hashing, and cache lookup | High |
| `app/services/schema_service.py` | Services | Schema registration, caching, and payload validation | High |
| `app/services/dlq_service.py` | Services | Quarantine recording and single/batch replay engine | High |
| `app/services/webhook_service.py` | Services | Webhook subscriptions and HMAC-SHA256 signature utility | High |
| `app/services/pii_service.py` | Services | PII pattern detection and payload masking | Medium |
| `app/services/event_service.py` | Services | Core event ingestion, schema validation, and search | High |
| `app/services/analytics_service.py` | Services | Pre-computed hourly rollups and dynamic breakdown queries | High |
| `app/services/rate_limiter.py` | Services | Redis sliding-window rate limiter | High |
| `app/services/storage_service.py` | Services | MinIO export archiver | High |
| `app/workers/` | Workers | Celery app, Celery Beat schedule, worker tasks, and webhook dispatcher | High |
| `app/websocket/` | WebSockets | ConnectionManager tracking active socket connections | Medium |
| `app/observability/` | Telemetry | OpenTelemetry TracerProvider and Prometheus exporter setup | Medium |
| `app/api/v1/` | API Routers | Endpoint route definitions for auth, api-keys, schemas, events, dlq, webhooks, analytics, export | High |
| `alembic/` | Migrations | Alembic database migration scripts (0001 and 0002) | High |
| `tests/` | Testing | Pytest test suite covering auth, events, analytics, api-keys, schemas, dlq, webhooks | Low |
| `k6/` | Load Testing | k6 load test script for performance benchmarking | Low |
| `nginx/` | Reverse Proxy | Nginx configuration file supporting HTTP and WebSockets | Medium |
| `docker/` | Docker Infrastructure | Multi-stage Dockerfile and entrypoint script | High |
| `docker-compose.yml` | Container Orchestration | Docker Compose 12-service stack definition | High |

---

## 4. Feature Extension and Modification Guide (For Future Maintenance)

### 4.1 Adding a New API Endpoint
1. Create a new route file in `app/api/v1/` or edit an existing router file.
2. Define the route function with appropriate Pydantic request/response schemas.
3. Inject the required RBAC guard dependency: `Depends(RoleChecker([UserRole.ADMIN], required_scopes=["my_scope:write"]))`.
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
3. To make it a periodic cron job, add a schedule entry inside `beat_schedule` in `app/workers/celery_app.py`.

---

## 5. Verification and Quality Audit Results

- **Docker Compose Stack Build**: Clean startup of all 12 services (`app`, `postgres`, `redis`, `rabbitmq`, `minio`, `nginx`, `prometheus`, `grafana`, `loki`, `tempo`, `celery_worker`, `celery_beat`).
- **Database Migrations**: Verified smooth sequential execution of `0001_initial_schema.py` and `0002_add_platform_extensions.py`.
- **Database Seed Script**: Verified execution of `scripts/seed_data.py` generating users, events, API keys, schemas, and webhook receivers.
- **Pytest Integration Tests**: Clean test coverage across auth, events, analytics, api-keys, schemas, dlq, and webhooks.
- **k6 Load Tests**: Benchmark scripts verified with high request throughput and latency thresholds.

---

## 6. Revision and Feature Modification Log

| Version | Date | Author | Description of Changes / Extensions | Status |
| :--- | :--- | :--- | :--- | :--- |
| **1.0.0** | 2026-08-05 | Aishik Tokdar | Initial production release: REST/WebSocket event ingestion, PostgreSQL TSVector search, Redis rate limiting, Celery Beat hourly analytics, MinIO export pipeline, OpenTelemetry/Prometheus/Grafana observability stack, Docker Compose orchestration. | Production-Ready |
| **1.1.0** | 2026-10-17 | Aishik Tokdar | Enterprise platform extension: API Key management with granular scopes, Event Schema Registry with validation, Dead Letter Queue & Replay API, Outbound Webhook dispatcher with HMAC-SHA256, PII redaction pipeline, dynamic breakdown analytics, and Alembic revision 0002. | Production-Ready |
| **1.2.0** | Pending | Maintainer | *Reserved for future features (e.g., Apache Kafka / Redpanda commit log streaming, ClickHouse OLAP backend).* | Planned |

---

## Author and License

- **Author**: Aishik Tokdar
- **Repository**: https://github.com/AishikTokdar/PulseStream
- **License**: MIT License - see the [LICENSE](LICENSE) file for complete details.

Copyright (c) 2026 Aishik Tokdar. All rights reserved.
