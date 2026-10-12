import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import secrets
import uuid
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, engine
from app.core.security import get_password_hash
from app.db.models.api_key import ApiKey
from app.db.models.event import Event
from app.db.models.schema import EventSchema
from app.db.models.user import User
from app.db.models.webhook import WebhookSubscription


async def seed_data() -> None:
    print("Seeding initial data into PulseStream database...")
    async with AsyncSessionLocal() as session:
        # 1. Seed Users
        users_to_seed = [
            {
                "email": "admin@pulsestream.com",
                "password": "admin123password",
                "role": "admin",
            },
            {
                "email": "analyst@pulsestream.com",
                "password": "analyst123password",
                "role": "analyst",
            },
            {
                "email": "ingest@pulsestream.com",
                "password": "ingest123password",
                "role": "ingest",
            },
        ]

        for u_data in users_to_seed:
            stmt = select(User).where(User.email == u_data["email"])
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                user = User(
                    email=u_data["email"],
                    hashed_password=get_password_hash(u_data["password"]),
                    role=u_data["role"],
                )
                session.add(user)
                print(f"Created user: {u_data['email']} ({u_data['role']})")

        # 2. Seed Sample Events
        now = datetime.now(timezone.utc)
        sample_events = [
            {
                "tenant_id": "tenant_alpha",
                "event_type": "user_signup",
                "idempotency_key": "idempotency_seed_001",
                "payload": {"user_id": "usr_991", "plan": "enterprise", "country": "US"},
                "timestamp": now - timedelta(minutes=45),
            },
            {
                "tenant_id": "tenant_alpha",
                "event_type": "checkout_completed",
                "idempotency_key": "idempotency_seed_002",
                "payload": {"cart_id": "cart_123", "amount": 299.99, "currency": "USD"},
                "timestamp": now - timedelta(minutes=30),
            },
            {
                "tenant_id": "tenant_alpha",
                "event_type": "page_view",
                "idempotency_key": "idempotency_seed_003",
                "payload": {"page": "/dashboard", "referrer": "google.com"},
                "timestamp": now - timedelta(minutes=15),
            },
            {
                "tenant_id": "tenant_beta",
                "event_type": "sensor_reading",
                "idempotency_key": "idempotency_seed_004",
                "payload": {"sensor_id": "sens_44", "temperature": 72.5, "humidity": 45},
                "timestamp": now - timedelta(minutes=10),
            },
        ]

        for ev_data in sample_events:
            stmt = select(Event).where(
                Event.tenant_id == ev_data["tenant_id"],
                Event.idempotency_key == ev_data["idempotency_key"],
            )
            existing_ev = (await session.execute(stmt)).scalar_one_or_none()
            if not existing_ev:
                payload_text = f"{ev_data['event_type']} {json.dumps(ev_data['payload'])}"
                event = Event(
                    tenant_id=ev_data["tenant_id"],
                    event_type=ev_data["event_type"],
                    idempotency_key=ev_data["idempotency_key"],
                    payload=ev_data["payload"],
                    timestamp=ev_data["timestamp"],
                    search_vector=func.to_tsvector("english", payload_text),
                )
                session.add(event)
                print(f"Created sample event: {ev_data['event_type']} for {ev_data['tenant_id']}")

        # 3. Seed Sample API Keys
        sample_raw_key = "ps_live_alpha_ingest_key_seed_001"
        hashed_sample_key = hashlib.sha256(sample_raw_key.encode("utf-8")).hexdigest()
        stmt_key = select(ApiKey).where(ApiKey.hashed_key == hashed_sample_key)
        existing_key = (await session.execute(stmt_key)).scalar_one_or_none()
        if not existing_key:
            seeded_api_key = ApiKey(
                tenant_id="tenant_alpha",
                name="Alpha Production Ingest Key",
                key_prefix=sample_raw_key[:16],
                hashed_key=hashed_sample_key,
                scopes=["events:write", "events:read", "analytics:read"],
                is_active=True,
            )
            session.add(seeded_api_key)
            print(f"Created seeded API Key for tenant_alpha: {sample_raw_key}")

        # 4. Seed Sample Event Schema
        schema_def = {
            "type": "object",
            "required": ["cart_id", "amount", "currency"],
            "properties": {
                "cart_id": {"type": "string"},
                "amount": {"type": "number", "minimum": 0},
                "currency": {"type": "string", "minLength": 3, "maxLength": 3},
            },
        }
        stmt_schema = select(EventSchema).where(
            EventSchema.tenant_id == "tenant_alpha",
            EventSchema.event_type == "checkout_completed",
        )
        existing_schema = (await session.execute(stmt_schema)).scalar_one_or_none()
        if not existing_schema:
            sample_schema = EventSchema(
                tenant_id="tenant_alpha",
                event_type="checkout_completed",
                version=1,
                json_schema=schema_def,
                is_strict=True,
                description="Strict schema validation for e-commerce checkout events",
            )
            session.add(sample_schema)
            print("Created seeded EventSchema for checkout_completed")

        # 5. Seed Sample Webhook Subscription
        stmt_hook = select(WebhookSubscription).where(
            WebhookSubscription.tenant_id == "tenant_alpha",
            WebhookSubscription.target_url == "https://webhook.site/pulsestream-test",
        )
        existing_hook = (await session.execute(stmt_hook)).scalar_one_or_none()
        if not existing_hook:
            sample_webhook = WebhookSubscription(
                tenant_id="tenant_alpha",
                target_url="https://webhook.site/pulsestream-test",
                secret_key=secrets.token_hex(32),
                event_types=["user_signup", "checkout_completed"],
                is_active=True,
                description="Sample external webhook receiver",
            )
            session.add(sample_webhook)
            print("Created seeded WebhookSubscription for tenant_alpha")

        await session.commit()
        print("Data seeding completed successfully.")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed_data())
