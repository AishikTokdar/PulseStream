"""add platform extensions

Revision ID: 0002_add_platform_extensions
Revises: 0001_initial_schema
Create Date: 2026-10-12 21:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '0002_add_platform_extensions'
down_revision: Union[str, None] = '0001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. API Keys Table
    op.create_table(
        'api_keys',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('key_prefix', sa.String(length=20), nullable=False),
        sa.Column('hashed_key', sa.String(length=64), nullable=False),
        sa.Column('scopes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_api_keys_tenant_id'), 'api_keys', ['tenant_id'])
    op.create_index(op.f('ix_api_keys_key_prefix'), 'api_keys', ['key_prefix'])
    op.create_index(op.f('ix_api_keys_hashed_key'), 'api_keys', ['hashed_key'], unique=True)
    op.create_index('idx_api_keys_tenant_active', 'api_keys', ['tenant_id', 'is_active'])

    # 2. Event Schemas Table
    op.create_table(
        'event_schemas',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('json_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('is_strict', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('description', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'event_type', 'version', name='uq_tenant_event_schema_version')
    )
    op.create_index(op.f('ix_event_schemas_tenant_id'), 'event_schemas', ['tenant_id'])
    op.create_index(op.f('ix_event_schemas_event_type'), 'event_schemas', ['event_type'])
    op.create_index('idx_event_schemas_tenant_type', 'event_schemas', ['tenant_id', 'event_type'])

    # 3. Dead Letter Events Table
    op.create_table(
        'dead_letter_events',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('failure_reason', sa.String(length=100), nullable=False),
        sa.Column('error_details', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(length=30), nullable=False, server_default='quarantined'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('replayed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dead_letter_events_tenant_id'), 'dead_letter_events', ['tenant_id'])
    op.create_index(op.f('ix_dead_letter_events_event_type'), 'dead_letter_events', ['event_type'])
    op.create_index(op.f('ix_dead_letter_events_failure_reason'), 'dead_letter_events', ['failure_reason'])
    op.create_index(op.f('ix_dead_letter_events_status'), 'dead_letter_events', ['status'])
    op.create_index('idx_dlq_tenant_status', 'dead_letter_events', ['tenant_id', 'status'])
    op.create_index('idx_dlq_tenant_event_type', 'dead_letter_events', ['tenant_id', 'event_type'])

    # 4. Webhook Subscriptions Table
    op.create_table(
        'webhook_subscriptions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('target_url', sa.String(length=500), nullable=False),
        sa.Column('secret_key', sa.String(length=64), nullable=False),
        sa.Column('event_types', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('description', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_webhook_subscriptions_tenant_id'), 'webhook_subscriptions', ['tenant_id'])
    op.create_index(op.f('ix_webhook_subscriptions_is_active'), 'webhook_subscriptions', ['is_active'])
    op.create_index('idx_webhooks_tenant_active', 'webhook_subscriptions', ['tenant_id', 'is_active'])

    # 5. Webhook Delivery Logs Table
    op.create_table(
        'webhook_delivery_logs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('subscription_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('event_id', sa.String(length=100), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('status_code', sa.Integer(), nullable=True),
        sa.Column('delivery_status', sa.String(length=30), nullable=False),
        sa.Column('duration_ms', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('attempt', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_webhook_delivery_logs_subscription_id'), 'webhook_delivery_logs', ['subscription_id'])
    op.create_index(op.f('ix_webhook_delivery_logs_tenant_id'), 'webhook_delivery_logs', ['tenant_id'])
    op.create_index(op.f('ix_webhook_delivery_logs_event_id'), 'webhook_delivery_logs', ['event_id'])
    op.create_index(op.f('ix_webhook_delivery_logs_delivery_status'), 'webhook_delivery_logs', ['delivery_status'])
    op.create_index('idx_webhook_logs_sub_created', 'webhook_delivery_logs', ['subscription_id', 'created_at'])


def downgrade() -> None:
    op.drop_table('webhook_delivery_logs')
    op.drop_table('webhook_subscriptions')
    op.drop_table('dead_letter_events')
    op.drop_table('event_schemas')
    op.drop_table('api_keys')
