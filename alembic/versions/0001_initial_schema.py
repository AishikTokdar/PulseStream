"""initial schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-08-05 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '0001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Users table
    op.create_table(
        'users',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('hashed_password', sa.String(length=255), nullable=False),
        sa.Column('role', sa.String(length=50), nullable=False, server_default='ingest'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_users_email'), 'users', ['email'], unique=True)

    # Events table
    op.create_table(
        'events',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('idempotency_key', sa.String(length=255), nullable=True),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('search_vector', postgresql.TSVECTOR(), nullable=True),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_events_tenant_timestamp', 'events', ['tenant_id', 'timestamp'])
    op.create_index('idx_events_tenant_event_type', 'events', ['tenant_id', 'event_type'])
    op.create_index('idx_events_tenant_idempotency', 'events', ['tenant_id', 'idempotency_key'], unique=True, postgresql_where=sa.text('idempotency_key IS NOT NULL'))
    op.create_index('idx_events_search_vector', 'events', ['search_vector'], postgresql_using='gin')
    op.create_index('idx_events_payload_gin', 'events', ['payload'], postgresql_using='gin')

    # Hourly Analytics table
    op.create_table(
        'hourly_analytics',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', sa.String(length=100), nullable=False),
        sa.Column('hour', sa.DateTime(timezone=True), nullable=False),
        sa.Column('event_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('avg_payload_size', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('top_event_types', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'hour', name='uq_hourly_analytics_tenant_hour')
    )
    op.create_index('idx_hourly_analytics_tenant_hour', 'hourly_analytics', ['tenant_id', 'hour'])


def downgrade() -> None:
    op.drop_index('idx_hourly_analytics_tenant_hour', table_name='hourly_analytics')
    op.drop_table('hourly_analytics')
    op.drop_index('idx_events_payload_gin', table_name='events')
    op.drop_index('idx_events_search_vector', table_name='events')
    op.drop_index('idx_events_tenant_idempotency', table_name='events')
    op.drop_index('idx_events_tenant_event_type', table_name='events')
    op.drop_index('idx_events_tenant_timestamp', table_name='events')
    op.drop_table('events')
    op.drop_index(op.f('ix_users_email'), table_name='users')
    op.drop_table('users')
