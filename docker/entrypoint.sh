#!/bin/sh
set -e

echo "Waiting for database connection at ${POSTGRES_SERVER}:${POSTGRES_PORT:-5432}..."

until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_SERVER" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q' >/dev/null 2>&1; do
  >&2 echo "Postgres is unavailable - sleeping..."
  sleep 2
done
echo "PostgreSQL is ready!"

if [ "$RUN_MIGRATIONS" = "true" ]; then
  echo "Running Alembic migrations..."
  alembic upgrade head
fi

echo "Executing container payload: $@"
exec "$@"
