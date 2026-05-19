#!/bin/bash
set -euo pipefail

cd /app

# Wait for Postgres (Superset metadata + traffic tables).
until python -c "import psycopg2; psycopg2.connect(host='${POSTGRES_HOST:-postgres}', port=${POSTGRES_PORT:-5432}, dbname='${POSTGRES_DB:-traffic_viz}', user='${POSTGRES_USER:-superset}', password='${POSTGRES_PASSWORD:-superset}')" 2>/dev/null; do
  echo "Waiting for Postgres..."
  sleep 3
done

superset db upgrade

if ! superset fab list-users 2>/dev/null | grep -q "${SUPERSET_ADMIN_USERNAME:-admin}"; then
  superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@traffic.local}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}"
fi

superset init
python /app/superset_init/bootstrap.py || echo "bootstrap.py warning (datasets can be added in UI)"

exec superset run -h 0.0.0.0 -p 8088 --with-threads
