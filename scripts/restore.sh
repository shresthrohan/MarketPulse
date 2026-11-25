#!/usr/bin/env sh
# Restore a dump made by scripts/backup.sh, following TimescaleDB's pre/post-restore procedure.
#
#   scripts/restore.sh backups/marketpulse-X.dump               # into a scratch DB "marketpulse_restore_check"
#   CONFIRM=yes scripts/restore.sh backups/marketpulse-X.dump --live
#
# The default is non-destructive: it restores into a scratch database and prints row counts, which is
# how backups are verified. --live replaces the running database: it stops every writer first, drops
# and recreates the database, restores, then starts the services again.
set -eu
C="${COMPOSE:-docker compose}"
dump="${1:?usage: scripts/restore.sh <dump> [--live]}"
mode="${2:-check}"
cd "$(dirname "$0")/.."
[ -s "$dump" ] || { echo "no such dump: $dump" >&2; exit 1; }

# shellcheck disable=SC2086
pg() { $C exec -T postgres sh -c "$1"; }

if [ "$mode" = "--live" ]; then
  [ "${CONFIRM:-}" = "yes" ] || { echo "refusing to replace the live database without CONFIRM=yes" >&2; exit 1; }
  # shellcheck disable=SC2086
  $C stop ingestion processing backend frontend
  target='$POSTGRES_DB'
else
  target=marketpulse_restore_check
fi

pg "dropdb -U \"\$POSTGRES_USER\" --if-exists --force $target && createdb -U \"\$POSTGRES_USER\" $target"
pg "psql -v ON_ERROR_STOP=1 -q -U \"\$POSTGRES_USER\" -d $target -c 'CREATE EXTENSION IF NOT EXISTS timescaledb' -c 'SELECT timescaledb_pre_restore()'" > /dev/null
# shellcheck disable=SC2086
$C exec -T postgres sh -c "pg_restore -U \"\$POSTGRES_USER\" -d $target --no-owner --no-privileges" < "$dump"
pg "psql -v ON_ERROR_STOP=1 -q -U \"\$POSTGRES_USER\" -d $target -c 'SELECT timescaledb_post_restore()'" > /dev/null
pg "psql -U \"\$POSTGRES_USER\" -d $target -c \"SELECT 'instruments' AS table_name, count(*) FROM instruments UNION ALL SELECT 'market_events', count(*) FROM market_events UNION ALL SELECT 'historical_aggregates', count(*) FROM historical_aggregates UNION ALL SELECT 'watchlists', count(*) FROM watchlists\""

if [ "$mode" = "--live" ]; then
  # shellcheck disable=SC2086
  $C up -d --wait
  echo "live database restored from $dump"
  echo "Optional: replay Kafka's retained events newer than the dump (dedup makes this safe), see docs/deployment.md"
else
  echo "restore check OK: $dump restores cleanly into $target (drop it with: dropdb $target)"
fi
