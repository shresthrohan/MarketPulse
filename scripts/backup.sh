#!/usr/bin/env sh
# Logical backup of PostgreSQL/TimescaleDB (pg_dump custom format) into ./backups.
#
#   scripts/backup.sh                                   # local stack
#   COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production" scripts/backup.sh
#
# Keeps the newest $KEEP dumps (default 7). Safe while the stack runs (pg_dump takes a consistent snapshot).
set -eu
C="${COMPOSE:-docker compose}"
KEEP="${KEEP:-7}"
cd "$(dirname "$0")/.."
mkdir -p backups
out="backups/marketpulse-$(date -u +%Y%m%dT%H%M%SZ).dump"
# shellcheck disable=SC2086  # $C is a command line on purpose
$C exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' > "$out.partial"
mv "$out.partial" "$out"  # a dump appears only once complete
ls -1t backups/marketpulse-*.dump | tail -n "+$((KEEP + 1))" | while read -r old; do rm -f -- "$old"; done
echo "backup written: $out ($(du -h "$out" | cut -f1))"
