#!/usr/bin/env bash
# Deploy or upgrade MarketPulse on the production host (run from the repository checkout there).
#
#   scripts/deploy.sh <git ref>      e.g. scripts/deploy.sh origin/main, or a commit SHA from CI
#
# Steps: back up the database, check out the ref, build, start with health checks, smoke-test the
# public URL. If any step after the checkout fails, it checks out the previous commit and brings
# that version back up, then exits non-zero. Migrations are forward-only: if a new migration ran and
# the old code cannot use the new schema, restore the backup taken in step 1 (docs/deployment.md).
set -euo pipefail
cd "$(dirname "$0")/.."
ref="${1:?usage: scripts/deploy.sh <git ref>}"
[ -f .env.production ] || { echo ".env.production is missing (copy deploy/.env.production.example)" >&2; exit 1; }
host=$(grep -E '^PUBLIC_HOST=' .env.production | cut -d= -f2-)
[ -n "$host" ] || { echo "PUBLIC_HOST is empty in .env.production" >&2; exit 1; }
C="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production"
export COMPOSE="$C"

previous=$(git rev-parse HEAD)
up() { $C up -d --build --remove-orphans --wait --wait-timeout 600; }
smoke() {
  for _ in $(seq 1 30); do
    curl -fsS --max-time 10 "https://$host/api/health" > /dev/null && return 0
    sleep 2
  done
  return 1
}
rollback() {
  echo "deploy of $ref failed; rolling back to $previous" >&2
  git checkout --quiet --detach "$previous"
  up && smoke && echo "rolled back to $previous" >&2
  exit 1
}

if [ -n "$($C ps -q postgres 2>/dev/null)" ]; then
  scripts/backup.sh
fi
git fetch --quiet origin
git checkout --quiet --detach "$ref"
echo "deploying $(git rev-parse --short HEAD) to https://$host"
up || rollback
smoke || rollback
echo "deployed $(git rev-parse --short HEAD); https://$host/api/health is healthy"
