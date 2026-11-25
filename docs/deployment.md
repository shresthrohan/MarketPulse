# Deployment

MarketPulse deploys as one Docker Compose stack on one Linux host, with Caddy terminating HTTPS.
That is the simplest reliable layout for this architecture: Kafka, two always-on workers and a WebSocket
server need long-running processes, which serverless or static hosting (e.g. Vercel) cannot provide.
No Kubernetes: one host does not need an orchestrator.

> Status: the production configuration is built and verified automatically in CI (`production-config` job:
> runtime images, HTTPS, WSS, health, port exposure). No public instance is running. Deploying one needs
> a server (below), so there is no public URL to report.

## Architecture

```
                 Internet
                    │ 80/443 only
            ┌───────▼────────┐   Let's Encrypt certificate (auto-renewed), HTTP→HTTPS, HSTS
            │     Caddy      │
            └───────┬────────┘
            ┌───────▼────────┐   static React build + reverse proxy for /api and /ws
            │     nginx      │   (security headers; re-resolves the backend on restarts)
            └───────┬────────┘
            ┌───────▼────────┐   REST + WebSocket, rate limits and WS caps per real client IP
            │    FastAPI     │
            └──┬─────────┬───┘
   ┌───────────┘         └────────────┐            private Docker network 172.28.0.0/16
┌──▼───────────┐  ┌─────────┐  ┌──────▼─────┐      nothing below is published on the host
│ PostgreSQL + │  │  Redis  │  │   Kafka    │◄── ingestion (simulator / Binance / Finnhub)
│ TimescaleDB  │  │  (AOF)  │  │  (KRaft)   │──► processing (validate, store, cache, metrics)
└──────────────┘  └─────────┘  └────────────┘
 volumes: pgdata, redisdata, kafkadata, caddy_data (certificates)
```

Files: `docker-compose.yml` (base stack, also used for development and CI) +
`docker-compose.prod.yml` (production overlay) + `infrastructure/caddy/Caddyfile` + `.env.production` (server only).

What the production overlay changes:

| Concern | Production setting |
|---|---|
| Exposure | Only Caddy publishes ports (80, 443). API, nginx, PostgreSQL, Redis and Kafka are internal. |
| Images | `runtime` build targets: no tests, no pytest/httpx; base images pinned by digest; Python dependencies installed with full lock files (`*/requirements.lock`); frontend with `npm ci`. |
| Restarts | `restart: always` (containers come back after crashes and reboots). |
| Health | Health checks on every long-running service; `up --wait` blocks until all are healthy. |
| Memory | Limits sized for a 4 GB host: PostgreSQL 1 GB, Kafka 900 MB (heap 512 MB), backend 768 MB, processing 384 MB, ingestion 256 MB, Redis 320 MB, Caddy 128 MB, nginx 64 MB. |
| Logs | json-file driver, 10 MB × 5 files per container (bounded disk). |
| Client IPs | Backend trusts `X-Forwarded-For` only from the private network (`FORWARDED_ALLOW_IPS`), so rate limits and WebSocket caps use the real client address. |
| CORS | `https://$PUBLIC_HOST` only. WebSocket handshakes from other browser origins are refused. |
| Retention | 7 days of events (sized for a 40 GB disk at ~0.5 M events/day). |

## Required cloud resources

One x86_64 VM, Ubuntu 24.04 LTS, **2 vCPU / 4 GB RAM / 40 GB SSD**, a public IPv4 address, inbound
22 (SSH, ideally restricted to your IP), 80 and 443. A DNS name pointing at it (your domain, or a free
`<ip-with-dashes>.sslip.io` name).

Approximate monthly prices for that size (check the provider's pricing page; they change):

| Provider | Plan | Approx. cost |
|---|---|---|
| Hetzner Cloud | CX22 (2 vCPU, 4 GB, 40 GB) | €4–6 |
| DigitalOcean | Basic Droplet 4 GB | $24 |
| AWS Lightsail | 4 GB instance | $24 |
| Oracle Cloud | Always Free Ampere A1 (ARM) | $0 (capacity often unavailable; images then build for arm64) |

Outbound internet is needed for Binance (crypto data) and Let's Encrypt. binance.com refuses US addresses:
for a US-hosted server set `BINANCE_WS_URL=wss://stream.binance.us:9443` and `BINANCE_REST_URL=https://api.binance.us`.

## Environment variables

Production values live only in `/opt/marketpulse/.env.production` on the server (`chmod 600`, git-ignored).
Template: [`deploy/.env.production.example`](../deploy/.env.production.example). Development template: [`.env.example`](../.env.example).

| Variable | Used by | Meaning |
|---|---|---|
| `PUBLIC_HOST` | Caddy, backend (CORS) | Public DNS name served over HTTPS |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | PostgreSQL, backend, processing, migrate | Database credentials (password: letters and digits, e.g. `openssl rand -hex 24`) |
| `PROVIDERS` | ingestion, migrate | Comma-separated: `simulator`, `binance`, `finnhub_ws`, `finnhub_rest` |
| `FINNHUB_API_KEY` | ingestion, migrate | Only for Finnhub; never sent to the browser, never logged |
| `BINANCE_WS_URL` / `BINANCE_REST_URL` | ingestion / migrate | Binance stream and REST hosts |
| `SIM_*`, `SEED_RANDOM_SEED` | ingestion, migrate | Simulator behaviour and the generated history |
| `MAX_FUTURE_SKEW_S`, `MAX_EVENT_AGE_H` | processing | Timestamp acceptance window |
| `*_RETENTION_DAYS` | processing | Data retention per table |
| `RATE_LIMIT_PER_MINUTE` | backend | Requests per client address per minute |
| `WS_MAX_CONNECTIONS`, `WS_MAX_PER_CLIENT`, `WS_QUEUE_SIZE` | backend | WebSocket capacity, per-client cap, slow-client buffer |
| `WATCHLIST_MAX` | backend | Upper bound on shared watchlists |
| `LOG_LEVEL` | all Python services | `INFO` in production |

The frontend needs no build-time configuration: it calls `/api` and `/ws` on its own origin, so the same
build works on any host (in development Vite proxies both to `localhost:8000`).

## First deployment

On the server, as a sudo user:

```bash
# 1. Docker Engine + Compose plugin, a deploy user, firewall
curl -fsSL https://get.docker.com | sudo sh
sudo adduser --disabled-password --gecos "" deploy && sudo usermod -aG docker deploy
sudo ufw allow OpenSSH && sudo ufw allow 80,443/tcp && sudo ufw allow 443/udp && sudo ufw --force enable
#    (Docker publishes only Caddy's 80/443; nothing else is reachable even if the firewall were open.)

# 2. Code and configuration (as deploy)
sudo -iu deploy
git clone https://github.com/shresthrohan/MarketPulse.git /opt/marketpulse   # /opt/marketpulse must be writable by deploy
cd /opt/marketpulse
cp deploy/.env.production.example .env.production && chmod 600 .env.production
sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 24)/" .env.production
nano .env.production            # set PUBLIC_HOST (and providers)

# 3. Deploy (builds, migrates + seeds, starts with health checks, smoke-tests https://PUBLIC_HOST)
scripts/deploy.sh origin/main
```

`scripts/deploy.sh <ref>` backs up the database (when it is running), checks out the ref, runs
`docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production up -d --build --wait`,
then polls `https://$PUBLIC_HOST/api/health`. On failure it checks out the previous commit, brings it back
up and exits non-zero.

## Domain and HTTPS

- **Your domain:** create an `A` record (e.g. `marketpulse.example.com`) pointing at the server IP; set
  `PUBLIC_HOST` to that name.
- **No domain:** `PUBLIC_HOST=<ip-with-dashes>.sslip.io` (for 203.0.113.7: `203-0-113-7.sslip.io`) resolves to
  the IP with no setup.

Caddy obtains and renews the Let's Encrypt certificate automatically (HTTP-01 on port 80), redirects HTTP to
HTTPS with 308 and adds HSTS. Certificates persist in the `caddy_data` volume; do not delete it casually
(Let's Encrypt rate-limits re-issuance). WebSockets work through the same host as `wss://`.

## CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | Checks |
|---|---|
| `python` | ruff lint; unit tests for ingestion, processing, backend and the seed |
| `spark` | Spark job tests in the Spark image |
| `frontend` | `tsc --noEmit`, Vitest, production build |
| `integration` | whole stack via Compose: `alembic check`, backend + pipeline integration tests, 7 failure drills, backup + restore check |
| `production-config` | production overlay on the runner with `PUBLIC_HOST=localhost`: runtime images build, HTTPS + HTTP→HTTPS redirect + HSTS, WSS live quote, every health endpoint, internal ports not published, runtime images contain no tests |
| `deploy` | **only** on `main`, **only** after all jobs above passed, **only** if the repository variable `DEPLOY_HOST` is set; otherwise skipped |

To enable deploys: repository **variables** `DEPLOY_HOST` (the public host name) and optionally
`DEPLOY_USER` (default `deploy`); **secrets** `DEPLOY_SSH_KEY` (a private key whose public half is in
`~deploy/.ssh/authorized_keys` on the server) and `DEPLOY_KNOWN_HOSTS` (output of `ssh-keyscan <server ip>`, so
the runner cannot be pointed at an impostor). Optionally add required reviewers to the `production`
environment for a manual approval gate. The job SSHes in and runs `scripts/deploy.sh <commit sha>`, then checks
`https://DEPLOY_HOST/api/health` from outside.

## Database migrations

Alembic migrations run in the one-shot `migrate` service on every `up`, before any service that uses the
schema starts (`alembic upgrade head`, then the idempotent seed). They are forward-only. `scripts/deploy.sh`
takes a backup first, so a bad migration is recovered by restoring that backup (below) and deploying the
previous commit. Manual run: `$PROD run --rm migrate` with
`PROD="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production"`.

## Backup and restore

```bash
export COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production"
scripts/backup.sh                                  # backups/marketpulse-<UTC>.dump, keeps the newest 7 (KEEP=n)
scripts/restore.sh backups/marketpulse-X.dump      # non-destructive check: restores into a scratch DB, prints counts
CONFIRM=yes scripts/restore.sh backups/marketpulse-X.dump --live   # replace the live DB (stops writers first)
```

- Schedule daily backups, e.g. `crontab -e`:
  `15 3 * * * cd /opt/marketpulse && COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production" scripts/backup.sh >> backups/backup.log 2>&1`
- Copy backups off the host (object storage, another machine): a backup on the same disk does not survive
  losing the disk.
- Restores follow TimescaleDB's procedure (`timescaledb_pre_restore()` / `timescaledb_post_restore()`).
- After restoring an older dump, events newer than the dump that are still in Kafka (24 h retention) can be
  replayed; storage is idempotent, so replaying already-stored events adds nothing:
  `$COMPOSE stop processing && $COMPOSE exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group marketpulse-processor --topic market.raw --reset-offsets --to-datetime <dump time, e.g. 2026-11-25T03:15:00.000> --execute && $COMPOSE start processing`
- Redis needs no backup: it is rebuilt from PostgreSQL. Kafka data is transient (24 h).

## Operating the stack

```bash
PROD="docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production"
$PROD ps                                   # state + health of every service
$PROD logs -f --tail 100 processing        # structured JSON logs (one object per line)
$PROD restart backend                      # restart one service (clients reconnect by themselves)
$PROD up -d --wait                         # start everything / apply .env.production changes
$PROD stop                                 # graceful stop (processor finishes its batch; Kafka shuts down cleanly)
curl -s https://$PUBLIC_HOST/api/health    # overall + per-component status
```

Health endpoints: `/api/health` (summary), `/api/health/database`, `/api/health/redis`, `/api/health/kafka`
(includes offset reconciliation), `/api/health/system` (CPU, memory, API latency, per-service heartbeats). The UI's
System Health and Pipeline Monitor pages show the same data. All services restart automatically after a crash
or a reboot; data lives in named volumes and survives `down` (but not `down -v`).

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| Browser shows a certificate error | DNS not pointing at the server yet, or port 80 blocked (ACME challenge) | `dig +short $PUBLIC_HOST`; open 80/443; `$PROD logs caddy` |
| `up --wait` times out on `migrate` | Database not ready or failed migration | `$PROD logs migrate postgres` |
| UI shows "HALTED" / ingestion degraded | Provider unreachable (e.g. Binance blocked in the server's country) | System Health → ingestion detail; `$PROD logs ingestion`; set `BINANCE_WS_URL`/`BINANCE_REST_URL` |
| Crypto instruments have no history | Binance REST unreachable during the seed | Fix connectivity, then `$PROD run --rm migrate` (backfills instruments without history) |
| Events rejected as `invalid_field` "holds simulated data" | Provider switched without a fresh database | See "Switching data sources" |
| Events rejected as `invalid_timestamp` | Host clock drift | Enable NTP (`timedatectl set-ntp true`); `MAX_FUTURE_SKEW_S` |
| HTTP 429 | Per-client rate limit | Raise `RATE_LIMIT_PER_MINUTE` if legitimate |
| WebSocket closes with 1013 | Server/client connection cap or slow client | `WS_MAX_CONNECTIONS`, `WS_MAX_PER_CLIENT` |
| Disk filling | Retention too long for the event rate | Lower `EVENT_RETENTION_DAYS`; `docker system df` |
| Processor restarting | Database unavailable longer than its retry window (~15 s) | It resumes from committed offsets once the DB is back; check `$PROD logs postgres` |

## Switching data sources

Every instrument records where its data comes from (`instruments.data_mode`), set once and never mixed: the
processor rejects events of the other kind, analytics refuse to combine simulated and live instruments, and the UI
labels each row LIVE or SIM whenever both kinds are present.

- **Add live crypto (Binance):** include `binance` in `PROVIDERS` and `$PROD up -d --wait`. The seed adds the
  pairs from `config/binance.json` with a year of real daily and 7 days of real 1-minute history; no key needed.
- **Remove Binance:** drop it from `PROVIDERS`. The pairs stay with their stored data but stop updating
  (their quotes then show as stale).
- **Simulator → Finnhub for equities:** the equities hold simulated history, so real quotes must not be appended
  to them. Use a fresh database: back up, `$PROD down`, `docker volume rm marketpulse_pgdata marketpulse_redisdata marketpulse_kafkadata`,
  set `PROVIDERS=finnhub_ws,binance` (or `finnhub_rest`) and `FINNHUB_API_KEY`, `$PROD up -d --wait`. The
  seed then creates the equities as live instruments without simulated history. Without a key, ingestion falls
  back to the simulator and labels the data SIMULATED.
- **Finnhub → simulator:** same procedure in reverse.

## Verified (locally and in CI)

- Clean checkout → `docker compose up --build --wait` → all suites green (README, "Test results").
- Production overlay boots behind HTTPS with `PUBLIC_HOST=localhost`; only 80/443 published; WSS delivers live
  quotes; runtime images carry no tests.
- Data survives `down`/`up` and container restarts (PostgreSQL, Redis, Kafka offsets); services restart after
  crashes; graceful shutdown; failure drills for Redis, PostgreSQL, Kafka, processor and backend restarts.
- Backup → restore check, and a live restore.

Not verified: a deployment on a public cloud host with a real Let's Encrypt certificate (no server was
provisioned). The production-config job exercises everything except the public certificate issuance.
