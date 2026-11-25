import redis.asyncio as aioredis

from app.core.config import settings

# Request-path client: short timeouts so a slow Redis degrades responses instead of hanging them.
redis_client = aioredis.from_url(settings.redis_url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
# Pub/sub client: no read timeout (idle channels are normal), liveness via health checks.
pubsub_client = aioredis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=2, health_check_interval=30)

QUOTES_KEY = "mp:quotes"
TICKS_CHANNEL = "mp:ticks"
HEARTBEAT_PATTERN = "mp:svc:*"
