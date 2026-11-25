from functools import cached_property

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    database_url: str = "postgresql+asyncpg://marketpulse:marketpulse@localhost:5432/marketpulse"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "marketpulse-processor"
    kafka_topics: str = "market.raw,market.validated,market.processed,market.errors"
    cors_origins: str = "http://localhost:5173,http://localhost:8080,http://127.0.0.1:5173,http://127.0.0.1:8080"
    rate_limit_per_minute: int = 600
    db_pool_size: int = 10
    db_max_overflow: int = 10
    ws_max_connections: int = 200
    ws_max_per_client: int = 20
    watchlist_max: int = 50
    ws_queue_size: int = 256
    heartbeat_stale_s: float = 15.0
    log_level: str = "INFO"

    @cached_property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @cached_property
    def topic_list(self) -> list[str]:
        return [t.strip() for t in self.kafka_topics.split(",") if t.strip()]


settings = Settings()
