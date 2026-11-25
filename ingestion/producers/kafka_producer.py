from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Producer
from confluent_kafka import Message as KafkaMessage

from ingestion.providers.base import Message

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ProducerStats:
    produced: int = 0
    delivered: int = 0
    delivery_failed: int = 0
    last_delivered_at: str | None = None
    started_at: float = field(default_factory=time.time)


def encode(message: Message, now: datetime) -> tuple[bytes | None, bytes]:
    """(key, value). Key = symbol so one instrument's events stay ordered in one partition."""
    if isinstance(message, bytes):
        return None, message
    payload = dict(message, ingested_at=now.isoformat(timespec="microseconds"))
    symbol = message.get("symbol")
    key = symbol.encode() if isinstance(symbol, str) else None
    return key, json.dumps(payload, separators=(",", ":")).encode()


class EventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self.stats = ProducerStats()
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "marketpulse-ingestion",
                "acks": "all",
                "enable.idempotence": True,  # broker-side dedupe of producer retries
                "linger.ms": 5,
                "compression.type": "lz4",
                "message.timeout.ms": 30000,  # bounds how long an undeliverable message occupies the queue
            }
        )

    def _on_delivery(self, err: KafkaError | None, msg: KafkaMessage) -> None:
        if err is not None:
            self.stats.delivery_failed += 1
            if self.stats.delivery_failed == 1 or self.stats.delivery_failed % 1000 == 0:
                log.error("kafka delivery failed (%d total): %s", self.stats.delivery_failed, err)
            return
        self.stats.delivered += 1
        self.stats.last_delivered_at = datetime.now(UTC).isoformat()

    def send(self, message: Message) -> None:
        key, value = encode(message, datetime.now(UTC))
        while True:
            try:
                self._producer.produce(self._topic, value=value, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue full. It drains as messages deliver or hit
                # message.timeout.ms, so this wait is bounded.
                self._producer.poll(0.5)
        self.stats.produced += 1
        self._producer.poll(0)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout_s: float = 10.0) -> int:
        return self._producer.flush(timeout_s)
